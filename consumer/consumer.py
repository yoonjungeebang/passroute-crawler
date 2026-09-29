"""S3 폴링 → PostgreSQL(pgvector) 저장 consumer. EC2 에서 Docker 컨테이너로 실행."""
import json
import logging
import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from crawler.base import JobDetail
from storage.pgvector import PgVectorStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))


def _build_storage() -> PgVectorStorage:
    return PgVectorStorage(dsn=os.environ["DATABASE_URL"])


def _build_s3():
    return boto3.client("s3")


def process_parsed_files(s3, bucket: str, storage: PgVectorStorage) -> int:
    """parsed/ 하위 JSON 을 읽어 PostgreSQL 에 저장하고 S3 객체를 삭제한다."""
    saved = 0
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix="parsed/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            try:
                resp = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(resp["Body"].read().decode("utf-8"))

                detail = JobDetail(
                    source=data["source"],
                    external_id=data["external_id"],
                    url=data["url"],
                    company_name=data["company_name"],
                    title=data["title"],
                    raw_text=data["raw_text"],
                    tech_stack=tuple(data.get("tech_stack", [])),
                    deadline=data.get("deadline", ""),
                    crawled_at=data["crawled_at"],
                    career_level=data.get("career_level", ""),
                )

                embedding = data.get("embedding")
                storage.save(detail, embedding=embedding)
                s3.delete_object(Bucket=bucket, Key=key)
                saved += 1
            except Exception:
                logger.exception("parsed 파일 처리 실패: %s", key)

    return saved


def process_delete_requests(s3, bucket: str, storage: PgVectorStorage) -> int:
    """delete-requests/ 하위 JSON 을 읽어 마감 공고를 삭제하고 S3 객체를 삭제한다."""
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix="delete-requests/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            try:
                resp = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(resp["Body"].read().decode("utf-8"))
                now_ts = data.get("now_ts")
                if now_ts is None:
                    now_iso = data.get("now_iso", "")
                    now_ts = int(datetime.fromisoformat(now_iso).timestamp())
                count = storage.delete_expired(now_ts)
                deleted += count
                s3.delete_object(Bucket=bucket, Key=key)
                logger.info("삭제 요청 처리 완료: %s (%d건 삭제)", key, count)
            except Exception:
                logger.exception("삭제 요청 처리 실패: %s", key)

    return deleted


def update_url_index(s3, bucket: str, storage: PgVectorStorage) -> int:
    """PostgreSQL 의 전체 URL 목록을 url-index.json 으로 갱신한다."""
    urls = storage.get_all_urls()
    body = json.dumps({"urls": sorted(urls)}, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key="url-index.json", Body=body.encode("utf-8"))
    logger.info("url-index.json 갱신 완료: %d건", len(urls))
    return len(urls)


def run_cycle(s3, bucket: str, storage: PgVectorStorage) -> None:
    """한 번의 폴링 사이클을 실행한다."""
    deleted = process_delete_requests(s3, bucket, storage)
    saved = process_parsed_files(s3, bucket, storage)

    if deleted > 0 or saved > 0:
        update_url_index(s3, bucket, storage)
        logger.info("사이클 완료: 저장 %d건, 삭제 %d건", saved, deleted)


def main():
    bucket = os.environ["S3_BUCKET"]
    storage = _build_storage()
    s3 = _build_s3()

    logger.info("consumer 시작: bucket=%s, poll_interval=%ds", bucket, POLL_INTERVAL)

    while True:
        try:
            run_cycle(s3, bucket, storage)
        except Exception:
            logger.exception("사이클 실행 중 예기치 않은 오류")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
