"""S3 중간 저장소. Lambda 가 크롤링 결과를 S3 에 저장하면 EC2 consumer 가 PostgreSQL 로 옮긴다."""
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from crawler.base import JobDetail

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


class S3Storage:

    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3 = boto3.client("s3")

    def get_all_urls(self) -> set[str]:
        """S3 의 url-index.json 에서 기존 URL 목록을 가져온다."""
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key="url-index.json")
            data = json.loads(resp["Body"].read().decode("utf-8"))
            return set(data.get("urls", []))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info("url-index.json 없음, 빈 set 반환 (첫 실행)")
                return set()
            raise

    def save(self, detail: JobDetail, *, embedding: list[float] | None = None) -> None:
        """크롤링 결과를 parsed/{source}/{external_id}.json 으로 S3 에 저장."""
        key = f"parsed/{detail.source}/{detail.external_id}.json"
        data = {
            "source": detail.source,
            "external_id": detail.external_id,
            "url": detail.url,
            "company_name": detail.company_name,
            "title": detail.title,
            "raw_text": detail.raw_text,
            "tech_stack": list(detail.tech_stack),
            "deadline": detail.deadline,
            "crawled_at": detail.crawled_at,
            "career_level": detail.career_level,
        }
        if embedding is not None:
            data["embedding"] = embedding
        body = json.dumps(data, ensure_ascii=False)

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("S3 저장 완료: %s", key)

    def delete_expired(self, now_iso: str) -> bool:
        """마감 삭제 요청을 S3 에 기록. 실제 삭제는 EC2 consumer 가 처리.

        Returns:
            S3 저장 성공 여부.
        """
        now_dt = datetime.fromisoformat(now_iso)
        now_ts = int(now_dt.timestamp())
        timestamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
        key = f"delete-requests/{timestamp}.json"
        body = json.dumps({"now_ts": now_ts, "requested_at": timestamp})

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("삭제 요청 저장: %s", key)
        return True
