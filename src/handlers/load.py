"""Lambda 핸들러: DB 로더 (SQS 트리거, S3 parsed/ 이벤트)."""
import dataclasses
import json
import logging
import time
from datetime import datetime

import boto3

from crawler.base import JobDetail
from storage.s3 import validate_raw_schema

from ._common import archive_and_delete, get_pg_storage

logger = logging.getLogger(__name__)


def db_loader(event, context):
    """SQS 트리거. S3 PutObject 이벤트 → parsed/ DB 저장 또는 delete-requests/ 만료 삭제."""
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.time()
    metrics = MetricsLogger(function_name="db_loader")
    load_count = 0
    s3 = boto3.client("s3")
    pg = get_pg_storage()

    for record in event["Records"]:
        envelope = json.loads(record["body"])
        bucket = envelope["detail"]["bucket"]["name"]
        key = envelope["detail"]["object"]["key"]

        if key.startswith("parsed/") and key.endswith(".json"):
            t0 = time.time()
            _load_parsed_file(s3, bucket, key, pg)
            metrics.put_duration("DbWriteDuration", t0)
            load_count += 1
        elif key.startswith("delete-requests/") and key.endswith(".json"):
            _load_delete_request(s3, bucket, key, pg)

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("LoadedCount", load_count)
    metrics.flush()
    return {"statusCode": 200}


def _load_parsed_file(s3, bucket, key, pg):
    """S3 parsed JSON 1건 → PgVectorStorage.save() → S3 삭제."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(resp["Body"].read().decode("utf-8"))

    schema_errors = validate_raw_schema(data)
    if schema_errors:
        logger.error("DB 적재 전 스키마 검증 실패: key=%s, errors=%s", key, schema_errors)
        archive_and_delete(s3, bucket, key)
        return

    field_names = {f.name for f in dataclasses.fields(JobDetail)}
    kwargs = {k: v for k, v in data.items() if k in field_names}
    kwargs.setdefault("career_level", "")
    kwargs.setdefault("deadline", 0)
    if "tech_stack" in kwargs:
        kwargs["tech_stack"] = tuple(kwargs["tech_stack"])
    detail = JobDetail(**kwargs)
    embedding = data.get("embedding")
    embedding_status = data.get("embedding_status", "ok")
    pg.save(detail, embedding=embedding, embedding_status=embedding_status)
    archive_and_delete(s3, bucket, key)
    logger.info("DB 적재 완료: %s", key)


def _load_delete_request(s3, bucket, key, pg):
    """S3 delete-request JSON 1건 → PgVectorStorage.delete_expired() → S3 삭제."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(resp["Body"].read().decode("utf-8"))

    now_ts = data.get("now_ts")
    if now_ts is None:
        now_ts = int(datetime.fromisoformat(data["now_iso"]).timestamp())

    deleted = pg.delete_expired(now_ts)
    archive_and_delete(s3, bucket, key)
    logger.info("만료 공고 삭제 완료: %d건, key=%s", deleted, key)
