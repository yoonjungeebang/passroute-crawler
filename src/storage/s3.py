"""S3 중간 저장소. Lambda 가 크롤링 결과를 S3 에 저장하면 db_loader Lambda 가 PostgreSQL 로 옮긴다."""
import json
import logging
import uuid
from datetime import datetime

import boto3

from core import KST
from crawler.base import JobDetail

logger = logging.getLogger(__name__)

_REQUIRED_RAW_FIELDS: dict[str, type | tuple[type, ...]] = {
    "source": str,
    "external_id": str,
    "url": str,
    "company_name": str,
    "title": str,
    "raw_text": str,
    "tech_stack": list,
    "deadline": (str, int),
    "crawled_at": str,
}


def validate_raw_schema(data: dict) -> list[str]:
    """raw JSON 데이터의 스키마를 검증한다. 위반 목록을 반환."""
    errors: list[str] = []
    for field, expected_type in _REQUIRED_RAW_FIELDS.items():
        if field not in data:
            errors.append(f"필수 필드 누락: {field}")
        elif not isinstance(data[field], expected_type):
            errors.append(
                f"타입 불일치: {field}={type(data[field]).__name__} "
                f"(expected: {expected_type})"
            )
    return errors


class S3Storage:

    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3 = boto3.client("s3")

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

    def save_raw(self, detail: JobDetail) -> str:
        """크롤링 결과를 raw/{source}/{external_id}.json 으로 저장. 임베딩 미포함."""
        key = f"raw/{detail.source}/{detail.external_id}.json"
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
        errors = validate_raw_schema(data)
        if errors:
            logger.error("스키마 검증 실패: %s, errors=%s", detail.external_id, errors)
            raise ValueError(f"스키마 검증 실패 ({detail.external_id}): {errors}")

        body = json.dumps(data, ensure_ascii=False)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("S3 raw 저장 완료: %s", key)
        return key

    def save_raw_dict(self, data: dict) -> str:
        """dict 를 raw/{source}/{external_id}.json 으로 저장."""
        key = f"raw/{data['source']}/{data['external_id']}.json"
        body = json.dumps(data, ensure_ascii=False)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("S3 raw 저장 완료: %s", key)
        return key

    def delete_expired(self, now_iso: str) -> bool:
        """마감 삭제 요청을 S3 에 기록. 실제 삭제는 db_loader Lambda 가 처리.

        Returns:
            S3 저장 성공 여부.
        """
        now_dt = datetime.fromisoformat(now_iso)
        now_ts = int(now_dt.timestamp())
        timestamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
        key = f"delete-requests/{timestamp}_{uuid.uuid4().hex[:8]}.json"
        body = json.dumps({"now_ts": now_ts, "requested_at": timestamp})

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("삭제 요청 저장: %s", key)
        return True
