"""S3 중간 저장소. Lambda 가 크롤링 결과를 S3 에 저장하면 db_loader Lambda 가 PostgreSQL 로 옮긴다."""
import dataclasses
import hashlib
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
    "deadline": int,
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
        data = _detail_to_dict(detail)
        if embedding is not None:
            data["embedding"] = embedding
        body = json.dumps(data, ensure_ascii=False)

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("S3 저장 완료: %s", key)

    def save_raw(self, detail: JobDetail, trace_id: str = "") -> str:
        """크롤링 결과를 raw/{source}/{external_id}.json 으로 저장. 임베딩 미포함.

        trace_id: 종단 간 추적 식별자. collect → crawl → embed → load 전 과정을
        추적하기 위한 고유 ID.
        """
        key = f"raw/{detail.source}/{detail.external_id}.json"
        data = _detail_to_dict(detail)
        if trace_id:
            data["trace_id"] = trace_id
        errors = validate_raw_schema(data)
        if errors:
            logger.error("스키마 검증 실패: %s, errors=%s", detail.external_id, errors)
            raise ValueError(f"스키마 검증 실패 ({detail.external_id}): {errors}")

        body = json.dumps(data, ensure_ascii=False)
        data["content_hash"] = hashlib.sha256(body.encode("utf-8")).hexdigest()
        body = json.dumps(data, ensure_ascii=False)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("S3 raw 저장 완료: %s trace_id=%s", key, trace_id)
        return key

    def save_raw_dict(self, data: dict) -> str:
        """dict 를 raw/{source}/{external_id}.json 으로 저장."""
        key = f"raw/{data['source']}/{data['external_id']}.json"
        data.setdefault("_schema_version", SCHEMA_VERSION)
        data.setdefault("trace_id", uuid.uuid4().hex)
        body = json.dumps(data, ensure_ascii=False)
        data["content_hash"] = hashlib.sha256(body.encode("utf-8")).hexdigest()
        body = json.dumps(data, ensure_ascii=False)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("S3 raw 저장 완료: %s trace_id=%s", key, data["trace_id"])
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


SCHEMA_VERSION = 1


def _detail_to_dict(detail: JobDetail) -> dict:
    """JobDetail → JSON-safe dict. 필드가 추가되면 자동 반영된다."""
    data = dataclasses.asdict(detail)
    data["tech_stack"] = list(detail.tech_stack)
    data["_schema_version"] = SCHEMA_VERSION
    return data


def verify_content_hash(data: dict) -> bool:
    """content_hash 필드로 데이터 무결성을 검증한다.

    content_hash는 해시 계산 전의 JSON 직렬화 결과에서 산출된다.
    해시가 없는 데이터(이전 버전)는 검증을 건너뛴다(하위 호환).
    """
    stored_hash = data.pop("content_hash", None)
    if stored_hash is None:
        return True
    body = json.dumps(data, ensure_ascii=False)
    actual_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    return stored_hash == actual_hash
