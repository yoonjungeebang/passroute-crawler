"""Lambda 핸들러: DB 로더 (SQS 트리거)."""
import dataclasses
import json
import logging
import os
import time

from crawler.base import JobDetail

from ._common import get_pg_storage

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


def _validate_raw_schema(data: dict) -> list[str]:
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

logger = logging.getLogger(__name__)


def db_loader(event, context):
    """SQS 트리거. 임베딩 완료 데이터 → PostgreSQL 저장 또는 만료 삭제.

    메시지 타입으로 분기:
    - type="load": 공고 데이터 DB 적재
    - type="delete_expired": 마감 공고 삭제

    SQS at-least-once 특성상 동일 메시지가 재전달될 수 있다.
    실패한 레코드만 batchItemFailures로 반환하여 성공한 메시지의 불필요한
    재처리를 방지한다. DB 쓰기 자체는 UPSERT로 멱등적이다.
    """
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.monotonic()
    metrics = MetricsLogger(function_name="db_loader")
    load_count = 0
    failures: list[dict] = []
    pg = get_pg_storage()

    for record in event["Records"]:
        try:
            data = json.loads(record["body"])
            msg_type = data.get("type", "load")

            if msg_type == "load":
                t0 = time.monotonic()
                _load_record(data, pg)
                metrics.put_duration("DbWriteDuration", t0)
                load_count += 1
            elif msg_type == "delete_expired":
                _handle_delete_expired(data, pg)
            else:
                logger.warning("알 수 없는 메시지 타입: %s", msg_type)
        except Exception:
            logger.exception("레코드 처리 실패")
            failures.append({"itemIdentifier": record["messageId"]})

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("LoadedCount", load_count)
    metrics.flush()
    return {"statusCode": 200, "batchItemFailures": failures}


def _load_record(data: dict, pg) -> None:
    """SQS 메시지 1건 → PgVectorStorage.save().

    UPSERT(ON CONFLICT DO UPDATE)로 멱등성이 보장되므로 중복 처리 시에도
    데이터 오염은 발생하지 않는다.
    """
    trace_id = data.get("trace_id", "")

    schema_errors = _validate_raw_schema(data)
    if schema_errors:
        logger.error(
            "스키마 검증 실패: trace_id=%s errors=%s", trace_id, schema_errors,
        )
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

    request_id = data.get("request_id", "")
    if request_id:
        completed = pg.mark_job_loaded(request_id)
        if completed:
            _notify_source_complete(request_id)

    logger.info("DB 적재 완료: source=%s id=%s trace_id=%s request_id=%s",
                data.get("source"), data.get("external_id"), trace_id, request_id)


def _notify_source_complete(request_id: str) -> None:
    """소스 내 전체 공고 적재 완료 시 Spring Boot 로 콜백을 발행한다."""
    queue_url = os.environ.get("CRAWL_COMPLETE_QUEUE_URL")
    if not queue_url:
        logger.warning("CRAWL_COMPLETE_QUEUE_URL 미설정, 콜백 스킵")
        return
    try:
        import boto3
        sqs = boto3.client("sqs")
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"request_id": request_id}, ensure_ascii=False),
        )
        logger.info("수집 완료 콜백 발행: request_id=%s", request_id)
    except Exception:
        logger.exception("수집 완료 콜백 발행 실패: request_id=%s", request_id)


def _handle_delete_expired(data: dict, pg) -> None:
    """만료 공고 삭제 메시지 처리."""
    now_ts = data.get("now_ts")
    if now_ts is None:
        logger.error("delete_expired 메시지에 now_ts 누락")
        return

    deleted = pg.delete_expired(now_ts)
    logger.info("만료 공고 삭제 완료: %d건", deleted)
