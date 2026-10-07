"""Lambda 핸들러: 목록 수집 (cron) + 소스별 수집 (SQS 트리거) + 뉴스/블로그 수집."""
import json
import logging
import uuid
from datetime import datetime

import boto3

from core import KST
from crawler.base import JobListingRef
from crawler.registry import get_crawler, iter_sources

from ._common import SQS_BATCH_SIZE, get_pg_storage, make_storage, required_env

logger = logging.getLogger(__name__)


# ── Lambda 1: 목록 수집 (cron) ──


def job_list_collector(event, context):
    """마감 공고 삭제 → 소스별 수집 메시지를 SourceCollectQueue 로 발행."""
    sqs = boto3.client("sqs")
    storage = make_storage()
    source_queue_url = required_env("SOURCE_COLLECT_QUEUE_URL")

    delete_requested = storage.delete_expired(datetime.now(KST).isoformat())

    sources = list(iter_sources())
    for source in sources:
        sqs.send_message(
            QueueUrl=source_queue_url,
            MessageBody=json.dumps({"source": source}, ensure_ascii=False),
        )
        logger.info("소스 수집 메시지 전송: %s", source)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "dispatched_sources": sources,
            "delete_requested": delete_requested,
        }),
    }


def source_collect_worker(event, context):
    """소스 1건 목록 수집 → 신규 공고 JobDetailQueue 전송.

    Tier 2 크롤러는 robots.txt 를 사전 확인하여 차단 시 자동 스킵.
    """
    from crawler.robots_check import RobotsChecker  # noqa: C0415

    sqs = boto3.client("sqs")
    pg = get_pg_storage()
    queue_url = required_env("JOB_DETAIL_QUEUE_URL")
    robots_checker = RobotsChecker()

    failures: list[dict] = []

    for record in event["Records"]:
        message = json.loads(record["body"])
        source = message["source"]

        try:
            crawler = get_crawler(source)

            if crawler.base_url and not robots_checker.check_and_alert(crawler.base_url, source):
                logger.warning("source=%s: robots.txt 차단, 스킵", source)
                continue

            logger.info("source=%s 목록 수집 시작", source)
            refs = crawler.collect_listings()

            _check_listing_quality(source, refs)
            candidate_urls = [ref.url for ref in refs]
            existing_urls = pg.get_existing_urls(candidate_urls)
            new_count = _dispatch_new_listings(sqs, queue_url, refs, existing_urls)
            logger.info("source=%s 수집 완료: %d건 중 신규 %d건", source, len(refs), new_count)
        except Exception:
            logger.exception("source=%s 처리 실패", source)
            failures.append({"itemIdentifier": record["messageId"]})

    return {"statusCode": 200, "batchItemFailures": failures}


def _check_listing_quality(source: str, refs: list[JobListingRef]) -> None:
    """목록 수집 결과의 품질 메트릭을 검사하고, 이상 시 #monitor 알림."""
    total = len(refs)
    if total == 0:
        return

    empty_title = sum(1 for r in refs if not r.title.strip())
    empty_company = sum(1 for r in refs if not r.company_name.strip())

    alerts: list[str] = []
    if empty_title / total > 0.3:
        alerts.append(f"제목 비어있음: {empty_title}/{total}건 ({empty_title / total:.0%})")
    if empty_company / total > 0.3:
        alerts.append(f"회사명 비어있음: {empty_company}/{total}건 ({empty_company / total:.0%})")

    if not alerts:
        return

    logger.warning("source=%s 품질 열화 감지: %s", source, alerts)
    try:
        from core.notify import send_monitor_alert  # noqa: C0415

        send_monitor_alert(
            title=f"\U0001f4c9 {source} 목록 수집 품질 열화",
            description=(
                f"**{source}** 목록 수집 {total}건 중 품질 이상 감지:\n\n"
                + "\n".join(f"- {a}" for a in alerts)
            ),
        )
    except Exception:
        logger.exception("품질 열화 알림 전송 실패")


def _dispatch_new_listings(
    sqs, queue_url: str, refs: list[JobListingRef], existing_urls: set[str],
) -> int:
    """신규 공고만 JobDetailQueue 로 배치 전송.

    SQS send_message_batch 는 부분 실패를 반환할 수 있다(일부 메시지만 전송 실패).
    실패한 메시지를 로깅하여 부분 장애를 감지한다.
    """
    new_count = 0
    failed_count = 0
    batch: list[dict] = []

    for ref in refs:
        if ref.url in existing_urls:
            continue

        trace_id = uuid.uuid4().hex
        batch.append({
            "Id": str(len(batch)),
            "MessageBody": json.dumps({
                "source": ref.source,
                "external_id": ref.external_id,
                "url": ref.url,
                "company_name": ref.company_name,
                "title": ref.title,
                "trace_id": trace_id,
            }, ensure_ascii=False),
        })
        new_count += 1

        if len(batch) == SQS_BATCH_SIZE:
            failed_count += _send_batch(sqs, queue_url, batch)
            batch = []

    if batch:
        failed_count += _send_batch(sqs, queue_url, batch)

    if failed_count:
        logger.error("SQS 배치 전송 부분 실패: %d건 실패 (전체 %d건)", failed_count, new_count)

    return new_count


def _send_batch(sqs, queue_url: str, entries: list[dict]) -> int:
    """SQS 배치 전송 후 부분 실패 건수를 반환한다.

    부분 실패한 메시지는 1회 재시도한다. SQS 부분 실패는 일시적 서비스 오류가
    대부분이므로 즉시 재시도로 복구할 수 있다. 재시도 후에도 실패하면 로깅한다.
    """
    resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = resp.get("Failed", [])
    if not failed:
        return 0

    failed_ids = {f["Id"] for f in failed}
    retry_entries = [e for e in entries if e["Id"] in failed_ids]
    retry_resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=retry_entries)
    still_failed = retry_resp.get("Failed", [])
    for f in still_failed:
        logger.warning(
            "SQS 메시지 전송 재시도 후에도 실패: Id=%s Code=%s Message=%s",
            f.get("Id"), f.get("Code"), f.get("Message"),
        )
    return len(still_failed)


# ── URL 인덱스 ──


def _load_url_index(s3, bucket: str) -> set[str]:
    """S3 의 url-index.json 을 읽어 기존 URL 집합을 반환."""
    try:
        resp = s3.get_object(Bucket=bucket, Key="url-index.json")
        data = json.loads(resp["Body"].read().decode("utf-8"))
        return set(data.get("urls", []))
    except Exception:
        logger.warning("url-index.json 로드 실패, 빈 집합으로 진행")
        return set()


# ── url_index_rebuilder (cron) ──


def url_index_rebuilder(event, context):
    """PostgreSQL 의 전체 URL 을 url-index.json 으로 S3 에 저장.

    추가로 파이프라인 무결성 감사를 수행한다:
    - 임베딩 실패 레코드 수 확인 (embedding_status='failed')
    - S3 raw/parsed 잔류 파일 수 확인 (처리 지연 또는 누락 감지)
    이상 발견 시 Discord #monitor 로 알림을 보낸다.
    """
    pg = get_pg_storage()
    storage = make_storage()

    all_urls = pg.get_all_urls()
    body = json.dumps({"urls": sorted(all_urls)}, ensure_ascii=False)
    storage.s3.put_object(
        Bucket=storage.bucket,
        Key="url-index.json",
        Body=body.encode("utf-8"),
    )
    logger.info("URL 인덱스 재구축 완료: %d건", len(all_urls))

    audit_result = _run_integrity_audit(pg, storage)

    result = {"url_count": len(all_urls)}
    result.update(audit_result)
    return {"statusCode": 200, "body": json.dumps(result)}


def _run_integrity_audit(pg, storage) -> dict:
    """파이프라인 무결성 감사. 자가 검증(self-validation)으로 데이터 정합성을 확인."""
    audit = {}
    alerts: list[str] = []

    try:
        with pg.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM job_descriptions WHERE embedding_status = 'failed'"
            )
            failed_embeddings = cur.fetchone()[0]
            audit["failed_embeddings"] = failed_embeddings
            if failed_embeddings > 0:
                alerts.append(f"임베딩 실패 레코드: {failed_embeddings}건")

            cur.execute("SELECT COUNT(*) FROM job_descriptions")
            total_records = cur.fetchone()[0]
            audit["total_records"] = total_records
    except Exception:
        logger.exception("DB 감사 질의 실패")

    try:
        raw_count = _count_s3_prefix(storage, "raw/")
        parsed_count = _count_s3_prefix(storage, "parsed/")
        audit["pending_raw"] = raw_count
        audit["pending_parsed"] = parsed_count
        if raw_count > 50:
            alerts.append(f"S3 raw/ 잔류 파일: {raw_count}건 (처리 지연 가능)")
        if parsed_count > 50:
            alerts.append(f"S3 parsed/ 잔류 파일: {parsed_count}건 (적재 지연 가능)")
    except Exception:
        logger.exception("S3 감사 조회 실패")

    if alerts:
        logger.warning("무결성 감사 이상 감지: %s", alerts)
        try:
            from core.notify import send_monitor_alert  # noqa: C0415

            send_monitor_alert(
                title="\U0001f50d 파이프라인 무결성 감사 결과",
                description="\n".join(f"- {a}" for a in alerts),
            )
        except Exception:
            logger.exception("감사 알림 전송 실패")
    else:
        logger.info("무결성 감사 통과: %s", audit)

    return audit


def _count_s3_prefix(storage, prefix: str) -> int:
    """S3 프리픽스 하위 객체 수를 센다."""
    paginator = storage.s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        count += page.get("KeyCount", 0)
    return count


