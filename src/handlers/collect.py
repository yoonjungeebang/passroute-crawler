"""Lambda 핸들러: 목록 수집 (cron) + 소스별 수집 (SQS 트리거)."""
import json
import logging
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

        crawler = get_crawler(source)
        existing_urls = pg.get_all_urls(source=source)

        if crawler.base_url and not robots_checker.check_and_alert(crawler.base_url, source):
            logger.warning("source=%s: robots.txt 차단, 스킵", source)
            continue

        logger.info("source=%s 목록 수집 시작", source)
        try:
            refs = crawler.collect_listings()
        except Exception:
            logger.exception("source=%s 목록 수집 실패", source)
            failures.append({"itemIdentifier": record["messageId"]})
            continue

        _check_listing_quality(source, refs)
        new_count = _dispatch_new_listings(sqs, queue_url, refs, existing_urls)
        logger.info("source=%s 수집 완료: %d건 중 신규 %d건", source, len(refs), new_count)

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
    """신규 공고만 JobDetailQueue 로 배치 전송."""
    new_count = 0
    batch: list[dict] = []

    for ref in refs:
        if ref.url in existing_urls:
            continue

        batch.append({
            "Id": str(len(batch)),
            "MessageBody": json.dumps({
                "source": ref.source,
                "external_id": ref.external_id,
                "url": ref.url,
                "company_name": ref.company_name,
                "title": ref.title,
            }, ensure_ascii=False),
        })
        new_count += 1

        if len(batch) == SQS_BATCH_SIZE:
            sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)
            batch = []

    if batch:
        sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)

    return new_count
