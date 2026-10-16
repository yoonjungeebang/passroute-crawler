"""Lambda 핸들러: 소스별 목록 수집 (SQS 트리거)."""
import json
import logging
import uuid
import boto3
from crawler.base import JobListingRef
from crawler.registry import get_crawler
from core.normalize import normalize_company_name, normalize_title
from ._common import get_pg_storage, required_env

SQS_BATCH_SIZE = 10

logger = logging.getLogger(__name__)


def source_collect_worker(event, context):
    """소스 1건 목록 수집 → 신규 공고 JobDetailQueue 전송.
    크롤러는 robots.txt 를 사전 확인하여 차단 시 자동 스킵.
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

            company_norms = [normalize_company_name(r.company_name) for r in refs]
            existing_titles = pg.get_existing_company_titles(company_norms)

            new_count, title_dup_count = _dispatch_new_listings(
                sqs, queue_url, refs, existing_urls, existing_titles,
            )

            logger.info(
                "source=%s 수집 완료: %d건 중 신규 %d건 (제목 중복 제외 %d건)",
                source, len(refs), new_count, title_dup_count,
            )
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
    sqs, queue_url: str, refs: list[JobListingRef],
    existing_urls: set[str],
    existing_titles: set[tuple[str, str]],
) -> tuple[int, int]:
    """신규 공고만 JobDetailQueue 로 배치 전송.

    SQS send_message_batch 는 부분 실패를 반환할 수 있다(일부 메시지만 전송 실패).
    실패한 메시지를 로깅하여 부분 장애를 감지한다.
    """
    new_count = 0
    title_dup_count = 0
    failed_count = 0
    batch: list[dict] = []

    for ref in refs:
        if ref.url in existing_urls:
            continue

        pair = (normalize_company_name(ref.company_name), normalize_title(ref.title))
        if pair in existing_titles:
            title_dup_count += 1
            logger.info(
                "제목 중복 제외: source=%s company=%s title=%s",
                ref.source, ref.company_name, ref.title,
            )
            continue

        trace_id = uuid.uuid4().hex
        body = {
            "source": ref.source,
            "external_id": ref.external_id,
            "url": ref.url,
            "company_name": ref.company_name,
            "title": ref.title,
            "trace_id": trace_id,
        }
        batch.append({
            "Id": str(len(batch)),
            "MessageBody": json.dumps(body, ensure_ascii=False),
        })
        new_count += 1

        if len(batch) == SQS_BATCH_SIZE:
            failed_count += _send_batch(sqs, queue_url, batch)
            batch = []

    if batch:
        failed_count += _send_batch(sqs, queue_url, batch)

    if failed_count:
        logger.error("SQS 배치 전송 부분 실패: %d건 실패 (전체 %d건)", failed_count, new_count)

    return new_count, title_dup_count


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
