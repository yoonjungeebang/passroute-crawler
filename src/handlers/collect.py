"""Lambda 핸들러: 목록 수집 (cron) + 소스별 수집 (SQS 트리거) + 뉴스/블로그 수집."""
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
        candidate_urls = [ref.url for ref in refs]
        existing_urls = pg.get_existing_urls(candidate_urls)
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
            failed_count += _send_batch(sqs, queue_url, batch)
            batch = []

    if batch:
        failed_count += _send_batch(sqs, queue_url, batch)

    if failed_count:
        logger.error("SQS 배치 전송 부분 실패: %d건 실패 (전체 %d건)", failed_count, new_count)

    return new_count


def _send_batch(sqs, queue_url: str, entries: list[dict]) -> int:
    """SQS 배치 전송 후 부분 실패 건수를 반환한다."""
    resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = resp.get("Failed", [])
    for f in failed:
        logger.warning(
            "SQS 메시지 전송 실패: Id=%s Code=%s Message=%s",
            f.get("Id"), f.get("Code"), f.get("Message"),
        )
    return len(failed)


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
    """PostgreSQL 의 전체 URL 을 url-index.json 으로 S3 에 저장."""
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

    return {"statusCode": 200, "body": json.dumps({"url_count": len(all_urls)})}


# ── news_collector (cron) ──


def news_collector(event, context):
    """네이버 뉴스 수집 → S3 raw/ 저장. 기존 파이프라인(embed → load)이 후속 처리.

    중복 확인은 PostgreSQL에서 직접 수행한다.
    S3 url-index.json은 최종적 일관성만 제공하므로(cron 주기까지 갱신 안 됨),
    동시 호출이나 연속 호출 시 동일 뉴스를 중복 저장할 수 있다.
    PostgreSQL PK 룩업은 최신 쓰기를 즉시 반영하므로 이 문제를 방지한다.
    """
    from collector.naver_news import NaverNewsCollector, news_item_to_detail_dict  # noqa: C0415
    from core.secrets import get_naver_credentials  # noqa: C0415

    storage = make_storage()
    pg = get_pg_storage()

    client_id, client_secret = get_naver_credentials()
    collector = NaverNewsCollector(client_id=client_id, client_secret=client_secret)
    items = collector.collect_all()

    candidate_urls = [item.url for item in items]
    existing_urls = pg.get_existing_urls(candidate_urls)

    saved_count = 0
    for item in items:
        if item.url in existing_urls:
            continue

        data = news_item_to_detail_dict(item)
        storage.save_raw_dict(data)
        saved_count += 1

    logger.info("뉴스 수집 완료: 전체 %d건, 신규 저장 %d건", len(items), saved_count)
    return {
        "statusCode": 200,
        "body": json.dumps({"total": len(items), "saved": saved_count}),
    }


# ── blog_collector (cron) ──


def blog_collector(event, context):
    """기술 블로그 RSS 수집 → S3 raw/ 저장. 기존 파이프라인이 후속 처리.

    중복 확인은 PostgreSQL에서 직접 수행한다.
    이유는 news_collector와 동일: S3 인덱스의 최종적 일관성 문제 방지.
    """
    from collector.tech_blog import TechBlogCollector, blog_article_to_detail_dict  # noqa: C0415

    storage = make_storage()
    pg = get_pg_storage()

    collector = TechBlogCollector()
    articles = collector.collect_all()

    candidate_urls = [article.url for article in articles]
    existing_urls = pg.get_existing_urls(candidate_urls)

    saved_count = 0
    for article in articles:
        if article.url in existing_urls:
            continue

        data = blog_article_to_detail_dict(article)
        storage.save_raw_dict(data)
        saved_count += 1

    logger.info("블로그 수집 완료: 전체 %d건, 신규 저장 %d건", len(articles), saved_count)
    return {
        "statusCode": 200,
        "body": json.dumps({"total": len(articles), "saved": saved_count}),
    }
