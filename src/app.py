"""passroute-crawler Lambda 핸들러.

Stage 1 - Crawl:
  EventBridge cron → [job_list_collector] → SQS → [job_crawl] → S3(raw/)
  EventBridge cron → [news_collector] → S3(raw/)
  EventBridge cron → [blog_collector] → S3(raw/)
Stage 2 - Embed:
  S3(raw/) → EventBridge → SQS → [embed_worker] → S3(parsed/)
Stage 3 - Load:
  S3(parsed/) → EventBridge → SQS → [db_loader] → PostgreSQL(pgvector)
"""
import json
import logging
import os
import random
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from collector.naver_news import NaverNewsCollector, news_item_to_detail_dict
from core import KST
from crawler.base import JobDetail, JobListingRef
from crawler.registry import get_crawler, iter_sources
from storage.s3 import S3Storage

logger = logging.getLogger(__name__)

SQS_BATCH_SIZE = 10


def _required_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"필수 환경변수 누락: {name}")
    return val


def _make_storage() -> S3Storage:
    return S3Storage(bucket=_required_env("S3_BUCKET"))


# ── Lambda 1: 목록 수집 (cron) ──


def job_list_collector(event, context):
    """마감 공고 삭제 → 소스별 수집 메시지를 SourceCollectQueue 로 발행."""
    sqs = boto3.client("sqs")
    storage = _make_storage()
    source_queue_url = _required_env("SOURCE_COLLECT_QUEUE_URL")

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
    storage = _make_storage()
    queue_url = _required_env("JOB_DETAIL_QUEUE_URL")
    robots_checker = RobotsChecker()

    for record in event["Records"]:
        message = json.loads(record["body"])
        source = message["source"]

        existing_urls = storage.get_all_urls()
        crawler = get_crawler(source)

        # robots.txt 사전 확인
        if crawler.base_url and not robots_checker.check_and_alert(crawler.base_url, source):
            logger.warning("source=%s: robots.txt 차단, 스킵", source)
            continue

        logger.info("source=%s 목록 수집 시작", source)
        try:
            refs = crawler.collect_listings()
        except Exception:
            logger.exception("source=%s 목록 수집 실패", source)
            raise

        new_count = _dispatch_new_listings(sqs, queue_url, refs, existing_urls)
        logger.info("source=%s 수집 완료: %d건 중 신규 %d건", source, len(refs), new_count)

    return {"statusCode": 200}


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


# ── Stage 1: 상세 크롤러 (SQS 트리거) ──


def job_crawl(event, context):
    """SQS 트리거. 공고 1건 상세 크롤링 → S3(raw/) 저장."""
    storage = _make_storage()

    for record in event["Records"]:
        message = json.loads(record["body"])
        ref = JobListingRef(
            source=message["source"],
            external_id=message["external_id"],
            url=message["url"],
            title=message.get("title", ""),
            company_name=message.get("company_name", ""),
        )

        crawler = get_crawler(ref.source)
        logger.info("상세 크롤링: source=%s id=%s (%s)", ref.source, ref.external_id, ref.company_name)

        try:
            detail = crawler.fetch_detail(ref)
            if detail is None:
                logger.info("상세 정보 없음, 스킵: id=%s", ref.external_id)
                continue

            storage.save_raw(detail)
        except Exception:
            logger.exception("상세 크롤링 실패: id=%s", ref.external_id)
            raise

        time.sleep(random.uniform(1.0, 2.5))

    return {"statusCode": 200}


# ── Stage 1: 뉴스 수집 (cron) ──


def news_collector(event, context):
    """주요 기업의 기술/사업 동향 뉴스를 수집하여 S3(raw/) 에 저장."""
    from core.secrets import get_naver_credentials  # noqa: C0415

    client_id, client_secret = get_naver_credentials()
    storage = _make_storage()

    existing_urls = storage.get_all_urls()

    collector = NaverNewsCollector(client_id=client_id, client_secret=client_secret)
    items = collector.collect_all()

    saved = 0
    skipped = 0
    for item in items:
        if item.url in existing_urls:
            skipped += 1
            continue

        data = news_item_to_detail_dict(item)
        storage.save_raw_dict(data)
        saved += 1

    logger.info("뉴스 수집 완료: 저장 %d건, 중복 스킵 %d건", saved, skipped)
    return {
        "statusCode": 200,
        "body": json.dumps({"saved": saved, "skipped": skipped}),
    }


# ── Stage 1: 기술 블로그 수집 (cron) ──


def blog_collector(event, context):
    """주요 기업 기술 블로그 RSS 피드를 수집하여 S3(raw/) 에 저장."""
    from collector.tech_blog import TechBlogCollector, blog_article_to_detail_dict  # noqa: C0415

    storage = _make_storage()
    existing_urls = storage.get_all_urls()

    collector = TechBlogCollector()

    saved = 0
    skipped = 0

    for feed in collector.feeds:
        articles = collector._fetch_feed(feed)
        logger.info("feed=%s: %d건 수집", feed.company_name, len(articles))

        for article in articles:
            if article.url in existing_urls:
                skipped += 1
                continue
            data = blog_article_to_detail_dict(article)
            storage.save_raw_dict(data)
            saved += 1
            existing_urls.add(article.url)

        del articles

    logger.info("블로그 수집 완료: S3 저장 %d건, 중복 스킵 %d건", saved, skipped)
    return {
        "statusCode": 200,
        "body": json.dumps({"saved": saved, "skipped": skipped}),
    }


# ── Stage 2: 임베딩 워커 (SQS 트리거, S3 raw/ 이벤트) ──


def embed_worker(event, context):
    """SQS 트리거. S3 raw/ PutObject 이벤트 → 임베딩 → S3(parsed/) 저장."""
    from core.embedding import build_document, embed_text  # noqa: C0415

    s3 = boto3.client("s3")

    for record in event["Records"]:
        envelope = json.loads(record["body"])
        bucket = envelope["detail"]["bucket"]["name"]
        key = envelope["detail"]["object"]["key"]

        if not (key.startswith("raw/") and key.endswith(".json")):
            logger.warning("embed_worker: 예상하지 못한 키, 스킵: %s", key)
            continue

        # 멱등성 가드: parsed/ 에 이미 존재하면 skip
        parsed_check_key = "parsed/" + key[len("raw/"):]
        try:
            s3.head_object(Bucket=bucket, Key=parsed_check_key)
            s3.delete_object(Bucket=bucket, Key=key)
            logger.info("이미 처리됨, 스킵: %s", key)
            continue
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        logger.info("임베딩 시작: %s", key)

        resp = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read().decode("utf-8"))

        embedding_failed = False
        try:
            document = build_document(data["raw_text"], tuple(data.get("tech_stack", [])))
            embedding = embed_text(document)
            data["embedding"] = embedding
        except Exception:
            embedding_failed = True
            logger.exception(
                "임베딩 실패, 임베딩 없이 저장: id=%s source=%s",
                data.get("external_id"), data.get("source"),
            )

        data["embedding_status"] = "failed" if embedding_failed else "ok"

        parsed_key = f"parsed/{data['source']}/{data['external_id']}.json"
        s3.put_object(
            Bucket=bucket,
            Key=parsed_key,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        )
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info("임베딩 완료: %s → %s", key, parsed_key)

    return {"statusCode": 200}


# ── Stage 3: DB 로더 (SQS 트리거, S3 parsed/ 이벤트) ──


_pg_storage = None


def _get_pg_storage():
    """PgVectorStorage 싱글턴. Lambda 웜 스타트 시 커넥션 재사용."""
    global _pg_storage
    if _pg_storage is None:
        from core.secrets import get_database_url  # noqa: C0415
        from storage.pgvector import PgVectorStorage  # noqa: C0415

        _pg_storage = PgVectorStorage(dsn=get_database_url())
    return _pg_storage


def db_loader(event, context):
    """SQS 트리거. S3 PutObject 이벤트 → parsed/ DB 저장 또는 delete-requests/ 만료 삭제."""
    s3 = boto3.client("s3")
    pg = _get_pg_storage()

    for record in event["Records"]:
        envelope = json.loads(record["body"])
        bucket = envelope["detail"]["bucket"]["name"]
        key = envelope["detail"]["object"]["key"]

        if key.startswith("parsed/") and key.endswith(".json"):
            _load_parsed_file(s3, bucket, key, pg)
        elif key.startswith("delete-requests/") and key.endswith(".json"):
            _load_delete_request(s3, bucket, key, pg)

    return {"statusCode": 200}


def _load_parsed_file(s3, bucket, key, pg):
    """S3 parsed JSON 1건 → PgVectorStorage.save() → S3 삭제."""
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
    pg.save(detail, embedding=embedding)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("DB 적재 완료: %s", key)


def _load_delete_request(s3, bucket, key, pg):
    """S3 delete-request JSON 1건 → PgVectorStorage.delete_expired() → S3 삭제."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(resp["Body"].read().decode("utf-8"))

    now_ts = data.get("now_ts")
    if now_ts is None:
        now_ts = int(datetime.fromisoformat(data["now_iso"]).timestamp())

    deleted = pg.delete_expired(now_ts)
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("만료 공고 삭제 완료: %d건, key=%s", deleted, key)


# ── url-index.json 재구축 (cron) ──


def url_index_rebuilder(event, context):
    """url-index.json 재구축. PostgreSQL 에서 전체 URL 을 조회하여 S3 에 저장."""
    pg = _get_pg_storage()
    s3 = boto3.client("s3")
    bucket = _required_env("S3_BUCKET")

    urls = pg.get_all_urls()
    body = json.dumps({"urls": sorted(urls)}, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key="url-index.json", Body=body.encode("utf-8"))

    logger.info("url-index.json 재구축 완료: %d건", len(urls))
    return {
        "statusCode": 200,
        "body": json.dumps({"url_count": len(urls)}),
    }


# ── Phase 4: 검색 API (API Gateway 트리거) ──


def search_api(event, context):
    """검색 API. 채용공고 유사도 검색 + 네이버 뉴스/블로그 실시간 검색."""
    from core.embedding import embed_text  # noqa: C0415
    from core.secrets import get_database_url, get_naver_credentials  # noqa: C0415
    from search.naver_realtime import _make_session, search_blog, search_news  # noqa: C0415
    from search.pgvector_search import search_jobs  # noqa: C0415

    params = event.get("queryStringParameters") or {}
    query = params.get("q", "").strip()
    company = params.get("company", "").strip() or None
    limit = min(int(params.get("limit", "10")), 30)

    if not query:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "q 파라미터가 필요합니다."}),
        }

    # 1. 쿼리 임베딩
    query_embedding = embed_text(query)

    # 2. pgvector 채용공고 유사도 검색
    pg = _get_pg_storage()
    job_results = search_jobs(pg.conn, query_embedding, limit=limit, company=company)

    for job in job_results:
        job["similarity"] = round(float(job["similarity"]), 4)

    # 3. 네이버 실시간 검색 (뉴스 + 블로그)
    search_query = f"{company} {query}" if company else query
    client_id, client_secret = get_naver_credentials()
    naver_session = _make_session(client_id, client_secret)

    news_results = search_news(naver_session, f"{search_query} 기술", display=5)
    blog_results = search_blog(naver_session, f"{search_query} 기술 블로그", display=5)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "query": query,
            "company": company,
            "jobs": job_results,
            "news": news_results,
            "blogs": blog_results,
        }, ensure_ascii=False, default=str),
    }
