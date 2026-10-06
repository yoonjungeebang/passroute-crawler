"""passroute-crawler Lambda 핸들러.

Stage 1 - Crawl:
  EventBridge cron → [job_list_collector] → SQS → [job_crawl] → S3(raw/)
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

from core import KST
from core.circuit_breaker import CircuitOpenError, get_breaker
from crawler.base import JobDetail, JobListingRef
from crawler.registry import get_crawler, iter_sources
from crawler.validation import CrawlValidationError
from storage.s3 import S3Storage, validate_raw_schema

logger = logging.getLogger(__name__)

SQS_BATCH_SIZE = 10


def _archive_and_delete(s3, bucket: str, key: str) -> None:
    """S3 객체를 archive/ 프리픽스로 복사한 뒤 원본을 삭제한다.

    문제 발생 시 archive/ 에서 원본 데이터를 확인하거나 재처리할 수 있다.
    archive/ 객체는 S3 Lifecycle 규칙에 의해 14일 후 자동 만료된다.
    """
    archive_key = f"archive/{key}"
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=archive_key,
    )
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("아카이브 완료: %s → %s", key, archive_key)


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
    pg = _get_pg_storage()
    queue_url = _required_env("JOB_DETAIL_QUEUE_URL")
    robots_checker = RobotsChecker()

    for record in event["Records"]:
        message = json.loads(record["body"])
        source = message["source"]

        existing_urls = pg.get_all_urls()
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

        _check_listing_quality(source, refs)
        new_count = _dispatch_new_listings(sqs, queue_url, refs, existing_urls)
        logger.info("source=%s 수집 완료: %d건 중 신규 %d건", source, len(refs), new_count)

    return {"statusCode": 200}


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


# ── Stage 1: 상세 크롤러 (SQS 트리거) ──


def job_crawl(event, context):
    """SQS 트리거. 공고 1건 상세 크롤링 → S3(raw/) 저장."""
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.time()
    metrics = MetricsLogger(function_name="job_crawl")
    crawl_count = 0
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

        breaker = get_breaker(ref.source)
        try:
            with breaker:
                detail = crawler.fetch_detail(ref)
            if detail is None:
                logger.info("상세 정보 없음, 스킵: id=%s", ref.external_id)
                continue

            storage.save_raw(detail)
            crawl_count += 1
        except CrawlValidationError as e:
            logger.error("크롤링 검증 실패: %s", e)
            try:
                from core.notify import send_monitor_alert  # noqa: C0415
                send_monitor_alert(
                    title=f"\u26a0\ufe0f {ref.source} API 구조 변경 감지",
                    description=(
                        f"**{ref.source}** API 응답 구조가 예상과 다릅니다.\n\n"
                        f"**공고**: `{ref.external_id}`\n"
                        f"**오류**: {e}"
                    ),
                    color=0xFF4444,
                )
            except Exception:
                logger.exception("검증 실패 알림 전송 실패")
            breaker.record_failure(e)
            continue
        except (ValueError, TypeError) as e:
            logger.error("데이터 품질 검증 실패: id=%s, %s", ref.external_id, e)
            try:
                from core.notify import send_monitor_alert  # noqa: C0415
                send_monitor_alert(
                    title=f"\u26a0\ufe0f {ref.source} 데이터 품질 문제",
                    description=(
                        f"**공고**: `{ref.external_id}`\n"
                        f"**오류**: {e}"
                    ),
                )
            except Exception:
                logger.exception("품질 검증 알림 전송 실패")
            continue
        except CircuitOpenError:
            logger.warning("서킷 OPEN, 상세 크롤링 스킵: source=%s id=%s", ref.source, ref.external_id)
            continue
        except Exception:
            logger.exception("상세 크롤링 실패: id=%s", ref.external_id)
            raise

        time.sleep(random.uniform(1.0, 2.5))

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("CrawledCount", crawl_count)
    metrics.flush()
    return {"statusCode": 200}


# ── Stage 2: 임베딩 워커 (SQS 트리거, S3 raw/ 이벤트) ──


def embed_worker(event, context):
    """SQS 트리거. S3 raw/ PutObject 이벤트 → 임베딩 → S3(parsed/) 저장."""
    from core.embedding import build_document, embed_text  # noqa: C0415
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.time()
    metrics = MetricsLogger(function_name="embed_worker")
    embed_count = 0
    embed_fail_count = 0
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
            _archive_and_delete(s3, bucket, key)
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
            t0 = time.time()
            document = build_document(data["raw_text"], tuple(data.get("tech_stack", [])))
            embedding = embed_text(document)
            metrics.put_duration("EmbeddingDuration", t0)
            data["embedding"] = embedding
            embed_count += 1
        except Exception:
            embedding_failed = True
            embed_fail_count += 1
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
        _archive_and_delete(s3, bucket, key)
        logger.info("임베딩 완료: %s → %s", key, parsed_key)

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("EmbeddedCount", embed_count)
    metrics.put_count("EmbedFailCount", embed_fail_count)
    metrics.flush()
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
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.time()
    metrics = MetricsLogger(function_name="db_loader")
    load_count = 0
    s3 = boto3.client("s3")
    pg = _get_pg_storage()

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
        _archive_and_delete(s3, bucket, key)
        return

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
    embedding_status = data.get("embedding_status", "ok")
    pg.save(detail, embedding=embedding, embedding_status=embedding_status)
    _archive_and_delete(s3, bucket, key)
    logger.info("DB 적재 완료: %s", key)


def _load_delete_request(s3, bucket, key, pg):
    """S3 delete-request JSON 1건 → PgVectorStorage.delete_expired() → S3 삭제."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(resp["Body"].read().decode("utf-8"))

    now_ts = data.get("now_ts")
    if now_ts is None:
        now_ts = int(datetime.fromisoformat(data["now_iso"]).timestamp())

    deleted = pg.delete_expired(now_ts)
    _archive_and_delete(s3, bucket, key)
    logger.info("만료 공고 삭제 완료: %d건, key=%s", deleted, key)


# ── Phase 4: 검색 API (API Gateway 트리거) ──


def _api_error(status: int, message: str) -> dict:
    """API Gateway 에러 응답을 생성한다."""
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }


def _safe_int(value: str, *, default: int, min_val: int = 1, max_val: int = 100) -> int:
    """문자열을 정수로 변환한다. 실패 시 default, 범위 초과 시 클램핑."""
    try:
        n = int(value)
    except (ValueError, TypeError):
        return default
    return max(min_val, min(n, max_val))


_MAX_QUERY_LENGTH = 200
_MAX_COMPANY_LENGTH = 50


def search_api(event, context):
    """검색 API. 채용공고 유사도 검색 + 네이버 뉴스 실시간 검색."""
    from core.embedding import embed_text  # noqa: C0415
    from core.metrics import MetricsLogger  # noqa: C0415
    from core.secrets import get_naver_credentials  # noqa: C0415
    from search.naver_realtime import _make_session, search_news  # noqa: C0415
    from search.pgvector_search import search_jobs  # noqa: C0415

    t_total = time.time()
    metrics = MetricsLogger(function_name="search_api")

    params = event.get("queryStringParameters") or {}
    query = params.get("q", "").strip()[:_MAX_QUERY_LENGTH]
    company = params.get("company", "").strip()[:_MAX_COMPANY_LENGTH] or None
    limit = _safe_int(params.get("limit", "10"), default=10, min_val=1, max_val=30)

    if not query:
        return _api_error(400, "q 파라미터가 필요합니다.")

    # 1. 쿼리 임베딩
    t0 = time.time()
    query_embedding = embed_text(query)
    metrics.put_duration("EmbeddingDuration", t0)

    # 2. pgvector 채용공고 유사도 검색
    t0 = time.time()
    pg = _get_pg_storage()
    job_results = search_jobs(pg.conn, query_embedding, limit=limit, company=company)
    metrics.put_duration("DbQueryDuration", t0)
    metrics.put_count("JobResultCount", len(job_results))

    for job in job_results:
        job["similarity"] = round(float(job["similarity"]), 4)

    # 3. 네이버 뉴스 실시간 검색
    t0 = time.time()
    search_query = f"{company} {query}" if company else query
    client_id, client_secret = get_naver_credentials()
    naver_session = _make_session(client_id, client_secret)
    news_results = search_news(naver_session, f"{search_query} 기술", display=5)
    metrics.put_duration("NewsApiDuration", t0)
    metrics.put_count("NewsResultCount", len(news_results))

    metrics.put_duration("TotalDuration", t_total)
    metrics.flush()

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
        }, ensure_ascii=False, default=str),
    }


# ── 기업별 실시간 수집 API (API Gateway 트리거) ──


def company_collect(event, context):
    """기업 데이터 수집 API. 면접 방 생성 시 해당 기업의 기술 블로그 + 뉴스를 실시간 수집하여 DB 저장."""
    from collector.naver_news import NaverNewsCollector, news_item_to_detail_dict  # noqa: C0415
    from collector.tech_blog import (  # noqa: C0415
        _fetch_page_content,
        _make_session as _make_blog_session,
        fetch_content_from_rss,
        find_rss_feed_url,
    )
    from core.metrics import MetricsLogger  # noqa: C0415
    from core.secrets import get_naver_credentials  # noqa: C0415
    from crawler.robots_check import RobotsChecker  # noqa: C0415
    from search.naver_realtime import _make_session, filter_webkr_results, search_webkr  # noqa: C0415

    t_total = time.time()
    metrics = MetricsLogger(function_name="company_collect")

    params = event.get("queryStringParameters") or {}
    company = params.get("company", "").strip()[:_MAX_COMPANY_LENGTH]

    if not company:
        return _api_error(400, "company 파라미터가 필요합니다.")

    pg = _get_pg_storage()
    news_saved = []

    # 1. 뉴스 수집: 해당 기업만 네이버 뉴스 API 호출
    t0 = time.time()
    client_id, client_secret = get_naver_credentials()
    news_collector_inst = NaverNewsCollector(
        client_id=client_id,
        client_secret=client_secret,
        companies=(company,),
    )
    news_items = news_collector_inst.collect_all()

    for item in news_items:
        data = news_item_to_detail_dict(item)
        detail = JobDetail(
            source=data["source"],
            external_id=data["external_id"],
            url=data["url"],
            company_name=data["company_name"],
            title=data["title"],
            raw_text=data["raw_text"],
            tech_stack=tuple(data.get("tech_stack", [])),
            deadline=data.get("deadline", 0),
            crawled_at=data["crawled_at"],
            career_level=data.get("career_level", ""),
        )
        pg.save(detail)
        news_saved.append({
            "title": item.title,
            "url": item.url,
            "company_name": item.company_name,
            "description": item.description,
        })

    metrics.put_duration("NewsCollectDuration", t0)
    metrics.put_count("NewsCount", len(news_items))
    logger.info("뉴스 수집: company=%s, %d건", company, len(news_items))

    # 2. 기술 블로그 수집: 웹문서 검색 → RSS 본문 우선 → HTML 크롤링 폴백
    t0 = time.time()
    _MAX_KEYWORDS = 10
    keywords_param = params.get("keywords", "").strip()
    keyword_list = [kw.strip()[:50] for kw in keywords_param.split(",") if kw.strip()][:_MAX_KEYWORDS]
    max_crawl = _safe_int(params.get("max_articles", "10"), default=10, min_val=1, max_val=20)

    naver_session = _make_session(client_id, client_secret)
    raw_results: list[dict] = []

    if keyword_list:
        for keyword in keyword_list:
            query = f"{company} 기술블로그 {keyword}"
            results = search_webkr(naver_session, query, display=5)
            raw_results.extend(results)
    else:
        query = f"{company} 기술블로그"
        raw_results = search_webkr(naver_session, query, display=10)

    filtered = filter_webkr_results(raw_results, keyword_list, max_results=max_crawl)

    logger.info(
        "웹문서 검색: company=%s, keywords=%s, 원본 %d건 → 필터 %d건",
        company, keywords_param, len(raw_results), len(filtered),
    )

    robots = RobotsChecker()
    blog_session = _make_blog_session()
    tech_articles = []

    for item in filtered:
        url = item["url"]
        content = ""

        # 1차: RSS에서 본문 추출 시도
        rss_feed_url = find_rss_feed_url(url)
        if rss_feed_url:
            content = fetch_content_from_rss(rss_feed_url, url)
            if content:
                logger.info("RSS에서 본문 추출 성공: %s", url)

        # 2차: RSS에 없으면 HTML 크롤링 (robots.txt 허용 시에만)
        if not content:
            if not robots.is_allowed(url):
                logger.info("RSS에 없고 크롤링 차단, 스킵: %s", url)
                continue
            content = _fetch_page_content(blog_session, url)
            time.sleep(0.5)

        if not content:
            logger.info("본문 추출 실패, 스킵: %s", url)
            continue

        tech_articles.append({
            "title": item["title"],
            "url": url,
            "content": content,
            "source": "tech_blog_webkr",
        })

        detail = JobDetail(
            source="tech_blog_webkr",
            external_id=url.rstrip("/").split("/")[-1][:16] or "webkr",
            url=url,
            company_name=company,
            title=item["title"],
            raw_text=content,
            tech_stack=tuple(),
            deadline=0,
            crawled_at=datetime.now(KST).isoformat(),
            career_level="",
        )
        pg.save(detail)

    metrics.put_duration("BlogCollectDuration", t0)
    metrics.put_count("BlogArticleCount", len(tech_articles))
    logger.info("기술 블로그 수집 완료: company=%s, %d건", company, len(tech_articles))

    metrics.put_duration("TotalDuration", t_total)
    metrics.flush()

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "company": company,
            "news": news_saved,
            "tech_articles": tech_articles,
            "summary": {
                "news_count": len(news_saved),
                "tech_article_count": len(tech_articles),
            },
        }, ensure_ascii=False, default=str),
    }
