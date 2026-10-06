"""Lambda 핸들러: 검색 API + 기업별 실시간 수집 API (API Gateway 트리거)."""
import json
import logging
import time
from datetime import datetime

from core import KST
from crawler.base import JobDetail

from ._common import (
    MAX_COMPANY_LENGTH,
    MAX_QUERY_LENGTH,
    api_error,
    get_pg_storage,
    safe_int,
)

logger = logging.getLogger(__name__)


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
    query = params.get("q", "").strip()[:MAX_QUERY_LENGTH]
    company = params.get("company", "").strip()[:MAX_COMPANY_LENGTH] or None
    limit = safe_int(params.get("limit", "10"), default=10, min_val=1, max_val=30)

    if not query:
        return api_error(400, "q 파라미터가 필요합니다.")

    t0 = time.time()
    query_embedding = embed_text(query)
    metrics.put_duration("EmbeddingDuration", t0)

    t0 = time.time()
    pg = get_pg_storage()
    job_results = search_jobs(pg.conn, query_embedding, limit=limit, company=company)
    metrics.put_duration("DbQueryDuration", t0)
    metrics.put_count("JobResultCount", len(job_results))

    for job in job_results:
        job["similarity"] = round(float(job["similarity"]), 4)

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
    company = params.get("company", "").strip()[:MAX_COMPANY_LENGTH]

    if not company:
        return api_error(400, "company 파라미터가 필요합니다.")

    pg = get_pg_storage()
    news_saved = []

    # ── 1. 뉴스 수집 ──
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

    # ── 2. 기술 블로그 수집 ──
    t0 = time.time()
    _MAX_KEYWORDS = 10
    keywords_param = params.get("keywords", "").strip()
    keyword_list = [kw.strip()[:50] for kw in keywords_param.split(",") if kw.strip()][:_MAX_KEYWORDS]
    max_crawl = safe_int(params.get("max_articles", "10"), default=10, min_val=1, max_val=20)

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

        rss_feed_url = find_rss_feed_url(url)
        if rss_feed_url:
            content = fetch_content_from_rss(rss_feed_url, url)
            if content:
                logger.info("RSS에서 본문 추출 성공: %s", url)

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
