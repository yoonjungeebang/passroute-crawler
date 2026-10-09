"""Lambda 핸들러: 검색 API + 기업별 실시간 수집 API (API Gateway 트리거).

AWS Lambda 함수: 서버 없이 코드만 배포해서 실행하는 AWS 서비스.
API Gateway: HTTP 요청을 받아서 Lambda 함수를 호출해주는 AWS 서비스.
→ 사용자가 URL을 호출하면 → API Gateway가 받아서 → Lambda 함수(여기)가 실행된다.
"""
import json
import logging
import time
from datetime import datetime

from core import KST                        # KST: 한국 표준시 타임존 객체
from crawler.base import JobDetail
from parser.common import make_external_id

# from ._common: 점(.)은 "현재 패키지"를 의미. 즉 같은 handlers/ 디렉토리의 _common.py 를 import.
# 이를 "상대 import" 라고 한다.
from ._common import (
    MAX_COMPANY_LENGTH,
    MAX_QUERY_LENGTH,
    _API_CONFIG,
    api_error,
    get_pg_storage,
    safe_int,
)

logger = logging.getLogger(__name__)


# Lambda 핸들러 함수: AWS Lambda가 호출하는 진입점.
# event: API Gateway가 전달하는 요청 정보 (URL 파라미터, 헤더 등이 담긴 딕셔너리)
# context: Lambda 실행 환경 정보 (남은 시간, 메모리 등). 여기서는 안 쓰고 있다.
def search_api(event, context):
    """검색 API. 채용공고 유사도 검색 + 네이버 뉴스 실시간 검색."""
    from core.embedding import embed_text  # noqa: C0415
    from core.metrics import MetricsLogger  # noqa: C0415
    from core.secrets import get_naver_credentials  # noqa: C0415
    from collector.naver_news import make_naver_session, search_news  # noqa: C0415
    from search.pgvector_search import search_jobs  # noqa: C0415

    # time.monotonic(): 단조 증가하는 시간값 (초). 경과 시간 측정용.
    # time.time() 과 달리 시스템 시계가 바뀌어도 영향 안 받음.
    t_total = time.monotonic()
    metrics = MetricsLogger(function_name="search_api")

    # event.get("queryStringParameters"): URL의 쿼리 파라미터를 딕셔너리로 가져옴
    # 예: /search?q=백엔드&company=카카오 → {"q": "백엔드", "company": "카카오"}
    # or {}: event에 queryStringParameters가 없으면(None) 빈 딕셔너리 사용
    params = event.get("queryStringParameters") or {}

    # 메서드 체이닝: .get().strip()[:N] 처럼 여러 메서드를 이어서 호출
    # .get("q", ""): "q" 키의 값을 가져오되, 없으면 빈 문자열
    # .strip(): 앞뒤 공백 제거
    # [:MAX_QUERY_LENGTH]: 최대 길이 제한 (보안: 너무 긴 입력 방지)
    query = params.get("q", "").strip()[:MAX_QUERY_LENGTH]
    # or None: 빈 문자열("")이면 None 으로 변환.
    # 파이썬에서 빈 문자열은 False로 평가되므로, "" or None → None
    company = params.get("company", "").strip()[:MAX_COMPANY_LENGTH] or None
    limit = safe_int(params.get("limit", "10"), default=10, min_val=1, max_val=30)

    if not query:
        return api_error(400, "q 파라미터가 필요합니다.")

    t0 = time.monotonic()
    query_embedding = embed_text(query)  # 검색어를 벡터(숫자 배열)로 변환
    metrics.put_duration("EmbeddingDuration", t0)

    t0 = time.monotonic()
    pg = get_pg_storage()  # PostgreSQL 연결 싱글턴 가져오기
    pg._ensure_alive()     # DB 연결이 살아있는지 확인 (끊어졌으면 재연결)
    job_results = search_jobs(pg.conn, query_embedding, limit=limit, company=company)
    metrics.put_duration("DbQueryDuration", t0)
    metrics.put_count("JobResultCount", len(job_results))

    for job in job_results:
        # round(숫자, 소수점자릿수): 반올림. round(0.12345, 4) → 0.1235
        # float(): 문자열이나 다른 타입을 실수로 변환
        job["similarity"] = round(float(job["similarity"]), 4)

    t0 = time.monotonic()
    # 삼항 연산자: A if 조건 else B
    search_query = f"{company} {query}" if company else query
    # 튜플 언패킹: 함수가 튜플을 반환하면 여러 변수에 한번에 대입 가능
    # 예: (id, secret) = ("abc", "xyz") → id="abc", secret="xyz"
    client_id, client_secret = get_naver_credentials()
    naver_session = make_naver_session(client_id, client_secret)
    news_results = search_news(naver_session, f"{search_query} 기술", display=_API_CONFIG["search_news_display"])
    metrics.put_duration("NewsApiDuration", t0)
    metrics.put_count("NewsResultCount", len(news_results))

    metrics.put_duration("TotalDuration", t_total)
    metrics.flush()

    # Lambda 함수는 딕셔너리를 반환하고, API Gateway가 이를 HTTP 응답으로 변환한다.
    return {
        "statusCode": 200,  # HTTP 200 OK
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",  # CORS: 모든 도메인에서 이 API 호출 허용
        },
        # json.dumps(): 파이썬 객체 → JSON 문자열
        # ensure_ascii=False: 한글을 \uXXXX 로 이스케이프하지 않고 그대로 출력
        # default=str: datetime 등 JSON 직렬화 안 되는 타입을 str()로 변환
        "body": json.dumps({
            "query": query,
            "company": company,
            "jobs": job_results,
            "news": news_results,
        }, ensure_ascii=False, default=str),
    }


def company_collect(event, context):
    """기업 데이터 수집 API. 면접 방 생성 시 해당 기업의 기술 블로그 + 뉴스를 실시간 수집하여 DB 저장."""

    from collector.naver_news import (  # noqa: C0415
        NaverNewsCollector,
        filter_webkr_results,
        make_naver_session,
        news_item_to_detail_dict,
        search_webkr,
    )
    from collector.tech_blog import (  # noqa: C0415
        _fetch_page_content,
        _make_session as _make_blog_session,
        fetch_content_from_rss,
        find_rss_feed_url,
    )
    from core.metrics import MetricsLogger  # noqa: C0415
    from core.secrets import get_naver_credentials  # noqa: C0415
    from crawler.robots_check import RobotsChecker  # noqa: C0415

    t_total = time.monotonic()
    metrics = MetricsLogger(function_name="company_collect")

    params = event.get("queryStringParameters") or {}
    company = params.get("company", "").strip()[:MAX_COMPANY_LENGTH]

    if not company:
        return api_error(400, "company 파라미터가 필요합니다.")

    pg = get_pg_storage()
    news_saved = []

    # ── 1. 뉴스 수집 ──
    t0 = time.monotonic()
    client_id, client_secret = get_naver_credentials()  # 튜플 언패킹
    news_collector_inst = NaverNewsCollector(
        client_id=client_id,
        client_secret=client_secret,
        companies=(company,),  # (company,) : 요소가 하나인 튜플. 쉼표가 중요!
                                # (company) 만 쓰면 그냥 괄호로 감싼 것이지 튜플이 아니다.
    )
    news_items = news_collector_inst.collect_all()

    # 복합 타입 힌트: list[tuple[A, B, C]]
    # "A, B, C 세 가지를 담는 튜플"의 리스트
    news_batch: list[tuple[JobDetail, list[float] | None, str]] = []
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
        news_batch.append((detail, None, "ok"))
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
    t0 = time.monotonic()
    _MAX_KEYWORDS = _API_CONFIG["max_keywords"]
    keywords_param = params.get("keywords", "").strip()
    # 리스트 컴프리헨션(list comprehension): [표현식 for 변수 in 반복 if 조건]
    # 한 줄로 리스트를 만드는 파이썬의 강력한 문법.
    # .split(","): 문자열을 쉼표로 나눠서 리스트로 만든다. "a,b,c" → ["a", "b", "c"]
    # kw.strip()[:50]: 각 키워드의 앞뒤 공백 제거 후 50자로 제한
    # if kw.strip(): 빈 문자열이 아닌 것만 포함
    # [:_MAX_KEYWORDS]: 최종 리스트를 최대 10개로 제한
    keyword_list = [kw.strip()[:50] for kw in keywords_param.split(",") if kw.strip()][:_MAX_KEYWORDS]
    max_crawl = safe_int(params.get("max_articles", "10"), default=10, min_val=1, max_val=20)

    naver_session = make_naver_session(client_id, client_secret)
    raw_results: list[dict] = []

    if keyword_list:
        for keyword in keyword_list:
            query = f"{company} 기술블로그 {keyword}"
            results = search_webkr(naver_session, query, display=_API_CONFIG["webkr_keyword_display"])
            raw_results.extend(results)
    else:
        query = f"{company} 기술블로그"
        raw_results = search_webkr(naver_session, query, display=_API_CONFIG["webkr_default_display"])

    filtered = filter_webkr_results(raw_results, keyword_list, max_results=max_crawl)

    if not raw_results:
        logger.info("기술 블로그 없음: company=%s, 검색 결과 0건", company)
    else:
        logger.info(
            "웹문서 검색: company=%s, keywords=%s, 원본 %d건 → 필터 %d건",
            company, keywords_param, len(raw_results), len(filtered),
        )

    robots = RobotsChecker()
    blog_session = _make_blog_session()
    tech_articles = []
    blog_batch: list[tuple[JobDetail, list[float] | None, str]] = []

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
            time.sleep(_API_CONFIG["blog_crawl_delay"])

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
            external_id=make_external_id(url),
            url=url,
            company_name=company,
            title=item["title"],
            raw_text=content,
            tech_stack=tuple(),
            deadline=0,
            crawled_at=datetime.now(KST).isoformat(),
            career_level="",
        )
        blog_batch.append((detail, None, "ok"))

    metrics.put_duration("BlogCollectDuration", t0)
    metrics.put_count("BlogArticleCount", len(tech_articles))
    logger.info("기술 블로그 수집 완료: company=%s, %d건", company, len(tech_articles))

    pg.save_batch(news_batch + blog_batch)

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
