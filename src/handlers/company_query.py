"""Lambda 핸들러: 기업별 데이터 조회 API (API Gateway 트리거)."""
import json
import logging

from ._common import _API_CONFIG, get_pg_storage

MAX_COMPANY_LENGTH: int = _API_CONFIG["max_company_length"]


def api_error(status: int, message: str) -> dict:
    """API Gateway 에러 응답을 생성한다."""
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }


logger = logging.getLogger(__name__)


def company_query(event, context):
    """기업 데이터 조회 API. DB에 저장된 뉴스/블로그를 조회하여 즉시 응답한다."""

    params = event.get("queryStringParameters") or {}
    company = params.get("company", "").strip()[:MAX_COMPANY_LENGTH]

    if not company:
        return api_error(400, "company 파라미터가 필요합니다.")

    position = params.get("position", "").strip()[:_API_CONFIG["max_position_length"]]
    tech_stack_param = params.get("tech_stack", "").strip()
    tech_stack_list = [
        ts.strip()[:50] for ts in tech_stack_param.split(",") if ts.strip()
    ][:_API_CONFIG["max_tech_stack"]]

    pg = get_pg_storage()
    pg._ensure_alive()

    news = _query_by_company(pg.conn, company, source="naver_news")
    tech_articles = _query_by_company(pg.conn, company, source="tech_blog_webkr")

    if tech_stack_list:
        news = _sort_by_tech_score(news, tech_stack_list)
        tech_articles = _sort_by_tech_score(tech_articles, tech_stack_list)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "company": company,
            "position": position or None,
            "tech_stack": tech_stack_list or [],
            "news": news,
            "tech_articles": tech_articles,
            "summary": {
                "news_count": len(news),
                "tech_article_count": len(tech_articles),
            },
        }, ensure_ascii=False, default=str),
    }


def _query_by_company(conn, company: str, *, source: str) -> list[dict]:
    """DB에서 해당 기업+소스의 데이터를 조회한다.
    """
    query = """
    SELECT url, title, document, company_name, tech_stack, crawled_at
    FROM job_descriptions
    WHERE company_name = %(company)s
      AND source = %(source)s
      AND (deadline = 0 OR deadline > EXTRACT(EPOCH FROM NOW()))
    ORDER BY crawled_at DESC
    LIMIT 50
    """
    with conn.cursor() as cur:
        cur.execute(query, {"company": company, "source": source})
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]


def _sort_by_tech_score(items: list[dict], tech_stack_list: list[str]) -> list[dict]:
    """tech_stack 키워드 포함 개수로 결과를 정렬한다."""
    def score(item: dict) -> int:
        text = (item.get("title", "") + " " + item.get("document", "")).lower()
        return sum(1 for ts in tech_stack_list if ts.lower() in text)
    return sorted(items, key=score, reverse=True)
