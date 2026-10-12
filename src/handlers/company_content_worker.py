"""기업별 수집 워커. SQS 트리거로 뉴스/블로그를 수집하고 WebSocket으로 결과를 푸시한다."""
import json
import logging
import os
import time
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key

from core import KST
from crawler.base import JobDetail
from core.normalize import make_external_id

from ._common import _API_CONFIG, get_pg_storage


def safe_int(value: str, *, default: int, min_val: int = 1, max_val: int = 100) -> int:
    """문자열을 정수로 변환한다. 실패 시 default, 범위 초과 시 클램핑."""
    try:
        n = int(value)
    except (ValueError, TypeError):
        return default
    return max(min_val, min(n, max_val))

logger = logging.getLogger(__name__)

_COLLECT_STATUS_TABLE = os.environ.get("COLLECT_STATUS_TABLE", "passroute-collect-status")
_CONNECTIONS_TABLE = os.environ.get("WS_CONNECTIONS_TABLE", "passroute-ws-connections")


def company_collect_worker(event, context):
    """SQS 트리거: 기업별 뉴스/블로그 수집 → DB 저장 → 전체 연결에 WebSocket 푸시."""
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.monotonic()
    metrics = MetricsLogger(function_name="company_collect_worker")
    failures: list[dict] = []

    for record in event["Records"]:
        try:
            message = json.loads(record["body"])
            company = message["company"]
            position = message.get("position", "")
            tech_stack_param = message.get("tech_stack", "")
            keywords_param = message.get("keywords", "")
            max_articles = message.get("max_articles", "10")

            logger.info("수집 시작: company=%s", company)

            result = _collect_company_data(
                company=company,
                position=position,
                tech_stack_param=tech_stack_param,
                keywords_param=keywords_param,
                max_articles=max_articles,
            )

            _update_collect_status(company, "done")

            # company GSI로 해당 기업을 요청한 전체 연결에 푸시
            _push_to_all_connections(company, result)

            logger.info("수집 완료: company=%s, 뉴스 %d건, 블로그 %d건",
                        company, len(result["news"]), len(result["tech_articles"]))

        except Exception:
            logger.exception("수집 실패: %s", record.get("body", ""))
            failures.append({"itemIdentifier": record["messageId"]})

    metrics.put_duration("TotalDuration", t_total)
    metrics.flush()
    return {"statusCode": 200, "batchItemFailures": failures}


def _collect_company_data(
    *,
    company: str,
    position: str,
    tech_stack_param: str,
    keywords_param: str,
    max_articles: str,
) -> dict:
    """기업 데이터를 수집하고 DB에 저장한다. 결과 dict를 반환."""
    from naver.news import (  # noqa: C0415
        NaverNewsCollector,
        news_item_to_detail_dict,
    )
    from naver.webkr import filter_webkr_results, search_webkr  # noqa: C0415
    from naver.session import make_naver_session  # noqa: C0415
    from naver.tech_blog import (  # noqa: C0415
        _fetch_page_content,
        _make_session as _make_blog_session,
        fetch_content_from_rss,
        find_rss_feed_url,
    )
    from config import load_naver_config  # noqa: C0415
    from core.secrets import get_naver_credentials  # noqa: C0415
    from crawler.robots_check import RobotsChecker  # noqa: C0415

    _MAX_KEYWORDS = _API_CONFIG["max_keywords"]

    position = position.strip()[:_API_CONFIG["max_position_length"]]
    tech_stack_list = [
        ts.strip()[:50] for ts in tech_stack_param.split(",") if ts.strip()
    ][:_API_CONFIG["max_tech_stack"]]

    pg = get_pg_storage()
    news_saved = []

    # ── 1. 뉴스 수집 ──
    client_id, client_secret = get_naver_credentials()

    custom_suffixes = None
    if position:
        default_suffixes = load_naver_config()["search_suffixes"]
        custom_suffixes = default_suffixes + (position,)

    news_collector_inst = NaverNewsCollector(
        client_id=client_id,
        client_secret=client_secret,
        companies=(company,),
        search_suffixes=custom_suffixes,
    )
    news_items = news_collector_inst.collect_all()

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

    # ── 2. 기술 블로그 수집 ──
    keyword_list = [kw.strip()[:50] for kw in keywords_param.split(",") if kw.strip()][:_MAX_KEYWORDS]

    if tech_stack_list:
        existing = {kw.lower() for kw in keyword_list}
        for ts in tech_stack_list:
            if ts.lower() not in existing:
                keyword_list.append(ts)
                existing.add(ts.lower())
        keyword_list = keyword_list[:_MAX_KEYWORDS]

    if position and position.lower() not in {kw.lower() for kw in keyword_list}:
        keyword_list.append(position)
        keyword_list = keyword_list[:_MAX_KEYWORDS]

    max_crawl = safe_int(max_articles, default=10, min_val=1, max_val=20)

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

    if tech_stack_list and filtered:
        def _tech_score(item: dict) -> int:
            text = (item.get("title", "") + " " + item.get("description", "")).lower()
            return sum(1 for ts in tech_stack_list if ts.lower() in text)
        filtered.sort(key=_tech_score, reverse=True)

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

        if not content:
            if not robots.check_and_alert(url, "tech_blog"):
                continue
            content = _fetch_page_content(blog_session, url)
            time.sleep(_API_CONFIG["blog_crawl_delay"])

        if not content:
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

    pg.save_batch(news_batch + blog_batch)

    return {
        "news": news_saved,
        "tech_articles": tech_articles,
    }


def _update_collect_status(company: str, status: str) -> None:
    """수집 상태를 DynamoDB에 업데이트한다."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(_COLLECT_STATUS_TABLE)
    now = int(time.time())
    cooldown = int(_API_CONFIG.get("collect_cooldown", 300))
    try:
        table.put_item(Item={
            "company": company,
            "status": status,
            "updatedAt": now,
            "ttl": now + cooldown + 60,
        })
    except Exception:
        logger.exception("수집 상태 업데이트 실패: %s", company)


def _push_to_all_connections(company: str, result: dict) -> None:
    """해당 company를 요청한 모든 WebSocket 연결에 결과를 푸시한다."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(_CONNECTIONS_TABLE)

    # company GSI로 해당 기업의 모든 연결 조회
    try:
        resp = table.query(
            IndexName="company-index",
            KeyConditionExpression=Key("company").eq(company),
        )
        connections = resp.get("Items", [])
    except Exception:
        logger.exception("연결 목록 조회 실패: company=%s", company)
        return

    if not connections:
        logger.info("푸시 대상 연결 없음: company=%s", company)
        return

    payload = json.dumps({
        "status": "done",
        "company": company,
        "news": result["news"],
        "tech_articles": result["tech_articles"],
        "summary": {
            "news_count": len(result["news"]),
            "tech_article_count": len(result["tech_articles"]),
        },
    }, ensure_ascii=False).encode("utf-8")

    pushed = 0
    for conn in connections:
        endpoint = conn.get("wsEndpoint", "")
        connection_id = conn["connectionId"]
        if not endpoint:
            continue

        apigw = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)
        try:
            apigw.post_to_connection(ConnectionId=connection_id, Data=payload)
            pushed += 1
        except apigw.exceptions.GoneException:
            # 이미 끊어진 연결은 정리
            try:
                table.delete_item(Key={"connectionId": connection_id})
            except Exception:
                pass
        except Exception:
            logger.exception("푸시 실패: connection=%s", connection_id)

    logger.info("결과 푸시 완료: company=%s, %d/%d건", company, pushed, len(connections))
