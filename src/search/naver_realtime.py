"""네이버 검색 API 실시간 호출 모듈. 뉴스/블로그를 검색 시점에 가져온다."""
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from parser.common import strip_html

logger = logging.getLogger(__name__)

_NEWS_URL = "https://openapi.naver.com/v1/search/news.json"
_BLOG_URL = "https://openapi.naver.com/v1/search/blog.json"


def _make_session(client_id: str, client_secret: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    })
    return session


def search_news(session: requests.Session, query: str, *, display: int = 10) -> list[dict]:
    """네이버 뉴스 검색 API 실시간 호출."""
    try:
        resp = session.get(_NEWS_URL, params={
            "query": query,
            "display": display,
            "sort": "date",
        }, timeout=5)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        logger.exception("네이버 뉴스 검색 실패: query=%s", query)
        return []

    return [
        {
            "title": strip_html(item.get("title", "")),
            "description": strip_html(item.get("description", "")),
            "url": item.get("originallink") or item.get("link", ""),
            "pub_date": item.get("pubDate", ""),
            "source": "naver_news",
        }
        for item in data.get("items", [])
    ]


def search_blog(session: requests.Session, query: str, *, display: int = 10) -> list[dict]:
    """네이버 블로그 검색 API 실시간 호출."""
    try:
        resp = session.get(_BLOG_URL, params={
            "query": query,
            "display": display,
            "sort": "date",
        }, timeout=5)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        logger.exception("네이버 블로그 검색 실패: query=%s", query)
        return []

    return [
        {
            "title": strip_html(item.get("title", "")),
            "description": strip_html(item.get("description", "")),
            "url": item.get("link", ""),
            "blogger_name": item.get("bloggername", ""),
            "pub_date": item.get("postdate", ""),
            "source": "naver_blog",
        }
        for item in data.get("items", [])
    ]
