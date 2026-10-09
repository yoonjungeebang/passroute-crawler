"""네이버 API HUB 검색 API 실시간 호출 모듈. 뉴스/웹문서를 검색 시점에 가져온다."""
import logging
import re
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_search_config
from core.circuit_breaker import get_breaker
from parser.common import strip_html

logger = logging.getLogger(__name__)

_SEARCH_CONFIG = load_search_config()
_NEWS_URL: str = _SEARCH_CONFIG["news_url"]
_WEBKR_URL: str = _SEARCH_CONFIG["webkr_url"]


def _make_session(api_key_id: str, api_key: str) -> requests.Session:
    """네이버 API HUB 세션 생성."""
    session = requests.Session()
    retry = Retry(
        total=_SEARCH_CONFIG["retry_total"],
        backoff_factor=_SEARCH_CONFIG["retry_backoff_factor"],
        status_forcelist=[429, 500, 502, 503],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "X-NCP-APIGW-API-KEY-ID": api_key_id,
        "X-NCP-APIGW-API-KEY": api_key,
    })
    return session


def _search(
    session: requests.Session,
    url: str,
    query: str,
    label: str,
    *,
    display: int = 10,
    extra_params: dict | None = None,
    item_mapper=None,
) -> list[dict]:
    """네이버 API HUB 검색 공통 로직."""
    breaker = get_breaker("naver_search_api")
    params = {"query": query, "display": display}
    if extra_params:
        params.update(extra_params)
    try:
        with breaker:
            resp = session.get(url, params=params, timeout=_SEARCH_CONFIG["timeout"])
            resp.raise_for_status()
            data = resp.json()
    except Exception:
        logger.exception("네이버 %s 검색 실패: query=%s", label, query)
        return []

    return [item_mapper(item) for item in data.get("items", [])]


def search_news(session: requests.Session, query: str, *, display: int = 10) -> list[dict]:
    """네이버 API HUB 뉴스 검색 실시간 호출."""
    return _search(
        session, _NEWS_URL, query, "뉴스",
        display=display,
        extra_params={"sort": "date"},
        item_mapper=lambda item: {
            "title": strip_html(item.get("title", "")),
            "description": strip_html(item.get("description", "")),
            "url": item.get("originallink") or item.get("link", ""),
            "pub_date": item.get("pubDate", ""),
            "source": "naver_news",
        },
    )


def search_webkr(session: requests.Session, query: str, *, display: int = 10) -> list[dict]:
    """네이버 API HUB 웹문서 검색 실시간 호출. 기술 블로그 등 웹 문서를 검색한다."""
    return _search(
        session, _WEBKR_URL, query, "웹문서",
        display=display,
        item_mapper=lambda item: {
            "title": strip_html(item.get("title", "")),
            "description": strip_html(item.get("description", "")),
            "url": item.get("link", ""),
            "source": "naver_webkr",
        },
    )


# ── 웹문서 검색 결과 필터링 ──

_SKIP_PATH_PATTERNS = re.compile(
    r"(/tag/|/tags/|/category/|/categories/|/page/|/archive)"
)


def _is_article_url(url: str) -> bool:
    """실제 글 URL인지 판별. 메인/태그/카테고리 페이지는 제외."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # 메인 페이지 (path가 비어있거나 / 하나)
    if not path or path == "":
        return False

    # 태그, 카테고리, 아카이브 페이지
    if _SKIP_PATH_PATTERNS.search(path):
        return False

    return True


def filter_webkr_results(
    results: list[dict],
    keywords: list[str],
    *,
    max_results: int = 10,
) -> list[dict]:
    """웹문서 검색 결과를 필터링한다.

    - 메인/태그/카테고리 페이지 제외
    - description에 키워드가 포함된 결과 우선
    - 중복 URL 제거
    - 상위 max_results건만 반환
    """
    seen_urls: set[str] = set()
    relevant: list[dict] = []
    others: list[dict] = []

    for item in results:
        url = item.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        if not _is_article_url(url):
            continue

        if item.get("description") and len(item["description"]) < _SEARCH_CONFIG["min_description_length"]:
            continue

        # 키워드가 제목이나 description에 포함되면 우선순위 높임
        text = (item.get("title", "") + " " + item.get("description", "")).lower()
        matched = any(kw.lower() in text for kw in keywords if kw)
        if matched:
            relevant.append(item)
        else:
            others.append(item)

    # 키워드 매칭된 것 먼저, 나머지는 뒤에
    filtered = relevant + others
    return filtered[:max_results]
