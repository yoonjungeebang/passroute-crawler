"""네이버 웹문서 검색 모듈. 기술 블로그 URL을 검색하고 필터링한다."""
import logging
import re
from urllib.parse import urlparse

import requests

from config import load_naver_config
from core.normalize import strip_html
from naver.session import search

logger = logging.getLogger(__name__)

_NAVER_CONFIG = load_naver_config()
_WEBKR_URL: str = _NAVER_CONFIG["webkr_url"]


def search_webkr(session: requests.Session, query: str, *, display: int = 10) -> list[dict]:
    """웹문서 검색 실시간 호출. 기술 블로그 등 웹 문서를 검색한다."""
    return search(
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

    if not path or path == "":
        return False

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

        if item.get("description") and len(item["description"]) < _NAVER_CONFIG["min_description_length"]:
            continue

        text = (item.get("title", "") + " " + item.get("description", "")).lower()
        matched = any(kw.lower() in text for kw in keywords if kw)
        if matched:
            relevant.append(item)
        else:
            others.append(item)

    filtered = relevant + others
    return filtered[:max_results]
