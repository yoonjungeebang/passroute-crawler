"""네이버 API HUB 연동 모듈.

뉴스 수집(대량 저장)과 실시간 검색(뉴스/웹문서)을 모두 담당한다.

저작권 주의사항:
- 네이버 API 가 제공하는 제목·요약(description)만 저장하며 기사 본문은 수집하지 않는다.
- 수집한 데이터는 내부 분석(임베딩·면접 질문 생성)용이며 원문을 외부에 재게시하지 않는다.
- DMCA takedown 요청 시 해당 데이터를 즉시 삭제할 수 있도록 delete-requests/ 경로를 사용한다.
"""
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_naver_news_config, load_search_config
from core import KST
from core.circuit_breaker import CircuitOpenError, get_breaker
from parser.common import make_external_id, strip_html

logger = logging.getLogger(__name__)

# ── 설정 파일에서 로드 ──

_NEWS_CONFIG = load_naver_news_config()
_SEARCH_CONFIG = load_search_config()

NAVER_NEWS_API_URL: str = _NEWS_CONFIG["api_url"]
NEWS_RETENTION_DAYS: int = _NEWS_CONFIG["retention_days"]
_DEFAULT_API_DELAY: float = _NEWS_CONFIG["api_delay"]
MAX_DISPLAY: int = _NEWS_CONFIG["max_display"]
_API_TIMEOUT: int = _NEWS_CONFIG["api_timeout"]
_SEARCH_SUFFIXES: tuple[str, ...] = _NEWS_CONFIG["search_suffixes"]
_EXCLUDE_TITLE_KEYWORDS: tuple[str, ...] = _NEWS_CONFIG["exclude_news_title_keywords"]

_WEBKR_URL: str = _SEARCH_CONFIG["webkr_url"]


# ── 공용 세션 생성 ──


def make_naver_session(api_key_id: str, api_key: str) -> requests.Session:
    """네이버 API HUB 세션 생성. 뉴스 수집과 실시간 검색 모두에서 사용."""
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


# ── 실시간 검색 (search_api 핸들러용) ──


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
    """뉴스 검색 실시간 호출. 검색 API 응답에 포함할 뉴스를 가져온다."""
    return _search(
        session, NAVER_NEWS_API_URL, query, "뉴스",
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
    """웹문서 검색 실시간 호출. 기술 블로그 등 웹 문서를 검색한다."""
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

        if item.get("description") and len(item["description"]) < _SEARCH_CONFIG["min_description_length"]:
            continue

        text = (item.get("title", "") + " " + item.get("description", "")).lower()
        matched = any(kw.lower() in text for kw in keywords if kw)
        if matched:
            relevant.append(item)
        else:
            others.append(item)

    filtered = relevant + others
    return filtered[:max_results]


# ── 뉴스 대량 수집 (company_collect 핸들러용) ──


@dataclass(frozen=True)
class NewsItem:
    """뉴스 검색 결과 1건."""
    company_name: str
    title: str
    description: str
    url: str
    pub_date: datetime
    collected_at: str


def _parse_pub_date(date_str: str) -> datetime:
    """RFC 2822 형식의 pubDate 를 datetime 으로 변환."""
    try:
        return parsedate_to_datetime(date_str)
    except (ValueError, TypeError):
        return datetime.now(KST)


def _is_noise(title: str) -> bool:
    """제목 기반 노이즈 판별."""
    for kw in _EXCLUDE_TITLE_KEYWORDS:
        if kw in title:
            return True
    return False


def _is_relevant(company: str, title: str) -> bool:
    """기업명이 제목에 포함되어야 관련 기사로 판정."""
    return company in title


def _deadline_from_pub_date(pub_date: datetime) -> int:
    """pub_date + 보존기간을 Unix timestamp 로 변환."""
    expiry = pub_date + timedelta(days=NEWS_RETENTION_DAYS)
    return int(expiry.timestamp())


class NaverNewsCollector:
    """네이버 뉴스 검색 API 를 사용해 기업별 기술/사업 동향 뉴스를 수집한다."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        *,
        companies: tuple[str, ...],
        search_suffixes: tuple[str, ...] | None = None,
        api_delay: float = _DEFAULT_API_DELAY,
    ):
        self.client_id = client_id or os.environ.get("NAVER_CLIENT_ID", "")
        self.client_secret = client_secret or os.environ.get("NAVER_CLIENT_SECRET", "")
        self.companies = companies
        self.search_suffixes = search_suffixes or _SEARCH_SUFFIXES
        self.api_delay = api_delay
        self.session = make_naver_session(self.client_id, self.client_secret)
        self._breaker = get_breaker("naver_news_api")

    def _call_api(self, query: str, display: int = MAX_DISPLAY, start: int = 1) -> dict:
        """네이버 뉴스 검색 API 호출."""
        params = {
            "query": query,
            "display": display,
            "start": start,
            "sort": "date",
        }
        with self._breaker:
            resp = self.session.get(
                NAVER_NEWS_API_URL, params=params, timeout=_API_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if "errorCode" in data:
                raise RuntimeError(
                    f"네이버 API 에러: {data.get('errorCode')} - {data.get('errorMessage', '')}"
                )
            return data

    def _search_company(self, company: str) -> list[NewsItem]:
        """한 기업에 대해 검색어 접미사별로 뉴스를 수집한다."""
        seen_urls: set[str] = set()
        items: list[NewsItem] = []
        now_iso = datetime.now(KST).isoformat()

        for suffix in self.search_suffixes:
            query = f"{company} {suffix}"
            try:
                data = self._call_api(query)
            except CircuitOpenError:
                raise
            except Exception:
                logger.exception("API 호출 실패: query=%s", query)
                continue

            for raw in data.get("items", []):
                url = raw.get("originallink") or raw.get("link", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                title = strip_html(raw.get("title", ""))
                description = strip_html(raw.get("description", ""))

                if _is_noise(title):
                    continue
                if not _is_relevant(company, title):
                    continue

                items.append(NewsItem(
                    company_name=company,
                    title=title,
                    description=description,
                    url=url,
                    pub_date=_parse_pub_date(raw.get("pubDate", "")),
                    collected_at=now_iso,
                ))

            time.sleep(self.api_delay)

        return items

    def collect_all(self) -> list[NewsItem]:
        """전체 기업에 대해 뉴스를 수집한다. URL 기준 전역 중복 제거."""
        all_items: list[NewsItem] = []
        global_seen_urls: set[str] = set()

        for company in self.companies:
            try:
                items = self._search_company(company)
            except CircuitOpenError:
                logger.warning("서킷 OPEN: 나머지 기업 수집 스킵")
                break

            for item in items:
                if item.url in global_seen_urls:
                    continue
                global_seen_urls.add(item.url)
                all_items.append(item)

            logger.info("company=%s: %d건 수집", company, len(items))

        logger.info("전체 뉴스 수집 완료: %d건 (기업 %d개)", len(all_items), len(self.companies))
        return all_items


def news_item_to_detail_dict(item: NewsItem) -> dict:
    """NewsItem 을 SQS 전달용 dict 로 변환. JobDetail 호환 형식."""
    raw_text = f"{item.title}\n\n{item.description}"
    return {
        "source": "naver_news",
        "external_id": make_external_id(item.url),
        "url": item.url,
        "company_name": item.company_name,
        "title": item.title,
        "raw_text": raw_text,
        "tech_stack": [],
        "deadline": _deadline_from_pub_date(item.pub_date),
        "crawled_at": item.collected_at,
        "career_level": "",
    }
