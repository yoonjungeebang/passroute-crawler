"""네이버 뉴스 검색/수집 모듈.

저작권 주의사항:
- 네이버 API 가 제공하는 제목·요약(description)만 저장하며 기사 본문은 수집하지 않는다.
- 수집한 데이터는 내부 분석(임베딩·면접 질문 생성)용이며 원문을 외부에 재게시하지 않는다.
- DMCA takedown 요청 시 해당 데이터를 즉시 삭제할 수 있도록 delete-requests/ 경로를 사용한다.
"""
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

from config import load_naver_config
from core import KST
from core.circuit_breaker import CircuitOpenError, get_breaker
from core.normalize import make_external_id, strip_html
from naver.session import make_naver_session

logger = logging.getLogger(__name__)

# ── 설정 파일에서 로드 ──

_NAVER_CONFIG = load_naver_config()

NAVER_NEWS_API_URL: str = _NAVER_CONFIG["news_url"]
NEWS_RETENTION_DAYS: int = _NAVER_CONFIG["retention_days"]
_DEFAULT_API_DELAY: float = _NAVER_CONFIG["api_delay"]
MAX_DISPLAY: int = _NAVER_CONFIG["max_display"]
_API_TIMEOUT: int = _NAVER_CONFIG["timeout"]
_SEARCH_SUFFIXES: tuple[str, ...] = _NAVER_CONFIG["search_suffixes"]
_EXCLUDE_TITLE_KEYWORDS: tuple[str, ...] = _NAVER_CONFIG["exclude_news_title_keywords"]


# ── 뉴스 대량 수집 ──


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
        """companies에 전달된 기업들의 뉴스를 수집한다. URL 기준 중복 제거."""
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
