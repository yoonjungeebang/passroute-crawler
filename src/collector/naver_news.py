"""네이버 뉴스 검색 API 연동 모듈.

주요 기업의 기술/사업 동향 뉴스를 수집하여 면접 질문 생성에 활용한다.

저작권 주의사항:
- 네이버 API 가 제공하는 제목·요약(description)만 저장하며 기사 본문은 수집하지 않는다.
- 수집한 데이터는 내부 분석(임베딩·면접 질문 생성)용이며 원문을 외부에 재게시하지 않는다.
- DMCA takedown 요청 시 해당 데이터를 즉시 삭제할 수 있도록 delete-requests/ 경로를 사용한다.
"""
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core import KST
from core.circuit_breaker import CircuitOpenError, get_breaker
from parser.common import strip_html

logger = logging.getLogger(__name__)

NAVER_NEWS_API_URL = "https://naverapihub.apigw.ntruss.com/search/v1/news"
MAX_DISPLAY = 100
NEWS_RETENTION_DAYS = 90

# ── 검색 대상 기업 목록 ──

_DEFAULT_COMPANIES: tuple[str, ...] = (
    # 네이버 계열
    "네이버", "네이버웹툰", "네이버클라우드", "네이버파이낸셜", "네이버랩스", "네이버제트",
    # 카카오 계열
    "카카오", "카카오뱅크", "카카오페이", "카카오모빌리티",
    "카카오엔터테인먼트", "카카오엔터프라이즈", "카카오스타일", "카카오게임즈",
    # 토스 계열
    "토스", "토스뱅크", "토스증권", "토스페이먼츠",
    # 삼성
    "삼성전자", "삼성SDS", "삼성카드",
    # LG
    "LG CNS", "LG전자",
    # SK
    "SK", "SK C&C", "SK플래닛", "SK텔레콤",
    # KT / 현대 / 한화 / 롯데 / CJ / 신세계
    "KT", "현대오토에버", "현대카드", "한화시스템",
    "롯데정보통신", "롯데ON", "CJ올리브네트웍스", "SSG.COM",
    # 금융권 — 시중은행
    "KB국민은행", "신한은행", "하나은행", "우리은행", "우리FIS", "iM뱅크",
    # 금융권 — 인터넷전문은행 (카카오뱅크·토스뱅크 위에서 포함)
    "케이뱅크",
    # 금융권 — 지방은행
    "부산은행", "경남은행", "전북은행", "광주은행", "제주은행",
    # 금융권 — 특수/국책은행
    "NH농협은행", "Sh수협은행", "KDB산업은행", "IBK기업은행",
    # 금융권 — 카드/증권
    "신한카드", "현대카드", "하나카드",
    "미래에셋증권", "한국투자증권", "NH투자증권", "삼성증권", "KB증권",
    # 주요 IT 기업
    "라인", "쿠팡", "우아한형제들", "당근",
)

# 검색어 접미사: 기업명 + 접미사 조합으로 검색
_SEARCH_SUFFIXES: tuple[str, ...] = ("기술", "AI", "개발")

# ── 노이즈 필터링 ──

_EXCLUDE_TITLE_KEYWORDS: tuple[str, ...] = (
    "주가", "주식", "주주", "배당", "시세", "종가", "상한가", "하한가",
    "인사", "승진", "부고", "소송", "재판", "기소", "구속",
    "부동산", "아파트", "분양",
    "선거", "후보", "의원", "국회", "정당",
)

# ── API 호출 간격 (초) ──

_DEFAULT_API_DELAY = 0.1


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


def _make_external_id(url: str) -> str:
    """URL 에서 결정적 external_id 를 생성."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _is_noise(title: str) -> bool:
    """제목 기반 노이즈 판별."""
    for kw in _EXCLUDE_TITLE_KEYWORDS:
        if kw in title:
            return True
    return False


def _is_relevant(company: str, title: str) -> bool:
    """기업명이 제목에 포함되어야 관련 기사로 판정.

    설명(description)에만 언급되는 경우 '카카오톡 대화' 같은
    우연한 언급이 대부분이므로 제목 기준으로 필터링한다.
    """
    return company in title


def _deadline_from_pub_date(pub_date: datetime) -> int:
    """pub_date + 90일을 Unix timestamp 로 변환. 기존 deadline 삭제 로직과 호환."""
    expiry = pub_date + timedelta(days=NEWS_RETENTION_DAYS)
    return int(expiry.timestamp())


class NaverNewsCollector:
    """네이버 뉴스 검색 API 를 사용해 기업별 기술/사업 동향 뉴스를 수집한다."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        *,
        companies: tuple[str, ...] | None = None,
        search_suffixes: tuple[str, ...] | None = None,
        api_delay: float = _DEFAULT_API_DELAY,
    ):
        self.client_id = client_id or os.environ.get("NAVER_CLIENT_ID", "")
        self.client_secret = client_secret or os.environ.get("NAVER_CLIENT_SECRET", "")
        self.companies = companies or _DEFAULT_COMPANIES
        self.search_suffixes = search_suffixes or _SEARCH_SUFFIXES
        self.api_delay = api_delay

        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({
            "X-NCP-APIGW-API-KEY-ID": self.client_id,
            "X-NCP-APIGW-API-KEY": self.client_secret,
        })
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
                NAVER_NEWS_API_URL, params=params, timeout=10,
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
    """NewsItem 을 S3 저장용 dict 로 변환. JobDetail 호환 형식."""
    raw_text = f"{item.title}\n\n{item.description}"
    return {
        "source": "naver_news",
        "external_id": _make_external_id(item.url),
        "url": item.url,
        "company_name": item.company_name,
        "title": item.title,
        "raw_text": raw_text,
        "tech_stack": [],
        "deadline": _deadline_from_pub_date(item.pub_date),
        "crawled_at": item.collected_at,
        "career_level": "",
    }
