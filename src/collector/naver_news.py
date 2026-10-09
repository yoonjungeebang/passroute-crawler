"""네이버 뉴스 검색 API 연동 모듈.

주요 기업의 기술/사업 동향 뉴스를 수집하여 면접 질문 생성에 활용한다.

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

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_naver_news_config
from core import KST
from core.circuit_breaker import CircuitOpenError, get_breaker
from parser.common import make_external_id, strip_html

logger = logging.getLogger(__name__)

NAVER_NEWS_API_URL = "https://naverapihub.apigw.ntruss.com/search/v1/news"
MAX_DISPLAY = 100
NEWS_RETENTION_DAYS = 90

# ── 설정 파일에서 로드 ──

_NEWS_CONFIG = load_naver_news_config()
_SEARCH_SUFFIXES: tuple[str, ...] = _NEWS_CONFIG["search_suffixes"]
_EXCLUDE_TITLE_KEYWORDS: tuple[str, ...] = _NEWS_CONFIG["exclude_news_title_keywords"]

# ── API 호출 간격 (초) ──

_DEFAULT_API_DELAY = 0.1


@dataclass(frozen=True)
class NewsItem:
    """뉴스 검색 결과 1건. @dataclass(frozen=True) 이므로 생성 후 값 변경 불가."""
    company_name: str
    title: str
    description: str    # 뉴스 요약문
    url: str
    pub_date: datetime  # 발행일 (datetime 객체)
    collected_at: str   # 수집 시각 (ISO 문자열)


def _parse_pub_date(date_str: str) -> datetime:
    """RFC 2822 형식의 pubDate 를 datetime 으로 변환."""
    # 함수 이름 앞 _ (밑줄): "이 모듈 내부에서만 쓰는 함수"라는 관례.
    # 외부에서 import 할 수는 있지만, from module import * 할 때 제외된다.
    try:
        return parsedate_to_datetime(date_str)
    except (ValueError, TypeError):
        # 파싱 실패 시 현재 시각을 반환 (fallback)
        return datetime.now(KST)




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

    # __init__ 매개변수 설명:
    # - client_id, client_secret: * 앞에 있으므로 위치 인자로도 전달 가능
    # - companies 이후: * 뒤에 있으므로 키워드 전용 인자 (이름 지정 필수)
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

        # HTTP 세션 설정 (base.py의 make_crawler_session과 동일한 패턴)
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        # 네이버 API HUB 인증 헤더 설정
        self.session.headers.update({
            "X-NCP-APIGW-API-KEY-ID": self.client_id,
            "X-NCP-APIGW-API-KEY": self.client_secret,
        })
        self._breaker = get_breaker("naver_news_api")

    def _call_api(self, query: str, display: int = MAX_DISPLAY, start: int = 1) -> dict:
        """네이버 뉴스 검색 API 호출."""
        # API에 전달할 쿼리 파라미터 딕셔너리
        params = {
            "query": query,       # 검색어
            "display": display,   # 한 번에 가져올 결과 수
            "start": start,       # 시작 위치 (페이징용)
            "sort": "date",       # 정렬 기준: 최신순
        }
        with self._breaker:  # 서킷 브레이커 보호 하에 실행
            # session.get(): HTTP GET 요청을 보낸다.
            # params=params: URL 뒤에 ?query=...&display=... 형태로 자동 추가됨
            # timeout=10: 10초 안에 응답이 없으면 에러
            resp = self.session.get(
                NAVER_NEWS_API_URL, params=params, timeout=10,
            )
            # raise_for_status(): HTTP 응답 코드가 4xx/5xx 이면 예외를 발생시킨다.
            resp.raise_for_status()
            # .json(): 응답 본문을 JSON → 파이썬 딕셔너리로 변환
            data = resp.json()
            # "errorCode" in data: 딕셔너리에 해당 키가 있는지 확인하는 in 연산자
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


# 모듈 레벨 함수 (클래스 밖에 정의된 함수): 클래스에 속하지 않는 독립적인 함수.
# 유틸리티 성격의 데이터 변환 함수에 자주 사용.
def news_item_to_detail_dict(item: NewsItem) -> dict:
    """NewsItem 을 SQS 전달용 dict 로 변환. JobDetail 호환 형식."""
    # \n\n: 줄바꿈 2개. 제목과 설명 사이에 빈 줄을 넣는다.
    raw_text = f"{item.title}\n\n{item.description}"
    # 딕셔너리 리터럴을 그대로 반환
    return {
        "source": "naver_news",
        "external_id": make_external_id(item.url),
        "url": item.url,
        "company_name": item.company_name,
        "title": item.title,
        "raw_text": raw_text,
        "tech_stack": [],    # 빈 리스트: 뉴스에는 기술스택 정보가 없으므로
        "deadline": _deadline_from_pub_date(item.pub_date),
        "crawled_at": item.collected_at,
        "career_level": "",  # 빈 문자열: 뉴스에는 경력 수준이 없으므로
    }
