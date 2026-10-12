"""크롤러 추상 클래스 + 페이지 순회 공통 로직."""
import logging
import os
import random
import time

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_crawler_config
from core.circuit_breaker import CircuitOpenError, get_breaker

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobListingRef:
    """목록 페이지에서 추출한 공고 식별 정보."""
    source: str
    external_id: str
    url: str
    title: str
    company_name: str
    career_level: str = ""


@dataclass(frozen=True)
class JobDetail:
    """상세 페이지에서 추출한 JD 데이터. 구조화는 AI 서버에서 처리."""
    source: str
    external_id: str
    url: str
    company_name: str
    title: str
    raw_text: str
    tech_stack: tuple[str, ...]
    deadline: int
    crawled_at: str
    career_level: str = ""

    def __post_init__(self):
        for field_name in ("source", "external_id", "url", "company_name", "title"):
            value = getattr(self, field_name)
            if value is None or not str(value).strip():
                raise ValueError(
                    f"JobDetail.{field_name}이 비어있음: "
                    f"source={self.source}, id={self.external_id}"
                )

        if len(self.raw_text.strip()) < 20:
            raise ValueError(
                f"raw_text가 너무 짧음({len(self.raw_text.strip())}자): "
                f"source={self.source}, id={self.external_id}"
            )


# ── 설정 파일에서 로드 ──

_CRAWLER_CONFIG = load_crawler_config()
DEFAULT_DELAY_MIN: float = _CRAWLER_CONFIG["delay_min"]
DEFAULT_DELAY_MAX: float = _CRAWLER_CONFIG["delay_max"]
DEFAULT_MAX_PAGES: int = _CRAWLER_CONFIG["max_pages"]
DEFAULT_STALE_PAGE_THRESHOLD: int = _CRAWLER_CONFIG["stale_page_threshold"]


def make_crawler_session(*, extra_headers: dict | None = None) -> requests.Session:
    """표준 크롤러 세션 생성: 재시도 정책 + 공통 헤더."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "Accept": "application/json",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "User-Agent": (
            "passroute-bot/1.0 "
            "(+https://github.com/yoonjungeebang/passroute-crawler; yezanee@gmail.com)"
        ),
    })
    if extra_headers:
        session.headers.update(extra_headers)
    return session


class JobCrawler(ABC):

    source: ClassVar[str]
    base_url: ClassVar[str] = ""

    def __init__(
        self,
        *,
        delay_min: float | None = None,
        delay_max: float | None = None,
        max_pages: int | None = None,
        stale_page_threshold: int = DEFAULT_STALE_PAGE_THRESHOLD,
        exclude_title_keywords: tuple[str, ...] = (),
    ):
        prefix = self.source.upper()
        self.delay_min = delay_min if delay_min is not None else float(os.environ.get(f"{prefix}_DELAY_MIN", DEFAULT_DELAY_MIN))
        self.delay_max = delay_max if delay_max is not None else float(os.environ.get(f"{prefix}_DELAY_MAX", DEFAULT_DELAY_MAX))
        self.max_pages = max_pages if max_pages is not None else int(os.environ.get(f"{prefix}_MAX_PAGES", DEFAULT_MAX_PAGES))
        self.stale_page_threshold = stale_page_threshold
        self.exclude_title_keywords = exclude_title_keywords
        self._breaker = get_breaker(self.source)
        self.session = make_crawler_session()

    @abstractmethod
    def fetch_listings_page(self, page: int) -> list[JobListingRef]: ...

    @abstractmethod
    def fetch_detail(self, ref: JobListingRef) -> JobDetail | None: ...

    def collect_listings(self) -> list[JobListingRef]:
        """전체 목록 페이지를 순회해 공고를 모은다. stale 감지로 조기 종료."""
        all_refs: list[JobListingRef] = []
        seen_ids: set[str] = set()
        stale_pages = 0

        for page in range(1, self.max_pages + 1):
            try:
                with self._breaker:
                    refs = self.fetch_listings_page(page)
            except CircuitOpenError:
                logger.warning("서킷 OPEN, 목록 수집 조기 종료: source=%s", self.source)
                break
            except Exception:
                logger.exception("목록 요청 실패: source=%s page=%d", self.source, page)
                break

            if not refs:
                break

            added = 0
            for ref in refs:
                if ref.external_id in seen_ids:
                    continue
                if self._is_excluded_title(ref.title):
                    continue
                seen_ids.add(ref.external_id)
                all_refs.append(ref)
                added += 1

            logger.info(
                "source=%s page=%d: 조회 %d건, 신규 %d건 (누적 %d건)",
                self.source, page, len(refs), added, len(all_refs),
            )

            if added == 0:
                stale_pages += 1
                if stale_pages >= self.stale_page_threshold:
                    logger.info("source=%s: stale %d페이지 연속, 조기 종료", self.source, stale_pages)
                    break
            else:
                stale_pages = 0

            self._delay()

        return all_refs

    def _is_excluded_title(self, title: str) -> bool:
        """제목에 제외 키워드가 포함되어 있는지 확인."""
        if not self.exclude_title_keywords:
            return False
        lowered = title.lower()
        return any(kw.lower() in lowered for kw in self.exclude_title_keywords)

    def _delay(self) -> None:
        """크롤링 간 랜덤 대기."""
        time.sleep(random.uniform(self.delay_min, self.delay_max))

    @staticmethod
    def _parse_deadline(deadline_str: str) -> int:
        """ISO 8601 마감일 문자열을 Unix timestamp 로 변환. 파싱 실패 시 0(상시채용)."""
        if not deadline_str:
            return 0
        try:
            dt = datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _parse_career(data: dict) -> str:
        """minCareer/maxCareer 필드로 경력 구간 문자열을 생성한다."""
        min_career = data.get("minCareer", -1)
        max_career = data.get("maxCareer", -1)
        if min_career <= 0 and max_career <= 0:
            return "신입"
        if min_career <= 0:
            return f"신입~{max_career}년"
        return f"{min_career}~{max_career}년"
