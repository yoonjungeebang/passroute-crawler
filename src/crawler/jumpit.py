"""점핏(Jumpit) 크롤러. JUMPIT_* 환경변수로 오버라이드 가능."""
import logging
import os
from datetime import datetime
from typing import ClassVar

from core import KST
from crawler.base import (
    DEFAULT_DELAY_MAX,
    DEFAULT_DELAY_MIN,
    DEFAULT_MAX_PAGES,
    JobCrawler,
    JobDetail,
    JobListingRef,
    make_crawler_session,
)
from crawler.validation import CrawlValidationError, require_keys, require_non_empty
from parser.common import normalize_tech_name

logger = logging.getLogger(__name__)

_API_BASE = "https://api.jumpit.co.kr/api"


class JumpitCrawler(JobCrawler):
    source: ClassVar[str] = "jumpit"
    base_url: ClassVar[str] = "https://www.jumpit.co.kr"

    def __init__(self, **kwargs):
        max_pages = int(os.environ.get("JUMPIT_MAX_PAGES", DEFAULT_MAX_PAGES))
        delay_min = float(os.environ.get("JUMPIT_DELAY_MIN", DEFAULT_DELAY_MIN))
        delay_max = float(os.environ.get("JUMPIT_DELAY_MAX", DEFAULT_DELAY_MAX))
        super().__init__(max_pages=max_pages, delay_min=delay_min, delay_max=delay_max, **kwargs)

        self.session = make_crawler_session()

    def fetch_listings_page(self, page: int) -> list[JobListingRef]:
        """점핏 채용공고 목록 API 호출."""
        params = {"page": page, "sort": "rsp_rate"}
        try:
            resp = self.session.get(f"{_API_BASE}/positions", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("점핏 목록 요청 실패: page=%d", page)
            return []

        result = data.get("result") or {}
        positions = result.get("positions") or []

        refs = []
        for pos in positions:
            pos_id = str(pos.get("id", ""))
            if not pos_id:
                continue
            refs.append(JobListingRef(
                source=self.source,
                external_id=f"jumpit_{pos_id}",
                url=f"{self.base_url}/position/{pos_id}",
                title=pos.get("title", ""),
                company_name=pos.get("companyName", ""),
                career_level=self._parse_career(pos),
            ))
        return refs

    def fetch_detail(self, ref: JobListingRef) -> JobDetail | None:
        """점핏 채용공고 상세 API 호출."""
        pos_id = ref.external_id.replace("jumpit_", "")
        try:
            resp = self.session.get(f"{_API_BASE}/position/{pos_id}", timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("점핏 상세 요청 실패: id=%s", pos_id)
            return None

        ctx = f"jumpit detail {pos_id}"
        require_keys(data, ["result"], context=ctx)
        result = data.get("result") or {}
        require_keys(result, ["title", "companyName"], context=ctx)
        require_non_empty(result.get("title"), "title", context=ctx)

        parts = [result.get("title") or ref.title]

        qualifications = result.get("qualifications", "")
        if qualifications:
            parts.append(f"[자격요건]\n{qualifications}")

        preferred = result.get("preferredQualifications", "")
        if preferred:
            parts.append(f"[우대사항]\n{preferred}")

        responsibilities = result.get("responsibilities", "")
        if responsibilities:
            parts.append(f"[주요업무]\n{responsibilities}")

        tech_stacks = result.get("techStacks") or []
        normalized = tuple(normalize_tech_name(t) for t in tech_stacks if t)

        deadline_str = result.get("closedAt", "")
        deadline = self._parse_deadline(deadline_str)

        return JobDetail(
            source=self.source,
            external_id=ref.external_id,
            url=ref.url,
            company_name=result.get("companyName") or ref.company_name,
            title=result.get("title") or ref.title,
            raw_text="\n\n".join(parts),
            tech_stack=normalized,
            deadline=deadline,
            crawled_at=datetime.now(KST).isoformat(),
            career_level=ref.career_level,
        )

    @staticmethod
    def _parse_career(pos: dict) -> str:
        min_career = pos.get("minCareer", 0)
        max_career = pos.get("maxCareer", 0)
        if min_career == 0 and max_career == 0:
            return "신입"
        if min_career == 0:
            return f"신입~{max_career}년"
        return f"{min_career}~{max_career}년"

    @staticmethod
    def _parse_deadline(deadline_str: str) -> str:
        if not deadline_str:
            return ""
        try:
            dt = datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
            return str(int(dt.timestamp()))
        except (ValueError, TypeError):
            return ""
