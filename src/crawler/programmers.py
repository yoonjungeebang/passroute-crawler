"""프로그래머스(Programmers) 크롤러. PROGRAMMERS_* 환경변수로 오버라이드 가능."""
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

_API_BASE = "https://career.programmers.co.kr/api"


class ProgrammersCrawler(JobCrawler):
    source: ClassVar[str] = "programmers"
    base_url: ClassVar[str] = "https://career.programmers.co.kr"

    def __init__(self, **kwargs):
        max_pages = int(os.environ.get("PROGRAMMERS_MAX_PAGES", DEFAULT_MAX_PAGES))
        delay_min = float(os.environ.get("PROGRAMMERS_DELAY_MIN", DEFAULT_DELAY_MIN))
        delay_max = float(os.environ.get("PROGRAMMERS_DELAY_MAX", DEFAULT_DELAY_MAX))
        super().__init__(max_pages=max_pages, delay_min=delay_min, delay_max=delay_max, **kwargs)

        self.session = make_crawler_session()

    def fetch_listings_page(self, page: int) -> list[JobListingRef]:
        """프로그래머스 채용공고 목록 API 호출."""
        params = {"page": page, "order": "recent"}
        try:
            resp = self.session.get(f"{_API_BASE}/job_positions", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("프로그래머스 목록 요청 실패: page=%d", page)
            return []

        job_positions = data.get("jobPositions") or []
        refs = []
        for job in job_positions:
            job_id = str(job.get("id", ""))
            if not job_id:
                continue
            company = job.get("company") or {}
            refs.append(JobListingRef(
                source=self.source,
                external_id=f"programmers_{job_id}",
                url=f"{self.base_url}/job_positions/{job_id}",
                title=job.get("title") or "",
                company_name=company.get("name") or "",
                career_level=self._parse_career(job),
            ))
        return refs

    def fetch_detail(self, ref: JobListingRef) -> JobDetail | None:
        """프로그래머스 채용공고 상세 API 호출."""
        job_id = ref.external_id.replace("programmers_", "")
        try:
            resp = self.session.get(f"{_API_BASE}/job_positions/{job_id}", timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("프로그래머스 상세 요청 실패: id=%s", job_id)
            return None

        ctx = f"programmers detail {job_id}"
        require_keys(data, ["jobPosition"], context=ctx)
        job = data.get("jobPosition") or data
        require_keys(job, ["title"], context=ctx)
        require_non_empty(job.get("title"), "title", context=ctx)

        parts = [job.get("title") or ref.title]

        description = job.get("description", "")
        if description:
            parts.append(description)

        requirements = job.get("requirement", "")
        if requirements:
            parts.append(f"[자격요건]\n{requirements}")

        preferred = job.get("preferredExperience", "")
        if preferred:
            parts.append(f"[우대사항]\n{preferred}")

        tech_stacks = job.get("technicalTags") or []
        normalized = tuple(normalize_tech_name(t.get("name", t) if isinstance(t, dict) else t) for t in tech_stacks if t)

        deadline_str = job.get("endAt", "")
        deadline = self._parse_deadline(deadline_str)

        company = job.get("company") or {}

        return JobDetail(
            source=self.source,
            external_id=ref.external_id,
            url=ref.url,
            company_name=company.get("name") or ref.company_name if isinstance(company, dict) else ref.company_name,
            title=job.get("title") or ref.title,
            raw_text="\n\n".join(parts),
            tech_stack=normalized,
            deadline=deadline,
            crawled_at=datetime.now(KST).isoformat(),
            career_level=ref.career_level,
        )

    @staticmethod
    def _parse_career(job: dict) -> str:
        min_career = job.get("minCareer", -1)
        max_career = job.get("maxCareer", -1)
        if min_career <= 0 and max_career <= 0:
            return "신입"
        if min_career <= 0:
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
