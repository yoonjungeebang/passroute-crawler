"""랠릿(Rallit) 크롤러."""
import logging
from datetime import datetime
from typing import ClassVar

from core import KST
from crawler.base import JobCrawler, JobDetail, JobListingRef
from crawler.validation import CrawlValidationError, require_keys, require_non_empty
from parser.common import normalize_tech_name

logger = logging.getLogger(__name__)

_API_BASE = "https://api.rallit.com"


class RallitCrawler(JobCrawler):
    source: ClassVar[str] = "rallit"
    base_url: ClassVar[str] = "https://www.rallit.com"

    def fetch_listings_page(self, page: int) -> list[JobListingRef]:
        """랠릿 채용공고 목록 API 호출."""
        params = {"page": page, "size": 20, "sort": "LATEST"}
        try:
            resp = self.session.get(f"{_API_BASE}/api/jobs", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("랠릿 목록 요청 실패: page=%d", page)
            return []

        jobs = data.get("data") or data.get("content") or []
        if isinstance(jobs, dict):
            jobs = jobs.get("content") or []

        refs = []
        for job in jobs:
            job_id = str(job.get("id", ""))
            if not job_id:
                continue
            company = job.get("company") or {}
            refs.append(JobListingRef(
                source=self.source,
                external_id=f"rallit_{job_id}",
                url=f"{self.base_url}/positions/{job_id}",
                title=job.get("title") or "",
                company_name=company.get("name", "") if isinstance(company, dict) else str(company),
                career_level=self._parse_career(job),
            ))
        return refs

    def fetch_detail(self, ref: JobListingRef) -> JobDetail | None:
        """랠릿 채용공고 상세 API 호출."""
        job_id = ref.external_id.replace("rallit_", "")
        try:
            resp = self.session.get(f"{_API_BASE}/api/jobs/{job_id}", timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("랠릿 상세 요청 실패: id=%s", job_id)
            return None

        ctx = f"rallit detail {job_id}"
        require_keys(data, ["data"], context=ctx)
        job = data.get("data") or data
        require_keys(job, ["title"], context=ctx)
        require_non_empty(job.get("title"), "title", context=ctx)

        parts = [job.get("title") or ref.title]

        description = job.get("description", "")
        if description:
            parts.append(description)

        qualifications = job.get("qualifications", "")
        if qualifications:
            parts.append(f"[자격요건]\n{qualifications}")

        preferred = job.get("preferredQualifications", "")
        if preferred:
            parts.append(f"[우대사항]\n{preferred}")

        tech_stacks = job.get("skills") or job.get("techStacks") or []
        normalized = tuple(normalize_tech_name(t.get("name", t) if isinstance(t, dict) else t) for t in tech_stacks if t)

        deadline_str = job.get("closedAt", job.get("endAt", ""))
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

