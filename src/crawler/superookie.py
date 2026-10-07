"""슈퍼루키(Superookie) 크롤러. SUPEROOKIE_* 환경변수로 오버라이드 가능.

슈퍼루키는 공개 REST API 를 제공하며, Secrets Manager 에 저장된 토큰으로 인증한다.
"""
import logging
import os
import re
from datetime import datetime
from typing import ClassVar

from core import KST
from crawler.validation import CrawlValidationError, require_keys
from crawler.base import (
    DEFAULT_DELAY_MAX,
    DEFAULT_DELAY_MIN,
    DEFAULT_MAX_PAGES,
    JobCrawler,
    JobDetail,
    JobListingRef,
    make_crawler_session,
)

logger = logging.getLogger(__name__)

_API_BASE = "https://www.superookie.com/api"


def _get_access_token() -> str:
    """Secrets Manager 에서 슈퍼루키 API 액세스 토큰을 가져온다."""
    from core.secrets import get_superookie_access_token  # noqa: C0415
    return get_superookie_access_token()


class SuperookieCrawler(JobCrawler):
    source: ClassVar[str] = "superookie"
    base_url: ClassVar[str] = "https://www.superookie.com"

    def __init__(self, **kwargs):
        max_pages = int(os.environ.get("SUPEROOKIE_MAX_PAGES", DEFAULT_MAX_PAGES))
        delay_min = float(os.environ.get("SUPEROOKIE_DELAY_MIN", DEFAULT_DELAY_MIN))
        delay_max = float(os.environ.get("SUPEROOKIE_DELAY_MAX", DEFAULT_DELAY_MAX))
        super().__init__(max_pages=max_pages, delay_min=delay_min, delay_max=delay_max, **kwargs)

        self.session = make_crawler_session()

    def fetch_listings_page(self, page: int) -> list[JobListingRef]:
        """슈퍼루키 채용공고 목록 API 호출."""
        params = {
            "page": page,
            "per_page": 20,
        }
        headers = {"Authorization": f"Bearer {_get_access_token()}"}
        try:
            resp = self.session.get(f"{_API_BASE}/jobs", params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("슈퍼루키 목록 요청 실패: page=%d", page)
            return []

        jobs = data.get("data") or []
        refs = []
        for job in jobs:
            job_id = str(job.get("_id", job.get("id", "")))
            if not job_id:
                continue

            company_name = job.get("team", "")
            if not company_name:
                company_obj = job.get("company", {})
                if isinstance(company_obj, dict):
                    company_name = company_obj.get("name", "")

            refs.append(JobListingRef(
                source=self.source,
                external_id=f"superookie_{job_id}",
                url=f"{self.base_url}/jobs/{job_id}",
                title=job.get("job_title", ""),
                company_name=company_name,
                career_level=self._parse_career_level(job),
            ))
        return refs

    def fetch_detail(self, ref: JobListingRef) -> JobDetail | None:
        """슈퍼루키 채용공고 상세 — 목록 API 에 상세 정보가 포함되어 있으므로 재호출."""
        job_id = ref.external_id.replace("superookie_", "")
        headers = {"Authorization": f"Bearer {_get_access_token()}"}
        try:
            resp = self.session.get(f"{_API_BASE}/jobs/{job_id}", headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("슈퍼루키 상세 요청 실패: id=%s", job_id)
            return None

        ctx = f"superookie detail {job_id}"
        require_keys(data, ["data"], context=ctx)
        job = data.get("data") or data
        require_keys(job, ["job_title"], context=ctx)

        parts = [job.get("job_title") or ref.title]

        custom_field = job.get("custom_field", "")
        if custom_field:
            cleaned = re.sub(r"<[^>]+>", " ", custom_field).strip()
            cleaned = re.sub(r"\s+", " ", cleaned)
            if cleaned:
                parts.append(cleaned)

        steps = job.get("steps", [])
        if steps:
            step_text = " → ".join(s.get("name", str(s)) if isinstance(s, dict) else str(s) for s in steps)
            parts.append(f"[채용절차] {step_text}")

        raw_text = "\n\n".join(parts)
        if not raw_text.strip() or raw_text.strip() == ref.title:
            return None

        deadline = 0
        if not job.get("is_until_recruit", False):
            end_at = job.get("end_at", "") or job.get("close_at", "") or job.get("deadline_at", "")
            if end_at:
                deadline = self._parse_deadline(end_at)

        return JobDetail(
            source=self.source,
            external_id=ref.external_id,
            url=ref.url,
            company_name=ref.company_name,
            title=ref.title,
            raw_text=raw_text,
            tech_stack=(),
            deadline=deadline,
            crawled_at=datetime.now(KST).isoformat(),
            career_level=ref.career_level,
        )

    @staticmethod
    def _parse_career_level(job: dict) -> str:
        level_id = job.get("job_level_id")
        if level_id is None:
            return ""
        return str(level_id)

    @staticmethod
    def _parse_deadline(date_str: str) -> int:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except (ValueError, TypeError):
            return 0
