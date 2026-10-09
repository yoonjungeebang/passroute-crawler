"""점핏(Jumpit) 크롤러."""
import logging
from datetime import datetime
from typing import ClassVar

from core import KST
from crawler.base import JobCrawler, JobDetail, JobListingRef
from crawler.validation import CrawlValidationError, require_keys, require_non_empty
from parser.common import normalize_tech_name

logger = logging.getLogger(__name__)

_API_BASE = "https://api.jumpit.co.kr/api"


# JumpitCrawler(JobCrawler): JobCrawler 추상 클래스를 상속받는다.
# JobCrawler의 모든 기능(세션, 딜레이, collect_listings 등)을 물려받고,
# 반드시 구현해야 하는 fetch_listings_page와 fetch_detail을 여기서 구현한다.
class JumpitCrawler(JobCrawler):
    # ClassVar: 이 값은 모든 JumpitCrawler 인스턴스가 공유하는 클래스 변수.
    source: ClassVar[str] = "jumpit"
    base_url: ClassVar[str] = "https://www.jumpit.co.kr"

    def fetch_listings_page(self, page: int) -> list[JobListingRef]:
        """점핏 채용공고 목록 API 호출. 부모 클래스의 @abstractmethod 를 구현한 것."""
        params = {"page": page, "sort": "rsp_rate"}
        try:
            # self.session: 부모 클래스(JobCrawler)의 __init__에서 만든 requests.Session
            resp = self.session.get(f"{_API_BASE}/positions", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("점핏 목록 요청 실패: page=%d", page)
            return []  # 빈 리스트 반환 = 이 페이지 실패

        # 중첩 딕셔너리 안전하게 접근: .get() 체이닝
        # data["result"]["positions"] 처럼 하면 키가 없을 때 KeyError가 나지만,
        # .get()을 쓰면 None을 반환하고 or {} / or [] 로 기본값을 지정.
        result = data.get("result") or {}
        positions = result.get("positions") or []

        refs = []
        for pos in positions:
            # str(): 어떤 값이든 문자열로 변환. int 123 → "123"
            pos_id = str(pos.get("id", ""))
            if not pos_id:
                continue
            # JobListingRef(): @dataclass 이므로 필드 이름=값 형태로 생성
            refs.append(JobListingRef(
                source=self.source,
                external_id=f"jumpit_{pos_id}",
                url=f"{self.base_url}/position/{pos_id}",
                title=pos.get("title", ""),
                company_name=pos.get("companyName", ""),
                # self._parse_career(): 부모 클래스(JobCrawler)에 정의된 @staticmethod
                career_level=self._parse_career(pos),
            ))
        return refs

    def fetch_detail(self, ref: JobListingRef) -> JobDetail | None:
        """점핏 채용공고 상세 API 호출."""
        # .replace("접두사", ""): 문자열에서 특정 부분을 치환.
        # "jumpit_12345".replace("jumpit_", "") → "12345"
        pos_id = ref.external_id.replace("jumpit_", "")
        try:
            resp = self.session.get(f"{_API_BASE}/position/{pos_id}", timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            logger.exception("점핏 상세 요청 실패: id=%s", pos_id)
            return None  # None 반환 = 이 공고 상세 조회 실패

        ctx = f"jumpit detail {pos_id}"
        require_keys(data, ["result"], context=ctx)      # 필수 키 존재 확인
        result = data.get("result") or {}
        require_keys(result, ["title", "companyName"], context=ctx)
        require_non_empty(result.get("title"), "title", context=ctx)  # 빈 값 확인

        # 리스트에 텍스트 조각들을 모은 뒤 나중에 join으로 합친다.
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

        # 기술스택 정규화
        tech_stacks = result.get("techStacks") or []
        # tuple(): 제너레이터 표현식의 결과를 튜플로 변환.
        # (표현식 for 변수 in 반복 if 조건): 조건을 만족하는 요소만 변환해서 모은다.
        normalized = tuple(normalize_tech_name(t) for t in tech_stacks if t)

        deadline_str = result.get("closedAt", "")
        deadline = self._parse_deadline(deadline_str)

        return JobDetail(
            source=self.source,
            external_id=ref.external_id,
            url=ref.url,
            company_name=result.get("companyName") or ref.company_name,
            title=result.get("title") or ref.title,
            # "\n\n".join(리스트): 리스트의 각 요소 사이에 "\n\n"(빈 줄)을 넣어 하나의 문자열로 합친다.
            raw_text="\n\n".join(parts),
            tech_stack=normalized,
            deadline=deadline,
            # datetime.now(KST): 현재 시각을 한국시간으로 가져옴
            # .isoformat(): "2024-01-15T14:30:00+09:00" 형식의 문자열로 변환
            crawled_at=datetime.now(KST).isoformat(),
            career_level=ref.career_level,
        )

