"""워크넷 공공데이터 API 연동 모듈.

data.go.kr 워크넷 채용정보 API 를 사용하여 IT 직종 채용공고를 수집한다.
Tier 1 소스: 공공데이터법에 따라 제공 의무가 있으므로 차단 불가.
"""
import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime

import defusedxml.ElementTree as ET
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core import KST

logger = logging.getLogger(__name__)

# 워크넷 채용정보 API 엔드포인트
WORKNET_API_URL = "https://openapi.work.go.kr/opi/opi/opia/wantedApi.do"

# IT/소프트웨어 관련 직종 코드 (워크넷 직종분류)
_IT_OCCUPATION_CODES = (
    "024",  # 정보통신 관련직
)

_DEFAULT_DISPLAY = 100
_DEFAULT_MAX_PAGES = 10
_DEFAULT_API_DELAY = 0.3
JOB_RETENTION_DAYS = 0  # 마감일 기반 삭제이므로 별도 retention 불필요


@dataclass(frozen=True)
class WorknetJobItem:
    """워크넷 채용공고 1건."""
    external_id: str
    company_name: str
    title: str
    raw_text: str
    url: str
    deadline: int  # Unix timestamp (0 = 상시채용)
    career_level: str
    collected_at: str


def _parse_deadline(deadline_str: str) -> int:
    """워크넷 마감일 문자열 → Unix timestamp. '채용시까지' 등은 0."""
    if not deadline_str or deadline_str in ("채용시까지", "상시채용", "상시", ""):
        return 0
    try:
        dt = datetime.strptime(deadline_str, "%Y%m%d")
        dt = dt.replace(hour=23, minute=59, second=59, tzinfo=KST)
        return int(dt.timestamp())
    except ValueError:
        return 0


def _make_external_id(wanted_auth_no: str) -> str:
    """워크넷 공고번호 → external_id."""
    return f"worknet_{wanted_auth_no}"


def _make_url(wanted_auth_no: str) -> str:
    """워크넷 공고 상세 URL 생성."""
    return f"https://www.work.go.kr/empInfo/empInfoSrch/detail/empDetailAuthView.do?wantedAuthNo={wanted_auth_no}"


class WorknetCollector:
    """워크넷 공공데이터 API 를 사용해 IT 채용공고를 수집한다."""

    def __init__(
        self,
        api_key: str,
        *,
        display: int = _DEFAULT_DISPLAY,
        max_pages: int = _DEFAULT_MAX_PAGES,
        api_delay: float = _DEFAULT_API_DELAY,
    ):
        self.api_key = api_key
        self.display = display
        self.max_pages = max_pages
        self.api_delay = api_delay

        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def _call_list_api(self, page: int, occupation: str = "024") -> ET.Element | None:
        """채용정보 목록 조회 API 호출."""
        params = {
            "authKey": self.api_key,
            "callTp": "L",
            "returnType": "XML",
            "startPage": page,
            "display": self.display,
            "occupation": occupation,
        }
        try:
            resp = self.session.get(WORKNET_API_URL, params=params, timeout=15)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception:
            logger.exception("워크넷 목록 API 호출 실패: page=%d", page)
            return None

    def _call_detail_api(self, wanted_auth_no: str) -> ET.Element | None:
        """채용정보 상세 조회 API 호출."""
        params = {
            "authKey": self.api_key,
            "callTp": "D",
            "returnType": "XML",
            "wantedAuthNo": wanted_auth_no,
        }
        try:
            resp = self.session.get(WORKNET_API_URL, params=params, timeout=15)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception:
            logger.exception("워크넷 상세 API 호출 실패: wantedAuthNo=%s", wanted_auth_no)
            return None

    def _parse_list_items(self, root: ET.Element) -> list[dict]:
        """목록 응답 XML 에서 공고 정보 추출."""
        items = []
        for wanted in root.iter("wanted"):
            auth_no = self._text(wanted, "wantedAuthNo")
            if not auth_no:
                continue
            items.append({
                "wanted_auth_no": auth_no,
                "company": self._text(wanted, "company"),
                "title": self._text(wanted, "title"),
                "career": self._text(wanted, "career"),
                "deadline": self._text(wanted, "closeDt"),
            })
        return items

    def _parse_detail(self, root: ET.Element, list_item: dict) -> WorknetJobItem | None:
        """상세 응답 XML 에서 텍스트 추출 → WorknetJobItem 생성."""
        wanted = root.find(".//wanted")
        if wanted is None:
            return None

        job_cont = self._text(wanted, "jobCont")
        pref_cont = self._text(wanted, "prefCont")
        etc_cont = self._text(wanted, "etcHopeCont")
        sal = self._text(wanted, "sal")

        parts = [list_item["title"]]
        if job_cont:
            parts.append(f"[직무내용]\n{job_cont}")
        if pref_cont:
            parts.append(f"[우대사항]\n{pref_cont}")
        if etc_cont:
            parts.append(f"[기타]\n{etc_cont}")
        if sal:
            parts.append(f"[급여]\n{sal}")

        raw_text = "\n\n".join(parts)

        return WorknetJobItem(
            external_id=_make_external_id(list_item["wanted_auth_no"]),
            company_name=list_item["company"],
            title=list_item["title"],
            raw_text=raw_text,
            url=_make_url(list_item["wanted_auth_no"]),
            deadline=_parse_deadline(list_item["deadline"]),
            career_level=list_item.get("career", ""),
            collected_at=datetime.now(KST).isoformat(),
        )

    def collect_all(self) -> list[WorknetJobItem]:
        """전체 IT 채용공고를 수집한다. 목록 → 상세 순차 호출."""
        if not self.api_key:
            logger.warning("워크넷 API 키 미설정, 수집 스킵")
            return []

        all_items: list[WorknetJobItem] = []
        seen_ids: set[str] = set()

        for occupation in _IT_OCCUPATION_CODES:
            for page in range(1, self.max_pages + 1):
                root = self._call_list_api(page, occupation)
                if root is None:
                    break

                list_items = self._parse_list_items(root)
                if not list_items:
                    break

                for li in list_items:
                    ext_id = _make_external_id(li["wanted_auth_no"])
                    if ext_id in seen_ids:
                        continue
                    seen_ids.add(ext_id)

                    time.sleep(self.api_delay)
                    detail_root = self._call_detail_api(li["wanted_auth_no"])
                    if detail_root is None:
                        continue

                    item = self._parse_detail(detail_root, li)
                    if item:
                        all_items.append(item)

                logger.info(
                    "워크넷 occupation=%s page=%d: 목록 %d건 (누적 %d건)",
                    occupation, page, len(list_items), len(all_items),
                )
                time.sleep(self.api_delay)

        logger.info("워크넷 수집 완료: %d건", len(all_items))
        return all_items

    @staticmethod
    def _text(elem: ET.Element, tag: str) -> str:
        child = elem.find(tag)
        return (child.text or "").strip() if child is not None else ""


def worknet_item_to_detail_dict(item: WorknetJobItem) -> dict:
    """WorknetJobItem 을 S3 저장용 dict 로 변환. JobDetail 호환 형식."""
    return {
        "source": "worknet",
        "external_id": item.external_id,
        "url": item.url,
        "company_name": item.company_name,
        "title": item.title,
        "raw_text": item.raw_text,
        "tech_stack": [],
        "deadline": item.deadline,
        "crawled_at": item.collected_at,
        "career_level": item.career_level,
    }
