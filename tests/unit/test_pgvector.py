"""PostgreSQL 스토리지 헬퍼 함수 단위 테스트."""
import sys
from unittest.mock import MagicMock

# psycopg2 가 테스트 환경에 없을 수 있으므로 stub 을 주입
if "psycopg2" not in sys.modules:
    sys.modules["psycopg2"] = MagicMock()
    sys.modules["psycopg2.extras"] = MagicMock()

from storage.postgres import _build_params, build_document
from crawler.base import JobDetail


def _detail(**overrides) -> JobDetail:
    base = dict(
        source="jobkorea",
        external_id="999",
        url="https://www.jobkorea.co.kr/Recruit/GI_Read/999",
        company_name="패스루트",
        title="백엔드 채용",
        raw_text="주요업무: 백엔드 서비스 개발 및 운영\n자격요건: Python 3년 이상",
        tech_stack=("Python", "AWS"),
        deadline=1777734399,
        crawled_at="2026-04-11T18:00:00+09:00",
    )
    base.update(overrides)
    return JobDetail(**base)


class TestBuildDocument:
    def test_includes_raw_text_and_tech_stack(self):
        doc = build_document(_detail())
        assert "주요업무: 백엔드 서비스 개발 및 운영" in doc
        assert "Python, AWS" in doc

    def test_empty_when_no_text(self):
        """raw_text 와 tech_stack 이 모두 비어있으면 빈 문자열 반환."""
        detail = MagicMock()
        detail.raw_text = ""
        detail.tech_stack = ()
        doc = build_document(detail)
        assert doc == ""


class TestBuildParams:
    def test_int_deadline_passthrough(self):
        params = _build_params(_detail(deadline=1714575599), "doc", None, "ok")
        assert params["deadline"] == 1714575599

    def test_non_int_deadline_returns_zero(self):
        params = _build_params(_detail(deadline="invalid"), "doc", None, "ok")
        assert params["deadline"] == 0
