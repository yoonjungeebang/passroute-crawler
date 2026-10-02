"""collector/worknet.py 단위 테스트."""
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

from collector.worknet import (
    WorknetCollector,
    WorknetJobItem,
    _make_external_id,
    _make_url,
    _parse_deadline,
    worknet_item_to_detail_dict,
)


class TestParseDeadline:
    def test_normal_date(self):
        assert _parse_deadline("20260501") > 0

    def test_always_hiring(self):
        assert _parse_deadline("채용시까지") == 0

    def test_empty(self):
        assert _parse_deadline("") == 0


class TestMakeExternalId:
    def test_prefix(self):
        assert _make_external_id("K123456").startswith("worknet_")

    def test_includes_auth_no(self):
        assert "K123456" in _make_external_id("K123456")


class TestMakeUrl:
    def test_contains_auth_no(self):
        url = _make_url("K123456")
        assert "K123456" in url
        assert "work.go.kr" in url


class TestWorknetItemToDetailDict:
    def test_schema(self):
        item = WorknetJobItem(
            external_id="worknet_K123",
            company_name="테스트기업",
            title="백엔드 개발자",
            raw_text="직무내용",
            url="https://www.work.go.kr/...",
            deadline=0,
            career_level="경력무관",
            collected_at="2026-04-11T18:00:00+09:00",
        )
        d = worknet_item_to_detail_dict(item)

        assert d["source"] == "worknet"
        assert d["external_id"] == "worknet_K123"
        assert d["company_name"] == "테스트기업"
        assert d["raw_text"] == "직무내용"
        assert d["tech_stack"] == []
        assert d["deadline"] == 0
        assert "crawled_at" in d


class TestWorknetCollector:
    def test_collect_all_empty_api_key(self):
        """API 키가 비어있으면 빈 리스트 반환."""
        collector = WorknetCollector(api_key="")
        assert collector.collect_all() == []

    def test_parse_list_items(self):
        """목록 XML 파싱."""
        xml_str = """<root>
            <wanted>
                <wantedAuthNo>K123</wantedAuthNo>
                <company>테스트기업</company>
                <title>백엔드</title>
                <career>경력무관</career>
                <closeDt>20260501</closeDt>
            </wanted>
        </root>"""
        root = ET.fromstring(xml_str)
        collector = WorknetCollector(api_key="test-key")
        items = collector._parse_list_items(root)

        assert len(items) == 1
        assert items[0]["wanted_auth_no"] == "K123"
        assert items[0]["company"] == "테스트기업"
        assert items[0]["title"] == "백엔드"

    def test_parse_detail(self):
        """상세 XML 파싱."""
        xml_str = """<root>
            <wanted>
                <jobCont>백엔드 개발</jobCont>
                <prefCont>AWS 경험</prefCont>
                <etcHopeCont>재택근무 가능</etcHopeCont>
                <sal>3000만원</sal>
            </wanted>
        </root>"""
        root = ET.fromstring(xml_str)
        collector = WorknetCollector(api_key="test-key")
        list_item = {
            "wanted_auth_no": "K123",
            "company": "테스트기업",
            "title": "백엔드",
            "career": "경력무관",
            "deadline": "20260501",
        }
        item = collector._parse_detail(root, list_item)

        assert item is not None
        assert "백엔드 개발" in item.raw_text
        assert "AWS 경험" in item.raw_text
        assert item.company_name == "테스트기업"
