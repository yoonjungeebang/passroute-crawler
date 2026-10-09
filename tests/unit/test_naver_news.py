"""네이버 뉴스 수집 모듈 단위 테스트.

API 호출은 mock 으로 대체하고, 노이즈 필터링·HTML 태그 제거·
pubDate 파싱·중복 제거·deadline 계산 등을 검증한다.
"""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from collector.naver_news import (
    NaverNewsCollector,
    NewsItem,
    _deadline_from_pub_date,
    _is_noise,
    _is_relevant,
    _parse_pub_date,
    news_item_to_detail_dict,
)
from parser.common import make_external_id
from parser.common import strip_html

KST = timezone(timedelta(hours=9))


# ── _strip_html ──


class TestStripHtml:
    def test_removes_bold_tags(self):
        assert strip_html("<b>카카오</b> AI 신사업") == "카카오 AI 신사업"

    def test_unescapes_html_entities(self):
        assert strip_html("A&amp;B &quot;test&quot;") == 'A&B "test"'

    def test_empty_string(self):
        assert strip_html("") == ""

    def test_no_tags(self):
        assert strip_html("순수 텍스트") == "순수 텍스트"


# ── _parse_pub_date ──


class TestParsePubDate:
    def test_rfc2822_format(self):
        dt = _parse_pub_date("Mon, 26 Sep 2016 07:50:00 +0900")
        assert dt.year == 2016
        assert dt.month == 9
        assert dt.day == 26

    def test_invalid_format_returns_now(self):
        dt = _parse_pub_date("invalid-date")
        assert dt.tzinfo is not None


# ── make_external_id ──


class TestMakeExternalId:
    def test_deterministic(self):
        url = "https://example.com/article/123"
        assert make_external_id(url) == make_external_id(url)

    def test_different_urls_differ(self):
        assert make_external_id("https://a.com") != make_external_id("https://b.com")

    def test_length(self):
        assert len(make_external_id("https://example.com")) == 16


# ── _is_noise ──


class TestIsNoise:
    @pytest.mark.parametrize("title", [
        "삼성전자 주가 급등",
        "카카오 인사 발령",
        "네이버 소송 결과",
        "토스 주주총회",
    ])
    def test_noise_detected(self, title):
        assert _is_noise(title) is True

    @pytest.mark.parametrize("title", [
        "카카오 AI 기반 추천 시스템 전면 개편",
        "네이버 클라우드 신규 서비스 출시",
        "토스 개발팀 기술 블로그 공개",
    ])
    def test_non_noise(self, title):
        assert _is_noise(title) is False


# ── _is_relevant ──


class TestIsRelevant:
    def test_company_in_title(self):
        assert _is_relevant("카카오", "카카오 AI 신사업 발표") is True

    def test_company_not_in_title(self):
        assert _is_relevant("카카오", "AI 신사업 발표") is False

    def test_company_substring_match(self):
        assert _is_relevant("카카오", "카카오뱅크 투자탭 출시") is True


# ── _deadline_from_pub_date ──


class TestDeadlineFromPubDate:
    def test_adds_90_days(self):
        pub_date = datetime(2026, 1, 1, 0, 0, 0, tzinfo=KST)
        expected = datetime(2026, 4, 1, 0, 0, 0, tzinfo=KST)
        assert _deadline_from_pub_date(pub_date) == int(expected.timestamp())


# ── news_item_to_detail_dict ──


class TestNewsItemToDetailDict:
    def test_fields(self):
        item = NewsItem(
            company_name="카카오",
            title="카카오 AI 신사업",
            description="카카오가 AI 기반 신사업을 발표했다.",
            url="https://example.com/news/1",
            pub_date=datetime(2026, 4, 1, 12, 0, 0, tzinfo=KST),
            collected_at="2026-04-01T12:00:00+09:00",
        )
        d = news_item_to_detail_dict(item)

        assert d["source"] == "naver_news"
        assert d["company_name"] == "카카오"
        assert d["title"] == "카카오 AI 신사업"
        assert "카카오 AI 신사업" in d["raw_text"]
        assert "카카오가 AI 기반 신사업을 발표했다." in d["raw_text"]
        assert d["tech_stack"] == []
        assert d["career_level"] == ""
        assert isinstance(d["deadline"], int)
        assert d["external_id"] == make_external_id(item.url)


# ── NaverNewsCollector ──


def _make_api_response(items: list[dict]) -> dict:
    return {"items": items}


def _make_raw_item(title: str, url: str, desc: str = "설명", pub_date: str = "Mon, 01 Apr 2026 12:00:00 +0900") -> dict:
    return {
        "title": title,
        "originallink": url,
        "link": f"https://n.news.naver.com/{url}",
        "description": desc,
        "pubDate": pub_date,
    }


class TestNaverNewsCollector:
    def _make_collector(self, **kwargs):
        defaults = dict(
            client_id="test_id",
            client_secret="test_secret",
            companies=("카카오",),
            search_suffixes=("기술",),
            api_delay=0,
        )
        defaults.update(kwargs)
        return NaverNewsCollector(**defaults)

    def test_collect_all_basic(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_api_response([
            _make_raw_item("<b>카카오</b> AI 신기술", "https://news.com/1"),
            _make_raw_item("카카오 클라우드 확장", "https://news.com/2"),
        ])
        mock_resp.raise_for_status = MagicMock()

        collector = self._make_collector()
        collector.session.get = MagicMock(return_value=mock_resp)
        items = collector.collect_all()

        assert len(items) == 2
        assert items[0].title == "카카오 AI 신기술"  # HTML 태그 제거됨
        assert items[0].company_name == "카카오"

    def test_noise_filtered(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_api_response([
            _make_raw_item("카카오 주가 급등", "https://news.com/1"),
            _make_raw_item("카카오 AI 출시", "https://news.com/2"),
        ])
        mock_resp.raise_for_status = MagicMock()

        collector = self._make_collector()
        collector.session.get = MagicMock(return_value=mock_resp)
        items = collector.collect_all()

        assert len(items) == 1
        assert items[0].title == "카카오 AI 출시"

    def test_url_deduplication_across_suffixes(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_api_response([
            _make_raw_item("카카오 AI 기술", "https://news.com/same"),
        ])
        mock_resp.raise_for_status = MagicMock()

        collector = self._make_collector(search_suffixes=("기술", "AI"))
        collector.session.get = MagicMock(return_value=mock_resp)
        items = collector.collect_all()

        assert len(items) == 1

    def test_url_deduplication_across_companies(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_api_response([
            _make_raw_item("카카오 네이버 공통 뉴스", "https://news.com/shared"),
        ])
        mock_resp.raise_for_status = MagicMock()

        collector = self._make_collector(companies=("카카오", "네이버"))
        collector.session.get = MagicMock(return_value=mock_resp)
        items = collector.collect_all()

        assert len(items) == 1

    def test_api_error_continues(self):
        """API 호출 실패 시 해당 기업은 건너뛰고 계속 진행."""
        collector = self._make_collector()
        collector.session.get = MagicMock(side_effect=Exception("API error"))
        items = collector.collect_all()

        assert items == []

    def test_api_headers_in_session(self):
        """세션에 인증 헤더가 설정되어 있는지 검증."""
        collector = self._make_collector()

        assert collector.session.headers["X-NCP-APIGW-API-KEY-ID"] == "test_id"
        assert collector.session.headers["X-NCP-APIGW-API-KEY"] == "test_secret"


