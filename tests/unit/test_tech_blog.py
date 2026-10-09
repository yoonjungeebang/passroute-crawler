"""tech_blog 수집 모듈 단위 테스트."""
from unittest.mock import MagicMock

from collector.tech_blog import (
    _extract_rss_content,
    find_rss_feed_url,
)
from parser.common import make_external_id
from parser.common import strip_html


# ─────────────────────────────────────────────────────────
# 헬퍼 함수 테스트
# ─────────────────────────────────────────────────────────


class TestStripHtml:
    def test_removes_tags(self):
        assert strip_html("<p>hello <b>world</b></p>") == "hello world"

    def test_unescapes_entities(self):
        assert strip_html("A &amp; B &lt;C&gt;") == "A & B <C>"

    def test_empty_string(self):
        assert strip_html("") == ""


class TestMakeExternalId:
    def test_deterministic(self):
        url = "https://tech.kakao.com/post/123"
        assert make_external_id(url) == make_external_id(url)

    def test_length_16(self):
        assert len(make_external_id("https://example.com")) == 16

    def test_different_urls_differ(self):
        assert make_external_id("https://a.com") != make_external_id("https://b.com")


class TestExtractRssContent:
    def test_prefers_content_encoded(self):
        """content:encoded 가 있으면 summary 보다 우선."""
        entry = MagicMock()
        entry.content = [{"value": "<p>" + "본문 전체 내용입니다. " * 20 + "</p>"}]
        entry.get = lambda k, d="": "잘린 요약…" if k == "summary" else d
        result = _extract_rss_content(entry)
        assert "본문 전체 내용입니다" in result

    def test_falls_back_to_summary(self):
        """content:encoded 없으면 summary 사용."""
        entry = MagicMock(spec=[])
        entry.get = lambda k, d="": "충분한 길이의 요약 텍스트. " * 10 if k == "summary" else d
        result = _extract_rss_content(entry)
        assert "충분한 길이의 요약 텍스트" in result

    def test_empty_when_nothing(self):
        """아무 콘텐츠도 없으면 빈 문자열."""
        entry = MagicMock(spec=[])
        entry.get = lambda k, d="": d
        result = _extract_rss_content(entry)
        assert result == ""


class TestFindRssFeedUrl:
    def test_known_domain(self):
        url = find_rss_feed_url("https://tech.kakao.com/post/123")
        assert url == "https://tech.kakao.com/feed"

    def test_unknown_domain(self):
        url = find_rss_feed_url("https://unknown-blog.example.com/post")
        assert url is None

    def test_medium_feed(self):
        url = find_rss_feed_url("https://medium.com/daangn/some-post")
        assert url == "https://medium.com/feed/daangn"
