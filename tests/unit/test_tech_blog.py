"""tech_blog 수집 모듈 및 blog_collector Lambda 핸들러 단위 테스트."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from collector.tech_blog import (
    BLOG_RETENTION_DAYS,
    BlogArticle,
    BlogFeed,
    TechBlogCollector,
    _classify_job_categories,
    _deadline_from_pub_date,
    _fetch_page_content,
    _is_truncated,
    _make_external_id,
    _parse_devocean_date,
    blog_article_to_detail_dict,
)
from parser.common import strip_html

KST = timezone(timedelta(hours=9))


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
        assert _make_external_id(url) == _make_external_id(url)

    def test_length_16(self):
        assert len(_make_external_id("https://example.com")) == 16

    def test_different_urls_differ(self):
        assert _make_external_id("https://a.com") != _make_external_id("https://b.com")


class TestClassifyJobCategories:
    def test_backend_keywords(self):
        cats = _classify_job_categories("Spring Boot 기반 MSA 전환기")
        assert "백엔드개발자" in cats

    def test_frontend_keywords(self):
        cats = _classify_job_categories("React 컴포넌트 디자인 시스템 구축")
        assert "프론트엔드개발자" in cats

    def test_ai_ml_keywords(self):
        cats = _classify_job_categories("LLM 기반 RAG 파이프라인 구축")
        assert "AI/ML엔지니어" in cats

    def test_multiple_categories(self):
        cats = _classify_job_categories("Kubernetes 위에서 ML 모델 서빙하기")
        assert "클라우드엔지니어" in cats
        assert "MLOps엔지니어" in cats

    def test_no_match(self):
        cats = _classify_job_categories("회사 워크숍 후기")
        assert cats == []


class TestDeadlineFromPubDate:
    def test_adds_retention_days(self):
        pub = datetime(2026, 1, 1, tzinfo=timezone.utc)
        expected = pub + timedelta(days=BLOG_RETENTION_DAYS)
        assert _deadline_from_pub_date(pub) == int(expected.timestamp())


# ─────────────────────────────────────────────────────────
# blog_article_to_detail_dict 변환 테스트
# ─────────────────────────────────────────────────────────


def _article(**overrides) -> BlogArticle:
    base = dict(
        company_name="카카오",
        title="Kafka 파티션 전략",
        summary="대규모 트래픽 환경에서의 Kafka 파티션 최적화 사례를 공유합니다.",
        url="https://tech.kakao.com/post/123",
        pub_date=datetime(2026, 3, 15, 10, 0, tzinfo=KST),
        collected_at="2026-05-02T10:00:00+09:00",
    )
    base.update(overrides)
    return BlogArticle(**base)


class TestBlogArticleToDetailDict:
    def test_source_is_tech_blog(self):
        data = blog_article_to_detail_dict(_article())
        assert data["source"] == "tech_blog"

    def test_raw_text_combines_title_and_summary(self):
        data = blog_article_to_detail_dict(_article())
        assert data["raw_text"].startswith("Kafka 파티션 전략")
        assert "대규모 트래픽" in data["raw_text"]

    def test_raw_text_title_only_when_no_summary(self):
        data = blog_article_to_detail_dict(_article(summary=""))
        assert data["raw_text"].startswith("Kafka 파티션 전략")

    def test_deadline_is_unix_timestamp(self):
        data = blog_article_to_detail_dict(_article())
        assert isinstance(data["deadline"], int)
        assert data["deadline"] > 0

    def test_tech_stack_empty(self):
        data = blog_article_to_detail_dict(_article())
        assert data["tech_stack"] == []

    def test_career_level_empty(self):
        data = blog_article_to_detail_dict(_article())
        assert data["career_level"] == ""

    def test_raw_text_includes_job_categories(self):
        data = blog_article_to_detail_dict(_article())
        assert "[직무]" in data["raw_text"]
        assert "데이터엔지니어" in data["raw_text"]

    def test_no_job_section_when_no_match(self):
        data = blog_article_to_detail_dict(_article(
            title="회사 워크숍 후기", summary="즐거운 시간이었습니다.",
        ))
        assert "[직무]" not in data["raw_text"]


# ─────────────────────────────────────────────────────────
# TechBlogCollector 테스트
# ─────────────────────────────────────────────────────────


def _mock_feed_result(entries):
    """feedparser.parse 의 반환값을 흉내내는 객체."""
    result = MagicMock()
    result.bozo = False
    result.entries = entries
    return result


def _feed_entry(title="테스트 글", url="https://blog.test/1", summary="요약"):
    return {
        "title": title,
        "link": url,
        "summary": summary,
        "published": "Sat, 01 Mar 2026 10:00:00 +0900",
    }


class TestIsTruncated:
    def test_ellipsis_unicode(self):
        assert _is_truncated("안녕하세요 카카오…") is True

    def test_ellipsis_dots(self):
        assert _is_truncated("내용이 잘려서...") is True

    def test_complete_sentence(self):
        assert _is_truncated("이 글에서는 기술을 소개합니다.") is False

    def test_empty_string(self):
        assert _is_truncated("") is False


class TestParseDevoceanDate:
    def test_valid_date(self):
        dt = _parse_devocean_date("26.04.30")
        assert dt.year == 2026
        assert dt.month == 4
        assert dt.day == 30

    def test_invalid_date_returns_now(self):
        dt = _parse_devocean_date("invalid")
        assert dt.year >= 2026


class TestTechBlogCollector:
    @patch("collector.tech_blog.TechBlogCollector._fetch_devocean", return_value=[])
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_collect_all_deduplicates_by_url(self, mock_sleep, mock_parse, mock_devocean):
        """동일 URL 이 여러 피드에 있어도 1건만 수집."""
        same_url = "https://blog.test/shared"
        mock_parse.return_value = _mock_feed_result([
            _feed_entry(url=same_url),
        ])

        feeds = (
            BlogFeed("A사", "https://a.com/feed"),
            BlogFeed("B사", "https://b.com/feed"),
        )
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert len(articles) == 1
        assert articles[0].url == same_url

    @patch("collector.tech_blog.TechBlogCollector._fetch_devocean", return_value=[])
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_collect_all_skips_entries_without_link(self, mock_sleep, mock_parse, mock_devocean):
        """link 가 없는 항목은 스킵."""
        mock_parse.return_value = _mock_feed_result([
            {"title": "제목만", "summary": "요약"},
            _feed_entry(title="정상 글", url="https://blog.test/ok"),
        ])

        feeds = (BlogFeed("테스트", "https://test.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert len(articles) == 1

    @patch("collector.tech_blog._fetch_page_content", return_value="본문 내용입니다.")
    @patch("collector.tech_blog.TechBlogCollector._fetch_devocean", return_value=[])
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_truncated_summary_fetches_page_content(self, mock_sleep, mock_parse, mock_devocean, mock_fetch):
        """잘린 요약은 페이지 본문을 가져와서 대체한다."""
        mock_parse.return_value = _mock_feed_result([
            _feed_entry(title="잘린 글", url="https://blog.test/truncated", summary="안녕하세요…"),
            _feed_entry(title="정상 글", url="https://blog.test/ok", summary="완결된 요약입니다."),
        ])

        feeds = (BlogFeed("테스트", "https://test.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert len(articles) == 2
        assert articles[0].summary == "본문 내용입니다."
        assert articles[1].summary == "완결된 요약입니다."
        mock_fetch.assert_called_once_with("https://blog.test/truncated")

    @patch("collector.tech_blog._fetch_page_content", return_value="")
    @patch("collector.tech_blog.TechBlogCollector._fetch_devocean", return_value=[])
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_truncated_summary_falls_back_to_empty(self, mock_sleep, mock_parse, mock_devocean, mock_fetch):
        """페이지 본문 추출도 실패하면 빈 요약으로 저장 (제목만)."""
        mock_parse.return_value = _mock_feed_result([
            _feed_entry(title="잘린 글", url="https://blog.test/truncated", summary="안녕하세요…"),
        ])

        feeds = (BlogFeed("테스트", "https://test.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert len(articles) == 1
        assert articles[0].summary == ""

    @patch("collector.tech_blog._fetch_page_content", return_value="본문")
    @patch("collector.tech_blog.TechBlogCollector._fetch_devocean", return_value=[])
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_fetch_content_limited_to_max(self, mock_sleep, mock_parse, mock_devocean, mock_fetch):
        """본문 추출은 최대 5건까지만 수행."""
        entries = [
            _feed_entry(title=f"글{i}", url=f"https://blog.test/{i}", summary="잘림…")
            for i in range(10)
        ]
        mock_parse.return_value = _mock_feed_result(entries)

        feeds = (BlogFeed("테스트", "https://test.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert mock_fetch.call_count == 5
        fetched = [a for a in articles if a.summary == "본문"]
        not_fetched = [a for a in articles if a.summary == ""]
        assert len(fetched) == 5
        assert len(not_fetched) == 5

    @patch("collector.tech_blog.TechBlogCollector._fetch_devocean", return_value=[])
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_collect_all_handles_feed_error(self, mock_sleep, mock_parse, mock_devocean):
        """피드 파싱 예외 시 해당 피드를 스킵하고 계속 진행."""
        mock_parse.side_effect = Exception("network error")

        feeds = (BlogFeed("에러피드", "https://error.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert articles == []


# ─────────────────────────────────────────────────────────
# SK 데보션 크롤링 테스트
# ─────────────────────────────────────────────────────────


_DEVOCEAN_LIST_HTML = """
<html><body>
<div>
  <div data-board-id="100">
    <strong class="tit">AI 모델 서빙 가이드</strong>
    <span class="date">26.04.24</span>
  </div>
  <div data-board-id="100"><span class="tit">AI 모델 서빙 가이드</span></div>
  <div data-board-id="200">
    <strong class="tit">Kafka 최적화 사례</strong>
    <span class="date">26.03.15</span>
  </div>
  <div data-board-id="200"><span class="tit">Kafka 최적화 사례</span></div>
</div>
</body></html>
"""

_DEVOCEAN_DETAIL_HTML = """
<html><body>
<div class="sub-view-cont">
DEVOTEE 요약
본 블로그는 AI 모델 서빙 파이프라인 구축 경험을 공유합니다.
CDK와 API Gateway를 활용한 실전 가이드입니다.

이 글은 실제 프로덕션에서 운영을 위해 개발을 진행했던 프로젝트 기반입니다.
</div>
</body></html>
"""

_DEVOCEAN_DETAIL_NO_DEVOTEE = """
<html><body>
<div class="sub-view-cont">
이 글에는 AI 요약 기능이 적용되지 않았습니다. 본문만 있습니다.
</div>
</body></html>
"""


class TestDevocean:
    @patch("collector.tech_blog.time.sleep")
    def test_fetch_devocean_extracts_devotee_summary(self, mock_sleep):
        """데보션 목록 + 상세에서 DEVOTEE 요약을 추출한다."""
        list_resp = MagicMock()
        list_resp.text = _DEVOCEAN_LIST_HTML
        list_resp.raise_for_status = MagicMock()

        detail_resp = MagicMock()
        detail_resp.text = _DEVOCEAN_DETAIL_HTML
        detail_resp.raise_for_status = MagicMock()

        collector = TechBlogCollector(feeds=(), request_delay=0)
        collector.session.get = MagicMock(side_effect=[list_resp, detail_resp, detail_resp])
        articles = collector._fetch_devocean()

        assert len(articles) == 2
        assert articles[0].company_name == "SK"
        assert articles[0].title == "AI 모델 서빙 가이드"
        assert "AI 모델 서빙 파이프라인" in articles[0].summary

    @patch("collector.tech_blog.time.sleep")
    def test_fetch_devocean_skips_without_devotee(self, mock_sleep):
        """DEVOTEE 요약이 없는 글은 스킵한다."""
        list_resp = MagicMock()
        list_resp.text = _DEVOCEAN_LIST_HTML
        list_resp.raise_for_status = MagicMock()

        no_devotee_resp = MagicMock()
        no_devotee_resp.text = _DEVOCEAN_DETAIL_NO_DEVOTEE
        no_devotee_resp.raise_for_status = MagicMock()

        collector = TechBlogCollector(feeds=(), request_delay=0)
        collector.session.get = MagicMock(side_effect=[list_resp, no_devotee_resp, no_devotee_resp])
        articles = collector._fetch_devocean()

        assert len(articles) == 0

    @patch("collector.tech_blog.time.sleep")
    def test_fetch_devocean_handles_list_error(self, mock_sleep):
        """목록 페이지 요청 실패 시 빈 리스트 반환."""
        collector = TechBlogCollector(feeds=(), request_delay=0)
        collector.session.get = MagicMock(side_effect=Exception("connection error"))
        articles = collector._fetch_devocean()

        assert articles == []


# ─────────────────────────────────────────────────────────
# blog_collector Lambda 핸들러 테스트
# ─────────────────────────────────────────────────────────


class TestBlogCollectorHandler:
    @patch("app.S3Storage")
    @patch("collector.tech_blog.TechBlogCollector")
    def test_saves_new_articles_to_s3_raw(self, mock_collector_cls, mock_storage_cls):
        """신규 블로그 글이 S3 raw/ 에 저장된다."""
        import app

        mock_storage = MagicMock()
        mock_storage.get_all_urls.return_value = set()
        mock_storage_cls.return_value = mock_storage

        mock_feed = MagicMock()
        mock_collector = MagicMock()
        mock_collector.feeds = [mock_feed]
        mock_collector._fetch_feed.return_value = [_article()]
        mock_collector._fetch_devocean.return_value = []
        mock_collector_cls.return_value = mock_collector

        result = app.blog_collector({}, None)

        body = json.loads(result["body"])
        assert body["saved"] == 1
        assert body["skipped"] == 0
        mock_storage.save_raw_dict.assert_called_once()

        saved_data = mock_storage.save_raw_dict.call_args.args[0]
        assert saved_data["source"] == "tech_blog"
        assert saved_data["company_name"] == "카카오"

    @patch("app.S3Storage")
    @patch("collector.tech_blog.TechBlogCollector")
    def test_skips_existing_urls(self, mock_collector_cls, mock_storage_cls):
        """이미 저장된 URL 은 스킵하고 저장하지 않는다."""
        import app

        mock_storage = MagicMock()
        mock_storage.get_all_urls.return_value = {"https://tech.kakao.com/post/123"}
        mock_storage_cls.return_value = mock_storage

        mock_feed = MagicMock()
        mock_collector = MagicMock()
        mock_collector.feeds = [mock_feed]
        mock_collector._fetch_feed.return_value = [_article()]
        mock_collector._fetch_devocean.return_value = []
        mock_collector_cls.return_value = mock_collector

        result = app.blog_collector({}, None)

        body = json.loads(result["body"])
        assert body["saved"] == 0
        assert body["skipped"] == 1
        mock_storage.save_raw_dict.assert_not_called()
