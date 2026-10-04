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
    _extract_rss_content,
    _make_external_id,
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


# ─────────────────────────────────────────────────────────
# blog_article_to_detail_dict 변환 테스트
# ─────────────────────────────────────────────────────────


def _article(**overrides) -> BlogArticle:
    base = dict(
        company_name="카카오",
        title="Kafka 파티션 전략",
        content="대규모 트래픽 환경에서의 Kafka 파티션 최적화 사례를 공유합니다.",
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

    def test_raw_text_combines_title_and_content(self):
        data = blog_article_to_detail_dict(_article())
        assert "Kafka 파티션 전략" in data["raw_text"]
        assert "대규모 트래픽" in data["raw_text"]

    def test_raw_text_title_only_when_no_content(self):
        data = blog_article_to_detail_dict(_article(content=""))
        assert data["raw_text"].startswith("Kafka 파티션 전략")
        assert "대규모 트래픽" not in data["raw_text"]

    def test_deadline_is_unix_timestamp(self):
        data = blog_article_to_detail_dict(_article())
        assert isinstance(data["deadline"], int)
        assert data["deadline"] > 0

    def test_raw_text_includes_job_categories(self):
        data = blog_article_to_detail_dict(_article())
        assert "[직무]" in data["raw_text"]
        assert "데이터엔지니어" in data["raw_text"]

    def test_no_job_section_when_no_match(self):
        data = blog_article_to_detail_dict(_article(
            title="회사 워크숍 후기", content="즐거운 시간이었습니다.",
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


def _feed_entry(title="테스트 글", url="https://blog.test/1", summary="요약 텍스트"):
    entry = MagicMock(spec=[])
    entry.get = lambda k, d="": {
        "title": title,
        "link": url,
        "summary": summary,
        "published": "Sat, 01 Mar 2026 10:00:00 +0900",
    }.get(k, d)
    return entry


class TestTechBlogCollector:
    @patch("collector.tech_blog.RobotsChecker")
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_collect_all_deduplicates_by_url(self, mock_sleep, mock_parse, mock_robots):
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

    @patch("collector.tech_blog.RobotsChecker")
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_collect_all_skips_entries_without_link(self, mock_sleep, mock_parse, mock_robots):
        """link 가 없는 항목은 스킵."""
        no_link = MagicMock(spec=[])
        no_link.get = lambda k, d="": {"title": "제목만"}.get(k, d)

        mock_parse.return_value = _mock_feed_result([
            no_link,
            _feed_entry(title="정상 글", url="https://blog.test/ok"),
        ])

        feeds = (BlogFeed("테스트", "https://test.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert len(articles) == 1

    @patch("collector.tech_blog._fetch_page_content", return_value="크롤링된 본문 내용입니다.")
    @patch("collector.tech_blog.RobotsChecker")
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_crawls_page_when_rss_content_insufficient(self, mock_sleep, mock_parse, mock_robots_cls, mock_fetch):
        """RSS 콘텐츠 부족 시 robots.txt 허용된 페이지를 크롤링."""
        mock_robots = MagicMock()
        mock_robots.is_allowed.return_value = True
        mock_robots_cls.return_value = mock_robots

        short_entry = MagicMock(spec=[])
        short_entry.get = lambda k, d="": {
            "title": "잘린 글",
            "link": "https://blog.test/truncated",
            "summary": "짧음",
            "published": "Sat, 01 Mar 2026 10:00:00 +0900",
        }.get(k, d)

        mock_parse.return_value = _mock_feed_result([short_entry])

        feeds = (BlogFeed("테스트", "https://test.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert len(articles) == 1
        assert articles[0].content == "크롤링된 본문 내용입니다."

    @patch("collector.tech_blog._fetch_page_content")
    @patch("collector.tech_blog.RobotsChecker")
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_skips_crawling_when_robots_blocked(self, mock_sleep, mock_parse, mock_robots_cls, mock_fetch):
        """robots.txt 차단 시 크롤링하지 않고 제목만 저장."""
        mock_robots = MagicMock()
        mock_robots.is_allowed.return_value = False
        mock_robots_cls.return_value = mock_robots

        short_entry = MagicMock(spec=[])
        short_entry.get = lambda k, d="": {
            "title": "차단된 블로그 글",
            "link": "https://blocked.test/post",
            "summary": "짧음",
            "published": "Sat, 01 Mar 2026 10:00:00 +0900",
        }.get(k, d)

        mock_parse.return_value = _mock_feed_result([short_entry])

        feeds = (BlogFeed("테스트", "https://test.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

        assert len(articles) == 1
        assert articles[0].content == "짧음"
        mock_fetch.assert_not_called()

    @patch("collector.tech_blog.RobotsChecker")
    @patch("collector.tech_blog.feedparser.parse")
    @patch("collector.tech_blog.time.sleep")
    def test_collect_all_handles_feed_error(self, mock_sleep, mock_parse, mock_robots):
        """피드 파싱 예외 시 해당 피드를 스킵하고 계속 진행."""
        mock_parse.side_effect = Exception("network error")

        feeds = (BlogFeed("에러피드", "https://error.com/feed"),)
        collector = TechBlogCollector(feeds=feeds, request_delay=0)
        articles = collector.collect_all()

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
        mock_collector_cls.return_value = mock_collector

        result = app.blog_collector({}, None)

        body = json.loads(result["body"])
        assert body["saved"] == 0
        assert body["skipped"] == 1
        mock_storage.save_raw_dict.assert_not_called()
