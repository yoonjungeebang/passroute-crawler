"""검색 API 단위 테스트."""
import json
from unittest.mock import MagicMock, patch

from collector.naver_news import search_news
from search.pgvector_search import search_jobs


class TestPgvectorSearch:
    def test_search_jobs_returns_results(self):
        """pgvector 검색이 결과를 반환한다."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [
            ("url",), ("source",), ("company_name",), ("title",),
            ("document",), ("career_level",), ("tech_stack",),
            ("deadline",), ("similarity",),
        ]
        mock_cursor.fetchall.return_value = [
            ("https://jumpit.com/1", "jumpit", "카카오", "백엔드 개발자",
             "Spring Boot 기반", "경력", "Java, Spring", 0, 0.92),
        ]

        results = search_jobs(mock_conn, [0.1] * 768, limit=5)

        assert len(results) == 1
        assert results[0]["company_name"] == "카카오"
        assert results[0]["similarity"] == 0.92

    def test_search_jobs_with_company_filter(self):
        """회사명 필터가 쿼리에 반영된다."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [
            ("url",), ("source",), ("company_name",), ("title",),
            ("document",), ("career_level",), ("tech_stack",),
            ("deadline",), ("similarity",),
        ]
        mock_cursor.fetchall.return_value = []

        results = search_jobs(mock_conn, [0.1] * 768, limit=5, company="카카오")

        assert results == []
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "company_name ILIKE" in executed_sql


class TestNaverRealtime:
    @patch("collector.naver_news.requests.Session")
    def test_search_news_returns_items(self, mock_session_cls):
        """네이버 뉴스 검색이 결과를 반환한다."""
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "items": [
                {
                    "title": "<b>카카오</b> AI 기술 발표",
                    "description": "카카오가 새로운 AI 기술을 발표했다.",
                    "originallink": "https://news.example.com/1",
                    "pubDate": "Sat, 01 Mar 2026 10:00:00 +0900",
                }
            ]
        }
        mock_session.get.return_value = mock_resp

        results = search_news(mock_session, "카카오 기술", display=5)

        assert len(results) == 1
        assert results[0]["title"] == "카카오 AI 기술 발표"
        assert results[0]["source"] == "naver_news"

    def test_search_news_handles_error(self):
        """API 호출 실패 시 빈 리스트 반환."""
        mock_session = MagicMock()
        mock_session.get.side_effect = Exception("timeout")

        results = search_news(mock_session, "카카오", display=5)
        assert results == []
