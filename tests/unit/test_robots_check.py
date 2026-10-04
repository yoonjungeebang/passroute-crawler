"""robots_check.py 단위 테스트."""
import time
from unittest.mock import MagicMock, patch

from crawler.robots_check import RobotsChecker


class TestRobotsChecker:
    def test_allowed_site(self):
        """robots.txt 가 허용하는 사이트에 대해 True 를 반환."""
        checker = RobotsChecker()
        with patch.object(checker, "_fetch_and_check", return_value=True):
            assert checker.is_allowed("https://www.jumpit.co.kr/positions") is True

    def test_blocked_site(self):
        """robots.txt 가 차단하는 사이트에 대해 False 를 반환."""
        checker = RobotsChecker()
        with patch.object(checker, "_fetch_and_check", return_value=False):
            assert checker.is_allowed("https://blocked.example.com/jobs") is False

    def test_cache_hit(self):
        """캐시 TTL 내 재호출 시 fetch 를 다시 하지 않음."""
        checker = RobotsChecker(cache_ttl=3600)
        with patch.object(checker, "_fetch_and_check", return_value=True) as mock_fetch:
            checker.is_allowed("https://www.jumpit.co.kr/positions")
            checker.is_allowed("https://www.jumpit.co.kr/other")
            mock_fetch.assert_called_once()

    def test_cache_expired(self):
        """캐시 TTL 초과 시 다시 fetch 함."""
        checker = RobotsChecker(cache_ttl=0)
        with patch.object(checker, "_fetch_and_check", return_value=True) as mock_fetch:
            checker.is_allowed("https://www.jumpit.co.kr/a")
            checker.is_allowed("https://www.jumpit.co.kr/b")
            assert mock_fetch.call_count == 2

    @patch("crawler.robots_check.RobotsChecker._send_block_alert")
    def test_check_and_alert_sends_on_block(self, mock_alert):
        """차단 감지 시 Discord 알림을 전송."""
        checker = RobotsChecker()
        with patch.object(checker, "_fetch_and_check", return_value=False):
            result = checker.check_and_alert("https://blocked.com/jobs", "blocked_site")
            assert result is False
            mock_alert.assert_called_once_with("blocked_site", "https://blocked.com/jobs")

    @patch("crawler.robots_check.RobotsChecker._send_block_alert")
    def test_check_and_alert_no_alert_on_allow(self, mock_alert):
        """허용 시 알림을 보내지 않음."""
        checker = RobotsChecker()
        with patch.object(checker, "_fetch_and_check", return_value=True):
            result = checker.check_and_alert("https://ok.com/jobs", "ok_site")
            assert result is True
            mock_alert.assert_not_called()

    def test_fetch_failure_returns_none(self):
        """robots.txt fetch 실패 시 None 을 반환."""
        checker = RobotsChecker()
        with patch("crawler.robots_check.RobotFileParser") as mock_rp_cls:
            mock_rp = MagicMock()
            mock_rp.read.side_effect = Exception("connection timeout")
            mock_rp_cls.return_value = mock_rp
            assert checker._fetch_and_check("https://unreachable.com/jobs") is None

    def test_fetch_failure_uses_expired_cache(self):
        """fetch 실패 시 만료된 캐시가 있으면 재사용."""
        checker = RobotsChecker(cache_ttl=0)
        # 먼저 정상 결과를 캐시
        checker._cache["blocked.com"] = (False, 0)
        # fetch 실패 시 만료된 캐시 값(False) 사용
        with patch.object(checker, "_fetch_and_check", return_value=None):
            assert checker.is_allowed("https://blocked.com/jobs") is False

    def test_fetch_failure_no_cache_fail_open(self):
        """fetch 실패 + 캐시 없음 → fail-open (허용)."""
        checker = RobotsChecker()
        with patch.object(checker, "_fetch_and_check", return_value=None):
            assert checker.is_allowed("https://new-site.com/jobs") is True
