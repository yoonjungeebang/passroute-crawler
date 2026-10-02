"""pgvector 스토리지 헬퍼 함수 단위 테스트."""
import sys
from unittest.mock import MagicMock

# psycopg2 가 테스트 환경에 없을 수 있으므로 stub 을 주입
if "psycopg2" not in sys.modules:
    sys.modules["psycopg2"] = MagicMock()
    sys.modules["psycopg2.extras"] = MagicMock()

from storage.pgvector import _deadline_to_ts


class TestDeadlineToTs:
    def test_int_passthrough(self):
        """int 값은 그대로 반환."""
        assert _deadline_to_ts(1714575599) == 1714575599

    def test_empty_string_returns_zero(self):
        assert _deadline_to_ts("") == 0

    def test_zero_int_returns_zero(self):
        assert _deadline_to_ts(0) == 0

    def test_numeric_string(self):
        """크롤러가 str(int(ts)) 형태로 반환하는 경우 정상 파싱."""
        assert _deadline_to_ts("1714575599") == 1714575599

    def test_iso_format(self):
        """ISO 8601 형식의 마감일을 timestamp 로 변환."""
        ts = _deadline_to_ts("2026-05-01T23:59:59+09:00")
        assert ts > 0

    def test_invalid_string_returns_zero(self):
        assert _deadline_to_ts("채용시까지") == 0

    def test_negative_numeric_string_returns_zero(self):
        """음수 timestamp 문자열은 ISO 파싱으로 폴백 후 0 반환."""
        assert _deadline_to_ts("-1") == 0
