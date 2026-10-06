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

    def test_zero_int_returns_zero(self):
        assert _deadline_to_ts(0) == 0

    def test_non_int_returns_zero(self):
        """int 가 아닌 값은 0 반환."""
        assert _deadline_to_ts("invalid") == 0
