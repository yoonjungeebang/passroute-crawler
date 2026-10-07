"""서킷 브레이커 단위 테스트."""
from unittest.mock import patch

import pytest

from core.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    _registry,
    get_breaker,
)


@pytest.fixture(autouse=True)
def _clear_registry():
    _registry.clear()
    yield
    _registry.clear()


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker("test")
        assert cb.state == CircuitState.CLOSED

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        # 2회 실패 (threshold 미달)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                with cb:
                    raise RuntimeError("fail")
        assert cb._failure_count == 2

        # 성공 → 카운트 리셋
        with cb:
            pass
        assert cb._failure_count == 0
        assert cb.state == CircuitState.CLOSED

    def test_consecutive_failures_open_circuit(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        for i in range(3):
            with pytest.raises(RuntimeError):
                with cb:
                    raise RuntimeError(f"fail {i}")

        assert cb.state == CircuitState.OPEN

    def test_open_circuit_raises_circuit_open_error(self):
        cb = CircuitBreaker("test", failure_threshold=2)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                with cb:
                    raise RuntimeError("fail")

        with pytest.raises(CircuitOpenError) as exc_info:
            with cb:
                pass  # 이 코드는 실행되지 않아야 함

        assert exc_info.value.name == "test"
        assert exc_info.value.remaining > 0

    def test_open_circuit_does_not_execute_body(self):
        cb = CircuitBreaker("test", failure_threshold=1)
        with pytest.raises(RuntimeError):
            with cb:
                raise RuntimeError("fail")

        executed = False
        with pytest.raises(CircuitOpenError):
            with cb:
                executed = True
        assert not executed

    @patch("core.circuit_breaker.time.monotonic")
    def test_recovery_timeout_transitions_to_half_open(self, mock_time):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=60.0)

        # 실패 시점: t=100
        mock_time.return_value = 100.0
        with pytest.raises(RuntimeError):
            with cb:
                raise RuntimeError("fail")
        assert cb._state == CircuitState.OPEN

        # t=159: 아직 OPEN
        mock_time.return_value = 159.0
        assert cb.state == CircuitState.OPEN

        # t=160: recovery_timeout(60초) 경과 → HALF_OPEN
        mock_time.return_value = 160.0
        assert cb.state == CircuitState.HALF_OPEN

    @patch("core.circuit_breaker.time.monotonic")
    def test_half_open_success_closes_circuit(self, mock_time):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=10.0)

        mock_time.return_value = 0.0
        with pytest.raises(RuntimeError):
            with cb:
                raise RuntimeError("fail")

        # HALF_OPEN 전환
        mock_time.return_value = 11.0
        assert cb.state == CircuitState.HALF_OPEN

        # 성공 → CLOSED
        with cb:
            pass
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 0

    @patch("core.circuit_breaker.time.monotonic")
    def test_half_open_failure_reopens_circuit(self, mock_time):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=10.0)

        mock_time.return_value = 0.0
        with pytest.raises(RuntimeError):
            with cb:
                raise RuntimeError("fail")

        # HALF_OPEN 전환
        mock_time.return_value = 11.0
        assert cb.state == CircuitState.HALF_OPEN

        # 실패 → 즉시 OPEN 복귀
        with pytest.raises(RuntimeError):
            with cb:
                raise RuntimeError("fail again")
        assert cb._state == CircuitState.OPEN


class TestGetBreaker:
    def test_returns_same_instance_for_same_name(self):
        b1 = get_breaker("api")
        b2 = get_breaker("api")
        assert b1 is b2

    def test_returns_different_instances_for_different_names(self):
        b1 = get_breaker("api_a")
        b2 = get_breaker("api_b")
        assert b1 is not b2

    def test_uses_provided_parameters(self):
        b = get_breaker("custom", failure_threshold=10, recovery_timeout=120.0)
        assert b.failure_threshold == 10
        assert b.recovery_timeout == 120.0

    def test_ignores_parameters_on_subsequent_calls(self):
        b1 = get_breaker("once", failure_threshold=3)
        b2 = get_breaker("once", failure_threshold=99)
        assert b2.failure_threshold == 3  # 첫 호출의 값 유지


class TestCircuitOpenError:
    def test_is_exception_subclass(self):
        """CircuitOpenError 는 Exception 의 하위 클래스이므로 기존 except Exception 에서 잡힌다."""
        err = CircuitOpenError("test", 30.0)
        assert isinstance(err, Exception)

    def test_message_format(self):
        err = CircuitOpenError("naver_api", 45.7)
        assert "naver_api" in str(err)
        assert "46초" in str(err)
