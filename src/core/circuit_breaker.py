"""경량 서킷 브레이커. 외부 API 장애 시 빠른 실패(fail-fast)를 제공한다.

상태 머신:
  CLOSED  --[연속 failure_threshold회 실패]--> OPEN
  OPEN    --[recovery_timeout 경과]---------> HALF_OPEN
  HALF_OPEN --[성공]------------------------> CLOSED
  HALF_OPEN --[실패]------------------------> OPEN
"""
import enum
import logging
import time

from config import load_circuit_breaker_config

logger = logging.getLogger(__name__)

_CB_CONFIG = load_circuit_breaker_config()


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """서킷이 OPEN 상태일 때 발생하는 예외."""

    def __init__(self, name: str, remaining: float):
        self.name = name
        self.remaining = remaining
        super().__init__(
            f"서킷 '{name}' OPEN 상태 (복구까지 {remaining:.0f}초 남음)"
        )


class CircuitBreaker:
    """컨텍스트 매니저 방식의 서킷 브레이커.

    사용법::

        breaker = CircuitBreaker("jumpit")

        with breaker:
            resp = session.get(url)
            resp.raise_for_status()
    """

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = _CB_CONFIG["failure_threshold"],
        recovery_timeout: float = _CB_CONFIG["recovery_timeout"],
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float = 0.0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(
                    "서킷 '%s': OPEN → HALF_OPEN (%.0f초 경과)", self.name, elapsed
                )
        return self._state

    def __enter__(self):
        current = self.state
        if current == CircuitState.OPEN:
            remaining = self.recovery_timeout - (
                time.monotonic() - self._last_failure_time
            )
            raise CircuitOpenError(self.name, max(remaining, 0))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._on_success()
        elif exc_type is not CircuitOpenError:
            self._on_failure()
        # 예외를 억제하지 않음
        return False

    def _on_success(self) -> None:
        if self._state != CircuitState.CLOSED:
            logger.info("서킷 '%s': %s → CLOSED (복구 성공)", self.name, self._state.value)
        self._failure_count = 0
        self._state = CircuitState.CLOSED

    def record_failure(self, exc: Exception | None = None) -> None:
        """with 블록 바깥에서 실패를 기록한다. 검증 실패 등에 사용."""
        self._on_failure()

    def _on_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            logger.warning(
                "서킷 '%s': HALF_OPEN → OPEN (복구 실패)", self.name
            )
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                "서킷 '%s': CLOSED → OPEN (연속 %d회 실패)",
                self.name,
                self._failure_count,
            )


# ── 모듈 수준 레지스트리 ──

_registry: dict[str, CircuitBreaker] = {}


def get_breaker(
    name: str,
    *,
    failure_threshold: int = _CB_CONFIG["failure_threshold"],
    recovery_timeout: float = _CB_CONFIG["recovery_timeout"],
) -> CircuitBreaker:
    """이름으로 서킷 브레이커를 가져오거나 생성한다.

    Lambda 웜 스타트 시 전역 상태가 유지되어 이전 호출의 서킷 상태를 기억한다.
    """
    if name not in _registry:
        _registry[name] = CircuitBreaker(
            name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
        )
    return _registry[name]
