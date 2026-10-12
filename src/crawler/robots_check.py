"""robots.txt 사전 확인 모듈. Tier 2 크롤링 전 정책 변경을 감지한다."""
import logging
import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from config import load_crawler_config

logger = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = (
    "passroute-bot/1.0 "
    "(+https://github.com/yoonjungeebang/passroute-crawler; yezanee@gmail.com)"
)
_CACHE_TTL: int = load_crawler_config()["robots_cache_ttl"]


class RobotsChecker:
    """robots.txt 를 확인하고 캐싱하여 Tier 2 크롤링 가드로 사용."""

    def __init__(self, user_agent: str = _DEFAULT_USER_AGENT, cache_ttl: int = _CACHE_TTL):
        self.user_agent = user_agent
        self.cache_ttl = cache_ttl
        self._cache: dict[str, tuple[bool, float]] = {}

    def is_allowed(self, url: str) -> bool:
        """url 에 대해 robots.txt 허용 여부를 반환. 캐시 유효 시 재사용.

        fetch 실패 시 만료된 캐시가 있으면 재사용하고,
        캐시도 없는 최초 요청에서만 fail-open(허용) 처리한다.
        """
        domain = self._domain(url)
        cached = self._cache.get(domain)
        if cached and (time.time() - cached[1]) < self.cache_ttl:
            return cached[0]

        result = self._fetch_and_check(url)
        if result is not None:
            self._cache[domain] = (result, time.time())
            return result

        # fetch 실패: 만료된 캐시가 있으면 재사용
        if cached:
            logger.info(
                "robots.txt 조회 실패, 만료된 캐시 사용: %s → %s",
                domain, "허용" if cached[0] else "차단",
            )
            return cached[0]

        # 캐시도 없으면 fail-open
        logger.warning("robots.txt 조회 실패, 캐시 없음 (fail-open): %s", domain)
        return True

    def check_and_alert(self, url: str, source: str) -> bool:
        """is_allowed() 호출 후, 차단 감지 시 Discord 알림. True 면 크롤링 가능."""
        allowed = self.is_allowed(url)
        if not allowed:
            logger.warning("robots.txt 차단 감지: source=%s url=%s", source, url)
            self._send_block_alert(source, url)
        return allowed

    def _fetch_and_check(self, url: str) -> bool | None:
        """robots.txt 를 다운로드하고 파싱. 실패 시 None 반환(is_allowed 에서 처리)."""
        robots_url = self._robots_url(url)
        rp = RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except Exception:
            logger.warning("robots.txt 조회 실패: %s", robots_url, exc_info=True)
            return None

        allowed = rp.can_fetch(self.user_agent, url)
        logger.info("robots.txt 확인: %s → %s", robots_url, "허용" if allowed else "차단")
        return allowed

    def _send_block_alert(self, source: str, url: str) -> None:
        """차단 감지 시 Discord #monitor 채널로 알림 전송."""
        try:
            from core.notify import send_monitor_alert  # noqa: C0415

            send_monitor_alert(
                title=f"\U0001f6ab robots.txt 차단 감지: {source}",
                description=(
                    f"**{source}** 사이트가 크롤링을 차단했습니다.\n"
                    f"해당 소스는 자동으로 스킵됩니다.\n\n"
                    f"URL: `{url}`"
                ),
                color=0xFF4444,
            )
        except Exception:
            logger.exception("robots.txt 차단 Discord 알림 전송 실패")

    @staticmethod
    def _domain(url: str) -> str:
        return urlparse(url).netloc

    @staticmethod
    def _robots_url(url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"