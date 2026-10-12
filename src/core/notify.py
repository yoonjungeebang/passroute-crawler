"""Discord 웹훅 알림 유틸리티."""

import json
import logging
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def _send_discord(webhook_url: str, payload: dict, *, max_retries: int = 2) -> None:
    """Discord 웹훅으로 payload 를 전송한다.

    일시적 네트워크 장애에 대비해 지수 백오프로 재시도한다.
    """
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "passroute-dlq-notifier/1.0",
        },
        method="POST",
    )
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info("Discord 알림 전송 완료: status=%d", resp.status)
                return
        except (urllib.error.URLError, OSError):
            if attempt < max_retries:
                delay = (attempt + 1) ** 2
                logger.warning("Discord 전송 실패, %.0f초 후 재시도 %d/%d", delay, attempt + 1, max_retries)
                time.sleep(delay)
            else:
                raise


def send_monitor_alert(title: str, description: str, color: int = 0xFF8800) -> None:
    """#monitor 채널로 알림 전송. robots.txt 차단, 품질 검증 실패 등에 사용."""
    from core.secrets import get_discord_monitor_webhook_url  # noqa: C0415

    try:
        webhook_url = get_discord_monitor_webhook_url()
    except Exception:
        logger.exception("Monitor 웹훅 URL 조회 실패")
        return

    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
        }],
    }

    try:
        _send_discord(webhook_url, payload)
    except Exception:
        logger.exception("Monitor 웹훅 전송 실패")
