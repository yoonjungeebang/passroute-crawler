"""DLQ 알람 → Discord 웹훅 알림 Lambda 핸들러."""

import json
import logging
import os
import time
import urllib.error
import urllib.request

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_DLQ_QUEUES = (
    "passroute-job-detail-dlq",
    "passroute-source-collect-dlq",
    "passroute-embed-dlq",
    "passroute-db-load-dlq",
)


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


def discord_notifier(event, _context):
    """SNS 메시지를 받아 Discord 웹훅으로 전송한다."""
    from core.secrets import get_discord_webhook_url  # noqa: C0415

    try:
        webhook_url = get_discord_webhook_url()
    except Exception:
        logger.exception("Discord 웹훅 URL 조회 실패")
        return

    if not webhook_url:
        logger.error("DISCORD_WEBHOOK_URL 환경변수 누락")
        return

    for record in event.get("Records", []):
        sns_message = record.get("Sns", {})
        subject = sns_message.get("Subject", "알람")
        raw_message = sns_message.get("Message", "")

        try:
            alarm = json.loads(raw_message)
            alarm_name = alarm.get("AlarmName", "")
            description = alarm.get("AlarmDescription", "")
            reason = alarm.get("NewStateReason", "")
            region = alarm.get("Region", "")
            timestamp = alarm.get("StateChangeTime", "")
            new_state = alarm.get("NewStateValue", "ALARM")

            if new_state == "OK":
                title = f"\u2705 [복구] {alarm_name}"
                color = 0x00C853
            else:
                title = f"\u26a0\ufe0f {alarm_name}"
                color = 0xFF4444

            embed = {
                "title": title,
                "description": description,
                "color": color,
                "fields": [
                    {"name": "\uc6d0\uc778", "value": reason, "inline": False},
                    {"name": "\ub9ac\uc804", "value": region, "inline": True},
                    {"name": "\ubc1c\uc0dd \uc2dc\uac01", "value": timestamp, "inline": True},
                ],
            }
            payload = {"embeds": [embed]}
        except (json.JSONDecodeError, KeyError):
            payload = {
                "content": f"**{subject}**\n```\n{raw_message[:1500]}\n```",
            }

        try:
            _send_discord(webhook_url, payload)
        except Exception:
            logger.exception("Discord 웹훅 전송 실패")
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


def dlq_daily_check(event, _context):
    """매일 DLQ 메시지 수를 확인하고, 1건 이상이면 Discord 로 재알림."""
    from core.secrets import get_discord_webhook_url  # noqa: C0415

    try:
        webhook_url = get_discord_webhook_url()
    except Exception:
        logger.exception("Discord 웹훅 URL 조회 실패")
        return

    if not webhook_url:
        logger.error("DISCORD_WEBHOOK_URL 환경변수 누락")
        return

    sqs = boto3.client("sqs")
    region = os.environ.get("AWS_REGION", "ap-northeast-2")
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    alerts: list[dict] = []
    for queue_name in _DLQ_QUEUES:
        queue_url = f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )["Attributes"]
            count = int(attrs.get("ApproximateNumberOfMessages", "0"))
        except Exception:
            logger.exception("DLQ 조회 실패: %s", queue_name)
            continue

        if count > 0:
            alerts.append({"name": queue_name, "value": f"{count}건", "inline": True})

    if not alerts:
        logger.info("DLQ 메시지 없음, 알림 스킵")
        return

    payload = {
        "embeds": [{
            "title": "\U0001f6a8 DLQ 미처리 메시지 잔존",
            "description": "처리되지 않은 실패 메시지가 DLQ에 남아 있습니다.",
            "color": 0xFF8800,
            "fields": alerts,
        }],
    }

    try:
        _send_discord(webhook_url, payload)
    except Exception:
        logger.exception("Discord 웹훅 전송 실패")
        raise
