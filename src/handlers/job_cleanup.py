"""Lambda 핸들러: 만료 공고 삭제 (cron)."""
import json
import logging
from datetime import datetime
import boto3
from core import KST
from ._common import required_env

logger = logging.getLogger(__name__)


def delete_expired_jobs(event, context):
    """매일 마감된 공고 삭제 요청을 DbLoadQueue 로 전송."""
    sqs = boto3.client("sqs")
    db_load_queue_url = required_env("DB_LOAD_QUEUE_URL")

    now_ts = int(datetime.now(KST).timestamp())
    timestamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    sqs.send_message(
        QueueUrl=db_load_queue_url,
        MessageBody=json.dumps({
            "type": "delete_expired",
            "now_ts": now_ts,
            "requested_at": timestamp,
        }),
    )
    logger.info("[만료삭제] DbLoadQueue 전송 완료: now_ts=%d", now_ts)

    return {"statusCode": 200, "body": json.dumps({"now_ts": now_ts})}
