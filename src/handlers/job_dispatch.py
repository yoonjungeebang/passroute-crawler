"""Lambda 핸들러: 목록 수집 디스패치 (cron)."""
import json
import logging
import boto3
from crawler.registry import iter_sources
from ._common import required_env

logger = logging.getLogger(__name__)


def job_list_collector(event, context):
    """소스별 수집 메시지를 SourceCollectQueue 로 발행."""
    sqs = boto3.client("sqs")
    source_queue_url = required_env("SOURCE_COLLECT_QUEUE_URL")

    sources = list(iter_sources())
    logger.info("[디스패치] 수집 대상 소스 %d건: %s", len(sources), sources)

    for source in sources:
        sqs.send_message(
            QueueUrl=source_queue_url,
            MessageBody=json.dumps({"source": source}, ensure_ascii=False),
        )
        logger.info("[디스패치] SourceCollectQueue 전송: %s", source)

    logger.info("[디스패치] 완료: %d개 소스 분배됨", len(sources))

    return {
        "statusCode": 200,
        "body": json.dumps({"dispatched_sources": sources}),
    }
