"""Lambda 핸들러: 임베딩 워커 (SQS 트리거)."""
import json
import logging
import time

import boto3

from ._common import required_env

logger = logging.getLogger(__name__)


def embed_worker(event, context):
    """SQS 트리거. 크롤링 데이터 수신 → 임베딩 → DbLoadQueue 로 전송.

    실패한 레코드만 batchItemFailures로 반환하여 성공한 메시지의
    불필요한 재처리를 방지한다.
    """
    from core.embedding import build_document, embed_text  # noqa: C0415
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.monotonic()
    metrics = MetricsLogger(function_name="embed_worker")
    embed_count = 0
    embed_fail_count = 0
    failures: list[dict] = []
    sqs = boto3.client("sqs")
    db_load_queue_url = required_env("DB_LOAD_QUEUE_URL")

    for record in event["Records"]:
        try:
            data = json.loads(record["body"])
            trace_id = data.get("trace_id", "")

            logger.info(
                "임베딩 시작: source=%s id=%s trace_id=%s",
                data.get("source"), data.get("external_id"), trace_id,
            )

            embedding_failed = False
            try:
                t0 = time.monotonic()
                document = build_document(data["raw_text"], tuple(data.get("tech_stack", [])))
                embedding = embed_text(document)
                metrics.put_duration("EmbeddingDuration", t0)
                data["embedding"] = embedding
                embed_count += 1
            except Exception:
                embedding_failed = True
                embed_fail_count += 1
                logger.exception(
                    "임베딩 실패, 임베딩 없이 저장: id=%s source=%s",
                    data.get("external_id"), data.get("source"),
                )

            data["embedding_status"] = "failed" if embedding_failed else "ok"
            data["type"] = "load"
            sqs.send_message(
                QueueUrl=db_load_queue_url,
                MessageBody=json.dumps(data, ensure_ascii=False),
            )
            logger.info(
                "임베딩 완료, DbLoadQueue 전송: source=%s id=%s trace_id=%s",
                data.get("source"), data.get("external_id"), trace_id,
            )
        except Exception:
            logger.exception("레코드 처리 실패")
            failures.append({"itemIdentifier": record["messageId"]})

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("EmbeddedCount", embed_count)
    metrics.put_count("EmbedFailCount", embed_fail_count)
    metrics.flush()
    return {"statusCode": 200, "batchItemFailures": failures}
