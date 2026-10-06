"""Lambda 핸들러: 임베딩 워커 (SQS 트리거, S3 raw/ 이벤트)."""
import json
import logging
import time

import boto3
from botocore.exceptions import ClientError

from ._common import archive_and_delete

logger = logging.getLogger(__name__)


def embed_worker(event, context):
    """SQS 트리거. S3 raw/ PutObject 이벤트 → 임베딩 → S3(parsed/) 저장.

    실패한 레코드만 batchItemFailures로 반환하여 성공한 메시지의
    불필요한 재처리를 방지한다. 이미 처리된 키는 head_object로 감지하여 스킵한다.
    """
    from core.embedding import build_document, embed_text  # noqa: C0415
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.monotonic()
    metrics = MetricsLogger(function_name="embed_worker")
    embed_count = 0
    embed_fail_count = 0
    failures: list[dict] = []
    s3 = boto3.client("s3")

    for record in event["Records"]:
        envelope = json.loads(record["body"])
        bucket = envelope["detail"]["bucket"]["name"]
        key = envelope["detail"]["object"]["key"]

        try:
            if not (key.startswith("raw/") and key.endswith(".json")):
                logger.warning("embed_worker: 예상하지 못한 키, 스킵: %s", key)
                continue

            parsed_check_key = "parsed/" + key[len("raw/"):]
            try:
                s3.head_object(Bucket=bucket, Key=parsed_check_key)
                archive_and_delete(s3, bucket, key)
                logger.info("이미 처리됨, 스킵: %s", key)
                continue
            except ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    raise

            logger.info("임베딩 시작: %s", key)

            resp = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(resp["Body"].read().decode("utf-8"))

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

            parsed_key = f"parsed/{data['source']}/{data['external_id']}.json"
            s3.put_object(
                Bucket=bucket,
                Key=parsed_key,
                Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            )
            archive_and_delete(s3, bucket, key)
            logger.info("임베딩 완료: %s → %s", key, parsed_key)
        except Exception:
            logger.exception("레코드 처리 실패: key=%s", key)
            failures.append({"itemIdentifier": record["messageId"]})

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("EmbeddedCount", embed_count)
    metrics.put_count("EmbedFailCount", embed_fail_count)
    metrics.flush()
    return {"statusCode": 200, "batchItemFailures": failures}
