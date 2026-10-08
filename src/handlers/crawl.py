"""Lambda 핸들러: 상세 크롤링 (SQS 트리거)."""
import dataclasses
import json
import logging
import random
import time

import boto3

from core.circuit_breaker import CircuitOpenError, get_breaker
from crawler.base import JobDetail, JobListingRef
from crawler.registry import get_crawler
from crawler.validation import CrawlValidationError

from ._common import required_env

logger = logging.getLogger(__name__)


def job_crawl(event, context):
    """SQS 트리거. 공고 1건 상세 크롤링 → EmbedQueue 로 전송.

    실패한 레코드만 batchItemFailures로 반환하여 성공한 메시지의
    불필요한 재처리를 방지한다. CircuitOpenError는 일시적 장애이므로
    메시지를 소비하지 않고 재시도 대상으로 남긴다.
    """
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.monotonic()
    metrics = MetricsLogger(function_name="job_crawl")
    crawl_count = 0
    failures: list[dict] = []
    sqs = boto3.client("sqs")
    embed_queue_url = required_env("EMBED_QUEUE_URL")

    for record in event["Records"]:
        message = json.loads(record["body"])
        trace_id = message.get("trace_id", "")
        ref = JobListingRef(
            source=message["source"],
            external_id=message["external_id"],
            url=message["url"],
            title=message.get("title", ""),
            company_name=message.get("company_name", ""),
        )

        crawler = get_crawler(ref.source)
        logger.info(
            "상세 크롤링: source=%s id=%s (%s) trace_id=%s",
            ref.source, ref.external_id, ref.company_name, trace_id,
        )

        breaker = get_breaker(ref.source)
        try:
            with breaker:
                detail = crawler.fetch_detail(ref)
            if detail is None:
                logger.info("상세 정보 없음, 스킵: id=%s", ref.external_id)
                continue

            body = _detail_to_dict(detail)
            if trace_id:
                body["trace_id"] = trace_id
            sqs.send_message(
                QueueUrl=embed_queue_url,
                MessageBody=json.dumps(body, ensure_ascii=False),
            )
            crawl_count += 1
        except CrawlValidationError as e:
            logger.error("크롤링 검증 실패: %s", e)
            try:
                from core.notify import send_monitor_alert  # noqa: C0415

                send_monitor_alert(
                    title=f"\u26a0\ufe0f {ref.source} API 구조 변경 감지",
                    description=(
                        f"**{ref.source}** API 응답 구조가 예상과 다릅니다.\n\n"
                        f"**공고**: `{ref.external_id}`\n"
                        f"**오류**: {e}"
                    ),
                    color=0xFF4444,
                )
            except Exception:
                logger.exception("검증 실패 알림 전송 실패")
            continue
        except (ValueError, TypeError) as e:
            logger.error("데이터 품질 검증 실패: id=%s, %s", ref.external_id, e)
            try:
                from core.notify import send_monitor_alert  # noqa: C0415

                send_monitor_alert(
                    title=f"\u26a0\ufe0f {ref.source} 데이터 품질 문제",
                    description=(
                        f"**공고**: `{ref.external_id}`\n"
                        f"**오류**: {e}"
                    ),
                )
            except Exception:
                logger.exception("품질 검증 알림 전송 실패")
            continue
        except CircuitOpenError:
            logger.warning("서킷 OPEN, 재시도 대기: source=%s id=%s", ref.source, ref.external_id)
            failures.append({"itemIdentifier": record["messageId"]})
            continue
        except Exception:
            logger.exception("상세 크롤링 실패: id=%s", ref.external_id)
            failures.append({"itemIdentifier": record["messageId"]})
            continue

        time.sleep(random.uniform(1.0, 2.5))

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("CrawledCount", crawl_count)
    metrics.flush()
    return {"statusCode": 200, "batchItemFailures": failures}


def _detail_to_dict(detail: JobDetail) -> dict:
    """JobDetail → JSON-safe dict."""
    data = dataclasses.asdict(detail)
    data["tech_stack"] = list(detail.tech_stack)
    return data
