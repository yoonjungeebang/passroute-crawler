"""Lambda 핸들러: 상세 크롤링 (SQS 트리거)."""
import json
import logging
import random
import time

from core.circuit_breaker import CircuitOpenError, get_breaker
from crawler.base import JobDetail, JobListingRef
from crawler.registry import get_crawler
from crawler.validation import CrawlValidationError

from ._common import make_storage

logger = logging.getLogger(__name__)


def job_crawl(event, context):
    """SQS 트리거. 공고 1건 상세 크롤링 → S3(raw/) 저장."""
    from core.metrics import MetricsLogger  # noqa: C0415

    t_total = time.time()
    metrics = MetricsLogger(function_name="job_crawl")
    crawl_count = 0
    storage = make_storage()

    for record in event["Records"]:
        message = json.loads(record["body"])
        ref = JobListingRef(
            source=message["source"],
            external_id=message["external_id"],
            url=message["url"],
            title=message.get("title", ""),
            company_name=message.get("company_name", ""),
        )

        crawler = get_crawler(ref.source)
        logger.info("상세 크롤링: source=%s id=%s (%s)", ref.source, ref.external_id, ref.company_name)

        breaker = get_breaker(ref.source)
        try:
            with breaker:
                detail = crawler.fetch_detail(ref)
            if detail is None:
                logger.info("상세 정보 없음, 스킵: id=%s", ref.external_id)
                continue

            storage.save_raw(detail)
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
            breaker.record_failure(e)
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
            logger.warning("서킷 OPEN, 상세 크롤링 스킵: source=%s id=%s", ref.source, ref.external_id)
            continue
        except Exception:
            logger.exception("상세 크롤링 실패: id=%s", ref.external_id)
            raise

        time.sleep(random.uniform(1.0, 2.5))

    metrics.put_duration("TotalDuration", t_total)
    metrics.put_count("CrawledCount", crawl_count)
    metrics.flush()
    return {"statusCode": 200}
