"""passroute-crawler Lambda 핸들러 진입점.

Handler 가 app.xxx 를 참조하므로,
각 handlers 모듈에서 핸들러 함수와 필요한 이름을 re-export 한다.

공고 수집 파이프라인:
  EventBridge cron → [job_list_collector] → SQS → [source_collect_worker]
    → SQS → [job_crawl] → SQS → [embed_worker] → SQS → [db_loader] → PostgreSQL

기업별 실시간 수집 (stale-while-revalidate):
  WebSocket [ws_collect] → SQS → [company_collect_worker]
    → 캐시 있으면 즉시 WebSocket 푸시(cached) → 실시간 수집 → 최신 데이터 푸시(done)
    → 쿨다운 중이면 캐시만 반환(cache_only)

조회/관리:
  API Gateway → [company_query]
  EventBridge cron → [delete_expired_jobs]
  SNS → [discord_notifier], cron → [dlq_daily_check]
"""

# ── 공통 의존성 re-export (테스트의 @patch("app.xxx") 경로 유지) ──
import boto3

# ── 크롤러 레지스트리 re-export (테스트의 @patch("app.get_crawler") 등) ──
from crawler.registry import get_crawler, iter_sources

# ── 핸들러 함수 re-export ──
from handlers.job_cleanup import delete_expired_jobs
from handlers.job_dispatch import job_list_collector
from handlers.job_list import source_collect_worker
from handlers.job_detail import job_crawl
from handlers.job_embed import embed_worker
from handlers.job_load import db_loader
from handlers.company_query import company_query
from handlers.websocket import ws_authorizer, ws_connect, ws_disconnect, ws_collect
from handlers.company_content_worker import company_collect_worker
from handlers.discord_notify import discord_notifier, dlq_daily_check

# ── time re-export (테스트의 @patch("app.time.sleep")) ──
import time
