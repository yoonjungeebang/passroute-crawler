"""passroute-crawler Lambda 핸들러 진입점.

Handler 가 app.xxx 를 참조하므로,
각 handlers 모듈에서 핸들러 함수와 필요한 이름을 re-export 한다.

Stage 1 - Crawl:
  EventBridge cron → [job_list_collector] → SQS → [job_crawl] → SQS(EmbedQueue)
Stage 2 - Embed:
  SQS(EmbedQueue) → [embed_worker] → SQS(DbLoadQueue)
Stage 3 - Load:
  SQS(DbLoadQueue) → [db_loader] → PostgreSQL(pgvector)
"""

# ── 공통 의존성 re-export (테스트의 @patch("app.xxx") 경로 유지) ──
import boto3

from handlers._common import get_pg_storage as _get_pg_storage

# ── 크롤러 레지스트리 re-export (테스트의 @patch("app.get_crawler") 등) ──
from crawler.registry import get_crawler, iter_sources

# ── 핸들러 함수 re-export ──
from handlers.collect import (
    job_list_collector,
    source_collect_worker,
    url_index_rebuilder,
    news_collector,
    blog_collector,
)
from handlers.crawl import job_crawl
from handlers.embed import embed_worker
from handlers.load import db_loader
from handlers.api import search_api, company_collect

# ── time re-export (테스트의 @patch("app.time.sleep")) ──
import time
