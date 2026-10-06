"""passroute-crawler Lambda 핸들러 진입점.

SAM template.yaml 의 Handler 가 app.xxx 를 참조하므로,
각 handlers 모듈에서 핸들러 함수와 필요한 이름을 re-export 한다.

Stage 1 - Crawl:
  EventBridge cron → [job_list_collector] → SQS → [job_crawl] → S3(raw/)
Stage 2 - Embed:
  S3(raw/) → EventBridge → SQS → [embed_worker] → S3(parsed/)
Stage 3 - Load:
  S3(parsed/) → EventBridge → SQS → [db_loader] → PostgreSQL(pgvector)
"""

# ── 공통 의존성 re-export (테스트의 @patch("app.xxx") 경로 유지) ──
import boto3  # noqa: F401
from storage.s3 import S3Storage  # noqa: F401

from handlers._common import get_pg_storage as _get_pg_storage  # noqa: F401
from handlers._common import make_storage as _make_storage  # noqa: F401

# ── 크롤러 레지스트리 re-export (테스트의 @patch("app.get_crawler") 등) ──
from crawler.registry import get_crawler, iter_sources  # noqa: F401

# ── 핸들러 함수 re-export ──
from handlers.collect import (  # noqa: F401
    job_list_collector,
    source_collect_worker,
    url_index_rebuilder,
    news_collector,
    blog_collector,
)
from handlers.crawl import job_crawl  # noqa: F401
from handlers.embed import embed_worker  # noqa: F401
from handlers.load import db_loader  # noqa: F401
from handlers.api import search_api, company_collect  # noqa: F401

# ── time re-export (테스트의 @patch("app.time.sleep")) ──
import time  # noqa: F401
