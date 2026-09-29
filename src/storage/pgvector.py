"""PostgreSQL + pgvector 스토리지 클라이언트. 임베딩과 JD 를 저장한다."""
import logging
from collections.abc import Iterable
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from crawler.base import JobDetail

logger = logging.getLogger(__name__)

_BATCH_SIZE = 500

_CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS job_descriptions (
    url         TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    external_id TEXT NOT NULL,
    company_name TEXT NOT NULL,
    title       TEXT NOT NULL,
    document    TEXT NOT NULL,
    embedding   vector(768),
    deadline    BIGINT NOT NULL DEFAULT 0,
    crawled_at  TEXT NOT NULL,
    tech_stack  TEXT NOT NULL DEFAULT '',
    career_level TEXT NOT NULL DEFAULT ''
)
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_job_descriptions_embedding
ON job_descriptions USING hnsw (embedding vector_cosine_ops)
"""

_UPSERT = """
INSERT INTO job_descriptions
    (url, source, external_id, company_name, title, document, embedding, deadline, crawled_at, tech_stack, career_level)
VALUES
    (%(url)s, %(source)s, %(external_id)s, %(company_name)s, %(title)s,
     %(document)s, %(embedding)s, %(deadline)s, %(crawled_at)s, %(tech_stack)s, %(career_level)s)
ON CONFLICT (url) DO UPDATE SET
    source       = EXCLUDED.source,
    external_id  = EXCLUDED.external_id,
    company_name = EXCLUDED.company_name,
    title        = EXCLUDED.title,
    document     = EXCLUDED.document,
    embedding    = EXCLUDED.embedding,
    deadline     = EXCLUDED.deadline,
    crawled_at   = EXCLUDED.crawled_at,
    tech_stack   = EXCLUDED.tech_stack,
    career_level = EXCLUDED.career_level
"""


class PgVectorStorage:

    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(_CREATE_EXTENSION)
            cur.execute(_CREATE_TABLE)
            cur.execute(_CREATE_INDEX)

    def save(self, detail: JobDetail, *, embedding: list[float] | None = None) -> None:
        """사전 계산된 embedding 과 document/metadata 를 PostgreSQL 에 저장."""
        parts: list[str] = []
        if detail.raw_text:
            parts.append(detail.raw_text)
        if detail.tech_stack:
            parts.append(f"[기술스택]\n{_tech_stack_to_str(detail.tech_stack)}")

        document = "\n\n".join(parts)
        if not document:
            logger.warning("저장할 텍스트 없음: %s", detail.url)
            return

        params = {
            "url": detail.url,
            "source": detail.source,
            "external_id": detail.external_id,
            "company_name": detail.company_name,
            "title": detail.title,
            "document": document,
            "embedding": _to_pg_vector(embedding),
            "deadline": _deadline_to_ts(detail.deadline),
            "crawled_at": detail.crawled_at,
            "tech_stack": _tech_stack_to_str(detail.tech_stack),
            "career_level": detail.career_level,
        }

        with self.conn.cursor() as cur:
            cur.execute(_UPSERT, params)
        logger.info("저장 완료: %s - %s", detail.company_name, detail.title)

    def delete_expired(self, now_ts: int) -> int:
        """마감일이 지난 공고 삭제. 상시채용(deadline=0)은 제외."""
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM job_descriptions WHERE deadline < %s AND deadline > 0",
                (now_ts,),
            )
            deleted = cur.rowcount

        if deleted > 0:
            logger.info("마감 공고 %d건 삭제", deleted)
        return deleted

    def get_all_urls(self) -> set[str]:
        """저장된 모든 공고 URL 을 조회."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT url FROM job_descriptions")
            return {row[0] for row in cur.fetchall()}

    def close(self) -> None:
        self.conn.close()


def _tech_stack_to_str(tech_stack: Iterable[str]) -> str:
    return ", ".join(tech_stack)


def _deadline_to_ts(deadline: str | int) -> int:
    """마감일을 Unix timestamp(초)로 변환. int 는 그대로 반환, 빈 값은 0."""
    if isinstance(deadline, int):
        return deadline
    if not deadline:
        return 0
    try:
        dt = datetime.fromisoformat(deadline)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return 0


def _to_pg_vector(embedding: list[float] | None) -> str | None:
    """embedding 리스트를 pgvector 문자열 형식으로 변환."""
    if embedding is None:
        return None
    return f"[{','.join(str(v) for v in embedding)}]"
