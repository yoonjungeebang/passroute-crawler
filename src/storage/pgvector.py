"""PostgreSQL + pgvector 스토리지 클라이언트. 임베딩과 JD 를 저장한다."""
import logging
from collections.abc import Iterable
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from crawler.base import JobDetail

logger = logging.getLogger(__name__)

_CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"
_CREATE_EXTENSION_TRGM = "CREATE EXTENSION IF NOT EXISTS pg_trgm"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS job_descriptions (
    url         TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    external_id TEXT NOT NULL,
    company_name TEXT NOT NULL,
    title       TEXT NOT NULL,
    document    TEXT NOT NULL,
    embedding   vector(768),
    embedding_status TEXT NOT NULL DEFAULT 'ok',
    deadline    BIGINT NOT NULL DEFAULT 0,
    crawled_at  TEXT NOT NULL,
    tech_stack  TEXT NOT NULL DEFAULT '',
    career_level TEXT NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_job_descriptions_embedding
ON job_descriptions USING hnsw (embedding vector_cosine_ops)
"""

_CREATE_INDEX_SOURCE = """
CREATE INDEX IF NOT EXISTS idx_job_descriptions_source
ON job_descriptions (source)
"""

_CREATE_INDEX_DEADLINE = """
CREATE INDEX IF NOT EXISTS idx_job_descriptions_deadline
ON job_descriptions (deadline) WHERE deadline > 0
"""

_CREATE_INDEX_COMPANY_TRGM = """
CREATE INDEX IF NOT EXISTS idx_job_descriptions_company_trgm
ON job_descriptions USING gin (company_name gin_trgm_ops)
"""

_UPSERT = """
INSERT INTO job_descriptions
    (url, source, external_id, company_name, title, document, embedding, embedding_status, deadline, crawled_at, tech_stack, career_level, updated_at)
VALUES
    (%(url)s, %(source)s, %(external_id)s, %(company_name)s, %(title)s,
     %(document)s, %(embedding)s, %(embedding_status)s, %(deadline)s, %(crawled_at)s, %(tech_stack)s, %(career_level)s, NOW())
ON CONFLICT (url) DO UPDATE SET
    source           = EXCLUDED.source,
    external_id      = EXCLUDED.external_id,
    company_name     = EXCLUDED.company_name,
    title            = EXCLUDED.title,
    document         = EXCLUDED.document,
    embedding        = EXCLUDED.embedding,
    embedding_status = EXCLUDED.embedding_status,
    deadline         = EXCLUDED.deadline,
    crawled_at       = EXCLUDED.crawled_at,
    tech_stack       = EXCLUDED.tech_stack,
    career_level     = EXCLUDED.career_level,
    updated_at       = NOW()
"""


class PgVectorStorage:

    def __init__(self, dsn: str):
        self._dsn = dsn
        self.conn = self._connect()
        self._ensure_schema()

    def _connect(self):
        """새 DB 커넥션을 생성한다."""
        conn = psycopg2.connect(self._dsn)
        conn.autocommit = True
        return conn

    def _ensure_alive(self) -> None:
        """커넥션이 끊어졌으면 재연결한다. Lambda 웜 스타트 시 stale 커넥션 방지."""
        try:
            if self.conn.closed:
                raise psycopg2.OperationalError("connection closed")
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("PostgreSQL 접속 끊김, 재접속 시도")
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = self._connect()

    def _ensure_schema(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(_CREATE_EXTENSION)
            cur.execute(_CREATE_EXTENSION_TRGM)
            cur.execute(_CREATE_TABLE)
            cur.execute("ALTER TABLE job_descriptions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
            cur.execute("ALTER TABLE job_descriptions ADD COLUMN IF NOT EXISTS embedding_status TEXT NOT NULL DEFAULT 'ok'")
            cur.execute(_CREATE_INDEX)
            cur.execute(_CREATE_INDEX_SOURCE)
            cur.execute(_CREATE_INDEX_DEADLINE)
            cur.execute(_CREATE_INDEX_COMPANY_TRGM)

    def save(self, detail: JobDetail, *, embedding: list[float] | None = None, embedding_status: str = "ok") -> None:
        """사전 계산된 embedding 과 document/metadata 를 PostgreSQL 에 저장."""
        self._ensure_alive()

        parts: list[str] = []
        if detail.raw_text:
            parts.append(detail.raw_text)
        if detail.tech_stack:
            parts.append(f"[기술스택]\n{_tech_stack_to_str(detail.tech_stack)}")

        document = "\n\n".join(parts)
        if not document:
            logger.warning(
                "저장할 텍스트 없음: %s", detail.url)
            return

        params = {
            "url": detail.url,
            "source": detail.source,
            "external_id": detail.external_id,
            "company_name": detail.company_name,
            "title": detail.title,
            "document": document,
            "embedding": _to_pg_vector(embedding),
            "embedding_status": embedding_status,
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
        self._ensure_alive()

        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM job_descriptions WHERE deadline < %s AND deadline > 0",
                (now_ts,),
            )
            deleted = cur.rowcount

        if deleted > 0:
            logger.info("마감 공고 %d건 삭제", deleted)
        return deleted

    def get_all_urls(self, source: str | None = None) -> set[str]:
        """저장된 공고 URL 을 조회. source 지정 시 해당 소스만 조회."""
        self._ensure_alive()

        urls: set[str] = set()
        with self.conn.cursor() as cur:
            if source:
                cur.execute("SELECT url FROM job_descriptions WHERE source = %s", (source,))
            else:
                cur.execute("SELECT url FROM job_descriptions")
            while True:
                batch = cur.fetchmany(2000)
                if not batch:
                    break
                urls.update(row[0] for row in batch)
        return urls

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _tech_stack_to_str(tech_stack: Iterable[str]) -> str:
    return ", ".join(tech_stack)


def _deadline_to_ts(deadline: str | int) -> int:
    """마감일을 Unix timestamp(초)로 변환. int 는 그대로 반환, 빈 값은 0."""
    if isinstance(deadline, int):
        return deadline
    if not deadline:
        return 0
    # 숫자형 문자열 (크롤러가 str(int(ts)) 형태로 반환하는 경우)
    try:
        ts = int(deadline)
        if ts > 0:
            return ts
    except ValueError:
        pass
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
