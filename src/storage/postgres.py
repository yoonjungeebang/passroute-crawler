"""PostgreSQL 스토리지 클라이언트. 공고 데이터와 임베딩을 저장한다."""
import logging

import psycopg2
import psycopg2.extras

from crawler.base import JobDetail
from core.normalize import normalize_company_name, normalize_title

logger = logging.getLogger(__name__)

# UPSERT: 데이터가 없으면 INSERT, url 중복이면 UPDATE
_UPSERT = """
INSERT INTO job_descriptions
    (url, source, external_id, company_name, company_name_norm, title, document,
     embedding, embedding_status, deadline, crawled_at, tech_stack, career_level,
     updated_at)
VALUES
    (%(url)s, %(source)s, %(external_id)s, %(company_name)s, %(company_name_norm)s,
     %(title)s, %(document)s, %(embedding)s, %(embedding_status)s, %(deadline)s,
     %(crawled_at)s, %(tech_stack)s, %(career_level)s, NOW())
ON CONFLICT (url) DO UPDATE SET
    source           = EXCLUDED.source,
    external_id      = EXCLUDED.external_id,
    company_name     = EXCLUDED.company_name,
    company_name_norm = EXCLUDED.company_name_norm,
    title            = EXCLUDED.title,
    document         = EXCLUDED.document,
    embedding        = COALESCE(EXCLUDED.embedding, job_descriptions.embedding),
    embedding_status = CASE
        WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding_status
        ELSE job_descriptions.embedding_status END,
    deadline         = EXCLUDED.deadline,
    crawled_at       = EXCLUDED.crawled_at,
    tech_stack       = EXCLUDED.tech_stack,
    career_level     = EXCLUDED.career_level,
    updated_at       = NOW()
"""

_FIND_DUPLICATE = """
SELECT url, source, embedding <=> %(embedding)s AS distance
FROM job_descriptions
WHERE company_name_norm = %(company_name_norm)s
  AND embedding IS NOT NULL
  AND url != %(url)s
ORDER BY distance ASC
LIMIT 1
"""


def build_document(detail: JobDetail) -> str:
    """JobDetail 로부터 검색용 document 텍스트를 생성한다."""
    parts: list[str] = []
    if detail.raw_text:
        parts.append(detail.raw_text)
    if detail.tech_stack:
        parts.append(f"[기술스택]\n{', '.join(detail.tech_stack)}")
    return "\n\n".join(parts)


def _build_params(
    detail: JobDetail,
    document: str,
    embedding: list[float] | None,
    embedding_status: str,
) -> dict:
    """UPSERT 쿼리에 사용할 파라미터 dict 를 생성한다."""
    return {
        "url": detail.url,
        "source": detail.source,
        "external_id": detail.external_id,
        "company_name": detail.company_name,
        "company_name_norm": normalize_company_name(detail.company_name),
        "title": detail.title,
        "document": document,
        "embedding": _to_pg_vector(embedding),
        "embedding_status": embedding_status,
        "deadline": detail.deadline if isinstance(detail.deadline, int) else 0,
        "crawled_at": detail.crawled_at,
        "tech_stack": ", ".join(detail.tech_stack),
        "career_level": detail.career_level,
    }


class PgVectorStorage:
    """PostgreSQL 데이터베이스에 공고 데이터를 저장, 조회, 삭제하는 클래스."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self.conn = self._connect()
        self._migrate()

    def _migrate(self):
        """스키마 마이그레이션. IF NOT EXISTS로 멱등성 보장."""
        with self.conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE job_descriptions"
                " ADD COLUMN IF NOT EXISTS company_name_norm TEXT"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_jd_company_norm"
                " ON job_descriptions (company_name_norm)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_jd_company_source"
                " ON job_descriptions (company_name, source)"
            )

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
            for attempt in range(3):
                try:
                    self.conn = self._connect()
                    return
                except psycopg2.OperationalError:
                    if attempt < 2:
                        import time
                        delay = (attempt + 1) ** 2
                        time.sleep(delay)
                        logger.warning("재접속 실패, %.0f초 후 재시도 %d/2", delay, attempt + 1)
                    else:
                        raise

    def save(self, detail: JobDetail, *, embedding: list[float] | None = None, embedding_status: str = "ok") -> None:
        """공고 1건을 UPSERT 로 저장한다."""
        self._ensure_alive()
        document = build_document(detail)
        if not document:
            logger.warning("저장할 텍스트 없음: %s", detail.url)
            return

        params = _build_params(detail, document, embedding, embedding_status)
        with self.conn.cursor() as cur:
            cur.execute(_UPSERT, params)
        logger.info("저장 완료: %s - %s", detail.company_name, detail.title)

    def save_batch(self, items: list[tuple[JobDetail, list[float] | None, str]]) -> int:
        """여러 레코드를 한번에 UPSERT. 각 항목은 (detail, embedding, embedding_status) 튜플."""
        if not items:
            return 0

        self._ensure_alive()
        params_list = []
        for detail, embedding, embedding_status in items:
            document = build_document(detail)
            if not document:
                continue
            params_list.append(_build_params(detail, document, embedding, embedding_status))

        if not params_list:
            return 0

        self.conn.autocommit = False
        try:
            with self.conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '30s'")
                psycopg2.extras.execute_batch(cur, _UPSERT, params_list)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = True
        logger.info("배치 저장 완료: %d건", len(params_list))
        return len(params_list)

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

    def find_duplicate(
        self,
        company_name_norm: str,
        embedding: list[float],
        exclude_url: str,
        threshold: float = 0.05,
    ) -> dict | None:
        """같은 정규화 회사명 내에서 임베딩 유사도가 임계값 이상인 기존 공고를 찾는다.

        pgvector <=> 연산자는 cosine distance 반환 (0 = 동일, 2 = 정반대).
        threshold=0.05 은 cosine similarity >= 0.95 에 해당.
        """
        self._ensure_alive()
        with self.conn.cursor() as cur:
            cur.execute(_FIND_DUPLICATE, {
                "company_name_norm": company_name_norm,
                "embedding": _to_pg_vector(embedding),
                "url": exclude_url,
            })
            row = cur.fetchone()

        if row and row[2] <= threshold:
            return {"url": row[0], "source": row[1], "distance": row[2]}
        return None

    def get_existing_company_titles(
        self, company_norms: list[str],
    ) -> set[tuple[str, str]]:
        """주어진 정규화 회사명 목록에 해당하는 (company_name_norm, title_norm) 쌍을 반환."""
        self._ensure_alive()
        if not company_norms:
            return set()
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT company_name_norm, title FROM job_descriptions"
                " WHERE company_name_norm = ANY(%s)",
                (list(set(company_norms)),),
            )
            return {
                (row[0], normalize_title(row[1]))
                for row in cur.fetchall()
            }

    def get_existing_urls(self, candidate_urls: list[str]) -> set[str]:
        """후보 URL 중 이미 저장된 것만 반환."""
        self._ensure_alive()
        if not candidate_urls:
            return set()
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT url FROM job_descriptions WHERE url = ANY(%s)",
                (candidate_urls,),
            )
            return {row[0] for row in cur.fetchall()}

    def close(self) -> None:
        """DB 연결을 닫는다."""
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _to_pg_vector(embedding: list[float] | None) -> str | None:
    """embedding 리스트를 pgvector 문자열 형식으로 변환."""
    if embedding is None:
        return None
    return f"[{','.join(str(v) for v in embedding)}]"
