"""PostgreSQL + pgvector 스토리지 클라이언트. 임베딩과 JD 를 저장한다."""
import logging
from collections.abc import Iterable
import psycopg2
import psycopg2.extras

from crawler.base import JobDetail

logger = logging.getLogger(__name__)

_CREATE_TABLE_SOURCE_CRAWL_PROGRESS = """
CREATE TABLE IF NOT EXISTS source_crawl_progress (
    request_id      TEXT PRIMARY KEY,
    company_name    TEXT NOT NULL,
    source          TEXT NOT NULL,
    total_jobs      INT NOT NULL,
    completed_jobs  INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
)
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
    embedding        = COALESCE(EXCLUDED.embedding, job_descriptions.embedding),
    embedding_status = CASE WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding_status ELSE job_descriptions.embedding_status END,
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
        self._ensure_crawler_tables()

    def _connect(self):
        """새 DB 커넥션을 생성한다."""
        conn = psycopg2.connect(self._dsn)
        conn.autocommit = True
        return conn

    def _ensure_crawler_tables(self) -> None:
        """크롤러 전용 테이블을 생성한다. IF NOT EXISTS로 멱등적."""
        with self.conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SOURCE_CRAWL_PROGRESS)

    def _ensure_alive(self) -> None:
        """커넥션이 끊어졌으면 재연결한다. Lambda 웜 스타트 시 stale 커넥션 방지.

        DB failover 시 재연결이 실패할 수 있으므로 지수 백오프로 최대 3회 재시도한다.
        고정 간격 재시도는 DB 과부하 시 부하를 악화시켜 연쇄 장애를 유발할 수 있다.
        """
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
                        delay = (attempt + 1) ** 2  # 1초, 4초
                        time.sleep(delay)
                        logger.warning("재접속 실패, %.0f초 후 재시도 %d/2", delay, attempt + 1)
                    else:
                        raise

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

    def save_batch(self, items: list[tuple[JobDetail, list[float] | None, str]]) -> int:
        """여러 레코드를 한번에 UPSERT. 각 항목은 (detail, embedding, embedding_status) 튜플.

        Returns:
            저장된 건수.
        """
        if not items:
            return 0

        self._ensure_alive()

        params_list = []
        for detail, embedding, embedding_status in items:
            parts: list[str] = []
            if detail.raw_text:
                parts.append(detail.raw_text)
            if detail.tech_stack:
                parts.append(f"[기술스택]\n{_tech_stack_to_str(detail.tech_stack)}")
            document = "\n\n".join(parts)
            if not document:
                continue

            params_list.append({
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
            })

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

    def get_existing_urls(self, candidate_urls: list[str]) -> set[str]:
        """후보 URL 중 이미 저장된 것만 반환. PK 인덱스 룩업으로 동작."""
        self._ensure_alive()

        if not candidate_urls:
            return set()

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT url FROM job_descriptions WHERE url = ANY(%s)",
                (candidate_urls,),
            )
            return {row[0] for row in cur.fetchall()}

    def init_source_progress(
        self, request_id: str, company_name: str, source: str, total_jobs: int,
    ) -> None:
        """소스 단위 수집 진행률 추적을 초기화한다."""
        self._ensure_alive()
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO source_crawl_progress (request_id, company_name, source, total_jobs) "
                "VALUES (%s, %s, %s, %s)",
                (request_id, company_name, source, total_jobs),
            )
        logger.info(
            "소스 진행률 초기화: request_id=%s company=%s total=%d",
            request_id, company_name, total_jobs,
        )

    def mark_job_loaded(self, request_id: str) -> bool:
        """공고 1건 적재 완료를 기록하고, 소스 전체 완료 여부를 반환한다.

        Returns:
            True 이면 이번 적재로 해당 소스의 모든 공고가 완료됨.
        """
        self._ensure_alive()
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE source_crawl_progress "
                "SET completed_jobs = completed_jobs + 1, "
                "    status = CASE "
                "        WHEN completed_jobs + 1 >= total_jobs THEN 'COMPLETED' "
                "        ELSE status END, "
                "    completed_at = CASE "
                "        WHEN completed_jobs + 1 >= total_jobs THEN NOW() "
                "        ELSE completed_at END "
                "WHERE request_id = %s "
                "RETURNING completed_jobs >= total_jobs",
                (request_id,),
            )
            row = cur.fetchone()
            return bool(row and row[0])

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


def _deadline_to_ts(deadline: int) -> int:
    """마감일 Unix timestamp(초). int 를 그대로 반환."""
    return deadline if isinstance(deadline, int) else 0


def _to_pg_vector(embedding: list[float] | None) -> str | None:
    """embedding 리스트를 pgvector 문자열 형식으로 변환."""
    if embedding is None:
        return None
    return f"[{','.join(str(v) for v in embedding)}]"
