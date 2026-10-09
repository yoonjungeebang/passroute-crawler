"""PostgreSQL 스토리지 클라이언트. 공고 데이터와 임베딩을 저장한다."""
import logging
# collections.abc: 추상 베이스 클래스 모음. Iterable = "반복 가능한 객체" 타입.
from collections.abc import Iterable

# psycopg2: 파이썬에서 PostgreSQL 데이터베이스에 연결하고 SQL을 실행하는 라이브러리.
import psycopg2
# psycopg2.extras: 배치 실행(execute_batch) 등 추가 기능 제공.
import psycopg2.extras

# import A as B: A를 B라는 이름으로 가져오기 (이름 충돌 방지용)
from core.embedding import build_document as _build_document
from crawler.base import JobDetail

logger = logging.getLogger(__name__)

# 여러 줄 문자열(triple-quoted string): """ ... """ 안에 여러 줄을 자유롭게 쓸 수 있다.
# 여기서는 SQL 쿼리를 담고 있다.
#
# SQL UPSERT: INSERT + ON CONFLICT 로 구성.
#   → 데이터가 없으면 INSERT (삽입), 이미 있으면(url 중복) UPDATE (갱신).
#
# %(변수명)s: psycopg2 의 Named Parameter 문법.
#   파이썬 딕셔너리의 키를 SQL에 바인딩한다.
#   예) {"url": "https://..."} → %(url)s 에 "https://..." 가 들어감.
#   SQL 인젝션 방지를 위해 f-string 대신 이 방식을 사용해야 한다.
_UPSERT = """
INSERT INTO job_descriptions
    (url, source, external_id, company_name, title, document, embedding,
     embedding_status, deadline, crawled_at, tech_stack, career_level, updated_at)
VALUES
    (%(url)s, %(source)s, %(external_id)s, %(company_name)s, %(title)s,
     %(document)s, %(embedding)s, %(embedding_status)s, %(deadline)s,
     %(crawled_at)s, %(tech_stack)s, %(career_level)s, NOW())
ON CONFLICT (url) DO UPDATE SET
    source           = EXCLUDED.source,
    external_id      = EXCLUDED.external_id,
    company_name     = EXCLUDED.company_name,
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


def build_document(detail: JobDetail) -> str:
    """JobDetail 로부터 검색용 document 텍스트를 생성한다."""
    return _build_document(detail.raw_text, detail.tech_stack)


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
    """PostgreSQL 데이터베이스에 데이터를 저장하는 클래스."""

    # dsn(Data Source Name): 데이터베이스 접속 정보 문자열.
    # 형식: "postgresql://사용자:비밀번호@호스트:포트/DB이름"
    def __init__(self, dsn: str):
        self._dsn = dsn
        self.conn = self._connect()

    def _connect(self):
        """새 DB 커넥션을 생성한다."""
        # psycopg2.connect(): PostgreSQL에 연결하고 connection 객체를 반환한다.
        conn = psycopg2.connect(self._dsn)
        # autocommit = True: 각 SQL 실행 후 자동으로 커밋(확정).
        # False 이면 conn.commit() 을 명시적으로 호출해야 변경이 확정된다.
        conn.autocommit = True
        return conn

    def _ensure_alive(self) -> None:
        """커넥션이 끊어졌으면 재연결한다. Lambda 웜 스타트 시 stale 커넥션 방지."""
        try:
            if self.conn.closed:  # .closed: 커넥션이 닫혔는지 확인하는 속성
                raise psycopg2.OperationalError("connection closed")
            # cursor: DB에 SQL을 보내는 "커서" 객체.
            # with 문을 쓰면 블록이 끝날 때 커서가 자동으로 닫힌다.
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")  # 헬스체크 쿼리: 응답이 오면 연결이 살아있는 것
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("PostgreSQL 접속 끊김, 재접속 시도")
            try:
                self.conn.close()
            except Exception:
                pass  # pass: "아무것도 하지 않는다". 빈 블록을 만들 때 사용.
            for attempt in range(3):  # 최대 3번 재연결 시도
                try:
                    self.conn = self._connect()
                    return
                except psycopg2.OperationalError:
                    if attempt < 2:  # 마지막 시도가 아니면
                        import time
                        # ** : 거듭제곱 연산자. (attempt+1)**2 → 1, 4, 9 (점점 늘어나는 대기)
                        delay = (attempt + 1) ** 2
                        time.sleep(delay)
                        logger.warning("재접속 실패, %.0f초 후 재시도 %d/2", delay, attempt + 1)
                    else:
                        raise  # raise (인자 없이): 현재 잡고 있는 예외를 다시 던진다.

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
        """여러 레코드를 한번에 UPSERT.

        각 항목은 (detail, embedding, embedding_status) 튜플.
        Returns: 저장된 건수.
        """
        if not items:
            return 0

        self._ensure_alive()
        params_list = []
        # 튜플 언패킹: (detail, embedding, embedding_status) 세 변수에 한꺼번에 대입
        for detail, embedding, embedding_status in items:
            document = build_document(detail)
            if not document:
                continue
            params_list.append(_build_params(detail, document, embedding, embedding_status))

        if not params_list:
            return 0

        # 트랜잭션 시작: autocommit을 끄면 여러 SQL을 하나의 단위로 묶을 수 있다.
        # 중간에 에러가 나면 전체를 취소(rollback)할 수 있다.
        self.conn.autocommit = False
        try:
            with self.conn.cursor() as cur:
                # SET LOCAL: 이 트랜잭션 안에서만 적용되는 설정. 30초 이내에 끝나야 함.
                cur.execute("SET LOCAL statement_timeout = '30s'")
                # execute_batch: 여러 개의 SQL을 한 번에 실행 (성능 최적화)
                # 각 params_list 요소가 _UPSERT 쿼리에 바인딩되어 실행된다.
                psycopg2.extras.execute_batch(cur, _UPSERT, params_list)
            self.conn.commit()    # 모든 변경을 확정
        except Exception:
            self.conn.rollback()  # 에러 발생 시 모든 변경을 취소
            raise
        # finally: try/except 와 함께 사용. 에러 발생 여부와 관계없이 항상 실행된다.
        # 자원 정리(cleanup)에 사용.
        finally:
            self.conn.autocommit = True  # 원래대로 복원
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
                # %s: psycopg2 의 위치 기반 파라미터. 두 번째 인자 튜플에서 순서대로 바인딩.
                # (source,): 요소가 1개인 튜플. 쉼표 필수!
                cur.execute("SELECT url FROM job_descriptions WHERE source = %s", (source,))
            else:
                cur.execute("SELECT url FROM job_descriptions")

            # while True: 무한 루프. 내부에서 break로 빠져나온다.
            while True:
                # fetchmany(N): 쿼리 결과를 N건씩 가져온다.
                # 결과가 수만 건이면 fetchall()로 한번에 메모리에 올리는 것보다 효율적.
                batch = cur.fetchmany(2000)
                if not batch:  # 더 이상 결과가 없으면 종료
                    break
                # set.update(): 여러 요소를 한번에 set에 추가.
                # row[0]: 각 행(row)의 첫 번째 컬럼 (= url)
                # 제너레이터 표현식으로 각 행에서 url만 추출해서 추가.
                urls.update(row[0] for row in batch)
        return urls

    def get_existing_urls(self, candidate_urls: list[str]) -> set[str]:
        """후보 URL 중 이미 저장된 것만 반환. PK 인덱스 룩업으로 동작."""
        self._ensure_alive()
        if not candidate_urls:
            return set()  # 빈 set 반환
        with self.conn.cursor() as cur:
            # ANY(%s): SQL의 IN 절과 비슷. 리스트의 어느 값이든 매칭.
            cur.execute(
                "SELECT url FROM job_descriptions WHERE url = ANY(%s)",
                (candidate_urls,),
            )
            # set 컴프리헨션: {표현식 for 변수 in 반복} → set을 만든다.
            # 리스트 컴프리헨션 [...]과 비슷하지만 {}를 써서 중복 없는 set을 만든다.
            # fetchall(): 남은 쿼리 결과를 모두 가져와서 리스트로 반환.
            return {row[0] for row in cur.fetchall()}

    def close(self) -> None:
        """DB 연결을 닫는다."""
        try:
            self.conn.close()
        except Exception:
            pass

    # __enter__, __exit__: 컨텍스트 매니저 프로토콜.
    # 이 두 메서드를 구현하면 with 문에서 사용할 수 있다:
    #   with PgVectorStorage(dsn) as pg:
    #       pg.save(...)
    #   # 블록을 나오면 자동으로 pg.close() 호출됨
    def __enter__(self):
        return self

    # *exc: 가변 인자. 예외 정보가 튜플로 전달된다 (예외 없으면 (None, None, None)).
    def __exit__(self, *exc):
        self.close()


def _to_pg_vector(embedding: list[float] | None) -> str | None:
    """embedding 리스트를 pgvector 문자열 형식으로 변환."""
    if embedding is None:
        return None
    # ','.join(): 리스트의 각 요소를 ','로 연결한 문자열 생성.
    # str(v) for v in embedding: 각 float를 문자열로 변환하는 제너레이터 표현식.
    # 결과 예: "[0.1,0.2,0.3,...]" (pgvector가 요구하는 형식)
    return f"[{','.join(str(v) for v in embedding)}]"
