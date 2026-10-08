"""핸들러 공통 유틸리티."""
import logging
import os

logger = logging.getLogger(__name__)


# SQS SendMessageBatch API의 최대 메시지 수 (AWS 제한: 10개)
SQS_BATCH_SIZE = 10


def required_env(name: str) -> str:
    """환경변수를 읽어오고, 없으면 즉시 에러를 발생시켜 Lambda 실행을 중단."""
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"필수 환경변수 누락: {name}")
    return val


# 모듈 레벨 싱글턴 — Lambda 웜 스타트 시 DB 커넥션을 재사용하기 위함
_pg_storage = None


def get_pg_storage():
    """PgVectorStorage 싱글턴. Lambda 웜 스타트 시 커넥션 재사용."""
    global _pg_storage
    if _pg_storage is None:
        from core.secrets import get_database_url  # noqa: C0415
        from storage.pgvector import PgVectorStorage  # noqa: C0415

        _pg_storage = PgVectorStorage(dsn=get_database_url())
    return _pg_storage


# 쿼리/회사명 입력의 최대 길이 제한 (악의적 입력 방지)
MAX_QUERY_LENGTH = 200
MAX_COMPANY_LENGTH = 50


def api_error(status: int, message: str) -> dict:
    """API Gateway 에러 응답을 생성한다."""
    import json
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }


def safe_int(value: str, *, default: int, min_val: int = 1, max_val: int = 100) -> int:
    """문자열을 정수로 변환한다. 실패 시 default, 범위 초과 시 클램핑."""
    try:
        n = int(value)
    except (ValueError, TypeError):
        return default
    return max(min_val, min(n, max_val))
