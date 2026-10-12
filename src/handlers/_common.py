"""핸들러 공통 유틸리티."""
import logging
import os
from config import load_api_config

logger = logging.getLogger(__name__)

_API_CONFIG = load_api_config()

def required_env(name: str) -> str:
    """환경변수를 읽어오고, 없으면 에러를 발생시킨다."""
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"필수 환경변수 누락: {name}")
    return val

# Lambda 웜 스타트 시 DB 커넥션 재사용을 위한 싱글턴
_pg_storage = None

def get_pg_storage():
    """PgVectorStorage 싱글턴. Lambda 웜 스타트 시 커넥션 재사용."""
    global _pg_storage
    if _pg_storage is None:
        from core.secrets import get_database_url  # noqa: C0415
        from storage.postgres import PgVectorStorage  # noqa: C0415

        _pg_storage = PgVectorStorage(dsn=get_database_url())
    return _pg_storage
