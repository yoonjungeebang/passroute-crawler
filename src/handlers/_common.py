"""핸들러 공통 유틸리티."""
import logging
import os

from config import load_api_config

logger = logging.getLogger(__name__)

_API_CONFIG = load_api_config()

# SQS SendMessageBatch API의 최대 메시지 수 (AWS 제한: 10개)
SQS_BATCH_SIZE = 10


def required_env(name: str) -> str:
    """환경변수를 읽어오고, 없으면 즉시 에러를 발생시켜 Lambda 실행을 중단."""
    val = os.environ.get(name)
    if not val:
        # RuntimeError: 실행 중 발생하는 일반적인 에러를 나타내는 내장 예외 클래스.
        # ValueError(값이 잘못됨), TypeError(타입이 잘못됨) 등과 구분해서 사용.
        raise RuntimeError(f"필수 환경변수 누락: {name}")
    return val


# 모듈 레벨 변수: 파일이 처음 import 될 때 한 번 실행되고, 이후에는 값이 유지된다.
# 싱글턴(singleton) 패턴: 객체를 하나만 만들어서 재사용하는 패턴.
# Lambda 웜 스타트 시 DB 커넥션을 재사용하기 위함.
_pg_storage = None  # None: 파이썬의 "값이 없음"을 나타내는 특수 객체


def get_pg_storage():
    """PgVectorStorage 싱글턴. Lambda 웜 스타트 시 커넥션 재사용."""

    # global 키워드: 함수 안에서 모듈 레벨 변수를 수정하려면 반드시 선언해야 한다.
    # global 없이 _pg_storage = ... 하면 파이썬은 새로운 지역변수를 만들어 버린다.
    global _pg_storage

    # is None: None 인지 확인할 때는 == 대신 is 를 사용하는 것이 관례.
    # is는 "같은 객체인가"를 비교, ==는 "같은 값인가"를 비교.
    if _pg_storage is None:
        # 함수 안에서 import 하는 이유: 순환 import(A가 B를 import하고 B가 A를 import)를 방지하거나,
        # import 시점을 늦춰서(lazy import) 불필요한 모듈 로딩을 피하기 위함.
        # noqa: C0415 — "이 줄은 lint 경고를 무시해라"는 주석 (함수 내부 import 경고 억제)
        from core.secrets import get_database_url  # noqa: C0415
        from storage.postgres import PgVectorStorage  # noqa: C0415

        _pg_storage = PgVectorStorage(dsn=get_database_url())
    return _pg_storage


# 쿼리/회사명 입력의 최대 길이 제한 (악의적 입력 방지)
MAX_QUERY_LENGTH: int = _API_CONFIG["max_query_length"]
MAX_COMPANY_LENGTH: int = _API_CONFIG["max_company_length"]


def api_error(status: int, message: str) -> dict:
    """API Gateway 에러 응답을 생성한다."""
    import json
    # 딕셔너리 리터럴: { } 안에 키: 값 쌍을 쉼표로 나열해서 만든다.
    # json.dumps(): 파이썬 딕셔너리를 JSON 문자열로 변환.
    #   {"error": "메시지"} → '{"error": "메시지"}'
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }


def safe_int(value: str, *, default: int, min_val: int = 1, max_val: int = 100) -> int:
    """문자열을 정수로 변환한다. 실패 시 default, 범위 초과 시 클램핑."""
    try:
        n = int(value)  # int(): 문자열을 정수로 변환. "42" → 42. "abc"면 ValueError 발생.
    except (ValueError, TypeError):
        return default

    # max(a, min(b, c)): 값을 min_val ~ max_val 범위 안으로 제한(clamp).
    # 예) n=150, min_val=1, max_val=100 이면:
    #   min(150, 100) → 100
    #   max(1, 100) → 100
    return max(min_val, min(n, max_val))
