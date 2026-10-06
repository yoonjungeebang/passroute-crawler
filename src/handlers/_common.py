"""핸들러 공통 유틸리티."""
import logging
import os

from storage.s3 import S3Storage

logger = logging.getLogger(__name__)


# SQS SendMessageBatch API의 최대 메시지 수 (AWS 제한: 10개)
SQS_BATCH_SIZE = 10


def archive_and_delete(s3, bucket: str, key: str) -> None:
    """S3 객체를 archive/ 프리픽스로 복사한 뒤 원본을 삭제한다.

    copy → delete 는 원자적이지 않다. 두 단계 사이에 Lambda가 죽으면
    원본이 남아 SQS 재전달로 재처리될 수 있지만, 다운스트림 핸들러가
    멱등적(UPSERT, head_object 중복 체크)이므로 데이터 정합성에 영향 없다.
    archive/ 객체는 S3 Lifecycle 규칙에 의해 14일 후 자동 만료된다.
    """
    archive_key = f"archive/{key}"
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=archive_key,
    )
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("아카이브 완료: %s → %s", key, archive_key)


def required_env(name: str) -> str:
    """환경변수를 읽어오고, 없으면 즉시 에러를 발생시켜 Lambda 실행을 중단."""
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"필수 환경변수 누락: {name}")
    return val


def make_storage() -> S3Storage:
    """S3_BUCKET 환경변수에서 버킷 이름을 읽어 S3Storage 인스턴스 생성."""
    return S3Storage(bucket=required_env("S3_BUCKET"))


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
