"""크롤링 데이터 검증 유틸리티.

1계층(API 경계): API 응답 구조가 예상과 다를 때 조기 감지.
"""
import logging

logger = logging.getLogger(__name__)


class CrawlValidationError(Exception):
    """크롤링 대상 사이트의 API 구조가 변경되었을 때 발생."""


def require_keys(data: dict, keys: list[str], *, context: str) -> None:
    """API 응답에 필수 키가 존재하는지 확인한다.

    Raises:
        CrawlValidationError: 필수 키가 하나라도 누락된 경우.
    """
    missing = [k for k in keys if k not in data]
    if missing:
        raise CrawlValidationError(
            f"[{context}] 필수 키 누락: {missing}. "
            f"API 구조 변경 가능성. 실제 키: {sorted(data.keys())}"
        )


def require_non_empty(value, field_name: str, *, context: str) -> None:
    """필드 값이 비어있지 않은지 확인한다.

    Raises:
        CrawlValidationError: 값이 None이거나 빈 문자열인 경우.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        raise CrawlValidationError(
            f"[{context}] '{field_name}' 값이 비어있음"
        )
