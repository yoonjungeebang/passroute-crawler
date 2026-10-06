"""db_loader Lambda 핸들러 단위 테스트.

S3, PgVectorStorage, Secrets Manager 는 mock 으로 대체한다.
EventBridge S3 이벤트가 SQS 로 래핑된 형태의 이벤트를 입력으로 사용한다.
"""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import app


def _make_s3_event(bucket: str, key: str) -> dict:
    """EventBridge S3 이벤트를 SQS 로 래핑한 형태의 이벤트를 생성한다."""
    envelope = {
        "version": "0",
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": {"name": bucket},
            "object": {"key": key, "size": 1234},
        },
    }
    return {"Records": [{"body": json.dumps(envelope)}]}


def _parsed_json_data(**overrides) -> dict:
    """S3 에 저장된 parsed JSON 데이터를 모사한다."""
    base = {
        "source": "jobkorea",
        "external_id": "123",
        "url": "https://www.jobkorea.co.kr/Recruit/GI_Read/123",
        "company_name": "A사",
        "title": "백엔드",
        "raw_text": "주요업무: 백엔드 서비스 개발 및 운영\n자격요건: Python 3년 이상",
        "tech_stack": ["Python"],
        "deadline": 1777648000,
        "crawled_at": "2026-04-12T18:00:00+09:00",
        "career_level": "",
        "embedding": [0.1] * 768,
    }
    base.update(overrides)
    return base


def _mock_s3_get_object(mock_s3: MagicMock, data: dict) -> None:
    """get_object 가 지정된 dict 를 JSON 으로 반환하도록 설정한다."""
    body_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: body_bytes),
    }


@patch("handlers.load.get_pg_storage")
@patch("handlers.load.boto3.client")
def test_db_loader_saves_parsed_file(mock_boto_client, mock_get_pg):
    """parsed/ 이벤트 시 PgVectorStorage.save() 가 호출되고 S3 객체가 삭제된다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    data = _parsed_json_data()
    _mock_s3_get_object(mock_s3, data)

    mock_pg = MagicMock()
    mock_get_pg.return_value = mock_pg

    event = _make_s3_event("test-bucket", "parsed/jobkorea/123.json")
    app.db_loader(event, None)

    mock_pg.save.assert_called_once()
    saved_detail = mock_pg.save.call_args.args[0]
    assert saved_detail.external_id == "123"
    assert saved_detail.tech_stack == ("Python",)
    assert mock_pg.save.call_args.kwargs["embedding"] == [0.1] * 768

    # parsed/ 아카이브 후 삭제
    mock_s3.copy_object.assert_called_once_with(
        Bucket="test-bucket",
        CopySource={"Bucket": "test-bucket", "Key": "parsed/jobkorea/123.json"},
        Key="archive/parsed/jobkorea/123.json",
    )
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="parsed/jobkorea/123.json",
    )


@patch("handlers.load.get_pg_storage")
@patch("handlers.load.boto3.client")
def test_db_loader_processes_delete_request(mock_boto_client, mock_get_pg):
    """delete-requests/ 이벤트 시 PgVectorStorage.delete_expired() 가 호출된다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    _mock_s3_get_object(mock_s3, {
        "now_ts": 1776164400,
        "requested_at": "20260412T180000",
    })

    mock_pg = MagicMock()
    mock_pg.delete_expired.return_value = 3
    mock_get_pg.return_value = mock_pg

    event = _make_s3_event("test-bucket", "delete-requests/20260412T180000.json")
    app.db_loader(event, None)

    call_arg = mock_pg.delete_expired.call_args.args[0]
    assert isinstance(call_arg, int)
    assert call_arg == 1776164400

    # delete-requests/ 아카이브 후 삭제
    mock_s3.copy_object.assert_called_once_with(
        Bucket="test-bucket",
        CopySource={"Bucket": "test-bucket", "Key": "delete-requests/20260412T180000.json"},
        Key="archive/delete-requests/20260412T180000.json",
    )
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="delete-requests/20260412T180000.json",
    )


@patch("handlers.load.get_pg_storage")
@patch("handlers.load.boto3.client")
def test_db_loader_skips_non_json(mock_boto_client, mock_get_pg):
    """.json 이 아닌 키는 무시한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    mock_pg = MagicMock()
    mock_get_pg.return_value = mock_pg

    event = _make_s3_event("test-bucket", "parsed/jobkorea/readme.txt")
    app.db_loader(event, None)

    mock_pg.save.assert_not_called()
    mock_s3.get_object.assert_not_called()


@patch("handlers.load.get_pg_storage")
@patch("handlers.load.boto3.client")
def test_db_loader_reraises_on_failure(mock_boto_client, mock_get_pg):
    """에러 발생 시 SQS 재시도를 위해 예외가 전파된다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.get_object.side_effect = RuntimeError("S3 접근 실패")

    mock_pg = MagicMock()
    mock_get_pg.return_value = mock_pg

    event = _make_s3_event("test-bucket", "parsed/jobkorea/123.json")

    with pytest.raises(RuntimeError, match="S3 접근 실패"):
        app.db_loader(event, None)


@patch("handlers.load.get_pg_storage")
@patch("handlers.load.boto3.client")
def test_db_loader_handles_legacy_now_iso(mock_boto_client, mock_get_pg):
    """기존 now_iso 형식의 삭제 요청도 timestamp 로 변환하여 처리한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    _mock_s3_get_object(mock_s3, {
        "now_iso": "2026-04-12T18:00:00+09:00",
        "requested_at": "20260412T180000",
    })

    mock_pg = MagicMock()
    mock_pg.delete_expired.return_value = 1
    mock_get_pg.return_value = mock_pg

    event = _make_s3_event("test-bucket", "delete-requests/20260412T180000.json")
    app.db_loader(event, None)

    call_args = mock_pg.delete_expired.call_args.args[0]
    assert isinstance(call_args, int)


@patch("handlers.load.get_pg_storage")
@patch("handlers.load.boto3.client")
def test_db_loader_skips_unknown_prefix(mock_boto_client, mock_get_pg):
    """parsed/ 또는 delete-requests/ 가 아닌 경로는 무시한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    mock_pg = MagicMock()
    mock_get_pg.return_value = mock_pg

    event = _make_s3_event("test-bucket", "unknown/something.json")
    app.db_loader(event, None)

    mock_pg.save.assert_not_called()
    mock_pg.delete_expired.assert_not_called()
    mock_s3.get_object.assert_not_called()
