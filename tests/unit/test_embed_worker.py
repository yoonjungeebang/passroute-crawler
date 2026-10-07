"""embed_worker Lambda 핸들러 단위 테스트.

S3 와 임베딩 모듈은 mock 으로 대체한다.
EventBridge S3 이벤트가 SQS 로 래핑된 형태의 이벤트를 입력으로 사용한다.
"""
import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

import app


def _make_raw_event(bucket: str, key: str) -> dict:
    """EventBridge S3 이벤트를 SQS 로 래핑한 형태의 이벤트를 생성한다."""
    envelope = {
        "version": "0",
        "source": "aws.s3",
        "detail-type": "Object Created",
        "detail": {
            "bucket": {"name": bucket},
            "object": {"key": key},
        },
    }
    return {"Records": [{"body": json.dumps(envelope)}]}


def _raw_json_data(**overrides) -> dict:
    base = {
        "source": "jobkorea",
        "external_id": "123",
        "url": "https://www.jobkorea.co.kr/Recruit/GI_Read/123",
        "company_name": "A사",
        "title": "백엔드",
        "raw_text": "주요업무: 백엔드 서비스 개발 및 운영\n자격요건: Python 3년 이상",
        "tech_stack": ["Python"],
        "deadline": "2026-05-01",
        "crawled_at": "2026-04-12T18:00:00+09:00",
        "career_level": "",
    }
    base.update(overrides)
    return base


def _mock_s3_get_object(mock_s3: MagicMock, data: dict) -> None:
    body_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: body_bytes),
    }


def _mock_head_object_not_found(mock_s3: MagicMock) -> None:
    """parsed/ 파일이 존재하지 않는 상태를 설정 (정상 처리 경로)."""
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject",
    )


@patch("core.embedding.embed_text", return_value=[0.1] * 768)
@patch("app.boto3.client")
def test_embed_worker_embeds_and_writes_parsed(mock_boto_client, mock_embed):
    """raw/ JSON 을 읽어 임베딩 후 parsed/ 에 저장하고 raw/ 를 삭제한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    _mock_head_object_not_found(mock_s3)
    data = _raw_json_data()
    _mock_s3_get_object(mock_s3, data)

    event = _make_raw_event("test-bucket", "raw/jobkorea/123.json")
    result = app.embed_worker(event, None)

    assert result["statusCode"] == 200

    # parsed/ 에 저장
    put_call = mock_s3.put_object.call_args
    assert put_call.kwargs["Key"] == "parsed/jobkorea/123.json"
    saved_body = json.loads(put_call.kwargs["Body"].decode("utf-8"))
    assert saved_body["embedding"] == [0.1] * 768
    assert saved_body["source"] == "jobkorea"

    # raw/ 아카이브 후 삭제
    mock_s3.copy_object.assert_called_once_with(
        Bucket="test-bucket",
        CopySource={"Bucket": "test-bucket", "Key": "raw/jobkorea/123.json"},
        Key="archive/raw/jobkorea/123.json",
    )
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="raw/jobkorea/123.json",
    )


@patch("core.embedding.embed_text", side_effect=RuntimeError("model error"))
@patch("app.boto3.client")
def test_embed_worker_saves_without_embedding_on_failure(mock_boto_client, mock_embed):
    """임베딩 실패 시에도 parsed/ 에 임베딩 없이 저장하고 raw/ 를 삭제한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    _mock_head_object_not_found(mock_s3)
    data = _raw_json_data()
    _mock_s3_get_object(mock_s3, data)

    event = _make_raw_event("test-bucket", "raw/jobkorea/123.json")
    result = app.embed_worker(event, None)

    assert result["statusCode"] == 200

    put_call = mock_s3.put_object.call_args
    saved_body = json.loads(put_call.kwargs["Body"].decode("utf-8"))
    assert "embedding" not in saved_body

    # raw/ 아카이브 후 삭제
    mock_s3.copy_object.assert_called_once()
    mock_s3.delete_object.assert_called_once()


@patch("app.boto3.client")
def test_embed_worker_skips_non_raw_key(mock_boto_client):
    """raw/ 가 아닌 키는 무시한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    event = _make_raw_event("test-bucket", "parsed/jobkorea/123.json")
    result = app.embed_worker(event, None)

    assert result["statusCode"] == 200
    mock_s3.get_object.assert_not_called()


@patch("app.boto3.client")
def test_embed_worker_skips_non_json_key(mock_boto_client):
    """raw/ 이지만 .json 이 아닌 키는 무시한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    event = _make_raw_event("test-bucket", "raw/jobkorea/readme.txt")
    result = app.embed_worker(event, None)

    assert result["statusCode"] == 200
    mock_s3.get_object.assert_not_called()


@patch("app.boto3.client")
def test_embed_worker_skips_already_processed(mock_boto_client):
    """parsed/ 에 이미 파일이 존재하면 임베딩을 건너뛰고 raw/ 만 삭제한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    # head_object 성공 = parsed/ 파일 존재
    mock_s3.head_object.return_value = {}

    event = _make_raw_event("test-bucket", "raw/jobkorea/123.json")
    result = app.embed_worker(event, None)

    assert result["statusCode"] == 200

    # 임베딩 처리 없이 raw/ 아카이브 후 삭제
    mock_s3.get_object.assert_not_called()
    mock_s3.put_object.assert_not_called()
    mock_s3.copy_object.assert_called_once_with(
        Bucket="test-bucket",
        CopySource={"Bucket": "test-bucket", "Key": "raw/jobkorea/123.json"},
        Key="archive/raw/jobkorea/123.json",
    )
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="raw/jobkorea/123.json",
    )
