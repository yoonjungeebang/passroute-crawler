"""embed_worker Lambda 핸들러 단위 테스트.

SQS 와 임베딩 모듈은 mock 으로 대체한다.
SQS 메시지에서 직접 크롤링 데이터를 수신하는 형태의 이벤트를 입력으로 사용한다.
"""
import json
from unittest.mock import MagicMock, patch

import app


def _make_sqs_event(data: dict, message_id: str = "msg-001") -> dict:
    """SQS 메시지 이벤트를 생성한다."""
    return {"Records": [{"messageId": message_id, "body": json.dumps(data)}]}


def _raw_json_data(**overrides) -> dict:
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
    }
    base.update(overrides)
    return base


@patch("core.embedding.embed_text", return_value=[0.1] * 768)
@patch("handlers.embed.boto3.client")
def test_embed_worker_embeds_and_sends_to_db_load_queue(mock_boto_client, mock_embed):
    """크롤링 데이터를 임베딩 후 DbLoadQueue 로 전송한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs
    data = _raw_json_data()

    event = _make_sqs_event(data)
    result = app.embed_worker(event, None)

    assert result["statusCode"] == 200

    mock_sqs.send_message.assert_called_once()
    sent_body = json.loads(mock_sqs.send_message.call_args.kwargs["MessageBody"])
    assert sent_body["embedding"] == [0.1] * 768
    assert sent_body["embedding_status"] == "ok"
    assert sent_body["type"] == "load"
    assert sent_body["source"] == "jobkorea"


@patch("core.embedding.embed_text", side_effect=RuntimeError("model error"))
@patch("handlers.embed.boto3.client")
def test_embed_worker_sends_without_embedding_on_failure(mock_boto_client, mock_embed):
    """임베딩 실패 시에도 임베딩 없이 DbLoadQueue 로 전송한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs
    data = _raw_json_data()

    event = _make_sqs_event(data)
    result = app.embed_worker(event, None)

    assert result["statusCode"] == 200

    mock_sqs.send_message.assert_called_once()
    sent_body = json.loads(mock_sqs.send_message.call_args.kwargs["MessageBody"])
    assert "embedding" not in sent_body
    assert sent_body["embedding_status"] == "failed"
    assert sent_body["type"] == "load"


@patch("handlers.embed.boto3.client")
def test_embed_worker_reports_failure_in_batch_item_failures(mock_boto_client):
    """레코드 처리 중 예외 발생 시 batchItemFailures 로 반환한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs
    mock_sqs.send_message.side_effect = RuntimeError("SQS 전송 실패")

    data = _raw_json_data()
    event = _make_sqs_event(data)
    result = app.embed_worker(event, None)

    assert len(result["batchItemFailures"]) == 1
    assert result["batchItemFailures"][0]["itemIdentifier"] == "msg-001"
