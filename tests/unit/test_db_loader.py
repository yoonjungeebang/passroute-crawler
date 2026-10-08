"""db_loader Lambda 핸들러 단위 테스트.

PgVectorStorage 는 mock 으로 대체한다.
SQS 메시지에서 직접 데이터를 수신하는 형태의 이벤트를 입력으로 사용한다.
"""
import json
from unittest.mock import MagicMock, patch

import app


def _make_sqs_event(data: dict, message_id: str = "msg-001") -> dict:
    """SQS 메시지 이벤트를 생성한다."""
    return {"Records": [{"messageId": message_id, "body": json.dumps(data)}]}


def _parsed_json_data(**overrides) -> dict:
    """DB 적재용 메시지 데이터를 생성한다."""
    base = {
        "type": "load",
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
        "embedding_status": "ok",
    }
    base.update(overrides)
    return base


@patch("handlers.load.get_pg_storage")
def test_db_loader_saves_record(mock_get_pg):
    """load 타입 메시지 시 PgVectorStorage.save() 가 호출된다."""
    mock_pg = MagicMock()
    mock_get_pg.return_value = mock_pg
    data = _parsed_json_data()

    event = _make_sqs_event(data)
    app.db_loader(event, None)

    mock_pg.save.assert_called_once()
    saved_detail = mock_pg.save.call_args.args[0]
    assert saved_detail.external_id == "123"
    assert saved_detail.tech_stack == ("Python",)
    assert mock_pg.save.call_args.kwargs["embedding"] == [0.1] * 768


@patch("handlers.load.get_pg_storage")
def test_db_loader_processes_delete_expired(mock_get_pg):
    """delete_expired 타입 메시지 시 PgVectorStorage.delete_expired() 가 호출된다."""
    mock_pg = MagicMock()
    mock_pg.delete_expired.return_value = 3
    mock_get_pg.return_value = mock_pg

    data = {
        "type": "delete_expired",
        "now_ts": 1776164400,
        "requested_at": "20260412T180000",
    }
    event = _make_sqs_event(data)
    app.db_loader(event, None)

    mock_pg.delete_expired.assert_called_once_with(1776164400)


@patch("handlers.load.get_pg_storage")
def test_db_loader_reports_failure_in_batch_item_failures(mock_get_pg):
    """에러 발생 시 batchItemFailures로 실패 레코드를 반환한다."""
    mock_pg = MagicMock()
    mock_pg.save.side_effect = RuntimeError("DB 접근 실패")
    mock_get_pg.return_value = mock_pg

    data = _parsed_json_data()
    event = _make_sqs_event(data)

    result = app.db_loader(event, None)
    assert len(result["batchItemFailures"]) == 1
    assert result["batchItemFailures"][0]["itemIdentifier"] == "msg-001"


@patch("handlers.load.get_pg_storage")
def test_db_loader_handles_unknown_type(mock_get_pg):
    """알 수 없는 타입의 메시지는 무시한다."""
    mock_pg = MagicMock()
    mock_get_pg.return_value = mock_pg

    data = {"type": "unknown_type", "data": "something"}
    event = _make_sqs_event(data)
    result = app.db_loader(event, None)

    assert result["statusCode"] == 200
    mock_pg.save.assert_not_called()
    mock_pg.delete_expired.assert_not_called()


@patch("handlers.load.get_pg_storage")
def test_db_loader_defaults_to_load_type(mock_get_pg):
    """type 필드가 없으면 load 로 간주한다."""
    mock_pg = MagicMock()
    mock_get_pg.return_value = mock_pg

    data = _parsed_json_data()
    del data["type"]
    event = _make_sqs_event(data)
    app.db_loader(event, None)

    mock_pg.save.assert_called_once()
