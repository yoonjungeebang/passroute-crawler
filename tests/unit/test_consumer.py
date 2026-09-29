"""consumer.py 단위 테스트. S3 와 PgVectorStorage 는 mock 으로 대체한다."""
import json
from unittest.mock import MagicMock, call

from consumer import (
    process_delete_requests,
    process_parsed_files,
    update_url_index,
)


def _mock_s3_list(keys: list[str]) -> MagicMock:
    """list_objects_v2 paginator 를 흉내내는 mock 을 반환한다."""
    mock_s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": k} for k in keys]},
    ]
    mock_s3.get_paginator.return_value = paginator
    return mock_s3


def _mock_s3_get_object(mock_s3: MagicMock, data: dict) -> None:
    """get_object 가 지정된 dict 를 JSON 으로 반환하도록 설정한다."""
    body_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: body_bytes),
    }


def test_process_parsed_files_saves_and_deletes():
    """parsed 파일을 읽어 storage.save 를 호출하고 S3 객체를 삭제한다."""
    mock_s3 = _mock_s3_list(["parsed/jobkorea/123.json"])
    _mock_s3_get_object(mock_s3, {
        "source": "jobkorea",
        "external_id": "123",
        "url": "https://www.jobkorea.co.kr/Recruit/GI_Read/123",
        "company_name": "A사",
        "title": "백엔드",
        "raw_text": "주요업무: 개발",
        "tech_stack": ["Python"],
        "deadline": "2026-05-01",
        "crawled_at": "2026-04-12T18:00:00+09:00",
    })

    mock_storage = MagicMock()
    saved = process_parsed_files(mock_s3, "test-bucket", mock_storage)

    assert saved == 1
    mock_storage.save.assert_called_once()
    saved_detail = mock_storage.save.call_args.args[0]
    assert saved_detail.external_id == "123"
    assert saved_detail.tech_stack == ("Python",)
    mock_s3.delete_object.assert_called_once_with(Bucket="test-bucket", Key="parsed/jobkorea/123.json")


def test_process_parsed_files_skips_non_json():
    """JSON 이 아닌 파일은 건너뛴다."""
    mock_s3 = _mock_s3_list(["parsed/jobkorea/readme.txt"])
    mock_storage = MagicMock()

    saved = process_parsed_files(mock_s3, "test-bucket", mock_storage)

    assert saved == 0
    mock_storage.save.assert_not_called()


def test_process_parsed_files_continues_on_error():
    """한 파일이 실패해도 나머지 파일은 계속 처리한다."""
    mock_s3 = _mock_s3_list(["parsed/jobkorea/bad.json", "parsed/jobkorea/good.json"])

    call_count = 0

    def side_effect(**kwargs):
        nonlocal call_count
        call_count += 1
        key = kwargs["Key"]
        if "bad" in key:
            raise ValueError("bad json")
        body_bytes = json.dumps({
            "source": "jobkorea", "external_id": "good",
            "url": "https://x.com/good", "company_name": "A",
            "title": "t", "raw_text": "text", "tech_stack": [],
            "deadline": "", "crawled_at": "2026-04-12T18:00:00+09:00",
        }).encode("utf-8")
        return {"Body": MagicMock(read=lambda: body_bytes)}

    mock_s3.get_object.side_effect = side_effect
    mock_storage = MagicMock()

    saved = process_parsed_files(mock_s3, "test-bucket", mock_storage)

    assert saved == 1
    mock_storage.save.assert_called_once()


def test_process_delete_requests_calls_delete_expired():
    """삭제 요청 파일을 읽어 storage.delete_expired 를 호출한다."""
    mock_s3 = _mock_s3_list(["delete-requests/20260412T180000.json"])
    _mock_s3_get_object(mock_s3, {
        "now_ts": 1776164400,
        "requested_at": "20260412T180000",
    })

    mock_storage = MagicMock()
    mock_storage.delete_expired.return_value = 3

    deleted = process_delete_requests(mock_s3, "test-bucket", mock_storage)

    assert deleted == 3
    mock_storage.delete_expired.assert_called_once_with(1776164400)


def test_process_delete_requests_legacy_now_iso():
    """기존 now_iso 형식의 삭제 요청도 timestamp 로 변환하여 처리한다."""
    mock_s3 = _mock_s3_list(["delete-requests/20260412T180000.json"])
    _mock_s3_get_object(mock_s3, {
        "now_iso": "2026-04-12T18:00:00+09:00",
        "requested_at": "20260412T180000",
    })

    mock_storage = MagicMock()
    mock_storage.delete_expired.return_value = 1

    deleted = process_delete_requests(mock_s3, "test-bucket", mock_storage)

    assert deleted == 1
    call_args = mock_storage.delete_expired.call_args.args[0]
    assert isinstance(call_args, int)
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="delete-requests/20260412T180000.json",
    )


def test_update_url_index_writes_sorted_urls():
    """PostgreSQL 의 URL 목록을 정렬하여 url-index.json 에 기록한다."""
    mock_s3 = MagicMock()
    mock_storage = MagicMock()
    mock_storage.get_all_urls.return_value = {"https://b.com", "https://a.com"}

    count = update_url_index(mock_s3, "test-bucket", mock_storage)

    assert count == 2
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "url-index.json"
    body = json.loads(call_kwargs["Body"].decode("utf-8"))
    assert body["urls"] == ["https://a.com", "https://b.com"]
