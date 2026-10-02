"""S3Storage 단위 테스트. boto3 는 mock 으로 대체한다."""
import json
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from crawler.base import JobDetail
from storage.s3 import S3Storage


def _detail(**overrides) -> JobDetail:
    base = dict(
        source="jobkorea",
        external_id="999",
        url="https://www.jobkorea.co.kr/Recruit/GI_Read/999",
        company_name="패스루트",
        title="백엔드 채용",
        raw_text="주요업무: 백엔드 개발",
        tech_stack=("Python", "AWS"),
        deadline="2026-05-01T23:59:59+09:00",
        crawled_at="2026-04-11T18:00:00+09:00",
    )
    base.update(overrides)
    return JobDetail(**base)


@patch("storage.s3.boto3.client")
def test_get_all_urls_returns_urls_from_index(mock_boto_client):
    """url-index.json 이 존재하면 URL set 을 반환한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    body_content = json.dumps({"urls": ["https://a.com", "https://b.com"]})
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: body_content.encode("utf-8")),
    }

    storage = S3Storage(bucket="test-bucket")
    urls = storage.get_all_urls()

    assert urls == {"https://a.com", "https://b.com"}
    mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="url-index.json")


@patch("storage.s3.boto3.client")
def test_get_all_urls_returns_empty_set_when_no_index(mock_boto_client):
    """url-index.json 이 없으면 빈 set 을 반환한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
    mock_s3.get_object.side_effect = ClientError(error_response, "GetObject")

    storage = S3Storage(bucket="test-bucket")
    urls = storage.get_all_urls()

    assert urls == set()


@patch("storage.s3.boto3.client")
def test_save_puts_correct_key_and_body(mock_boto_client):
    """save 가 올바른 S3 키와 JSON 본문으로 PUT 한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    storage = S3Storage(bucket="test-bucket")
    detail = _detail()
    storage.save(detail)

    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == "parsed/jobkorea/999.json"

    body = json.loads(call_kwargs["Body"].decode("utf-8"))
    assert body["source"] == "jobkorea"
    assert body["external_id"] == "999"
    assert body["tech_stack"] == ["Python", "AWS"]


@patch("storage.s3.boto3.client")
def test_delete_expired_writes_request_and_returns_true(mock_boto_client):
    """delete_expired 가 삭제 요청 파일을 S3 에 저장하고 True 를 반환한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    storage = S3Storage(bucket="test-bucket")
    result = storage.delete_expired("2026-04-12T18:00:00+09:00")

    assert result is True
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"].startswith("delete-requests/")
    assert call_kwargs["Key"].endswith(".json")

    body = json.loads(call_kwargs["Body"])
    from datetime import datetime
    expected_ts = int(datetime.fromisoformat("2026-04-12T18:00:00+09:00").timestamp())
    assert body["now_ts"] == expected_ts


@patch("storage.s3.boto3.client")
def test_delete_expired_keys_are_unique(mock_boto_client):
    """동시 호출 시 키 충돌이 발생하지 않아야 한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    storage = S3Storage(bucket="test-bucket")
    storage.delete_expired("2026-04-12T18:00:00+09:00")
    storage.delete_expired("2026-04-12T18:00:00+09:00")

    calls = mock_s3.put_object.call_args_list
    key1 = calls[0].kwargs["Key"]
    key2 = calls[1].kwargs["Key"]
    assert key1 != key2


@patch("storage.s3.boto3.client")
def test_save_raw_puts_to_raw_prefix(mock_boto_client):
    """save_raw 가 raw/ prefix 로 저장하고 embedding 을 포함하지 않는다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    storage = S3Storage(bucket="test-bucket")
    detail = _detail()
    key = storage.save_raw(detail)

    assert key == "raw/jobkorea/999.json"
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "raw/jobkorea/999.json"

    body = json.loads(call_kwargs["Body"].decode("utf-8"))
    assert body["source"] == "jobkorea"
    assert "embedding" not in body


@patch("storage.s3.boto3.client")
def test_save_raw_dict_puts_to_raw_prefix(mock_boto_client):
    """save_raw_dict 가 dict 를 raw/ prefix 로 저장한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    storage = S3Storage(bucket="test-bucket")
    data = {
        "source": "naver_news",
        "external_id": "abc123",
        "title": "뉴스 제목",
    }
    key = storage.save_raw_dict(data)

    assert key == "raw/naver_news/abc123.json"
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "raw/naver_news/abc123.json"
