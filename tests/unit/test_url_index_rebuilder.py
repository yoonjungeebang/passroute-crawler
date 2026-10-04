"""url_index_rebuilder Lambda 핸들러 단위 테스트."""
import json
from unittest.mock import MagicMock, patch

import app


@patch("app._get_pg_storage")
@patch("app.boto3.client")
def test_url_index_rebuilder_writes_urls_from_pg(mock_boto_client, mock_get_pg):
    """PostgreSQL 에서 조회한 URL 목록으로 url-index.json 을 재작성한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    mock_pg = MagicMock()
    mock_pg.get_all_urls.return_value = {"https://a.com", "https://b.com"}
    mock_get_pg.return_value = mock_pg

    result = app.url_index_rebuilder({}, None)

    assert result["statusCode"] == 200
    assert json.loads(result["body"])["url_count"] == 2

    put_kwargs = mock_s3.put_object.call_args.kwargs
    assert put_kwargs["Key"] == "url-index.json"

    body = json.loads(put_kwargs["Body"].decode("utf-8"))
    assert body["urls"] == ["https://a.com", "https://b.com"]


@patch("app._get_pg_storage")
@patch("app.boto3.client")
def test_url_index_rebuilder_handles_empty_db(mock_boto_client, mock_get_pg):
    """DB 에 URL 이 없으면 빈 목록으로 url-index.json 을 저장한다."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    mock_pg = MagicMock()
    mock_pg.get_all_urls.return_value = set()
    mock_get_pg.return_value = mock_pg

    result = app.url_index_rebuilder({}, None)

    assert json.loads(result["body"])["url_count"] == 0

    body = json.loads(mock_s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
    assert body["urls"] == []
