"""secrets.py 단위 테스트. Secrets Manager API 호출은 mock 으로 대체한다."""
import json
from unittest.mock import MagicMock, patch

import core.secrets as secrets_mod


@patch("core.secrets.boto3.client")
def test_get_secret_returns_parsed_json(mock_boto_client):
    """시크릿 조회 시 JSON 문자열을 dict 로 파싱하여 반환한다."""
    secrets_mod._cache.clear()
    secrets_mod._client = None

    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {
        "SecretString": json.dumps({"username": "user", "password": "pass"}),
    }
    mock_boto_client.return_value = mock_sm

    result = secrets_mod.get_secret("arn:test:secret")

    assert result == {"username": "user", "password": "pass"}


@patch("core.secrets.boto3.client")
def test_get_secret_caches_result(mock_boto_client):
    """같은 ARN 으로 두 번 호출하면 API 는 1회만 호출된다."""
    secrets_mod._cache.clear()
    secrets_mod._client = None

    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {
        "SecretString": json.dumps({"key": "value"}),
    }
    mock_boto_client.return_value = mock_sm

    secrets_mod.get_secret("arn:test:cached")
    secrets_mod.get_secret("arn:test:cached")

    assert mock_sm.get_secret_value.call_count == 1


@patch("core.secrets.get_secret")
def test_get_database_url_format(mock_get_secret):
    """DSN 문자열이 올바른 형식으로 조합되는지 확인한다."""
    mock_get_secret.return_value = {
        "host": "proxy.rds.amazonaws.com",
        "port": 5432,
        "username": "admin",
        "password": "secret123",
        "dbname": "passroute",
    }

    url = secrets_mod.get_database_url()

    assert url == "postgresql://admin:secret123@proxy.rds.amazonaws.com:5432/passroute"


@patch("core.secrets.get_secret")
def test_get_naver_credentials(mock_get_secret):
    """client_id, client_secret 튜플을 반환한다."""
    mock_get_secret.return_value = {
        "client_id": "naver-id",
        "client_secret": "naver-secret",
    }

    client_id, client_secret = secrets_mod.get_naver_credentials()

    assert client_id == "naver-id"
    assert client_secret == "naver-secret"


@patch("core.secrets.get_secret")
def test_get_discord_webhook_url(mock_get_secret):
    """Discord 웹훅 URL 문자열을 반환한다."""
    mock_get_secret.return_value = {"webhook_url": "https://discord.com/api/webhooks/test"}

    result = secrets_mod.get_discord_webhook_url()

    assert result == "https://discord.com/api/webhooks/test"


@patch("core.secrets.get_secret")
def test_get_worknet_api_key(mock_get_secret):
    """워크넷 API 키 문자열을 반환한다."""
    mock_get_secret.return_value = {"api_key": "test-worknet-key"}

    result = secrets_mod.get_worknet_api_key()

    assert result == "test-worknet-key"
