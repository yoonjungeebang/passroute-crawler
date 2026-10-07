"""authorizer Lambda 단위 테스트."""
import base64
import time

import jwt
import pytest

import authorizer


@pytest.fixture(autouse=True)
def _set_jwt_secret(monkeypatch):
    """테스트용 JWT 시크릿을 환경변수에 설정한다."""
    secret = b"test-secret-key-for-passroute-1234567890"
    b64_secret = base64.b64encode(secret).decode()
    monkeypatch.setenv("JWT_SECRET", b64_secret)
    authorizer._secret_key = None  # 캐시 초기화


def _make_token(payload: dict, secret: bytes = b"test-secret-key-for-passroute-1234567890") -> str:
    return jwt.encode(payload, secret, algorithm="HS256")


def _make_event(token: str) -> dict:
    return {
        "authorizationToken": f"Bearer {token}",
        "methodArn": "arn:aws:execute-api:ap-northeast-2:123456789012:abc123/prod/GET/v1/search",
    }


def test_valid_token_returns_allow():
    """유효한 JWT는 Allow 정책을 반환한다."""
    token = _make_token({"sub": "user123", "exp": int(time.time()) + 3600})
    result = authorizer.handler(_make_event(token), None)

    assert result["principalId"] == "user123"
    statement = result["policyDocument"]["Statement"][0]
    assert statement["Effect"] == "Allow"


def test_expired_token_returns_deny():
    """만료된 JWT는 Deny 정책을 반환한다."""
    token = _make_token({"sub": "user123", "exp": int(time.time()) - 100})
    result = authorizer.handler(_make_event(token), None)

    assert result["principalId"] == "unauthorized"
    statement = result["policyDocument"]["Statement"][0]
    assert statement["Effect"] == "Deny"


def test_invalid_signature_returns_deny():
    """잘못된 시크릿으로 서명된 JWT는 Deny를 반환한다."""
    token = _make_token(
        {"sub": "user123", "exp": int(time.time()) + 3600},
        secret=b"wrong-secret-key-that-does-not-match!!",
    )
    result = authorizer.handler(_make_event(token), None)

    assert result["principalId"] == "unauthorized"
    statement = result["policyDocument"]["Statement"][0]
    assert statement["Effect"] == "Deny"


def test_missing_bearer_prefix_returns_deny():
    """Bearer 접두사가 없으면 Deny를 반환한다."""
    event = {
        "authorizationToken": "just-a-token",
        "methodArn": "arn:aws:execute-api:ap-northeast-2:123456789012:abc123/prod/GET/v1/search",
    }
    result = authorizer.handler(event, None)

    assert result["principalId"] == "unauthorized"


def test_empty_token_returns_deny():
    """빈 토큰은 Deny를 반환한다."""
    event = {
        "authorizationToken": "",
        "methodArn": "arn:aws:execute-api:ap-northeast-2:123456789012:abc123/prod/GET/v1/search",
    }
    result = authorizer.handler(event, None)

    assert result["principalId"] == "unauthorized"


def test_allow_policy_covers_all_methods():
    """Allow 정책은 해당 스테이지의 모든 메서드를 포함한다."""
    token = _make_token({"sub": "user1", "exp": int(time.time()) + 3600})
    result = authorizer.handler(_make_event(token), None)

    resource = result["policyDocument"]["Statement"][0]["Resource"]
    assert resource.endswith("/prod/*")
