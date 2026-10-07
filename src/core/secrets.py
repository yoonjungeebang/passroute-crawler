"""Secrets Manager 헬퍼. 시크릿을 TTL 기반으로 캐싱하여 Lambda 웜 스타트 시 재사용."""
import json
import os
import time
from urllib.parse import quote_plus

import boto3

_client = None
_CACHE_TTL_SECONDS = 300  # 5분
_cache: dict[str, tuple[dict, float]] = {}


def _get_client():
    global _client
    if _client is None:
        _client = boto3.client("secretsmanager")
    return _client


def get_secret(arn: str) -> dict:
    """Secrets Manager에서 시크릿을 조회하고 TTL 캐싱. JSON 문자열을 dict로 반환."""
    entry = _cache.get(arn)
    if entry is not None:
        secret, expiry = entry
        if time.monotonic() < expiry:
            return secret
    resp = _get_client().get_secret_value(SecretId=arn)
    secret = json.loads(resp["SecretString"])
    _cache[arn] = (secret, time.monotonic() + _CACHE_TTL_SECONDS)
    return secret


def get_database_url() -> str:
    """DATABASE_SECRET_ARN에서 DSN 문자열을 조합. 특수문자 안전하게 인코딩."""
    arn = os.environ["DATABASE_SECRET_ARN"]
    s = get_secret(arn)
    return (
        f"postgresql://{quote_plus(str(s['username']))}:{quote_plus(str(s['password']))}"
        f"@{s['host']}:{s['port']}/{s['dbname']}"
    )


def get_naver_credentials() -> tuple[str, str]:
    """NAVER_API_SECRET_ARN에서 client_id, client_secret 반환."""
    arn = os.environ["NAVER_API_SECRET_ARN"]
    s = get_secret(arn)
    return s["client_id"], s["client_secret"]


def get_discord_webhook_url() -> str:
    """DISCORD_WEBHOOK_SECRET_ARN에서 웹훅 URL 반환."""
    arn = os.environ["DISCORD_WEBHOOK_SECRET_ARN"]
    s = get_secret(arn)
    return s["webhook_url"]


def get_discord_monitor_webhook_url() -> str:
    """DISCORD_MONITOR_WEBHOOK_SECRET_ARN에서 #monitor 채널 웹훅 URL 반환."""
    arn = os.environ["DISCORD_MONITOR_WEBHOOK_SECRET_ARN"]
    s = get_secret(arn)
    return s["webhook_url"]


def get_superookie_access_token() -> str:
    """SUPEROOKIE_API_SECRET_ARN에서 액세스 토큰 반환."""
    arn = os.environ["SUPEROOKIE_API_SECRET_ARN"]
    s = get_secret(arn)
    return s["access_token"]
