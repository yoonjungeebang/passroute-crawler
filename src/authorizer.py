"""API Gateway Lambda Authorizer.

Spring Boot 백엔드가 전달한 JWT(HS256)의 서명과 만료를 검증한다.
토큰 블랙리스트(Redis)는 백엔드에서 이미 확인한 뒤 요청하므로 여기서는 검증하지 않는다.
"""
import base64
import logging
import os

import jwt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_secret_key: bytes | None = None


def _get_secret_key() -> bytes:
    """JWT_SECRET 환경변수에서 Base64 디코딩된 시크릿 키를 반환한다."""
    global _secret_key
    if _secret_key is None:
        secret_b64 = os.environ["JWT_SECRET"]
        _secret_key = base64.b64decode(secret_b64)
    return _secret_key


def handler(event, context):
    """API Gateway REST API용 TOKEN 타입 Lambda Authorizer.

    event["authorizationToken"]에서 "Bearer <token>"을 추출하여 검증한다.
    """
    token_str = event.get("authorizationToken", "")
    method_arn = event.get("methodArn", "")

    if not token_str.startswith("Bearer "):
        logger.warning("Bearer 토큰 형식이 아님")
        return _deny_policy(method_arn)

    token = token_str[len("Bearer "):]

    try:
        payload = jwt.decode(
            token,
            _get_secret_key(),
            algorithms=["HS256"],
        )
    except jwt.ExpiredSignatureError:
        logger.info("토큰 만료")
        return _deny_policy(method_arn)
    except jwt.InvalidTokenError as e:
        logger.warning("토큰 검증 실패: %s", e)
        return _deny_policy(method_arn)

    # sub 클레임을 principalId로 사용
    principal_id = str(payload.get("sub", "unknown"))
    logger.info("인증 성공: principalId=%s", principal_id)

    return _allow_policy(principal_id, method_arn)


def _allow_policy(principal_id: str, method_arn: str) -> dict:
    """모든 API 메서드를 허용하는 IAM 정책을 반환한다."""
    arn_parts = method_arn.split(":")
    region = arn_parts[3]
    account_id = arn_parts[4]
    api_gateway_arn = arn_parts[5].split("/")
    api_id = api_gateway_arn[0]
    stage = api_gateway_arn[1]

    resource_arn = f"arn:aws:execute-api:{region}:{account_id}:{api_id}/{stage}/*"

    return {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "execute-api:Invoke",
                "Effect": "Allow",
                "Resource": resource_arn,
            }],
        },
    }


def _deny_policy(method_arn: str) -> dict:
    return {
        "principalId": "unauthorized",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "execute-api:Invoke",
                "Effect": "Deny",
                "Resource": method_arn or "*",
            }],
        },
    }
