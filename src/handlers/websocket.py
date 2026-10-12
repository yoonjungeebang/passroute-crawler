"""WebSocket 핸들러: 기업별 수집 요청 접수 + 연결 관리."""
import json
import logging
import os
import time

import boto3
from boto3.dynamodb.conditions import Attr

from ._common import _API_CONFIG

logger = logging.getLogger(__name__)

_TABLE_NAME = os.environ.get("WS_CONNECTIONS_TABLE", "passroute-ws-connections")
_COLLECT_STATUS_TABLE = os.environ.get("COLLECT_STATUS_TABLE", "passroute-collect-status")
_REQUEST_COUNTS_TABLE = os.environ.get("COMPANY_REQUEST_COUNTS_TABLE", "passroute-company-request-counts")
_QUEUE_URL = os.environ.get("COMPANY_COLLECT_QUEUE_URL", "")
_WS_CONNECTION_TTL = int(_API_CONFIG.get("ws_connection_ttl", 3600))
_COLLECT_COOLDOWN = int(_API_CONFIG.get("collect_cooldown", 300))


def ws_authorizer(event, context):
    """$connect Lambda Authorizer: API Key 검증."""
    from core.secrets import get_ws_api_key  # noqa: C0415

    query_params = event.get("queryStringParameters") or {}
    client_key = query_params.get("x-api-key", "")

    route_arn = event["methodArn"]

    try:
        ws_api_key = get_ws_api_key()
    except Exception:
        logger.exception("WS API Key 조회 실패")
        return _build_auth_policy(route_arn, "Deny")

    if ws_api_key and client_key == ws_api_key:
        logger.info("WebSocket 인증 성공")
        return _build_auth_policy(route_arn, "Allow")

    logger.warning("WebSocket 인증 실패")
    return _build_auth_policy(route_arn, "Deny")


def _build_auth_policy(method_arn: str, effect: str) -> dict:
    """IAM 정책 문서를 생성한다."""
    arn_parts = method_arn.split(":")
    region = arn_parts[3]
    account_id = arn_parts[4]
    api_gw_parts = arn_parts[5].split("/")
    api_id = api_gw_parts[0]
    stage = api_gw_parts[1]

    resource_arn = f"arn:aws:execute-api:{region}:{account_id}:{api_id}/{stage}/*"
    return {
        "principalId": "user",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "execute-api:Invoke",
                "Effect": effect,
                "Resource": resource_arn,
            }],
        },
    }


def ws_connect(event, context):
    """$connect: Authorizer 통과 후 연결 수락."""
    connection_id = event["requestContext"]["connectionId"]
    logger.info("WebSocket 연결: %s", connection_id)
    return {"statusCode": 200}


def ws_disconnect(event, context):
    """$disconnect: DynamoDB에서 연결 정보 삭제."""
    connection_id = event["requestContext"]["connectionId"]
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(_TABLE_NAME)

    try:
        table.delete_item(Key={"connectionId": connection_id})
    except Exception:
        logger.exception("연결 삭제 실패: %s", connection_id)

    logger.info("WebSocket 해제: %s", connection_id)
    return {"statusCode": 200}


def ws_collect(event, context):
    """collect 액션: 원자적 중복 체크 → DynamoDB 저장 → SQS 발송 → 응답."""
    connection_id = event["requestContext"]["connectionId"]
    domain = event["requestContext"]["domainName"]
    stage = event["requestContext"]["stage"]
    endpoint = f"https://{domain}/{stage}"

    try:
        body = json.loads(event.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        _send_to_client(endpoint, connection_id, {"status": "error", "message": "잘못된 JSON 형식입니다."})
        return {"statusCode": 400}

    company = body.get("company", "").strip()[:_API_CONFIG["max_company_length"]]
    if not company:
        _send_to_client(endpoint, connection_id, {"status": "error", "message": "company는 필수입니다."})
        return {"statusCode": 400}

    position = body.get("position", "").strip()[:_API_CONFIG["max_position_length"]]
    tech_stack = body.get("tech_stack", "").strip()
    keywords = body.get("keywords", "").strip()
    max_articles = str(body.get("max_articles", "10"))

    dynamodb = boto3.resource("dynamodb")
    now = int(time.time())
    ttl = now + _WS_CONNECTION_TTL

    # 연결 정보 저장 (company GSI를 통해 수집 완료 시 전체 푸시에 사용)
    conn_table = dynamodb.Table(_TABLE_NAME)
    conn_table.put_item(Item={
        "connectionId": connection_id,
        "company": company,
        "position": position,
        "tech_stack": tech_stack,
        "wsEndpoint": endpoint,
        "requestedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "ttl": ttl,
    })

    # 기업별 요청 횟수 기록 (향후 프리워밍 판단용)
    _increment_request_count(company)

    # 원자적 중복 수집 방지: ConditionExpression으로 race condition 해결
    need_collect = _try_claim_collect(dynamodb, company, now)
    mode = "collect" if need_collect else "cache_only"

    # SQS에 요청 적재 (워커가 캐시 반환 또는 실시간 수집 수행)
    sqs = boto3.client("sqs")
    try:
        sqs.send_message(
            QueueUrl=_QUEUE_URL,
            MessageBody=json.dumps({
                "company": company,
                "position": position,
                "tech_stack": tech_stack,
                "keywords": keywords,
                "max_articles": max_articles,
                "mode": mode,
            }, ensure_ascii=False),
        )
    except Exception:
        logger.exception("SQS 요청 전송 실패: company=%s", company)
        _send_to_client(endpoint, connection_id, {"status": "error", "message": "요청 처리 중 오류가 발생했습니다."})
        return {"statusCode": 500}

    _send_to_client(endpoint, connection_id, {"status": "accepted", "company": company})
    logger.info("수집 요청 접수: company=%s, mode=%s, connection=%s", company, mode, connection_id)
    return {"statusCode": 200}


def _try_claim_collect(dynamodb, company: str, now: int) -> bool:
    """원자적으로 수집 권한을 획득한다. 성공하면 True, 이미 수집 중이면 False.

    DynamoDB ConditionExpression을 사용해 race condition을 방지한다.
    - 레코드가 없으면: 생성 → True
    - 레코드가 있고 cooldown 지남: 덮어쓰기 → True
    - 레코드가 있고 cooldown 안 지남: 실패 → False
    """
    table = dynamodb.Table(_COLLECT_STATUS_TABLE)
    cutoff = now - _COLLECT_COOLDOWN

    try:
        table.put_item(
            Item={
                "company": company,
                "status": "collecting",
                "updatedAt": now,
                "ttl": now + _COLLECT_COOLDOWN + 60,
            },
            ConditionExpression=(
                Attr("company").not_exists()
                | Attr("updatedAt").lt(cutoff)
            ),
        )
        return True
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False
    except Exception:
        logger.exception("수집 상태 원자적 기록 실패: %s", company)
        return False


def _send_to_client(endpoint: str, connection_id: str, data: dict) -> None:
    """WebSocket API를 통해 클라이언트에 메시지를 전송한다."""
    apigw = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)
    try:
        apigw.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        )
    except apigw.exceptions.GoneException:
        logger.info("연결 이미 끊어짐: %s", connection_id)
    except Exception:
        logger.exception("WebSocket 전송 실패: %s", connection_id)


def _increment_request_count(company: str) -> None:
    """기업별 요청 횟수를 원자적으로 증가시킨다. 프리워밍 판단 데이터로 활용."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(_REQUEST_COUNTS_TABLE)
    try:
        table.update_item(
            Key={"company": company},
            UpdateExpression="ADD requestCount :inc",
            ExpressionAttributeValues={":inc": 1},
        )
    except Exception:
        logger.exception("요청 횟수 기록 실패: %s", company)
