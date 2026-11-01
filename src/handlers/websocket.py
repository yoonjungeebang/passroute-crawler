"""WebSocket 핸들러: 기업별 수집 요청 접수 + 연결 관리."""
import hashlib
import hmac
import json
import logging
import time

import boto3
from boto3.dynamodb.conditions import Attr

from ._common import _API_CONFIG, required_env

logger = logging.getLogger(__name__)

_WS_CONNECTION_TTL = int(_API_CONFIG.get("ws_connection_ttl", 3600))
_COLLECT_COOLDOWN = int(_API_CONFIG.get("collect_cooldown", 300))
_MAX_ARTICLES_LIMIT = int(_API_CONFIG.get("max_articles_limit", 20))

_apigw_clients: dict[str, object] = {}

# ws_disconnect / ws_collect 에서 사용하는 환경변수와 boto3 클라이언트.
# ws_authorizer / ws_connect Lambda는 이 값이 필요 없으므로
# 모듈 import 시점이 아닌 첫 호출 시점에 초기화한다.
_CONNECTIONS_TABLE: str | None = None
_COLLECT_STATUS_TABLE: str | None = None
_REQUEST_COUNTS_TABLE: str | None = None
_QUEUE_URL: str | None = None
_dynamodb = None
_sqs = None


def _ensure_collect_env():
    """ws_collect 계열 함수가 필요로 하는 환경변수와 boto3 클라이언트를 초기화한다."""
    global _CONNECTIONS_TABLE, _COLLECT_STATUS_TABLE, _REQUEST_COUNTS_TABLE, _QUEUE_URL, _dynamodb, _sqs
    if _CONNECTIONS_TABLE is not None:
        return
    _CONNECTIONS_TABLE = required_env("WS_CONNECTIONS_TABLE")
    _COLLECT_STATUS_TABLE = required_env("COLLECT_STATUS_TABLE")
    _REQUEST_COUNTS_TABLE = required_env("COMPANY_REQUEST_COUNTS_TABLE")
    _QUEUE_URL = required_env("COMPANY_COLLECT_QUEUE_URL")
    _dynamodb = boto3.resource("dynamodb")
    _sqs = boto3.client("sqs")


def ws_authorizer(event, context):
    """클라이언트가 보낸 API Key가 서버에 저장된 키와 일치하는지 확인한다."""
    from core.secrets import get_ws_api_key

    route_arn = event["methodArn"]

    query_params = event.get("queryStringParameters") or {}
    client_key = query_params.get("x-api-key", "")
    if not client_key:
        logger.warning("WebSocket 인증 실패: API Key 미제공")
        return _build_auth_policy(route_arn, "Deny")

    try:
        ws_api_key = get_ws_api_key()
    except Exception:
        logger.exception("WS API Key 조회 실패")
        return _build_auth_policy(route_arn, "Deny")

    if not ws_api_key:
        logger.error("WebSocket 인증 실패: 서버 측 API Key가 설정되지 않음")
        return _build_auth_policy(route_arn, "Deny")

    if hmac.compare_digest(client_key, ws_api_key):
        logger.info("WebSocket 인증 성공")
        return _build_auth_policy(route_arn, "Allow")

    logger.warning("WebSocket 인증 실패: API Key 불일치")
    return _build_auth_policy(route_arn, "Deny")


def _build_auth_policy(method_arn: str, effect: str, principal: str = "anonymous") -> dict:
    """methodArn에서 리소스를 추출해 Allow/Deny IAM 정책을 반환한다."""
    try:
        arn_parts = method_arn.split(":")
        region = arn_parts[3]
        account_id = arn_parts[4]
        api_gw_parts = arn_parts[5].split("/")
        api_id = api_gw_parts[0]
        stage = api_gw_parts[1]
    except (IndexError, AttributeError):
        logger.error("methodArn 파싱 실패: %s", method_arn)
        raise

    resource_arn = f"arn:aws:execute-api:{region}:{account_id}:{api_id}/{stage}/*"
    return {
        "principalId": principal,
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
    """인증 통과 후 WebSocket 연결을 수락한다."""
    connection_id = event["requestContext"]["connectionId"]
    logger.info("WebSocket 연결: %s", connection_id)
    return {"statusCode": 200}


def ws_disconnect(event, context):
    """연결이 끊어지면 DynamoDB에서 해당 연결 정보를 삭제한다."""
    _ensure_collect_env()
    connection_id = event["requestContext"]["connectionId"]
    table = _dynamodb.Table(_CONNECTIONS_TABLE)

    try:
        table.delete_item(Key={"connectionId": connection_id})
    except Exception:
        logger.exception("연결 삭제 실패: %s", connection_id)

    logger.info("WebSocket 해제: %s", connection_id)
    return {"statusCode": 200}


def ws_collect(event, context):
    """수집 요청을 받아 중복 확인 후 SQS에 작업을 넣고 클라이언트에 응답한다."""
    _ensure_collect_env()
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
    tech_stack = body.get("tech_stack", "").strip()[:_API_CONFIG["max_company_length"]]
    keywords = body.get("keywords", "").strip()[:_API_CONFIG["max_company_length"]]

    raw_max = body.get("max_articles", 10)
    max_articles = min(max(int(raw_max), 1), _MAX_ARTICLES_LIMIT) if isinstance(raw_max, (int, float)) else 10

    now = int(time.time())
    ttl = now + _WS_CONNECTION_TTL

    # 연결 정보 저장 (company GSI를 통해 수집 완료 시 전체 푸시에 사용)
    conn_table = _dynamodb.Table(_CONNECTIONS_TABLE)
    conn_table.put_item(Item={
        "connectionId": connection_id,
        "company": company,
        "position": position,
        "tech_stack": tech_stack,
        "wsEndpoint": endpoint,
        "requestedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "ttl": ttl,
    })

    # 기업별 요청 횟수 기록 (자주 조회되는 기업 캐시 사전 준비용)
    _increment_request_count(company)

    # 같은 기업이라도 키워드 조합이 다르면 별도 수집 실행
    collect_key = _make_collect_key(company, position, tech_stack, keywords)

    # 원자적 중복 수집 방지: ConditionExpression으로 race condition 해결
    need_collect = _try_claim_collect(collect_key, now)
    mode = "collect" if need_collect else "cache_only"

    # SQS에 요청 적재 (워커가 캐시 반환 또는 실시간 수집 수행)
    try:
        _sqs.send_message(
            QueueUrl=_QUEUE_URL,
            MessageBody=json.dumps({
                "company": company,
                "position": position,
                "tech_stack": tech_stack,
                "keywords": keywords,
                "max_articles": max_articles,
                "mode": mode,
                "collect_key": collect_key,
            }, ensure_ascii=False),
        )
    except Exception:
        logger.exception("SQS 요청 전송 실패: company=%s", company)
        _send_to_client(endpoint, connection_id, {"status": "error", "message": "요청 처리 중 오류가 발생했습니다."})
        return {"statusCode": 500}

    _send_to_client(endpoint, connection_id, {"status": "accepted", "company": company})
    logger.info("수집 요청 접수: company=%s, mode=%s, connection=%s", company, mode, connection_id)
    return {"statusCode": 200}


def _try_claim_collect(collect_key: str, now: int) -> bool:
    """동시 요청이 와도 하나만 수집하도록 선착순으로 잠금을 건다."""
    table = _dynamodb.Table(_COLLECT_STATUS_TABLE)
    cutoff = now - _COLLECT_COOLDOWN

    try:
        table.put_item(
            Item={
                "collectKey": collect_key,
                "status": "collecting",
                "updatedAt": now,
                "ttl": now + _COLLECT_COOLDOWN + 60,
            },
            ConditionExpression=(Attr("collectKey").not_exists() | Attr("updatedAt").lt(cutoff)),
        )
        return True
    except _dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False
    except Exception:
        logger.exception("수집 상태 원자적 기록 실패: %s", collect_key)
        return False


def _get_apigw_client(endpoint: str):
    """endpoint별 API Gateway Management 클라이언트를 캐싱해서 반환한다."""
    if endpoint not in _apigw_clients:
        _apigw_clients[endpoint] = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)
    return _apigw_clients[endpoint]


def _send_to_client(endpoint: str, connection_id: str, data: dict) -> None:
    """WebSocket으로 클라이언트에 메시지를 보낸다."""
    apigw = _get_apigw_client(endpoint)
    try:
        apigw.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        )
    except apigw.exceptions.GoneException:
        logger.info("연결 이미 끊어짐: %s", connection_id)
    except Exception:
        logger.exception("WebSocket 전송 실패: %s", connection_id)


def _make_collect_key(company: str, position: str, tech_stack: str, keywords: str) -> str:
    """기업명 + 검색 조건을 조합해 중복 수집 방지용 키를 만든다."""
    parts = sorted(filter(None, [position.lower(), tech_stack.lower(), keywords.lower()]))
    params_str = "|".join(parts)
    if not params_str:
        return company
    params_hash = hashlib.sha256(params_str.encode()).hexdigest()[:8]
    return f"{company}#{params_hash}"


def _increment_request_count(company: str) -> None:
    """기업별 요청 횟수를 기록한다. 자주 조회되는 기업의 캐시 사전 준비에 활용."""
    table = _dynamodb.Table(_REQUEST_COUNTS_TABLE)
    try:
        table.update_item(
            Key={"company": company},
            UpdateExpression="ADD requestCount :inc",
            ExpressionAttributeValues={":inc": 1},
        )
    except Exception:
        logger.exception("요청 횟수 기록 실패: %s", company)
