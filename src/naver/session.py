"""네이버 API HUB 공용 세션 및 검색 공통 로직."""
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import load_naver_config
from core.circuit_breaker import get_breaker

logger = logging.getLogger(__name__)

_NAVER_CONFIG = load_naver_config()


def make_naver_session(api_key_id: str, api_key: str) -> requests.Session:
    """네이버 API HUB 세션 생성. 뉴스 수집과 실시간 검색 모두에서 사용."""
    session = requests.Session()
    retry = Retry(
        total=_NAVER_CONFIG["retry_total"],
        backoff_factor=_NAVER_CONFIG["retry_backoff_factor"],
        status_forcelist=[429, 500, 502, 503],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "X-NCP-APIGW-API-KEY-ID": api_key_id,
        "X-NCP-APIGW-API-KEY": api_key,
    })
    return session


def search(
    session: requests.Session,
    url: str,
    query: str,
    label: str,
    *,
    display: int = 10,
    extra_params: dict | None = None,
    item_mapper=None,
) -> list[dict]:
    """네이버 API HUB 검색 공통 로직."""
    breaker = get_breaker("naver_search_api")
    params = {"query": query, "display": display}
    if extra_params:
        params.update(extra_params)
    try:
        with breaker:
            resp = session.get(url, params=params, timeout=_NAVER_CONFIG["timeout"])
            resp.raise_for_status()
            data = resp.json()
    except Exception:
        logger.exception("네이버 %s 검색 실패: query=%s", label, query)
        return []

    return [item_mapper(item) for item in data.get("items", [])]
