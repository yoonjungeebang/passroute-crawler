"""pytest 공통 설정.

- ``src/`` 를 sys.path 최상위에 두어 Lambda 런타임과 동일한 import 경로를 만든다.
- ``app.py`` 가 핸들러 진입 시점에 검증하는 필수 환경변수를 autouse fixture 로 채운다.
"""
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(autouse=True)
def _reset_module_state():
    """secrets 캐시와 app 글로벌 상태를 테스트마다 초기화."""
    import core.circuit_breaker as cb_mod
    import core.secrets as secrets_mod

    import handlers._common as common_mod

    secrets_mod._cache.clear()
    secrets_mod._client = None
    common_mod._pg_storage = None
    cb_mod._registry.clear()
    yield
    secrets_mod._cache.clear()
    secrets_mod._client = None
    common_mod._pg_storage = None
    cb_mod._registry.clear()


@pytest.fixture(autouse=True)
def _set_required_env(monkeypatch):
    """app._required_env 가 던지지 않도록 모든 테스트에 더미 값을 주입."""
    monkeypatch.setenv("JOB_DETAIL_QUEUE_URL", "https://sqs.test/detail")
    monkeypatch.setenv("SOURCE_COLLECT_QUEUE_URL", "https://sqs.test/source-collect")
    monkeypatch.setenv("EMBED_QUEUE_URL", "https://sqs.test/embed")
    monkeypatch.setenv("DB_LOAD_QUEUE_URL", "https://sqs.test/db-load")
    monkeypatch.setenv("DATABASE_SECRET_ARN", "arn:aws:secretsmanager:ap-northeast-2:123456:secret:passroute/database")
    monkeypatch.setenv("NAVER_API_SECRET_ARN", "arn:aws:secretsmanager:ap-northeast-2:123456:secret:passroute/naver-api")
    monkeypatch.setenv("DISCORD_WEBHOOK_SECRET_ARN", "arn:aws:secretsmanager:ap-northeast-2:123456:secret:passroute/discord")
    monkeypatch.setenv("SUPEROOKIE_API_SECRET_ARN", "arn:aws:secretsmanager:ap-northeast-2:123456:secret:passroute/superookie")
