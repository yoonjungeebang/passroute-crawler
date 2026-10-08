"""app.py 의 Lambda 핸들러에 대한 단위 테스트.

외부 I/O(SQS, 크롤러)는 전부 mock 으로 대체한다.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

import app
from crawler.base import JobDetail, JobListingRef


# ─────────────────────────────────────────────────────────
# 공용 헬퍼
# ─────────────────────────────────────────────────────────


def _make_sqs_event(body: dict) -> dict:
    return {"Records": [{"body": json.dumps(body), "messageId": "test-msg-001"}]}


def _ref(external_id: str, company: str, title: str) -> JobListingRef:
    return JobListingRef(
        source="jumpit",
        external_id=external_id,
        url=f"https://www.jumpit.co.kr/position/{external_id}",
        title=title,
        company_name=company,
    )


def _collect_sent_messages(mock_sqs) -> list[dict]:
    sent: list[dict] = []
    for call in mock_sqs.send_message_batch.call_args_list:
        for entry in call.kwargs["Entries"]:
            sent.append(json.loads(entry["MessageBody"]))
    return sent


def _detail(**overrides) -> JobDetail:
    base = dict(
        source="jumpit",
        external_id="999",
        url="https://www.jumpit.co.kr/position/999",
        company_name="패스루트",
        title="백엔드 채용",
        raw_text="주요업무: 백엔드 개발\n자격요건: Python 3년",
        tech_stack=("Python", "AWS"),
        deadline=1777734399,
        crawled_at="2026-04-11T18:00:00+09:00",
    )
    base.update(overrides)
    return JobDetail(**base)


# ─────────────────────────────────────────────────────────
# job_list_collector (dispatcher)
# ─────────────────────────────────────────────────────────


@patch("handlers.collect.iter_sources", return_value=iter(["jumpit", "programmers"]))
@patch("handlers.collect.boto3.client")
def test_job_list_collector_dispatches_sources(mock_boto_client, mock_iter):
    """소스별로 SourceCollectQueue 에 메시지를 전송한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs

    result = app.job_list_collector({}, None)

    body = json.loads(result["body"])
    assert body["dispatched_sources"] == ["jumpit", "programmers"]
    # delete_expired 1건 + 소스 2건 = 3건
    assert mock_sqs.send_message.call_count == 3


@patch("handlers.collect.iter_sources", return_value=iter(["jumpit"]))
@patch("handlers.collect.boto3.client")
def test_job_list_collector_sends_delete_expired(mock_boto_client, mock_iter):
    """목록 수집 전에 만료 삭제 메시지가 DbLoadQueue 로 전송되어야 한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs

    app.job_list_collector({}, None)

    first_call = mock_sqs.send_message.call_args_list[0]
    msg = json.loads(first_call.kwargs["MessageBody"])
    assert msg["type"] == "delete_expired"
    assert "now_ts" in msg


@patch("handlers.collect.boto3.client")
def test_job_list_collector_fails_fast_when_queue_url_missing(
    mock_boto_client, monkeypatch,
):
    """필수 환경변수 SOURCE_COLLECT_QUEUE_URL 이 없으면 RuntimeError."""
    mock_boto_client.return_value = MagicMock()
    monkeypatch.delenv("SOURCE_COLLECT_QUEUE_URL", raising=False)

    with pytest.raises(RuntimeError, match="SOURCE_COLLECT_QUEUE_URL"):
        app.job_list_collector({}, None)


# ─────────────────────────────────────────────────────────
# source_collect_worker
# ─────────────────────────────────────────────────────────


@patch("crawler.robots_check.RobotsChecker", autospec=True)
@patch("handlers.collect.boto3.client")
@patch("handlers.collect.get_pg_storage")
@patch("handlers.collect.get_crawler")
def test_source_collect_worker_dispatches_new_listings(
    mock_get_crawler, mock_get_pg, mock_boto_client, mock_robots_cls,
):
    """신규 공고만 JobDetailQueue 로 전송되어야 한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs

    mock_pg = MagicMock()
    mock_pg.get_existing_urls.return_value = {
        "https://www.jumpit.co.kr/position/111",
    }
    mock_get_pg.return_value = mock_pg

    mock_robots = MagicMock()
    mock_robots.check_and_alert.return_value = True
    mock_robots_cls.return_value = mock_robots

    mock_crawler = MagicMock()
    mock_crawler.base_url = "https://www.jumpit.co.kr"
    mock_crawler.collect_listings.return_value = [
        _ref("111", "A", "t1"),
        _ref("333", "C", "t3"),
    ]
    mock_get_crawler.return_value = mock_crawler

    event = _make_sqs_event({"source": "jumpit"})
    app.source_collect_worker(event, None)

    sent = _collect_sent_messages(mock_sqs)
    assert [m["external_id"] for m in sent] == ["333"]


@patch("crawler.robots_check.RobotsChecker", autospec=True)
@patch("handlers.collect.boto3.client")
@patch("handlers.collect.get_pg_storage")
@patch("handlers.collect.get_crawler")
def test_source_collect_worker_skips_blocked_source(
    mock_get_crawler, mock_get_pg, mock_boto_client, mock_robots_cls,
):
    """robots.txt 차단 시 해당 소스를 스킵한다."""
    mock_boto_client.return_value = MagicMock()
    mock_pg = MagicMock()
    mock_pg.get_existing_urls.return_value = set()
    mock_get_pg.return_value = mock_pg

    mock_robots = MagicMock()
    mock_robots.check_and_alert.return_value = False
    mock_robots_cls.return_value = mock_robots

    mock_crawler = MagicMock()
    mock_crawler.base_url = "https://www.example.com"
    mock_get_crawler.return_value = mock_crawler

    event = _make_sqs_event({"source": "blocked_site"})
    app.source_collect_worker(event, None)

    mock_crawler.collect_listings.assert_not_called()


@patch("crawler.robots_check.RobotsChecker", autospec=True)
@patch("handlers.collect.boto3.client")
@patch("handlers.collect.get_pg_storage")
@patch("handlers.collect.get_crawler")
def test_source_collect_worker_partial_failure(
    mock_get_crawler, mock_get_pg, mock_boto_client, mock_robots_cls,
):
    """크롤링 실패 시 batchItemFailures로 해당 메시지만 재시도."""
    mock_boto_client.return_value = MagicMock()
    mock_pg = MagicMock()
    mock_pg.get_existing_urls.return_value = set()
    mock_get_pg.return_value = mock_pg

    mock_robots = MagicMock()
    mock_robots.check_and_alert.return_value = True
    mock_robots_cls.return_value = mock_robots

    mock_crawler = MagicMock()
    mock_crawler.base_url = "https://www.jumpit.co.kr"
    mock_crawler.collect_listings.side_effect = RuntimeError("timeout")
    mock_get_crawler.return_value = mock_crawler

    event = _make_sqs_event({"source": "jumpit"})
    result = app.source_collect_worker(event, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "test-msg-001"}]


# ─────────────────────────────────────────────────────────
# job_crawl
# ─────────────────────────────────────────────────────────


@patch("handlers.crawl.time.sleep")
@patch("handlers.crawl.boto3.client")
@patch("handlers.crawl.get_crawler")
def test_job_crawl_sends_to_embed_queue(mock_get_crawler, mock_boto_client, mock_sleep):
    """상세 크롤링 성공 시 SQS EmbedQueue 로 메시지가 전송되어야 한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs

    mock_crawler = MagicMock()
    mock_crawler.fetch_detail.return_value = _detail()
    mock_get_crawler.return_value = mock_crawler

    event = _make_sqs_event({
        "source": "jumpit",
        "external_id": "999",
        "url": "https://www.jumpit.co.kr/position/999",
        "company_name": "패스루트",
        "title": "백엔드 채용",
    })
    app.job_crawl(event, None)

    mock_sqs.send_message.assert_called_once()
    sent_body = json.loads(mock_sqs.send_message.call_args.kwargs["MessageBody"])
    assert sent_body["source"] == "jumpit"
    assert sent_body["external_id"] == "999"
    assert sent_body["tech_stack"] == ["Python", "AWS"]
    mock_sleep.assert_called_once()


@patch("handlers.crawl.time.sleep")
@patch("handlers.crawl.boto3.client")
@patch("handlers.crawl.get_crawler")
def test_job_crawl_skips_when_fetch_returns_none(
    mock_get_crawler, mock_boto_client, mock_sleep,
):
    """fetch_detail 이 None 을 반환하면 전송하지 않고 스킵한다."""
    mock_sqs = MagicMock()
    mock_boto_client.return_value = mock_sqs

    mock_crawler = MagicMock()
    mock_crawler.fetch_detail.return_value = None
    mock_get_crawler.return_value = mock_crawler

    event = _make_sqs_event({
        "source": "jumpit",
        "external_id": "999",
        "url": "https://www.jumpit.co.kr/position/999",
        "company_name": "A",
        "title": "t",
    })
    app.job_crawl(event, None)

    mock_sqs.send_message.assert_not_called()


@patch("handlers.crawl.time.sleep")
@patch("handlers.crawl.boto3.client")
@patch("handlers.crawl.get_crawler")
def test_job_crawl_reports_failure_in_batch_item_failures(
    mock_get_crawler, mock_boto_client, mock_sleep,
):
    """크롤링 예외 발생 시 batchItemFailures로 실패 레코드를 반환한다."""
    mock_boto_client.return_value = MagicMock()

    mock_crawler = MagicMock()
    mock_crawler.fetch_detail.side_effect = RuntimeError("boom")
    mock_get_crawler.return_value = mock_crawler

    event = _make_sqs_event({
        "source": "jumpit",
        "external_id": "999",
        "url": "https://www.jumpit.co.kr/position/999",
        "company_name": "A",
        "title": "t",
    })

    result = app.job_crawl(event, None)
    assert len(result["batchItemFailures"]) == 1
    assert result["batchItemFailures"][0]["itemIdentifier"] == "test-msg-001"
