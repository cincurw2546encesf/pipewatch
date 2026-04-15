"""Tests for pipewatch.alerts."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertConfig, _build_body, _build_subject, dispatch_alerts, send_email_alert
from pipewatch.checker import CheckResult, CheckStatus


LAST_RUN = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)


@pytest.fixture()
def stale_result():
    return CheckResult(
        pipeline_name="orders_etl",
        status=CheckStatus.STALE,
        message="Last successful run was 5.0h ago (threshold 2.0h).",
        last_run=LAST_RUN,
    )


@pytest.fixture()
def ok_result():
    return CheckResult(
        pipeline_name="orders_etl",
        status=CheckStatus.OK,
        message="Pipeline is healthy.",
        last_run=LAST_RUN,
    )


@pytest.fixture()
def alert_cfg():
    return AlertConfig(
        smtp_host="smtp.example.com",
        smtp_port=587,
        from_addr="pipewatch@example.com",
        to_addrs=["ops@example.com"],
    )


def test_build_subject_stale(stale_result):
    subject = _build_subject(stale_result, "[pw]")
    assert subject == "[pw] STALE: orders_etl"


def test_build_body_contains_fields(stale_result):
    body = _build_body(stale_result)
    assert "orders_etl" in body
    assert "stale" in body
    assert "2024-01-15" in body


def test_build_body_no_last_run():
    result = CheckResult("pipe", CheckStatus.MISSING, "No record.")
    body = _build_body(result)
    assert "never" in body


def test_send_email_alert_success(stale_result, alert_cfg):
    mock_smtp = MagicMock()
    mock_smtp.return_value.__enter__ = lambda s: mock_smtp.return_value
    mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

    result = send_email_alert(stale_result, alert_cfg, smtp_factory=mock_smtp)

    assert result is True
    mock_smtp.return_value.send_message.assert_called_once()


def test_send_email_alert_no_recipients(stale_result):
    cfg = AlertConfig(to_addrs=[])
    result = send_email_alert(stale_result, cfg)
    assert result is False


def test_dispatch_alerts_skips_ok(ok_result, stale_result, alert_cfg):
    mock_smtp = MagicMock()
    mock_smtp.return_value.__enter__ = lambda s: mock_smtp.return_value
    mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

    alerted = dispatch_alerts([ok_result, stale_result], alert_cfg, smtp_factory=mock_smtp)

    assert len(alerted) == 1
    assert alerted[0].pipeline_name == "orders_etl"
    assert alerted[0].status == CheckStatus.STALE


def test_dispatch_alerts_empty(alert_cfg):
    alerted = dispatch_alerts([], alert_cfg)
    assert alerted == []
