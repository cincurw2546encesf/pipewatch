"""Tests for pipewatch.notifier."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertConfig
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.notifier import (
    NotificationSummary,
    dispatch_notifications,
    should_notify,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def stale_result() -> CheckResult:
    return CheckResult(
        pipeline="pipe_a",
        status=CheckStatus.STALE,
        last_run=_NOW,
        message="Stale by 2h",
    )


@pytest.fixture()
def failed_result() -> CheckResult:
    return CheckResult(
        pipeline="pipe_b",
        status=CheckStatus.FAILED,
        last_run=_NOW,
        message="Last run failed",
    )


@pytest.fixture()
def ok_result() -> CheckResult:
    return CheckResult(
        pipeline="pipe_c",
        status=CheckStatus.OK,
        last_run=_NOW,
        message="All good",
    )


@pytest.fixture()
def alert_cfg() -> AlertConfig:
    return AlertConfig(
        enabled=True,
        smtp_host="localhost",
        smtp_port=25,
        from_addr="monitor@example.com",
        to_addrs=["ops@example.com"],
    )


def test_should_notify_stale(stale_result):
    assert should_notify(stale_result) is True


def test_should_notify_failed(failed_result):
    assert should_notify(failed_result) is True


def test_should_notify_ok(ok_result):
    assert should_notify(ok_result) is False


def test_dispatch_skips_ok(ok_result, alert_cfg):
    summary = dispatch_notifications([ok_result], alert_cfg)
    assert summary.total_sent == 0
    assert "pipe_c" in summary.skipped


def test_dispatch_sends_email_for_stale(stale_result, alert_cfg):
    with patch("pipewatch.notifier.send_email_alert") as mock_send:
        summary = dispatch_notifications([stale_result], alert_cfg)
    mock_send.assert_called_once_with(stale_result, alert_cfg)
    assert summary.total_sent == 1
    assert summary.total_failed == 0


def test_dispatch_records_email_failure(failed_result, alert_cfg):
    with patch("pipewatch.notifier.send_email_alert", side_effect=RuntimeError("conn refused")):
        summary = dispatch_notifications([failed_result], alert_cfg)
    assert summary.total_sent == 1
    assert summary.total_failed == 1
    assert "conn refused" in summary.sent[0].error


def test_dispatch_skips_when_no_alert_cfg(stale_result):
    summary = dispatch_notifications([stale_result], alert_cfg=None)
    assert summary.total_sent == 0
    assert "pipe_a" in summary.skipped


def test_dispatch_skips_when_alert_disabled(stale_result):
    cfg = AlertConfig(enabled=False, smtp_host="localhost", smtp_port=25,
                      from_addr="a@b.com", to_addrs=["c@d.com"])
    summary = dispatch_notifications([stale_result], cfg)
    assert summary.total_sent == 0


def test_notification_result_str(stale_result, alert_cfg):
    with patch("pipewatch.notifier.send_email_alert"):
        summary = dispatch_notifications([stale_result], alert_cfg)
    result = summary.sent[0]
    assert "pipe_a" in str(result)


def test_dispatch_mixed_results(stale_result, ok_result, failed_result, alert_cfg):
    """Dispatch handles a mix of OK, stale, and failed results correctly."""
    with patch("pipewatch.notifier.send_email_alert"):
        summary = dispatch_notifications([stale_result, ok_result, failed_result], alert_cfg)
    assert summary.total_sent == 2
    assert "pipe_c" in summary.skipped
