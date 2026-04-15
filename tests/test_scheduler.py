"""Tests for pipewatch.scheduler."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertConfig
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.scheduler import SchedulerStats, _should_alert, run_once
from pipewatch.state import RunRecord, StateStore


UTC = timezone.utc
_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture()
def store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.json")


@pytest.fixture()
def app_cfg() -> AppConfig:
    return AppConfig(
        pipelines=[
            PipelineConfig(name="etl_daily", max_age_minutes=60),
            PipelineConfig(name="etl_hourly", max_age_minutes=30),
        ]
    )


@pytest.fixture()
def alert_cfg() -> AlertConfig:
    return AlertConfig(
        smtp_host="localhost",
        smtp_port=25,
        sender="monitor@example.com",
        recipients=["ops@example.com"],
    )


def _now_fn():
    return _NOW


def test_should_alert_stale():
    result = CheckResult(pipeline_name="p", status=CheckStatus.STALE, message="stale")
    assert _should_alert(result) is True


def test_should_alert_failed():
    result = CheckResult(pipeline_name="p", status=CheckStatus.FAILED, message="err")
    assert _should_alert(result) is True


def test_should_not_alert_ok():
    result = CheckResult(pipeline_name="p", status=CheckStatus.OK, message="ok")
    assert _should_alert(result) is False


def test_run_once_returns_results(app_cfg, store):
    results = run_once(app_cfg, store, _now_fn=_now_fn)
    assert len(results) == 2
    assert all(isinstance(r, CheckResult) for r in results)


def test_run_once_all_missing_no_records(app_cfg, store):
    results = run_once(app_cfg, store, _now_fn=_now_fn)
    statuses = {r.pipeline_name: r.status for r in results}
    assert statuses["etl_daily"] == CheckStatus.MISSING
    assert statuses["etl_hourly"] == CheckStatus.MISSING


def test_run_once_sends_alert_for_stale(app_cfg, store, alert_cfg):
    rec = RunRecord(
        pipeline_name="etl_daily",
        run_id="r1",
        started_at=datetime(2024, 6, 1, 9, 0, 0, tzinfo=UTC),
        finished_at=datetime(2024, 6, 1, 9, 5, 0, tzinfo=UTC),
        status="success",
    )
    store.save(rec)

    with patch("pipewatch.scheduler.send_email_alert") as mock_send:
        results = run_once(app_cfg, store, alert_cfg, _now_fn=_now_fn)

    stale = [r for r in results if r.pipeline_name == "etl_daily"]
    assert stale[0].status == CheckStatus.STALE
    mock_send.assert_called_once()


def test_run_once_no_alert_when_ok(app_cfg, store, alert_cfg):
    rec = RunRecord(
        pipeline_name="etl_daily",
        run_id="r2",
        started_at=datetime(2024, 6, 1, 11, 50, 0, tzinfo=UTC),
        finished_at=datetime(2024, 6, 1, 11, 55, 0, tzinfo=UTC),
        status="success",
    )
    store.save(rec)

    with patch("pipewatch.scheduler.send_email_alert") as mock_send:
        run_once(app_cfg, store, alert_cfg, _now_fn=_now_fn)

    calls_for_daily = [
        c for c in mock_send.call_args_list if c.args[0].pipeline_name == "etl_daily"
    ]
    assert calls_for_daily == []


def test_run_once_no_alert_cfg_does_not_raise(app_cfg, store):
    results = run_once(app_cfg, store, alert_cfg=None, _now_fn=_now_fn)
    assert isinstance(results, list)
