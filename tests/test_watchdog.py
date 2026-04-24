"""Tests for pipewatch.watchdog."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.state import RunRecord
from pipewatch.watchdog import WatchdogEntry, WatchdogReport, run_watchdog


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _app_cfg(*names, max_age=60) -> AppConfig:
    pipelines = [
        PipelineConfig(name=n, max_age_minutes=max_age, schedule="")
        for n in names
    ]
    return AppConfig(pipelines=pipelines, state_file="state.json")


def _store(records: dict) -> MagicMock:
    store = MagicMock()
    store.latest.side_effect = lambda name: records.get(name)
    return store


def _record(minutes_ago: float, finished=True) -> RunRecord:
    ts = NOW - timedelta(minutes=minutes_ago)
    r = MagicMock(spec=RunRecord)
    r.finished_dt = ts if finished else None
    r.started_dt = ts
    return r


def test_healthy_when_all_pipelines_recent():
    cfg = _app_cfg("pipe_a", "pipe_b")
    store = _store({"pipe_a": _record(10), "pipe_b": _record(5)})
    report = run_watchdog(cfg, store, now_fn=lambda: NOW)
    assert report.healthy
    assert report.entries == []


def test_never_run_detected():
    cfg = _app_cfg("pipe_a")
    store = _store({})
    report = run_watchdog(cfg, store, now_fn=lambda: NOW)
    assert not report.healthy
    assert len(report.entries) == 1
    assert report.entries[0].issue == "never_run"
    assert report.entries[0].pipeline == "pipe_a"


def test_missing_state_detected():
    cfg = _app_cfg("pipe_a")
    r = MagicMock(spec=RunRecord)
    r.finished_dt = None
    r.started_dt = None
    store = _store({"pipe_a": r})
    report = run_watchdog(cfg, store, now_fn=lambda: NOW)
    assert not report.healthy
    assert report.entries[0].issue == "missing_state"


def test_stale_deadline_detected():
    # max_age=60, hard deadline = 180 minutes
    cfg = _app_cfg("pipe_a", max_age=60)
    store = _store({"pipe_a": _record(200)})
    report = run_watchdog(cfg, store, now_fn=lambda: NOW)
    assert not report.healthy
    assert report.entries[0].issue == "stale_deadline"


def test_stale_deadline_not_triggered_within_limit():
    cfg = _app_cfg("pipe_a", max_age=60)
    store = _store({"pipe_a": _record(170)})
    report = run_watchdog(cfg, store, now_fn=lambda: NOW)
    assert report.healthy


def test_multiple_issues_reported():
    cfg = _app_cfg("pipe_a", "pipe_b", "pipe_c")
    store = _store({
        "pipe_b": _record(10),
        "pipe_c": _record(250),
    })
    report = run_watchdog(cfg, store, now_fn=lambda: NOW)
    issues = {e.pipeline: e.issue for e in report.entries}
    assert issues["pipe_a"] == "never_run"
    assert issues["pipe_c"] == "stale_deadline"
    assert "pipe_b" not in issues


def test_summary_healthy():
    report = WatchdogReport(generated_at=NOW, entries=[])
    assert "all pipelines" in report.summary()


def test_summary_with_issues():
    entry = WatchdogEntry(pipeline="p", issue="never_run", note="No record.")
    report = WatchdogReport(generated_at=NOW, entries=[entry])
    summary = report.summary()
    assert "1 issue" in summary
    assert "p" in summary


def test_watchdog_entry_str_no_last_seen():
    e = WatchdogEntry(pipeline="p", issue="never_run")
    assert "never" in str(e)
    assert "never_run" in str(e)
