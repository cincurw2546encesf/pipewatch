"""Tests for pipewatch.shadow — shadow mode dry-run validation."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.shadow import (
    ShadowEntry,
    ShadowReport,
    run_shadow,
    save_shadow_report,
)

_TS = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _now_fn():
    return _TS


def _result(pipeline: str, status: CheckStatus, message: str = "") -> CheckResult:
    return CheckResult(pipeline=pipeline, status=status, message=message)


@pytest.fixture()
def ok_result():
    return _result("pipe_ok", CheckStatus.OK, "all good")


@pytest.fixture()
def stale_result():
    return _result("pipe_stale", CheckStatus.STALE, "last run 3h ago")


@pytest.fixture()
def failed_result():
    return _result("pipe_failed", CheckStatus.FAILED, "exit code 1")


def test_run_shadow_ok_does_not_alert(ok_result):
    report = run_shadow([ok_result], now_fn=_now_fn)
    assert len(report.entries) == 1
    entry = report.entries[0]
    assert entry.would_alert is False
    assert entry.reason is None


def test_run_shadow_stale_would_alert(stale_result):
    report = run_shadow([stale_result], now_fn=_now_fn)
    entry = report.entries[0]
    assert entry.would_alert is True
    assert entry.reason == "last run 3h ago"


def test_run_shadow_failed_would_alert(failed_result):
    report = run_shadow([failed_result], now_fn=_now_fn)
    entry = report.entries[0]
    assert entry.would_alert is True
    assert entry.status == "failed"


def test_run_shadow_mixed_results(ok_result, stale_result, failed_result):
    report = run_shadow([ok_result, stale_result, failed_result], now_fn=_now_fn)
    assert len(report.entries) == 3
    assert report.would_alert_count == 2


def test_summary_string(ok_result, stale_result):
    report = run_shadow([ok_result, stale_result], now_fn=_now_fn)
    assert report.summary == "Shadow: 2 checked, 1 would alert"


def test_generated_at_set(ok_result):
    report = run_shadow([ok_result], now_fn=_now_fn)
    assert report.generated_at == _TS.isoformat()


def test_entry_pipeline_name(stale_result):
    report = run_shadow([stale_result], now_fn=_now_fn)
    assert report.entries[0].pipeline == "pipe_stale"


def test_save_shadow_report_creates_file(tmp_path, ok_result, stale_result):
    report = run_shadow([ok_result, stale_result], now_fn=_now_fn)
    out = tmp_path / "shadow.json"
    save_shadow_report(report, out)
    assert out.exists()
    data = json.loads(out.read_text())
    assert "entries" in data
    assert len(data["entries"]) == 2


def test_save_shadow_report_structure(tmp_path, failed_result):
    report = run_shadow([failed_result], now_fn=_now_fn)
    out = tmp_path / "shadow.json"
    save_shadow_report(report, out)
    data = json.loads(out.read_text())
    entry = data["entries"][0]
    assert entry["pipeline"] == "pipe_failed"
    assert entry["would_alert"] is True
    assert entry["reason"] == "exit code 1"


def test_shadow_entry_roundtrip(stale_result):
    report = run_shadow([stale_result], now_fn=_now_fn)
    d = report.entries[0].to_dict()
    restored = ShadowEntry.from_dict(d)
    assert restored.pipeline == "pipe_stale"
    assert restored.would_alert is True
