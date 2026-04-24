"""Integration test: watchdog with real StateStore and config."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.state import StateStore, RunRecord
from pipewatch.watchdog import run_watchdog


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def state_file(tmp_path):
    return str(tmp_path / "state.json")


@pytest.fixture()
def app_cfg(state_file):
    return AppConfig(
        pipelines=[
            PipelineConfig(name="etl_daily", max_age_minutes=60, schedule=""),
            PipelineConfig(name="etl_hourly", max_age_minutes=30, schedule=""),
        ],
        state_file=state_file,
    )


def _write_state(state_file, records):
    with open(state_file, "w") as f:
        json.dump(records, f)


def test_no_state_file_flags_all_as_never_run(app_cfg, state_file):
    store = StateStore(state_file)
    report = run_watchdog(app_cfg, store, now_fn=lambda: NOW)
    assert not report.healthy
    names = {e.pipeline for e in report.entries}
    assert names == {"etl_daily", "etl_hourly"}
    assert all(e.issue == "never_run" for e in report.entries)


def test_recent_runs_produce_healthy_report(app_cfg, state_file):
    recent = (NOW - timedelta(minutes=10)).isoformat()
    _write_state(state_file, {
        "etl_daily": {"started": recent, "finished": recent, "status": "ok"},
        "etl_hourly": {"started": recent, "finished": recent, "status": "ok"},
    })
    store = StateStore(state_file)
    report = run_watchdog(app_cfg, store, now_fn=lambda: NOW)
    assert report.healthy


def test_one_stale_one_ok(app_cfg, state_file):
    recent = (NOW - timedelta(minutes=10)).isoformat()
    very_old = (NOW - timedelta(minutes=300)).isoformat()
    _write_state(state_file, {
        "etl_daily": {"started": very_old, "finished": very_old, "status": "ok"},
        "etl_hourly": {"started": recent, "finished": recent, "status": "ok"},
    })
    store = StateStore(state_file)
    report = run_watchdog(app_cfg, store, now_fn=lambda: NOW)
    assert not report.healthy
    assert len(report.entries) == 1
    assert report.entries[0].pipeline == "etl_daily"
    assert report.entries[0].issue == "stale_deadline"
