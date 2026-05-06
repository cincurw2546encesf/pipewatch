"""Tests for pipewatch.reaper."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.reaper import check_reaper, check_all_reaper, ReaperResult
from pipewatch.state import StateStore, RunRecord
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.config import AppConfig, PipelineConfig


@pytest.fixture()
def store(tmp_path: Path) -> StateStore:
    return StateStore(str(tmp_path / "state.json"))


DT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _now_fn():
    return DT


def _add(store: StateStore, name: str, finished: datetime) -> None:
    record = RunRecord(
        pipeline=name,
        started="2024-06-01T11:00:00+00:00",
        finished=finished.isoformat(),
        status="ok",
    )
    store.record(record)


def test_no_record_returns_expired(store):
    result = check_reaper("pipe", 3600, store, _now_fn)
    assert result.expired is True
    assert result.last_run is None
    assert result.seconds_since is None


def test_recent_run_not_expired(store):
    _add(store, "pipe", DT - timedelta(seconds=100))
    result = check_reaper("pipe", 3600, store, _now_fn)
    assert result.expired is False
    assert result.seconds_since == pytest.approx(100, abs=1)


def test_stale_run_expired(store):
    _add(store, "pipe", DT - timedelta(seconds=7200))
    result = check_reaper("pipe", 3600, store, _now_fn)
    assert result.expired is True
    assert result.seconds_since == pytest.approx(7200, abs=1)


def test_exactly_at_boundary_not_expired(store):
    _add(store, "pipe", DT - timedelta(seconds=3600))
    result = check_reaper("pipe", 3600, store, _now_fn)
    assert result.expired is False


def test_summary_never_run(store):
    result = check_reaper("pipe", 3600, store, _now_fn)
    assert "never run" in result.summary()
    assert "pipe" in result.summary()


def test_summary_expired(store):
    _add(store, "pipe", DT - timedelta(seconds=5000))
    result = check_reaper("pipe", 3600, store, _now_fn)
    assert "expired" in result.summary()


def test_summary_ok(store):
    _add(store, "pipe", DT - timedelta(seconds=60))
    result = check_reaper("pipe", 3600, store, _now_fn)
    assert "ok" in result.summary()


def test_check_all_reaper_skips_pipelines_without_expiry(store):
    cfg = AppConfig(
        pipelines=[
            PipelineConfig(name="a"),
            PipelineConfig(name="b"),
        ],
        state_file=str(store._path),
    )
    results = [
        CheckResult(pipeline="a", status=CheckStatus.STALE, message="stale"),
        CheckResult(pipeline="b", status=CheckStatus.STALE, message="stale"),
    ]
    out = check_all_reaper(cfg, store, results, _now_fn)
    assert out == []


def test_check_all_reaper_skips_ok_pipelines(store):
    _add(store, "a", DT - timedelta(seconds=60))
    cfg = AppConfig(
        pipelines=[PipelineConfig(name="a", expiry_seconds=3600)],
        state_file=str(store._path),
    )
    results = [CheckResult(pipeline="a", status=CheckStatus.OK, message="ok")]
    out = check_all_reaper(cfg, store, results, _now_fn)
    assert out == []
