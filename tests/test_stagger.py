"""Tests for pipewatch.stagger — stagger/cadence deviation detection."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig
from pipewatch.history import HistoryStore, HistoryEntry
from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.stagger import check_stagger, check_all_stagger, _mean_interval


def _dt(offset_seconds: float) -> datetime:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


def _pipeline(name: str = "pipe", max_age: int = 3600) -> PipelineConfig:
    return PipelineConfig(name=name, max_age_seconds=max_age)


def _result(name: str, status: CheckStatus = CheckStatus.OK) -> CheckResult:
    return CheckResult(
        pipeline=name,
        status=status,
        last_run=_dt(0),
        message="ok",
    )


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


@pytest.fixture()
def store(history_file: Path) -> HistoryStore:
    return HistoryStore(history_file)


def _seed(store: HistoryStore, name: str, offsets: list) -> None:
    for offset in offsets:
        entry = HistoryEntry.from_result(
            _result(name), checked_at=_dt(offset)
        )
        store.append(entry)


# ── _mean_interval ────────────────────────────────────────────────────────────

def test_mean_interval_empty():
    assert _mean_interval([]) is None


def test_mean_interval_single():
    assert _mean_interval([_dt(0)]) is None


def test_mean_interval_uniform():
    ts = [_dt(i * 3600) for i in range(5)]
    result = _mean_interval(ts)
    assert result == pytest.approx(3600.0)


def test_mean_interval_irregular():
    ts = [_dt(0), _dt(1000), _dt(3000)]
    result = _mean_interval(ts)
    assert result == pytest.approx(1500.0)


# ── check_stagger ─────────────────────────────────────────────────────────────

def test_no_max_age_skips(store: HistoryStore):
    p = PipelineConfig(name="pipe", max_age_seconds=None)
    r = check_stagger(p, store)
    assert not r.exceeded
    assert "no max_age_seconds" in r.reason


def test_insufficient_history(store: HistoryStore):
    p = _pipeline(max_age=3600)
    _seed(store, "pipe", [0, 3600])  # only 2 entries
    r = check_stagger(p, store, min_entries=3)
    assert not r.exceeded
    assert "insufficient history" in r.reason


def test_within_tolerance(store: HistoryStore):
    p = _pipeline(max_age=3600)
    # runs every ~3600s exactly
    _seed(store, "pipe", [0, 3600, 7200, 10800])
    r = check_stagger(p, store, tolerance=0.25, min_entries=3)
    assert not r.exceeded
    assert r.actual_interval_seconds == pytest.approx(3600.0)


def test_exceeds_tolerance(store: HistoryStore):
    p = _pipeline(max_age=3600)
    # runs every ~6000s — well above 3600 + 25%
    _seed(store, "pipe", [0, 6000, 12000, 18000])
    r = check_stagger(p, store, tolerance=0.25, min_entries=3)
    assert r.exceeded
    assert r.deviation_seconds is not None
    assert r.deviation_seconds > 3600 * 0.25


def test_summary_ok(store: HistoryStore):
    p = _pipeline(max_age=3600)
    _seed(store, "pipe", [0, 3600, 7200, 10800])
    r = check_stagger(p, store)
    assert "on schedule" in r.summary()


def test_summary_staggered(store: HistoryStore):
    p = _pipeline(max_age=3600)
    _seed(store, "pipe", [0, 6000, 12000, 18000])
    r = check_stagger(p, store, tolerance=0.25, min_entries=3)
    assert "staggered by" in r.summary()


# ── check_all_stagger ─────────────────────────────────────────────────────────

def test_check_all_returns_one_per_pipeline(store: HistoryStore):
    pipelines = [_pipeline("a"), _pipeline("b")]
    results = check_all_stagger(pipelines, store, min_entries=3)
    assert len(results) == 2
    assert {r.pipeline for r in results} == {"a", "b"}
