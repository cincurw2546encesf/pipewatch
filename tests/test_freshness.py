"""Tests for pipewatch.freshness."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig
from pipewatch.freshness import check_freshness, check_all_freshness, FreshnessResult
from pipewatch.state import StateStore, RunRecord


_BASE = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _now_fn():
    return _BASE


def _pipeline(name="pipe", max_age_seconds=300) -> PipelineConfig:
    return PipelineConfig(name=name, max_age_seconds=max_age_seconds)


@pytest.fixture()
def store(tmp_path):
    return StateStore(tmp_path / "state.json")


def _add_record(store: StateStore, name: str, finished_at: datetime):
    record = RunRecord(
        pipeline=name,
        started_at=finished_at - timedelta(seconds=10),
        finished_at=finished_at,
        status="ok",
    )
    store.record(record)


def test_no_max_age_returns_none(store):
    p = PipelineConfig(name="pipe")
    assert check_freshness(p, store, now_fn=_now_fn) is None


def test_never_run_exceeded(store):
    p = _pipeline()
    result = check_freshness(p, store, now_fn=_now_fn)
    assert result is not None
    assert result.exceeded is True
    assert result.seconds_since_last_run is None
    assert result.last_run is None


def test_recent_run_not_exceeded(store):
    _add_record(store, "pipe", _BASE - timedelta(seconds=100))
    p = _pipeline(max_age_seconds=300)
    result = check_freshness(p, store, now_fn=_now_fn)
    assert result is not None
    assert result.exceeded is False
    assert result.seconds_since_last_run == pytest.approx(100.0)


def test_stale_run_exceeded(store):
    _add_record(store, "pipe", _BASE - timedelta(seconds=400))
    p = _pipeline(max_age_seconds=300)
    result = check_freshness(p, store, now_fn=_now_fn)
    assert result is not None
    assert result.exceeded is True
    assert result.seconds_since_last_run == pytest.approx(400.0)


def test_exactly_at_boundary_not_exceeded(store):
    _add_record(store, "pipe", _BASE - timedelta(seconds=300))
    p = _pipeline(max_age_seconds=300)
    result = check_freshness(p, store, now_fn=_now_fn)
    assert result is not None
    assert result.exceeded is False


def test_summary_never_run(store):
    p = _pipeline()
    result = check_freshness(p, store, now_fn=_now_fn)
    assert "never run" in result.summary()
    assert "pipe" in result.summary()


def test_summary_ok(store):
    _add_record(store, "pipe", _BASE - timedelta(seconds=60))
    p = _pipeline(max_age_seconds=300)
    result = check_freshness(p, store, now_fn=_now_fn)
    assert "OK" in result.summary()


def test_summary_exceeded(store):
    _add_record(store, "pipe", _BASE - timedelta(seconds=500))
    p = _pipeline(max_age_seconds=300)
    result = check_freshness(p, store, now_fn=_now_fn)
    assert "EXCEEDED" in result.summary()


def test_check_all_skips_pipelines_without_max_age(store):
    pipelines = [
        PipelineConfig(name="no-age"),
        _pipeline(name="with-age", max_age_seconds=300),
    ]
    results = check_all_freshness(pipelines, store, now_fn=_now_fn)
    assert len(results) == 1
    assert results[0].pipeline == "with-age"


def test_check_all_returns_multiple(store):
    pipelines = [
        _pipeline(name="a", max_age_seconds=300),
        _pipeline(name="b", max_age_seconds=600),
    ]
    results = check_all_freshness(pipelines, store, now_fn=_now_fn)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}
