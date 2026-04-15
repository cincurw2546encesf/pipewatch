"""Tests for pipewatch.checker and pipewatch.state."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult, check_all, check_pipeline
from pipewatch.config import PipelineConfig
from pipewatch.state import RunRecord, StateStore


@pytest.fixture()
def store(tmp_path: Path) -> StateStore:
    s = StateStore(path=tmp_path / "state.json")
    s.load()
    return s


def _pipeline(name: str = "etl", max_age: int = 60) -> PipelineConfig:
    return PipelineConfig(name=name, max_age_minutes=max_age)


def _record(pipeline: str, status: str, age_minutes: float = 0, message: str | None = None) -> RunRecord:
    ts = (datetime.now(timezone.utc) - timedelta(minutes=age_minutes)).isoformat()
    return RunRecord(pipeline=pipeline, status=status, started_at=ts, finished_at=ts, message=message)


# --- StateStore tests ---

def test_store_empty_on_missing_file(store: StateStore) -> None:
    assert store.latest("etl") is None


def test_store_record_and_reload(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    s = StateStore(path=path)
    s.load()
    s.record_run(_record("etl", "success"))

    s2 = StateStore(path=path)
    s2.load()
    assert s2.latest("etl") is not None
    assert s2.latest("etl").status == "success"  # type: ignore[union-attr]


def test_store_all_pipelines(store: StateStore) -> None:
    store.record_run(_record("etl", "success"))
    store.record_run(_record("reports", "success"))
    assert set(store.all_pipelines()) == {"etl", "reports"}


# --- Checker tests ---

def test_no_runs_is_unhealthy(store: StateStore) -> None:
    result = check_pipeline(_pipeline(), store)
    assert not result.healthy
    assert "no runs" in result.reason  # type: ignore[operator]


def test_recent_success_is_healthy(store: StateStore) -> None:
    store.record_run(_record("etl", "success", age_minutes=5))
    result = check_pipeline(_pipeline(max_age=60), store)
    assert result.healthy


def test_failed_run_is_unhealthy(store: StateStore) -> None:
    store.record_run(_record("etl", "failed", message="timeout"))
    result = check_pipeline(_pipeline(), store)
    assert not result.healthy
    assert "timeout" in result.reason  # type: ignore[operator]


def test_stale_run_is_unhealthy(store: StateStore) -> None:
    store.record_run(_record("etl", "success", age_minutes=120))
    result = check_pipeline(_pipeline(max_age=60), store)
    assert not result.healthy
    assert "stale" in result.reason  # type: ignore[operator]


def test_check_all_returns_one_result_per_pipeline(store: StateStore) -> None:
    pipelines = [_pipeline("etl"), _pipeline("reports")]
    store.record_run(_record("etl", "success", age_minutes=1))
    results = check_all(pipelines, store)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"etl", "reports"}


def test_check_result_str_healthy(store: StateStore) -> None:
    store.record_run(_record("etl", "success", age_minutes=1))
    result = check_pipeline(_pipeline(), store)
    assert "✅" in str(result)


def test_check_result_str_unhealthy(store: StateStore) -> None:
    result = CheckResult(pipeline="etl", healthy=False, reason="no runs recorded")
    assert "❌" in str(result)
