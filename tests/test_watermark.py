"""Tests for pipewatch.watermark."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.watermark import WatermarkStore


@pytest.fixture()
def store(tmp_path: Path) -> WatermarkStore:
    return WatermarkStore(tmp_path / "watermarks.json")


def _dt(offset_hours: int = 0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(hours=offset_hours)


def test_empty_store_returns_none(store: WatermarkStore) -> None:
    assert store.get("pipe_a") is None


def test_empty_store_all_returns_empty(store: WatermarkStore) -> None:
    assert store.all() == []


def test_update_creates_entry(store: WatermarkStore) -> None:
    result = store.update("pipe_a", _dt(0))
    assert result.pipeline == "pipe_a"
    assert result.high_water == _dt(0)
    assert result.regressed is False
    assert result.previous is None


def test_update_advances_watermark(store: WatermarkStore) -> None:
    store.update("pipe_a", _dt(0))
    result = store.update("pipe_a", _dt(1))
    assert result.high_water == _dt(1)
    assert result.regressed is False
    assert result.previous == _dt(0)


def test_update_same_timestamp_not_regression(store: WatermarkStore) -> None:
    store.update("pipe_a", _dt(0))
    result = store.update("pipe_a", _dt(0))
    assert result.regressed is False


def test_update_detects_regression(store: WatermarkStore) -> None:
    store.update("pipe_a", _dt(5))
    result = store.update("pipe_a", _dt(3))
    assert result.regressed is True
    assert result.previous == _dt(5)
    assert result.high_water == _dt(5)  # unchanged on regression


def test_regression_does_not_overwrite_watermark(store: WatermarkStore, tmp_path: Path) -> None:
    store.update("pipe_a", _dt(5))
    store.update("pipe_a", _dt(2))  # regression — should not persist
    reloaded = WatermarkStore(tmp_path / "watermarks.json")
    entry = reloaded.get("pipe_a")
    assert entry is not None
    assert entry.high_water == _dt(5)


def test_persists_across_reload(store: WatermarkStore, tmp_path: Path) -> None:
    store.update("pipe_a", _dt(0))
    reloaded = WatermarkStore(tmp_path / "watermarks.json")
    entry = reloaded.get("pipe_a")
    assert entry is not None
    assert entry.high_water == _dt(0)


def test_reset_removes_entry(store: WatermarkStore) -> None:
    store.update("pipe_a", _dt(0))
    removed = store.reset("pipe_a")
    assert removed is True
    assert store.get("pipe_a") is None


def test_reset_unknown_returns_false(store: WatermarkStore) -> None:
    assert store.reset("nonexistent") is False


def test_summary_no_watermark() -> None:
    from pipewatch.watermark import WatermarkResult
    r = WatermarkResult(pipeline="p", high_water=None, regressed=False, previous=None)
    assert "no watermark" in r.summary()


def test_summary_regression() -> None:
    from pipewatch.watermark import WatermarkResult
    r = WatermarkResult(pipeline="p", high_water=_dt(5), regressed=True, previous=_dt(5))
    assert "REGRESSION" in r.summary()


def test_summary_ok() -> None:
    from pipewatch.watermark import WatermarkResult
    r = WatermarkResult(pipeline="p", high_water=_dt(1), regressed=False, previous=_dt(0))
    assert "OK" in r.summary()
