"""Tests for pipewatch.snapshot."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.snapshot import SnapshotEntry, load_snapshot, take_snapshot

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def ok_result():
    return CheckResult(
        pipeline="pipe_a",
        status=CheckStatus.OK,
        last_run=_NOW,
        message="OK",
    )


@pytest.fixture()
def stale_result():
    return CheckResult(
        pipeline="pipe_b",
        status=CheckStatus.STALE,
        last_run=None,
        message="stale",
    )


def test_snapshot_entry_from_result(ok_result):
    entry = SnapshotEntry.from_result(ok_result)
    assert entry.pipeline == "pipe_a"
    assert entry.status == "ok"
    assert entry.last_run == _NOW.isoformat()


def test_snapshot_entry_null_last_run(stale_result):
    entry = SnapshotEntry.from_result(stale_result)
    assert entry.last_run is None


def test_take_snapshot_creates_file(tmp_path, ok_result, stale_result):
    path = str(tmp_path / "snap.json")
    snap = take_snapshot([ok_result, stale_result], path, now_fn=lambda: _NOW)
    assert os.path.exists(path)
    assert len(snap.entries) == 2
    assert snap.taken_at == _NOW.isoformat()


def test_take_snapshot_json_structure(tmp_path, ok_result):
    path = str(tmp_path / "snap.json")
    take_snapshot([ok_result], path, now_fn=lambda: _NOW)
    with open(path) as fh:
        data = json.load(fh)
    assert "taken_at" in data
    assert data["entries"][0]["pipeline"] == "pipe_a"


def test_load_snapshot_missing_returns_none(tmp_path):
    result = load_snapshot(str(tmp_path / "missing.json"))
    assert result is None


def test_load_snapshot_roundtrip(tmp_path, ok_result, stale_result):
    path = str(tmp_path / "snap.json")
    take_snapshot([ok_result, stale_result], path, now_fn=lambda: _NOW)
    snap = load_snapshot(path)
    assert snap is not None
    assert len(snap.entries) == 2
    names = [e.pipeline for e in snap.entries]
    assert "pipe_a" in names
    assert "pipe_b" in names


def test_load_snapshot_statuses(tmp_path, ok_result, stale_result):
    path = str(tmp_path / "snap.json")
    take_snapshot([ok_result, stale_result], path, now_fn=lambda: _NOW)
    snap = load_snapshot(path)
    statuses = {e.pipeline: e.status for e in snap.entries}
    assert statuses["pipe_a"] == "ok"
    assert statuses["pipe_b"] == "stale"
