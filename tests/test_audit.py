"""Tests for pipewatch.audit and pipewatch.audit_cmd."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.audit import AuditEntry, AuditStore
from pipewatch.audit_cmd import audit_cmd


# ---------------------------------------------------------------------------
# AuditStore unit tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def store(tmp_path: Path) -> AuditStore:
    return AuditStore(tmp_path / "audit.json")


def _entry(action: str = "silence.add", pipeline: str = "pipe_a") -> AuditEntry:
    return AuditEntry(action=action, pipeline=pipeline, actor="cli", detail="test detail")


def test_empty_store_returns_no_entries(store: AuditStore) -> None:
    assert store.get() == []


def test_record_and_retrieve(store: AuditStore) -> None:
    store.record(_entry())
    entries = store.get()
    assert len(entries) == 1
    assert entries[0].action == "silence.add"
    assert entries[0].pipeline == "pipe_a"


def test_filter_by_pipeline(store: AuditStore) -> None:
    store.record(_entry(pipeline="pipe_a"))
    store.record(_entry(pipeline="pipe_b"))
    result = store.get(pipeline="pipe_a")
    assert len(result) == 1
    assert result[0].pipeline == "pipe_a"


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "audit.json"
    s1 = AuditStore(path)
    s1.record(_entry())
    s2 = AuditStore(path)
    assert len(s2.get()) == 1


def test_clear_all(store: AuditStore) -> None:
    store.record(_entry(pipeline="pipe_a"))
    store.record(_entry(pipeline="pipe_b"))
    removed = store.clear()
    assert removed == 2
    assert store.get() == []


def test_clear_by_pipeline(store: AuditStore) -> None:
    store.record(_entry(pipeline="pipe_a"))
    store.record(_entry(pipeline="pipe_b"))
    removed = store.clear(pipeline="pipe_a")
    assert removed == 1
    remaining = store.get()
    assert len(remaining) == 1
    assert remaining[0].pipeline == "pipe_b"


def test_to_dict_roundtrip() -> None:
    e = AuditEntry(
        action="retry.reset",
        pipeline="my_pipe",
        actor="scheduler",
        detail="reset after 3 failures",
        timestamp=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert AuditEntry.from_dict(e.to_dict()).action == e.action
    assert AuditEntry.from_dict(e.to_dict()).timestamp == e.timestamp


# ---------------------------------------------------------------------------
# audit_cmd CLI tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def ctx_obj(tmp_path: Path):
    class _Cfg:
        state_dir = str(tmp_path)
    return {"app_cfg": _Cfg()}


def invoke(runner, ctx_obj, *args):
    return runner.invoke(audit_cmd, list(args), obj=ctx_obj, catch_exceptions=False)


def test_list_empty(runner, ctx_obj) -> None:
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No audit entries" in result.output


def test_list_shows_entry(runner, ctx_obj, tmp_path: Path) -> None:
    store = AuditStore(tmp_path / "audit.json")
    store.record(AuditEntry(action="baseline.update", pipeline="etl_main", actor="cli", detail="updated avg"))
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "baseline.update" in result.output
    assert "etl_main" in result.output
