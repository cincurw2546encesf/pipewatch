"""Tests for pipewatch.deadletter and deadletter_cmd."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.deadletter import DeadLetterEntry, DeadLetterStore
from pipewatch.deadletter_cmd import deadletter_cmd


@pytest.fixture()
def store(tmp_path):
    return DeadLetterStore(str(tmp_path / "dl.json"))


def _entry(pipeline: str = "pipe_a", attempts: int = 3) -> DeadLetterEntry:
    return DeadLetterEntry(
        pipeline=pipeline,
        status="FAILED",
        last_run="2024-01-01T00:00:00+00:00",
        attempts=attempts,
        first_failed_at="2024-01-01T00:00:00+00:00",
    )


# --- DeadLetterStore unit tests ---

def test_empty_store_returns_no_entries(store):
    assert store.all() == []


def test_add_and_get(store):
    e = _entry()
    store.add(e)
    result = store.get("pipe_a")
    assert result is not None
    assert result.pipeline == "pipe_a"
    assert result.attempts == 3


def test_get_unknown_returns_none(store):
    assert store.get("nonexistent") is None


def test_add_overwrites_existing(store):
    store.add(_entry(attempts=1))
    store.add(_entry(attempts=5))
    assert store.get("pipe_a").attempts == 5


def test_remove_existing(store):
    store.add(_entry())
    removed = store.remove("pipe_a")
    assert removed is True
    assert store.get("pipe_a") is None


def test_remove_missing_returns_false(store):
    assert store.remove("ghost") is False


def test_persists_across_reload(tmp_path):
    path = str(tmp_path / "dl.json")
    s1 = DeadLetterStore(path)
    s1.add(_entry("pipe_b", attempts=2))
    s2 = DeadLetterStore(path)
    result = s2.get("pipe_b")
    assert result is not None
    assert result.attempts == 2


def test_clear_empties_store(store):
    store.add(_entry("a"))
    store.add(_entry("b"))
    store.clear()
    assert store.all() == []


def test_summary_contains_key_fields():
    e = _entry("my_pipe", attempts=4)
    s = e.summary()
    assert "my_pipe" in s
    assert "FAILED" in s
    assert "attempts=4" in s


# --- CLI tests ---

@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def ctx_obj(tmp_path):
    cfg = MagicMock()
    cfg.deadletter_file = str(tmp_path / "dl.json")
    return {"app_cfg": cfg}


def invoke(runner, ctx_obj, *args):
    return runner.invoke(deadletter_cmd, args, obj=ctx_obj, catch_exceptions=False)


def test_list_empty(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No dead-lettered" in result.output


def test_list_shows_entry(runner, ctx_obj):
    store = DeadLetterStore(ctx_obj["app_cfg"].deadletter_file)
    store.add(_entry("pipe_x", attempts=7))
    result = invoke(runner, ctx_obj, "list")
    assert "pipe_x" in result.output
    assert "attempts=7" in result.output


def test_remove_existing_pipeline(runner, ctx_obj):
    store = DeadLetterStore(ctx_obj["app_cfg"].deadletter_file)
    store.add(_entry("pipe_y"))
    result = invoke(runner, ctx_obj, "remove", "pipe_y")
    assert result.exit_code == 0
    assert "Removed" in result.output


def test_remove_missing_exits_nonzero(runner, ctx_obj):
    result = runner.invoke(deadletter_cmd, ["remove", "ghost"], obj=ctx_obj, catch_exceptions=False)
    assert result.exit_code != 0
