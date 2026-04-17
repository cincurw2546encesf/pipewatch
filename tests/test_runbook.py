"""Tests for pipewatch.runbook."""
import pytest
from pathlib import Path
from pipewatch.runbook import RunbookEntry, RunbookStore


@pytest.fixture
def store(tmp_path: Path) -> RunbookStore:
    return RunbookStore(tmp_path / "runbook.json")


def test_empty_store_returns_none(store):
    assert store.get("pipe_a") is None


def test_set_and_get(store):
    entry = store.set("pipe_a", url="https://wiki/pipe_a", note="Check logs")
    assert entry.pipeline == "pipe_a"
    assert entry.url == "https://wiki/pipe_a"
    assert entry.note == "Check logs"
    fetched = store.get("pipe_a")
    assert fetched is not None
    assert fetched.url == "https://wiki/pipe_a"


def test_set_url_only(store):
    store.set("pipe_b", url="https://wiki/pipe_b")
    e = store.get("pipe_b")
    assert e.note is None


def test_set_note_only(store):
    store.set("pipe_c", note="Restart the worker")
    e = store.get("pipe_c")
    assert e.url is None
    assert e.note == "Restart the worker"


def test_persists_across_reload(tmp_path):
    p = tmp_path / "runbook.json"
    s1 = RunbookStore(p)
    s1.set("pipe_x", url="https://example.com", note="see docs")
    s2 = RunbookStore(p)
    e = s2.get("pipe_x")
    assert e is not None
    assert e.url == "https://example.com"


def test_remove_existing(store):
    store.set("pipe_d", url="https://x.com")
    removed = store.remove("pipe_d")
    assert removed is True
    assert store.get("pipe_d") is None


def test_remove_nonexistent(store):
    assert store.remove("ghost") is False


def test_all_returns_all_entries(store):
    store.set("a", url="https://a.com")
    store.set("b", note="note b")
    names = {e.pipeline for e in store.all()}
    assert names == {"a", "b"}


def test_summary_with_both(store):
    e = RunbookEntry(pipeline="p", url="https://wiki", note="fix it")
    s = e.summary()
    assert "[p]" in s
    assert "https://wiki" in s
    assert "fix it" in s


def test_summary_url_only():
    e = RunbookEntry(pipeline="p", url="https://wiki", note=None)
    assert "(" not in e.summary()
