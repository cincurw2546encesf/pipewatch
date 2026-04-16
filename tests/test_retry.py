"""Tests for pipewatch.retry module."""
import pytest
from pathlib import Path
from pipewatch.retry import RetryStore, RetryEntry, should_retry


@pytest.fixture
def store(tmp_path: Path) -> RetryStore:
    return RetryStore(tmp_path / "retries.json")


def test_empty_store_returns_none(store: RetryStore) -> None:
    assert store.get("pipe_a") is None


def test_record_attempt_increments(store: RetryStore) -> None:
    e = store.record_attempt("pipe_a")
    assert e.attempts == 1
    e2 = store.record_attempt("pipe_a", error="timeout")
    assert e2.attempts == 2
    assert e2.last_error == "timeout"


def test_record_attempt_persists(tmp_path: Path) -> None:
    path = tmp_path / "retries.json"
    s1 = RetryStore(path)
    s1.record_attempt("pipe_b", error="oops")
    s2 = RetryStore(path)
    entry = s2.get("pipe_b")
    assert entry is not None
    assert entry.attempts == 1
    assert entry.last_error == "oops"


def test_reset_removes_entry(store: RetryStore) -> None:
    store.record_attempt("pipe_c")
    store.reset("pipe_c")
    assert store.get("pipe_c") is None


def test_reset_nonexistent_is_noop(store: RetryStore) -> None:
    store.reset("ghost")  # should not raise


def test_all_entries(store: RetryStore) -> None:
    store.record_attempt("a")
    store.record_attempt("b")
    names = {e.pipeline for e in store.all_entries()}
    assert names == {"a", "b"}


def test_should_retry_none_entry() -> None:
    assert should_retry(None, max_retries=3) is True


def test_should_retry_under_limit() -> None:
    e = RetryEntry(pipeline="x", attempts=2)
    assert should_retry(e, max_retries=3) is True


def test_should_retry_at_limit() -> None:
    e = RetryEntry(pipeline="x", attempts=3)
    assert should_retry(e, max_retries=3) is False


def test_last_attempt_set(store: RetryStore) -> None:
    e = store.record_attempt("pipe_d")
    assert e.last_attempt is not None
