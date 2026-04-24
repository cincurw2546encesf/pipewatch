"""Tests for pipewatch.escalation."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.escalation import (
    EscalationEntry,
    EscalationStore,
    check_escalation,
)


@pytest.fixture
def store(tmp_path: Path) -> EscalationStore:
    return EscalationStore(tmp_path / "escalation.json")


def test_empty_store_returns_zero_count(store: EscalationStore) -> None:
    entry = store.get("pipe_a")
    assert entry.failure_count == 0
    assert entry.first_failed_at is None


def test_record_failure_increments(store: EscalationStore) -> None:
    store.record_failure("pipe_a")
    store.record_failure("pipe_a")
    entry = store.get("pipe_a")
    assert entry.failure_count == 2


def test_first_failed_at_set_on_first_failure(store: EscalationStore) -> None:
    store.record_failure("pipe_a")
    entry = store.get("pipe_a")
    assert entry.first_failed_at is not None


def test_first_failed_at_not_overwritten(store: EscalationStore) -> None:
    store.record_failure("pipe_a")
    first = store.get("pipe_a").first_failed_at
    store.record_failure("pipe_a")
    second = store.get("pipe_a").first_failed_at
    assert first == second


def test_reset_removes_entry(store: EscalationStore) -> None:
    store.record_failure("pipe_a")
    store.reset("pipe_a")
    assert store.get("pipe_a").failure_count == 0


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "escalation.json"
    s1 = EscalationStore(path)
    s1.record_failure("pipe_b")
    s1.record_failure("pipe_b")
    s2 = EscalationStore(path)
    assert s2.get("pipe_b").failure_count == 2


def test_check_escalation_below_threshold(store: EscalationStore) -> None:
    store.record_failure("pipe_a")
    result = check_escalation("pipe_a", store, threshold=3)
    assert not result.should_escalate
    assert result.failure_count == 1


def test_check_escalation_at_threshold(store: EscalationStore) -> None:
    for _ in range(3):
        store.record_failure("pipe_a")
    result = check_escalation("pipe_a", store, threshold=3)
    assert result.should_escalate


def test_check_escalation_above_threshold(store: EscalationStore) -> None:
    for _ in range(5):
        store.record_failure("pipe_a")
    result = check_escalation("pipe_a", store, threshold=3)
    assert result.should_escalate
    assert result.failure_count == 5


def test_mark_escalated_updates_timestamp(store: EscalationStore) -> None:
    store.record_failure("pipe_a")
    store.mark_escalated("pipe_a")
    entry = store.get("pipe_a")
    assert entry.last_escalated_at is not None


def test_summary_contains_pipeline_name(store: EscalationStore) -> None:
    result = check_escalation("my_pipe", store, threshold=2)
    assert "my_pipe" in result.summary


def test_escalation_entry_roundtrip() -> None:
    e = EscalationEntry(
        pipeline="p",
        failure_count=4,
        first_failed_at="2024-01-01T00:00:00+00:00",
        last_escalated_at="2024-01-02T00:00:00+00:00",
    )
    restored = EscalationEntry.from_dict(e.to_dict())
    assert restored.pipeline == e.pipeline
    assert restored.failure_count == e.failure_count
    assert restored.first_failed_at == e.first_failed_at
    assert restored.last_escalated_at == e.last_escalated_at
