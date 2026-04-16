"""Tests for pipewatch.annotation."""
import pytest
from pathlib import Path
from pipewatch.annotation import Annotation, AnnotationStore


@pytest.fixture
def store(tmp_path: Path) -> AnnotationStore:
    return AnnotationStore(tmp_path / "annotations.json")


def test_empty_store_returns_no_entries(store: AnnotationStore) -> None:
    assert store.all() == []


def test_add_annotation(store: AnnotationStore) -> None:
    ann = store.add("etl_daily", "Looks slow today", "alice")
    assert ann.pipeline == "etl_daily"
    assert ann.note == "Looks slow today"
    assert ann.author == "alice"


def test_get_returns_only_matching(store: AnnotationStore) -> None:
    store.add("etl_daily", "note1", "alice")
    store.add("etl_weekly", "note2", "bob")
    results = store.get("etl_daily")
    assert len(results) == 1
    assert results[0].note == "note1"


def test_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "annotations.json"
    s1 = AnnotationStore(path)
    s1.add("pipe_a", "first note", "dev")
    s2 = AnnotationStore(path)
    assert len(s2.get("pipe_a")) == 1
    assert s2.get("pipe_a")[0].note == "first note"


def test_clear_removes_entries(store: AnnotationStore) -> None:
    store.add("pipe_a", "n1", "dev")
    store.add("pipe_a", "n2", "dev")
    store.add("pipe_b", "n3", "dev")
    removed = store.clear("pipe_a")
    assert removed == 2
    assert store.get("pipe_a") == []
    assert len(store.get("pipe_b")) == 1


def test_all_returns_all_entries(store: AnnotationStore) -> None:
    store.add("a", "n1", "x")
    store.add("b", "n2", "y")
    assert len(store.all()) == 2


def test_to_dict_round_trip() -> None:
    from datetime import datetime, timezone
    ann = Annotation("pipe", "note", "author", datetime(2024, 1, 1, tzinfo=timezone.utc))
    d = ann.to_dict()
    ann2 = Annotation.from_dict(d)
    assert ann2.pipeline == ann.pipeline
    assert ann2.note == ann.note
    assert ann2.author == ann.author
    assert ann2.created_at == ann.created_at
