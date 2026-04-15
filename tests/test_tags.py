"""Tests for pipewatch.tags — tag filtering and grouping."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.tags import TagIndex, filter_by_tags, group_by_tag


def _make_result(name: str, tags: list[str], status: CheckStatus = CheckStatus.OK) -> CheckResult:
    pipeline = MagicMock()
    pipeline.name = name
    pipeline.tags = tags
    result = MagicMock(spec=CheckResult)
    result.pipeline = pipeline
    result.status = status
    return result


@pytest.fixture()
def results():
    return [
        _make_result("etl-a", ["finance", "daily"]),
        _make_result("etl-b", ["marketing", "daily"]),
        _make_result("etl-c", ["finance", "hourly"]),
        _make_result("etl-d", []),
    ]


# --- TagIndex ---

def test_tag_index_all_tags(results):
    idx = TagIndex().build(results)
    assert idx.all_tags() == ["daily", "finance", "hourly", "marketing"]


def test_tag_index_get_known_tag(results):
    idx = TagIndex().build(results)
    finance = idx.get("finance")
    assert len(finance) == 2
    names = {r.pipeline.name for r in finance}
    assert names == {"etl-a", "etl-c"}


def test_tag_index_get_unknown_tag(results):
    idx = TagIndex().build(results)
    assert idx.get("nonexistent") == []


# --- filter_by_tags ---

def test_filter_include_single_tag(results):
    out = filter_by_tags(results, include=["finance"])
    assert {r.pipeline.name for r in out} == {"etl-a", "etl-c"}


def test_filter_include_multiple_tags_union(results):
    out = filter_by_tags(results, include=["finance", "marketing"])
    assert {r.pipeline.name for r in out} == {"etl-a", "etl-b", "etl-c"}


def test_filter_exclude_tag(results):
    out = filter_by_tags(results, exclude=["daily"])
    names = {r.pipeline.name for r in out}
    assert "etl-a" not in names
    assert "etl-b" not in names
    assert "etl-c" in names


def test_filter_include_and_exclude(results):
    out = filter_by_tags(results, include=["finance"], exclude=["hourly"])
    assert {r.pipeline.name for r in out} == {"etl-a"}


def test_filter_no_criteria_returns_all(results):
    out = filter_by_tags(results)
    assert len(out) == len(results)


def test_filter_pipeline_no_tags(results):
    """Pipelines with no tags are excluded when include filter is active."""
    out = filter_by_tags(results, include=["finance"])
    assert all(r.pipeline.name != "etl-d" for r in out)


# --- group_by_tag ---

def test_group_by_tag_keys(results):
    groups = group_by_tag(results)
    assert set(groups.keys()) == {"finance", "daily", "marketing", "hourly"}


def test_group_by_tag_counts(results):
    groups = group_by_tag(results)
    assert len(groups["daily"]) == 2
    assert len(groups["finance"]) == 2
    assert len(groups["hourly"]) == 1


def test_group_by_tag_no_tags_pipeline(results):
    """A pipeline with no tags does not appear in any group."""
    groups = group_by_tag(results)
    all_results = [r for lst in groups.values() for r in lst]
    assert all(r.pipeline.name != "etl-d" for r in all_results)
