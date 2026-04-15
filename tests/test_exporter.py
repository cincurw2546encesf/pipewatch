"""Tests for pipewatch.exporter."""

from __future__ import annotations

import csv
import json
import pathlib
from datetime import datetime, timezone

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.exporter import export_csv, export_json, export_results


_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def ok_result() -> CheckResult:
    return CheckResult(
        pipeline_name="orders",
        status=CheckStatus.OK,
        message="Last run 30 s ago",
        last_run=_NOW,
        age_seconds=30,
    )


@pytest.fixture()
def stale_result() -> CheckResult:
    return CheckResult(
        pipeline_name="inventory",
        status=CheckStatus.STALE,
        message="No run in 7200 s",
        last_run=None,
        age_seconds=None,
    )


def test_export_json_creates_file(tmp_path, ok_result, stale_result):
    out = tmp_path / "results.json"
    export_json([ok_result, stale_result], out)
    assert out.exists()


def test_export_json_structure(tmp_path, ok_result):
    out = tmp_path / "results.json"
    export_json([ok_result], out)
    data = json.loads(out.read_text())
    assert "exported_at" in data
    assert len(data["pipelines"]) == 1
    p = data["pipelines"][0]
    assert p["name"] == "orders"
    assert p["status"] == "ok"
    assert p["age_seconds"] == 30


def test_export_json_null_last_run(tmp_path, stale_result):
    out = tmp_path / "results.json"
    export_json([stale_result], out)
    data = json.loads(out.read_text())
    assert data["pipelines"][0]["last_run"] is None


def test_export_csv_creates_file(tmp_path, ok_result):
    out = tmp_path / "results.csv"
    export_csv([ok_result], out)
    assert out.exists()


def test_export_csv_columns(tmp_path, ok_result, stale_result):
    out = tmp_path / "results.csv"
    export_csv([ok_result, stale_result], out)
    reader = csv.DictReader(out.read_text().splitlines())
    rows = list(reader)
    assert len(rows) == 2
    assert set(rows[0].keys()) == {
        "pipeline_name", "status", "message", "last_run", "age_seconds"
    }
    assert rows[0]["pipeline_name"] == "orders"
    assert rows[1]["pipeline_name"] == "inventory"
    assert rows[1]["last_run"] == ""


def test_export_results_dispatches_json(tmp_path, ok_result):
    out = tmp_path / "out.json"
    export_results([ok_result], out, fmt="json")
    data = json.loads(out.read_text())
    assert "pipelines" in data


def test_export_results_dispatches_csv(tmp_path, ok_result):
    out = tmp_path / "out.csv"
    export_results([ok_result], out, fmt="csv")
    assert "pipeline_name" in out.read_text()


def test_export_results_invalid_format(tmp_path, ok_result):
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_results([ok_result], tmp_path / "out.xml", fmt="xml")
