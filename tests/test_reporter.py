"""Tests for pipewatch.reporter."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.checker import CheckResult, CheckStatus
from pipewatch.reporter import build_csv_report, build_json_report, build_report, build_text_report

FIXED_TS = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def ok_result() -> CheckResult:
    return CheckResult(pipeline_name="ingest", status=CheckStatus.OK, message=None)


@pytest.fixture()
def stale_result() -> CheckResult:
    return CheckResult(pipeline_name="transform", status=CheckStatus.STALE, message="overdue by 2h")


@pytest.fixture()
def failed_result() -> CheckResult:
    return CheckResult(pipeline_name="export", status=CheckStatus.FAILED, message="last run failed")


# --- text ---

def test_text_report_contains_pipeline_names(ok_result, stale_result):
    report = build_text_report([ok_result, stale_result], generated_at=FIXED_TS)
    assert "ingest" in report
    assert "transform" in report


def test_text_report_shows_summary(ok_result, stale_result):
    report = build_text_report([ok_result, stale_result], generated_at=FIXED_TS)
    assert "1/2 pipelines healthy" in report


def test_text_report_shows_message_for_stale(stale_result):
    report = build_text_report([stale_result], generated_at=FIXED_TS)
    assert "overdue by 2h" in report


# --- json ---

def test_json_report_is_valid_json(ok_result, stale_result, failed_result):
    raw = build_json_report([ok_result, stale_result, failed_result], generated_at=FIXED_TS)
    data = json.loads(raw)
    assert data["summary"]["total"] == 3
    assert data["summary"]["ok"] == 1
    assert data["summary"]["stale"] == 1
    assert data["summary"]["failed"] == 1


def test_json_report_pipeline_entries(ok_result):
    raw = build_json_report([ok_result], generated_at=FIXED_TS)
    data = json.loads(raw)
    assert data["pipelines"][0]["name"] == "ingest"
    assert data["pipelines"][0]["status"] == "ok"


# --- csv ---

def test_csv_report_has_header_and_rows(ok_result, stale_result):
    raw = build_csv_report([ok_result, stale_result], generated_at=FIXED_TS)
    lines = raw.strip().splitlines()
    assert lines[0].startswith("generated_at")
    assert len(lines) == 3  # header + 2 rows


def test_csv_report_contains_status(stale_result):
    raw = build_csv_report([stale_result], generated_at=FIXED_TS)
    assert "stale" in raw


# --- dispatch ---

def test_build_report_dispatches_text(ok_result):
    report = build_report([ok_result], fmt="text")
    assert "PipeWatch Report" in report


def test_build_report_dispatches_json(ok_result):
    report = build_report([ok_result], fmt="json")
    json.loads(report)  # must not raise


def test_build_report_dispatches_csv(ok_result):
    report = build_report([ok_result], fmt="csv")
    assert "pipeline_name" in report


def test_build_report_raises_on_unknown_format(ok_result):
    with pytest.raises(ValueError, match="Unsupported report format"):
        build_report([ok_result], fmt="xml")  # type: ignore[arg-type]
