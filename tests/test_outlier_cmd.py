"""Tests for pipewatch.outlier_cmd."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.history import HistoryEntry, HistoryStore
from pipewatch.outlier import OutlierResult
from pipewatch.outlier_cmd import outlier_cmd


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_pipeline(name: str = "pipe") -> PipelineConfig:
    return PipelineConfig(name=name, max_age_minutes=60)


def _make_app_cfg(*names: str) -> AppConfig:
    return AppConfig(pipelines=[_make_pipeline(n) for n in names])


def _ctx_obj(tmp_path: Path, *names: str) -> dict:
    return {
        "config": _make_app_cfg(*names),
        "history_file": str(tmp_path / "history.json"),
    }


def _invoke(runner: CliRunner, ctx: dict, *args):
    return runner.invoke(outlier_cmd, ["check", *args], obj=ctx, catch_exceptions=False)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_check_no_outliers_exits_zero(runner: CliRunner, tmp_path: Path):
    ctx = _ctx_obj(tmp_path, "alpha")
    result = _invoke(runner, ctx)
    assert result.exit_code == 0
    assert "No outliers detected" in result.output


def test_check_with_outlier_exits_nonzero(runner: CliRunner, tmp_path: Path):
    ok_result = OutlierResult(
        pipeline="alpha", mean_seconds=10.0, stddev_seconds=1.0,
        last_duration_seconds=50.0, z_score=40.0, is_outlier=True, threshold=3.0,
    )
    ctx = _ctx_obj(tmp_path, "alpha")
    with patch("pipewatch.outlier_cmd.check_all_outliers", return_value=[ok_result]):
        result = _invoke(runner, ctx)
    assert result.exit_code == 1
    assert "1 outlier(s) detected" in result.output


def test_check_output_contains_pipeline_name(runner: CliRunner, tmp_path: Path):
    ctx = _ctx_obj(tmp_path, "my_pipeline")
    result = _invoke(runner, ctx)
    assert "my_pipeline" in result.output


def test_check_custom_threshold_passed(runner: CliRunner, tmp_path: Path):
    ctx = _ctx_obj(tmp_path, "pipe")
    captured = {}

    def fake_check_all(pipelines, store, threshold, window):
        captured["threshold"] = threshold
        return []

    with patch("pipewatch.outlier_cmd.check_all_outliers", side_effect=fake_check_all):
        _invoke(runner, ctx, "--threshold", "2.0")

    assert captured["threshold"] == pytest.approx(2.0)


def test_check_custom_window_passed(runner: CliRunner, tmp_path: Path):
    ctx = _ctx_obj(tmp_path, "pipe")
    captured = {}

    def fake_check_all(pipelines, store, threshold, window):
        captured["window"] = window
        return []

    with patch("pipewatch.outlier_cmd.check_all_outliers", side_effect=fake_check_all):
        _invoke(runner, ctx, "--window", "10")

    assert captured["window"] == 10
