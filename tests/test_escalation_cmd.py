"""Tests for pipewatch.escalation_cmd."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.escalation import EscalationStore
from pipewatch.escalation_cmd import escalation_cmd


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _make_pipeline(name: str) -> MagicMock:
    p = MagicMock()
    p.name = name
    return p


@pytest.fixture
def ctx_obj(tmp_path: Path) -> dict:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.escalation_threshold = 3
    cfg.pipelines = [_make_pipeline("pipe_a"), _make_pipeline("pipe_b")]
    return {"app_cfg": cfg}


def invoke(runner: CliRunner, ctx_obj: dict, *args: str):
    return runner.invoke(escalation_cmd, list(args), obj=ctx_obj)


def test_list_shows_pipelines(runner: CliRunner, ctx_obj: dict) -> None:
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "pipe_b" in result.output


def test_list_no_pipelines(runner: CliRunner, ctx_obj: dict) -> None:
    ctx_obj["app_cfg"].pipelines = []
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No pipelines" in result.output


def test_reset_outputs_confirmation(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = EscalationStore(tmp_path / "escalation.json")
    store.record_failure("pipe_a")
    result = invoke(runner, ctx_obj, "reset", "pipe_a")
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "reset" in result.output.lower()


def test_reset_clears_counter(runner: CliRunner, ctx_obj: dict, tmp_path: Path) -> None:
    store = EscalationStore(tmp_path / "escalation.json")
    store.record_failure("pipe_a")
    store.record_failure("pipe_a")
    invoke(runner, ctx_obj, "reset", "pipe_a")
    reloaded = EscalationStore(tmp_path / "escalation.json")
    assert reloaded.get("pipe_a").failure_count == 0
