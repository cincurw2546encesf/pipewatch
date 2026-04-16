"""Tests for pipewatch.silence_cmd CLI commands."""
from __future__ import annotations

from pathlib import Path
from click.testing import CliRunner
import pytest

from pipewatch.silence_cmd import silence_cmd


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def ctx_obj(tmp_path: Path):
    return {"state_dir": str(tmp_path)}


def invoke(runner, ctx_obj, *args):
    return runner.invoke(silence_cmd, args, obj=ctx_obj, catch_exceptions=False)


def test_add_silence_output(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "add", "pipe_a", "--hours", "3", "--reason", "deploy")
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "deploy" in result.output


def test_list_no_silences(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "list")
    assert result.exit_code == 0
    assert "No active silences" in result.output


def test_add_then_list(runner, ctx_obj):
    invoke(runner, ctx_obj, "add", "pipe_b", "--hours", "1")
    result = invoke(runner, ctx_obj, "list")
    assert "pipe_b" in result.output


def test_remove_existing(runner, ctx_obj):
    invoke(runner, ctx_obj, "add", "pipe_c", "--hours", "2")
    result = invoke(runner, ctx_obj, "remove", "pipe_c")
    assert "Removed" in result.output


def test_remove_nonexistent(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "remove", "ghost_pipe")
    assert "No active silence" in result.output


def test_prune_output(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "prune")
    assert result.exit_code == 0
    assert "Pruned" in result.output
