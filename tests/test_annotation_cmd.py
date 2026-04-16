"""Tests for pipewatch.annotation_cmd."""
import pytest
from pathlib import Path
from click.testing import CliRunner
from pipewatch.annotation_cmd import annotation_cmd


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def ctx_obj(tmp_path: Path) -> dict:
    class _Cfg:
        state_dir = str(tmp_path)
    return {"app_cfg": _Cfg()}


def invoke(runner, ctx_obj, *args):
    return runner.invoke(annotation_cmd, args, obj=ctx_obj)


def test_add_annotation_output(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "add", "etl_daily", "looks slow", "--author", "alice")
    assert result.exit_code == 0
    assert "etl_daily" in result.output
    assert "alice" in result.output


def test_list_no_annotations(runner, ctx_obj):
    result = invoke(runner, ctx_obj, "list", "etl_daily")
    assert result.exit_code == 0
    assert "No annotations" in result.output


def test_list_shows_annotation(runner, ctx_obj):
    invoke(runner, ctx_obj, "add", "etl_daily", "my note", "--author", "bob")
    result = invoke(runner, ctx_obj, "list", "etl_daily")
    assert "my note" in result.output
    assert "bob" in result.output


def test_clear_annotations(runner, ctx_obj):
    invoke(runner, ctx_obj, "add", "etl_daily", "n1", "--author", "dev")
    invoke(runner, ctx_obj, "add", "etl_daily", "n2", "--author", "dev")
    result = invoke(runner, ctx_obj, "clear", "etl_daily")
    assert "2" in result.output
    list_result = invoke(runner, ctx_obj, "list", "etl_daily")
    assert "No annotations" in list_result.output
