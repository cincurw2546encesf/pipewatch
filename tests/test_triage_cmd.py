import pytest
from click.testing import CliRunner
from unittest.mock import patch
from pipewatch.triage_cmd import triage_cmd
from pipewatch.checker import CheckResult, CheckStatus
from datetime import datetime, timezone


@pytest.fixture
def runner():
    return CliRunner()


def _utc(y, m, d, h=0, mi=0):
    return datetime(y, m, d, h, mi, tzinfo=timezone.utc)


def _make_result(name, status, last_run=None):
    return CheckResult(
        pipeline=name,
        status=status,
        last_run=last_run or _utc(2024, 1, 1),
        message=f"{name} is {status.value}",
    )


@pytest.fixture
def ok_result():
    return _make_result("pipe-ok", CheckStatus.OK, _utc(2024, 6, 1, 10))


@pytest.fixture
def stale_result():
    return _make_result("pipe-stale", CheckStatus.STALE, _utc(2024, 5, 28))


@pytest.fixture
def failed_result():
    return _make_result("pipe-failed", CheckStatus.FAILED, _utc(2024, 5, 25))


@pytest.fixture
def ctx_obj(ok_result, stale_result, failed_result):
    return {"app_cfg": object(), "results": [ok_result, stale_result, failed_result]}


def invoke(runner, ctx_obj, args):
    return runner.invoke(triage_cmd, args, obj=ctx_obj, catch_exceptions=False)


def test_show_triage_lists_issues(runner, ctx_obj):
    result = invoke(runner, ctx_obj, ["show"])
    assert result.exit_code == 0
    assert "pipe-stale" in result.output or "pipe-failed" in result.output


def test_show_triage_filter_by_status(runner, ctx_obj):
    result = invoke(runner, ctx_obj, ["show", "--status", "failed"])
    assert result.exit_code == 0
    assert "pipe-failed" in result.output
    assert "pipe-stale" not in result.output


def test_show_triage_json_output(runner, ctx_obj):
    result = invoke(runner, ctx_obj, ["show", "--json"])
    assert result.exit_code == 0
    import json
    data = json.loads(result.output)
    assert isinstance(data, list)
    pipelines = [d["pipeline"] for d in data]
    assert "pipe-stale" in pipelines or "pipe-failed" in pipelines


def test_show_triage_no_issues(runner):
    ctx = {"app_cfg": object(), "results": []}
    result = invoke(runner, ctx, ["show"])
    assert result.exit_code == 0
    assert "No issues" in result.output


def test_summary_triage_output(runner, ctx_obj):
    result = invoke(runner, ctx_obj, ["summary"])
    assert result.exit_code == 0
    assert len(result.output.strip()) > 0
