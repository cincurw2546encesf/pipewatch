"""CLI commands for pipeline lag reporting."""
from __future__ import annotations

import click

from pipewatch.checker import PipelineChecker
from pipewatch.config import AppConfig
from pipewatch.lag import check_all_lag, lag_summary
from pipewatch.state import StateStore


@click.group("lag")
def lag_cmd() -> None:
    """Lag tracking commands."""


@lag_cmd.command("check")
@click.option("--pipeline", default=None, help="Check a specific pipeline only.")
@click.pass_context
def check_lag_cmd(ctx: click.Context, pipeline: str | None) -> None:
    """Show how far behind each pipeline is vs its expected cadence."""
    app_cfg: AppConfig = ctx.obj["config"]
    store = StateStore(app_cfg.state_file)

    checker = PipelineChecker(store)
    pipelines = app_cfg.pipelines
    if pipeline:
        pipelines = [p for p in pipelines if p.name == pipeline]
        if not pipelines:
            click.echo(f"Pipeline '{pipeline}' not found.", err=True)
            ctx.exit(1)
            return

    results = [checker.check(p) for p in pipelines]
    lag_results = check_all_lag(results)

    click.echo(lag_summary(lag_results))

    exceeded = [r for r in lag_results if r.exceeded]
    if exceeded:
        ctx.exit(1)
