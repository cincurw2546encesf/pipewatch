"""CLI command group for expiry checks."""
from __future__ import annotations

import click

from pipewatch.checker import PipelineChecker
from pipewatch.config import AppConfig
from pipewatch.expiry import check_all_expiry


@click.group("expiry")
def expiry_cmd() -> None:
    """Expiry tracking commands."""


@expiry_cmd.command("check")
@click.pass_context
def check_expiry_cmd(ctx: click.Context) -> None:
    """Check all pipelines for expiry violations."""
    app_cfg: AppConfig = ctx.obj["config"]
    checker = PipelineChecker(app_cfg)
    results = checker.check_all()

    expiry_results = check_all_expiry(app_cfg.pipelines, results)

    if not expiry_results:
        click.echo("No expiry configuration found for any pipeline.")
        return

    any_expired = False
    for er in expiry_results:
        icon = "\u274c" if er.expired else "\u2705"
        click.echo(f"  {icon}  {er.summary()}")
        if er.expired:
            any_expired = True

    if any_expired:
        ctx.exit(1)
