"""CLI commands for drain detection."""
from __future__ import annotations

import click

from pipewatch.drain import check_all_drain
from pipewatch.history import HistoryStore


@click.group("drain")
def drain_cmd():
    """Detect pipelines running with suspiciously short durations."""


@drain_cmd.command("check")
@click.option("--window", default=10, show_default=True, help="Number of recent runs to examine.")
@click.pass_context
def check_drain_cmd(ctx: click.Context, window: int):
    """Check all pipelines for drain conditions."""
    app_cfg = ctx.obj["app_cfg"]
    store = HistoryStore(app_cfg.history_file)
    pipelines = app_cfg.pipelines

    results = check_all_drain(pipelines, store, window=window)

    if not results:
        click.echo("No pipelines have min_duration_seconds configured.")
        return

    any_draining = False
    for r in results:
        icon = "\u26a0\ufe0f" if r.is_draining else "\u2705"
        click.echo(f"{icon}  {r.summary()}")
        if r.is_draining:
            any_draining = True

    ctx.exit(1 if any_draining else 0)
