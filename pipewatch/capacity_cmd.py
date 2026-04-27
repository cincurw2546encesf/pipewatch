"""CLI commands for capacity checks."""
from __future__ import annotations

import sys
import click

from pipewatch.capacity import check_all_capacity
from pipewatch.history import HistoryStore
from pipewatch.config import AppConfig


@click.group(name="capacity")
def capacity_cmd() -> None:
    """Check pipeline run-duration capacity utilisation."""


@capacity_cmd.command(name="check")
@click.option("--window", default=10, show_default=True, help="Number of recent runs to average.")
@click.pass_context
def check_capacity_cmd(ctx: click.Context, window: int) -> None:
    """Print capacity utilisation for all pipelines."""
    app_cfg: AppConfig = ctx.obj["config"]
    history_file: str = ctx.obj.get("history_file", "pipewatch_history.json")
    store = HistoryStore(history_file)

    results = check_all_capacity(app_cfg.pipelines, store, window=window)

    any_exceeded = False
    for r in results:
        icon = "\u274c" if r.exceeded else "\u2705"
        click.echo(f"{icon}  {r.summary()}")
        if r.exceeded:
            any_exceeded = True

    if any_exceeded:
        click.echo("\nOne or more pipelines have exceeded their duration budget.", err=True)
        sys.exit(1)
