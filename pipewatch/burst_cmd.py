"""CLI commands for burst detection."""
from __future__ import annotations

import click

from pipewatch.burst import check_all_burst
from pipewatch.history import HistoryStore


@click.group("burst")
def burst_cmd() -> None:
    """Detect pipelines that have run too many times in a short window."""


@burst_cmd.command("check")
@click.pass_context
def check_burst_cmd(ctx: click.Context) -> None:
    """Check all pipelines for burst activity."""
    app_cfg = ctx.obj["config"]
    history_file = ctx.obj.get("history_file", "pipewatch_history.json")
    store = HistoryStore(history_file)

    results = check_all_burst(app_cfg.pipelines, store)

    if not results:
        click.echo("No pipelines have burst configuration.")
        return

    has_violation = False
    for r in results:
        icon = "\u274c" if r.exceeded else "\u2705"
        click.echo(f"{icon}  {r.summary()}")
        if r.exceeded:
            has_violation = True

    if has_violation:
        ctx.exit(1)
