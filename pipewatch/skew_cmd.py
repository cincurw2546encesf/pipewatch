"""CLI sub-command group for skew checks."""
from __future__ import annotations

import click

from pipewatch.history import HistoryStore
from pipewatch.skew import check_all_skew


@click.group("skew")
def skew_cmd() -> None:
    """Detect pipelines whose start times deviate from their expected schedule."""


@skew_cmd.command("check")
@click.pass_context
def check_skew_cmd(ctx: click.Context) -> None:
    """Run skew checks across all configured pipelines."""
    app_cfg = ctx.obj["config"]
    store = HistoryStore(app_cfg.history_file)

    results = check_all_skew(app_cfg.pipelines, store)

    if not results:
        click.echo("No pipelines have skew configuration (expected_hour + max_skew_minutes).")
        return

    any_exceeded = False
    for r in results:
        click.echo(r.summary())
        if r.exceeded:
            any_exceeded = True

    ctx.exit(1 if any_exceeded else 0)
