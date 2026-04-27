"""CLI command group for velocity checks."""
from __future__ import annotations

import sys

import click

from pipewatch.history import HistoryStore
from pipewatch.velocity import check_all_velocity


@click.group("velocity")
def velocity_cmd() -> None:
    """Velocity: detect pipelines running less often than expected."""


@velocity_cmd.command("check")
@click.pass_context
def check_velocity_cmd(ctx: click.Context) -> None:
    """Check run-frequency velocity for all configured pipelines."""
    app_cfg = ctx.obj["config"]
    history_file = ctx.obj.get("history_file", "pipewatch_history.json")

    store = HistoryStore(history_file)
    results = check_all_velocity(app_cfg.pipelines, store)

    if not results:
        click.echo("No velocity configuration found for any pipeline.")
        return

    violations = [r for r in results if r.exceeded]

    for r in results:
        click.echo(r.summary())

    if violations:
        click.echo(
            f"\n{len(violations)} pipeline(s) below expected velocity.",
            err=True,
        )
        sys.exit(1)
    else:
        click.echo("\nAll pipelines within expected velocity.")
