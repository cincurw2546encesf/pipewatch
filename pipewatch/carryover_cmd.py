"""CLI commands for carryover checks."""
from __future__ import annotations

import sys

import click

from pipewatch.carryover import check_all_carryover
from pipewatch.state import StateStore


@click.group("carryover")
def carryover_cmd():
    """Detect pipelines stuck in a running state."""


@carryover_cmd.command("check")
@click.pass_context
def check_carryover_cmd(ctx):
    """Check all pipelines for carryover (stuck runs)."""
    app_cfg = ctx.obj["app_cfg"]
    store = StateStore(app_cfg.state_file)

    results = check_all_carryover(app_cfg, store)

    if not results:
        click.echo("No active (unfinished) runs detected.")
        return

    exceeded = [r for r in results if r.exceeded]
    ok = [r for r in results if not r.exceeded]

    for r in ok:
        click.echo(f"  OK   {r.summary()}")
    for r in exceeded:
        click.echo(f"  WARN {r.summary()}")

    if exceeded:
        click.echo(f"\n{len(exceeded)} pipeline(s) exceeded carryover limit.", err=True)
        sys.exit(1)
    else:
        click.echo(f"\n{len(ok)} pipeline(s) running within limits.")
