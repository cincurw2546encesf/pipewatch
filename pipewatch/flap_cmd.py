"""CLI sub-command for flap detection."""
from __future__ import annotations

import click

from pipewatch.flap import check_all_flap, DEFAULT_THRESHOLD, DEFAULT_WINDOW
from pipewatch.history import HistoryStore


@click.group("flap")
def flap_cmd() -> None:
    """Detect pipelines that oscillate between OK and failed/stale."""


@flap_cmd.command("check")
@click.option("--window", default=DEFAULT_WINDOW, show_default=True,
              help="Number of recent runs to inspect.")
@click.option("--threshold", default=DEFAULT_THRESHOLD, show_default=True,
              help="Minimum transitions to flag as flapping.")
@click.pass_context
def check_flap_cmd(ctx: click.Context, window: int, threshold: int) -> None:
    """Check all configured pipelines for flapping behaviour."""
    app_cfg = ctx.obj["config"]
    history_file = ctx.obj.get("history_file", "pipewatch_history.json")
    store = HistoryStore(history_file)

    pipeline_names = [p.name for p in app_cfg.pipelines]
    if not pipeline_names:
        click.echo("No pipelines configured.")
        return

    results = check_all_flap(pipeline_names, store, window=window, threshold=threshold)
    flapping = [r for r in results if r.is_flapping]

    for r in results:
        icon = "⚠" if r.is_flapping else "✓"
        click.echo(f"  {icon}  {r.summary}")

    click.echo("")
    if flapping:
        click.echo(f"{len(flapping)} pipeline(s) are flapping.", err=False)
        ctx.exit(1)
    else:
        click.echo("All pipelines are stable.")
