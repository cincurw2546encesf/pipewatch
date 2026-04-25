"""CLI sub-command: `pipewatch window check`."""
from __future__ import annotations

import sys

import click

from pipewatch.history import HistoryStore
from pipewatch.window import check_all_windows


@click.group("window")
def window_cmd() -> None:
    """Sliding-window success-rate checks."""


@window_cmd.command("check")
@click.option("--hours", default=24, show_default=True, help="Window size in hours.")
@click.option(
    "--threshold",
    default=0.8,
    show_default=True,
    help="Minimum success rate (0–1).",
)
@click.pass_context
def check_window_cmd(ctx: click.Context, hours: int, threshold: float) -> None:
    """Check success rates across a sliding time window."""
    app_cfg = ctx.obj["app_cfg"]
    history_file = getattr(app_cfg, "history_file", "pipewatch_history.json")
    store = HistoryStore(history_file)

    pipeline_names = [p.name for p in app_cfg.pipelines]
    results = check_all_windows(
        pipeline_names, store, window_hours=hours, threshold=threshold
    )

    if not results:
        click.echo("No history found for any pipeline.")
        return

    violations = [r for r in results if r.violated]

    for r in results:
        icon = "\u274c" if r.violated else "\u2705"
        click.echo(f"  {icon}  {r.summary()}")

    click.echo()
    if violations:
        click.echo(
            f"Window check FAILED: {len(violations)}/{len(results)} pipeline(s) "
            f"below {threshold * 100:.0f}% threshold."
        )
        sys.exit(1)
    else:
        click.echo(f"Window check passed: all {len(results)} pipeline(s) within threshold.")
