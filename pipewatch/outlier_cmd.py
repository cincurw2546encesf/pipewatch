"""CLI commands for outlier detection."""
from __future__ import annotations

import click

from pipewatch.history import HistoryStore
from pipewatch.outlier import check_all_outliers


@click.group("outlier")
def outlier_cmd() -> None:
    """Detect pipelines with anomalous run durations."""


@outlier_cmd.command("check")
@click.option("--threshold", default=3.0, show_default=True,
              help="Z-score threshold above which a run is flagged as an outlier.")
@click.option("--window", default=30, show_default=True,
              help="Number of recent history entries to consider.")
@click.pass_context
def check_outlier_cmd(ctx: click.Context, threshold: float, window: int) -> None:
    """Check all pipelines for duration outliers."""
    cfg = ctx.obj["config"]
    history_path = ctx.obj.get("history_file", "pipewatch_history.json")
    store = HistoryStore(history_path)

    results = check_all_outliers(
        cfg.pipelines,
        store,
        threshold=threshold,
        window=window,
    )

    outliers = [r for r in results if r.is_outlier]
    for r in results:
        click.echo(r.summary())

    if outliers:
        click.echo(f"\n{len(outliers)} outlier(s) detected.")
        ctx.exit(1)
    else:
        click.echo("\nNo outliers detected.")
