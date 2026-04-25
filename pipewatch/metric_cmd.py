"""CLI commands for pipeline duration metrics."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.metric import MetricStore


def _get_store(ctx: click.Context) -> MetricStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "metrics.json"
    return MetricStore(path)


@click.group("metric")
def metric_cmd() -> None:
    """Manage pipeline duration metrics."""


@metric_cmd.command("record")
@click.argument("pipeline")
@click.argument("duration", type=float)
@click.pass_context
def record_metric(ctx: click.Context, pipeline: str, duration: float) -> None:
    """Record a duration (seconds) for PIPELINE."""
    store = _get_store(ctx)
    entry = store.record(pipeline, duration)
    click.echo(f"Recorded {entry.duration_seconds:.2f}s for '{pipeline}' at {entry.recorded_at}")


@metric_cmd.command("summary")
@click.argument("pipeline")
@click.pass_context
def show_summary(ctx: click.Context, pipeline: str) -> None:
    """Show duration summary for PIPELINE."""
    store = _get_store(ctx)
    summary = store.summarise(pipeline)
    if summary is None:
        click.echo(f"No metrics recorded for '{pipeline}'.")
        return
    click.echo(str(summary))


@metric_cmd.command("list")
@click.argument("pipeline")
@click.option("--limit", default=20, show_default=True, help="Max entries to show.")
@click.pass_context
def list_metrics(ctx: click.Context, pipeline: str, limit: int) -> None:
    """List recent duration entries for PIPELINE."""
    store = _get_store(ctx)
    entries = store.get(pipeline)[-limit:]
    if not entries:
        click.echo(f"No metrics for '{pipeline}'.")
        return
    for e in entries:
        click.echo(f"  {e.recorded_at}  {e.duration_seconds:.2f}s")


@metric_cmd.command("clear")
@click.argument("pipeline")
@click.pass_context
def clear_metrics(ctx: click.Context, pipeline: str) -> None:
    """Remove all recorded metrics for PIPELINE."""
    store = _get_store(ctx)
    removed = store.clear(pipeline)
    click.echo(f"Removed {removed} metric entry/entries for '{pipeline}'.")
