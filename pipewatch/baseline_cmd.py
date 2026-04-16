"""CLI commands for baseline management."""
from __future__ import annotations
import click
from pathlib import Path

from pipewatch.baseline import BaselineStore
from pipewatch.baseline_checker import check_baseline
from pipewatch.history import HistoryStore


def _get_baseline_store(ctx: click.Context) -> BaselineStore:
    cfg = ctx.obj["app_cfg"]
    return BaselineStore(Path(cfg.state_dir) / "baselines.json")


def _get_history_store(ctx: click.Context) -> HistoryStore:
    cfg = ctx.obj["app_cfg"]
    return HistoryStore(Path(cfg.state_dir) / "history.json")


@click.group("baseline")
def baseline_cmd() -> None:
    """Manage pipeline duration baselines."""


@baseline_cmd.command("list")
@click.pass_context
def list_baselines(ctx: click.Context) -> None:
    """List all recorded baselines."""
    store = _get_baseline_store(ctx)
    entries = store.all()
    if not entries:
        click.echo("No baselines recorded.")
        return
    for e in entries:
        click.echo(f"{e.pipeline}: avg={e.avg_duration_seconds}s samples={e.sample_count}")


@baseline_cmd.command("check")
@click.argument("pipeline")
@click.option("--threshold", default=50.0, show_default=True, help="Deviation % to flag as slow")
@click.pass_context
def check_cmd(ctx: click.Context, pipeline: str, threshold: float) -> None:
    """Check a pipeline run duration against its baseline."""
    bs = _get_baseline_store(ctx)
    hs = _get_history_store(ctx)
    result = check_baseline(pipeline, bs, hs, threshold_pct=threshold)
    click.echo(result.summary())
    if result.exceeded_threshold:
        ctx.exit(1)


@baseline_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_baseline(ctx: click.Context, pipeline: str) -> None:
    """Remove a pipeline's baseline."""
    store = _get_baseline_store(ctx)
    removed = store.remove(pipeline)
    if removed:
        click.echo(f"Baseline for '{pipeline}' removed.")
    else:
        click.echo(f"No baseline found for '{pipeline}'.")
