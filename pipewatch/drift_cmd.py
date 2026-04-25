"""CLI commands for drift detection."""
from __future__ import annotations

import click

from pipewatch.drift import check_drift, check_all_drift
from pipewatch.history import HistoryStore


@click.group("drift")
def drift_cmd() -> None:
    """Detect cadence drift in pipeline run intervals."""


@drift_cmd.command("check")
@click.option("--pipeline", "-p", default=None, help="Single pipeline to check.")
@click.option("--tolerance", default=0.5, show_default=True, help="Fractional tolerance (e.g. 0.5 = 50%).")
@click.pass_context
def check_drift_cmd(ctx: click.Context, pipeline: str | None, tolerance: float) -> None:
    """Check pipelines for run-interval drift."""
    app_cfg = ctx.obj["config"]
    history_file = ctx.obj.get("history_file", "pipewatch_history.json")
    store = HistoryStore(history_file)

    if pipeline:
        cfg = app_cfg.pipelines.get(pipeline)
        if cfg is None:
            click.echo(f"Unknown pipeline: {pipeline}", err=True)
            raise SystemExit(1)
        interval = getattr(cfg, "expected_interval_seconds", None)
        if interval is None:
            click.echo(f"{pipeline}: no expected_interval_seconds configured")
            return
        result = check_drift(pipeline, interval, store, tolerance=tolerance)
        icon = "\u26a0\ufe0f" if result.exceeded else "\u2705"
        click.echo(f"{icon}  {result.summary()}")
        if result.exceeded:
            raise SystemExit(2)
        return

    results = check_all_drift(app_cfg.pipelines, store, tolerance=tolerance)
    if not results:
        click.echo("No pipelines have expected_interval_seconds configured.")
        return

    any_exceeded = False
    for r in results:
        icon = "\u26a0\ufe0f" if r.exceeded else "\u2705"
        click.echo(f"{icon}  {r.summary()}")
        if r.exceeded:
            any_exceeded = True

    if any_exceeded:
        raise SystemExit(2)
