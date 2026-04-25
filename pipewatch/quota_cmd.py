"""CLI commands for quota management."""

from __future__ import annotations

from pathlib import Path

import click

from pipewatch.quota import QuotaStore


def _get_store(ctx: click.Context) -> QuotaStore:
    cfg = ctx.obj["app_cfg"]
    return QuotaStore(Path(cfg.state_dir) / "quota.json")


@click.group("quota")
def quota_cmd() -> None:
    """Manage per-pipeline failure quotas."""


@quota_cmd.command("configure")
@click.argument("pipeline")
@click.option("--max-failures", required=True, type=int, help="Max failures allowed in window.")
@click.option("--window-hours", default=24.0, show_default=True, type=float, help="Rolling window in hours.")
@click.pass_context
def configure_quota(ctx: click.Context, pipeline: str, max_failures: int, window_hours: float) -> None:
    """Configure a quota for a pipeline."""
    store = _get_store(ctx)
    store.configure(pipeline, max_failures, window_hours)
    click.echo(f"Quota set for '{pipeline}': {max_failures} failures / {window_hours}h window.")


@quota_cmd.command("record")
@click.argument("pipeline")
@click.pass_context
def record_failure(ctx: click.Context, pipeline: str) -> None:
    """Record a failure for a pipeline and report quota status."""
    store = _get_store(ctx)
    entry = store.record_failure(pipeline)
    if entry is None:
        click.echo(f"No quota configured for '{pipeline}'. Use 'quota configure' first.", err=True)
        raise SystemExit(1)
    count = entry.failures_in_window()
    status = "EXCEEDED" if entry.exceeded() else "OK"
    click.echo(f"[{status}] '{pipeline}': {count}/{entry.max_failures} failures in {entry.window_hours}h window.")


@quota_cmd.command("list")
@click.pass_context
def list_quotas(ctx: click.Context) -> None:
    """List all configured quotas and their current usage."""
    store = _get_store(ctx)
    entries = store.all()
    if not entries:
        click.echo("No quotas configured.")
        return
    for entry in entries:
        count = entry.failures_in_window()
        status = "EXCEEDED" if entry.exceeded() else "OK"
        click.echo(f"  [{status}] {entry.pipeline}: {count}/{entry.max_failures} in {entry.window_hours}h")


@quota_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_quota(ctx: click.Context, pipeline: str) -> None:
    """Reset failure count for a pipeline."""
    store = _get_store(ctx)
    if store.reset(pipeline):
        click.echo(f"Quota reset for '{pipeline}'.")
    else:
        click.echo(f"No quota found for '{pipeline}'.", err=True)
        raise SystemExit(1)
