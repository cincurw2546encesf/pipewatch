"""CLI commands for viewing the pipewatch audit log."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.audit import AuditStore


def _get_store(ctx: click.Context) -> AuditStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "audit.json"
    return AuditStore(path)


@click.group("audit")
def audit_cmd() -> None:
    """View and manage the audit log."""


@audit_cmd.command("list")
@click.option("--pipeline", default=None, help="Filter by pipeline name.")
@click.pass_context
def list_audit(ctx: click.Context, pipeline: str | None) -> None:
    """List recorded audit entries."""
    store = _get_store(ctx)
    entries = store.get(pipeline=pipeline)
    if not entries:
        click.echo("No audit entries found.")
        return
    for e in entries:
        ts = e.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        click.echo(f"[{ts}] {e.action:<22} {e.pipeline:<20} ({e.actor}) {e.detail}")


@audit_cmd.command("clear")
@click.option("--pipeline", default=None, help="Clear only entries for this pipeline.")
@click.confirmation_option(prompt="Are you sure you want to clear audit entries?")
@click.pass_context
def clear_audit(ctx: click.Context, pipeline: str | None) -> None:
    """Clear audit log entries."""
    store = _get_store(ctx)
    removed = store.clear(pipeline=pipeline)
    label = f"pipeline '{pipeline}'" if pipeline else "all pipelines"
    click.echo(f"Removed {removed} audit entr{'y' if removed == 1 else 'ies'} for {label}.")
