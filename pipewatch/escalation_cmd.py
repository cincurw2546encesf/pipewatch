"""CLI sub-commands for managing escalation state."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.escalation import EscalationStore, check_escalation


def _get_store(ctx: click.Context) -> EscalationStore:
    cfg = ctx.obj["app_cfg"]
    path = Path(cfg.state_dir) / "escalation.json"
    return EscalationStore(path)


@click.group("escalation")
def escalation_cmd() -> None:
    """Manage pipeline escalation state."""


@escalation_cmd.command("list")
@click.pass_context
def list_escalations(ctx: click.Context) -> None:
    """List current escalation counters."""
    store = _get_store(ctx)
    cfg = ctx.obj["app_cfg"]
    threshold = getattr(cfg, "escalation_threshold", 3)
    pipelines = [p.name for p in cfg.pipelines]
    if not pipelines:
        click.echo("No pipelines configured.")
        return
    for name in pipelines:
        result = check_escalation(name, store, threshold)
        click.echo(result.summary)


@escalation_cmd.command("reset")
@click.argument("pipeline")
@click.pass_context
def reset_escalation(ctx: click.Context, pipeline: str) -> None:
    """Reset the failure counter for PIPELINE."""
    store = _get_store(ctx)
    store.reset(pipeline)
    click.echo(f"Escalation state reset for '{pipeline}'.")
