"""CLI commands for inspecting and managing the dead-letter queue."""
from __future__ import annotations

import click

from pipewatch.deadletter import DeadLetterStore


def _get_store(ctx: click.Context) -> DeadLetterStore:
    cfg = ctx.obj["app_cfg"]
    path = getattr(cfg, "deadletter_file", "deadletter.json")
    return DeadLetterStore(path)


@click.group("deadletter")
def deadletter_cmd() -> None:
    """Manage pipelines stuck in the dead-letter queue."""


@deadletter_cmd.command("list")
@click.pass_context
def list_deadletter(ctx: click.Context) -> None:
    """List all dead-lettered pipelines."""
    store = _get_store(ctx)
    entries = store.all()
    if not entries:
        click.echo("No dead-lettered pipelines.")
        return
    for e in entries:
        click.echo(e.summary())
        if e.note:
            click.echo(f"  note: {e.note}")


@deadletter_cmd.command("remove")
@click.argument("pipeline")
@click.pass_context
def remove_deadletter(ctx: click.Context, pipeline: str) -> None:
    """Remove a pipeline from the dead-letter queue."""
    store = _get_store(ctx)
    if store.remove(pipeline):
        click.echo(f"Removed '{pipeline}' from dead-letter queue.")
    else:
        click.echo(f"'{pipeline}' not found in dead-letter queue.", err=True)
        ctx.exit(1)


@deadletter_cmd.command("clear")
@click.confirmation_option(prompt="Clear all dead-letter entries?")
@click.pass_context
def clear_deadletter(ctx: click.Context) -> None:
    """Clear the entire dead-letter queue."""
    store = _get_store(ctx)
    store.clear()
    click.echo("Dead-letter queue cleared.")
