"""CLI commands for pipeline probing."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.probe import ProbeStore, run_probe


def _get_store(ctx: click.Context) -> ProbeStore:
    cfg = ctx.obj["app_cfg"]
    return ProbeStore(Path(cfg.state_dir) / "probes.json")


@click.group("probe")
def probe_cmd() -> None:
    """Liveness and readiness probe commands."""


@probe_cmd.command("check")
@click.argument("pipeline")
@click.argument("url")
@click.option("--type", "probe_type", default="liveness", show_default=True, help="Probe type label.")
@click.option("--timeout", default=5.0, show_default=True, help="Request timeout in seconds.")
@click.pass_context
def check_probe(ctx: click.Context, pipeline: str, url: str, probe_type: str, timeout: float) -> None:
    """Run a probe against URL and record the result."""
    store = _get_store(ctx)
    result = run_probe(pipeline, url, probe_type=probe_type, timeout=timeout)
    store.record(result)
    click.echo(result.summary())
    if not result.reachable:
        ctx.exit(1)


@probe_cmd.command("list")
@click.pass_context
def list_probes(ctx: click.Context) -> None:
    """List the most recent probe result for every known pipeline."""
    store = _get_store(ctx)
    results = store.all()
    if not results:
        click.echo("No probe results recorded.")
        return
    for r in results:
        click.echo(r.summary())


@probe_cmd.command("show")
@click.argument("pipeline")
@click.pass_context
def show_probe(ctx: click.Context, pipeline: str) -> None:
    """Show the latest probe result for PIPELINE."""
    store = _get_store(ctx)
    result = store.get(pipeline)
    if result is None:
        click.echo(f"No probe result found for '{pipeline}'.")
        ctx.exit(1)
        return
    click.echo(result.summary())
    if result.error:
        click.echo(f"  error: {result.error}")
