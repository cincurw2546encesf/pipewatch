"""CLI commands for the watchdog feature."""
from __future__ import annotations

import json

import click

from pipewatch.state import StateStore
from pipewatch.watchdog import run_watchdog


@click.group("watchdog")
def watchdog_cmd() -> None:
    """Detect pipelines that have never run or exceeded hard deadlines."""


@watchdog_cmd.command("check")
@click.option("--format", "fmt", type=click.Choice(["text", "json"]), default="text")
@click.pass_context
def check_watchdog(ctx: click.Context, fmt: str) -> None:
    """Run the watchdog check across all configured pipelines."""
    app_cfg = ctx.obj["config"]
    store = StateStore(app_cfg.state_file)
    report = run_watchdog(app_cfg, store)

    if fmt == "json":
        data = {
            "generated_at": report.generated_at.isoformat(),
            "healthy": report.healthy,
            "issues": [
                {
                    "pipeline": e.pipeline,
                    "issue": e.issue,
                    "last_seen": e.last_seen.isoformat() if e.last_seen else None,
                    "note": e.note,
                }
                for e in report.entries
            ],
        }
        click.echo(json.dumps(data, indent=2))
    else:
        click.echo(report.summary())

    if not report.healthy:
        ctx.exit(1)
