"""Command-line interface for pipewatch."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click

from pipewatch.alerts import AlertConfig, dispatch_alerts
from pipewatch.checker import CheckStatus, check_all
from pipewatch.config import load_config
from pipewatch.state import StateStore

_STATUS_COLOURS = {
    CheckStatus.OK: "green",
    CheckStatus.STALE: "yellow",
    CheckStatus.FAILED: "red",
    CheckStatus.MISSING: "magenta",
}


@click.group()
@click.option(
    "--config",
    "config_path",
    default="pipewatch.yaml",
    show_default=True,
    help="Path to pipewatch YAML config.",
)
@click.pass_context
def cli(ctx: click.Context, config_path: str) -> None:
    """pipewatch — ETL pipeline health monitor."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@cli.command("check")
@click.option("--alert", is_flag=True, default=False, help="Send e-mail alerts for unhealthy pipelines.")
@click.pass_context
def check_cmd(ctx: click.Context, alert: bool) -> None:
    """Run health checks for all configured pipelines."""
    app_cfg = load_config(ctx.obj["config_path"])
    store = StateStore(app_cfg.state_path)
    results = check_all(app_cfg.pipelines, store)

    any_unhealthy = False
    for result in results:
        colour = _STATUS_COLOURS.get(result.status, "white")
        click.echo(click.style(str(result), fg=colour))
        if result.status != CheckStatus.OK:
            any_unhealthy = True

    if alert and any_unhealthy:
        alert_cfg = AlertConfig(
            smtp_host=app_cfg.smtp_host,
            smtp_port=app_cfg.smtp_port,
            from_addr=app_cfg.alert_from,
            to_addrs=app_cfg.alert_to,
        )
        dispatch_alerts(results, alert_cfg)

    sys.exit(1 if any_unhealthy else 0)


@cli.command("list")
@click.pass_context
def list_cmd(ctx: click.Context) -> None:
    """List all configured pipelines."""
    app_cfg = load_config(ctx.obj["config_path"])
    for pipeline in app_cfg.pipelines:
        click.echo(f"{pipeline.name}  (max_age={pipeline.max_age_seconds}s)")


def main() -> None:  # pragma: no cover
    cli(obj={})


if __name__ == "__main__":  # pragma: no cover
    main()
