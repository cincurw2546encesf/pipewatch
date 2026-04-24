"""CLI commands for SLA management and reporting."""
from __future__ import annotations

from datetime import time

import click

from pipewatch.checker import CheckResult
from pipewatch.sla import SLAConfig, SLAReport, check_sla


@click.group(name="sla")
def sla_cmd() -> None:
    """SLA deadline tracking commands."""


@sla_cmd.command(name="check")
@click.pass_context
def check_sla_cmd(ctx: click.Context) -> None:
    """Evaluate SLA compliance for all configured pipelines."""
    obj = ctx.obj or {}
    app_cfg = obj.get("config")
    results: list[CheckResult] = obj.get("results", [])

    if app_cfg is None:
        click.echo("No config loaded.", err=True)
        ctx.exit(1)
        return

    sla_configs = _build_sla_configs(app_cfg)
    if not sla_configs:
        click.echo("No SLA configurations found.")
        return

    report = check_sla(sla_configs, results)
    _print_report(report)

    if not report.healthy:
        ctx.exit(2)


@sla_cmd.command(name="list")
@click.pass_context
def list_slas(ctx: click.Context) -> None:
    """List all configured SLA deadlines."""
    obj = ctx.obj or {}
    app_cfg = obj.get("config")

    if app_cfg is None:
        click.echo("No config loaded.", err=True)
        ctx.exit(1)
        return

    sla_configs = _build_sla_configs(app_cfg)
    if not sla_configs:
        click.echo("No SLA configurations found.")
        return

    for cfg in sla_configs:
        days_str = ",".join(str(d) for d in cfg.days) if cfg.days else "every day"
        click.echo(f"  {cfg.pipeline:<30} deadline={cfg.deadline.isoformat()} UTC  days=[{days_str}]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_sla_configs(app_cfg) -> list[SLAConfig]:
    configs = []
    for p in app_cfg.pipelines:
        raw = getattr(p, "sla", None)
        if not raw:
            continue
        deadline_str = raw.get("deadline")
        if not deadline_str:
            continue
        h, m = (int(x) for x in deadline_str.split(":")[:2])
        days = [int(d) for d in raw.get("days", [])]
        configs.append(SLAConfig(pipeline=p.name, deadline=time(h, m), days=days))
    return configs


def _print_report(report: SLAReport) -> None:
    for name in report.passed:
        click.echo(f"  ✅  {name}")
    for name in report.skipped:
        click.echo(f"  ⏭️   {name} (skipped)")
    for v in report.violations:
        click.echo(f"  ❌  {v}")
    click.echo(report.summary())
