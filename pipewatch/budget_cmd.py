"""CLI commands for runtime budget management."""
from __future__ import annotations

import click

from pipewatch.budget import BudgetConfig, check_all_budgets


@click.group("budget")
def budget_cmd() -> None:
    """Runtime budget commands."""


@budget_cmd.command("check")
@click.pass_context
def check_budget_cmd(ctx: click.Context) -> None:
    """Check all pipelines against their runtime budgets."""
    obj = ctx.obj or {}
    results = obj.get("results", [])
    app_cfg = obj.get("app_cfg")

    if app_cfg is None:
        click.echo("No configuration loaded.")
        ctx.exit(1)
        return

    budgets = [
        BudgetConfig(pipeline=p.name, max_seconds=p.max_runtime_seconds)
        for p in app_cfg.pipelines
        if getattr(p, "max_runtime_seconds", None) is not None
    ]

    if not budgets:
        click.echo("No budget configurations found.")
        return

    budget_results = check_all_budgets(results, budgets)
    exceeded = [b for b in budget_results if b.exceeded]

    for b in budget_results:
        icon = "\u274c" if b.exceeded else "\u2705"
        click.echo(f"  {icon}  {b.summary()}")

    if exceeded:
        click.echo(f"\n{len(exceeded)} pipeline(s) exceeded their runtime budget.")
        ctx.exit(2)
    else:
        click.echo("\nAll pipelines within budget.")
