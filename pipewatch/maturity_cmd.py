"""CLI commands for pipeline maturity scoring."""
from __future__ import annotations

import click

from pipewatch.maturity import score_all


@click.group("maturity")
def maturity_cmd() -> None:
    """Pipeline maturity scoring."""


@maturity_cmd.command("check")
@click.option("--min-grade", default="C", show_default=True,
              help="Minimum acceptable grade (A/B/C/D/F).")
@click.pass_context
def check_maturity_cmd(ctx: click.Context, min_grade: str) -> None:
    """Score all pipelines and flag those below --min-grade."""
    obj = ctx.obj or {}
    results = obj.get("results", [])
    store = obj.get("history_store")

    grade_order = ["A", "B", "C", "D", "F"]
    threshold = grade_order.index(min_grade.upper()) if min_grade.upper() in grade_order else 2

    scores = score_all(results, store=store)
    if not scores:
        click.echo("No pipelines to score.")
        return

    failed_count = 0
    for s in sorted(scores, key=lambda x: x.score):
        icon = "✓" if s.healthy else "✗"
        click.echo(f"  {icon}  {s.summary()}")
        if s.reasons:
            for r in s.reasons:
                click.echo(f"       - {r}")
        if grade_order.index(s.grade) > threshold:
            failed_count += 1

    click.echo("")
    if failed_count:
        click.echo(f"{failed_count} pipeline(s) below grade {min_grade.upper()}.")
        ctx.exit(1)
    else:
        click.echo("All pipelines meet the minimum grade.")
