"""Runtime budget tracking — flag pipelines that exceed expected duration."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipewatch.checker import CheckResult, CheckStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BudgetConfig:
    """Maximum allowed runtime in seconds for a pipeline."""
    pipeline: str
    max_seconds: float


@dataclass
class BudgetResult:
    pipeline: str
    max_seconds: float
    actual_seconds: Optional[float]
    exceeded: bool

    def summary(self) -> str:
        if self.actual_seconds is None:
            return f"{self.pipeline}: no runtime recorded"
        status = "EXCEEDED" if self.exceeded else "OK"
        return (
            f"{self.pipeline}: {self.actual_seconds:.1f}s "
            f"(budget {self.max_seconds:.1f}s) [{status}]"
        )


def check_budget(result: CheckResult, max_seconds: float) -> BudgetResult:
    """Compare a pipeline's last run duration against its budget."""
    actual: Optional[float] = None
    if result.last_run is not None and result.last_finished is not None:
        delta = (result.last_finished - result.last_run).total_seconds()
        actual = max(delta, 0.0)

    exceeded = actual is not None and actual > max_seconds
    return BudgetResult(
        pipeline=result.pipeline,
        max_seconds=max_seconds,
        actual_seconds=actual,
        exceeded=exceeded,
    )


def check_all_budgets(
    results: list[CheckResult],
    configs: list[BudgetConfig],
) -> list[BudgetResult]:
    """Run budget checks for every pipeline that has a budget configured."""
    budget_map = {c.pipeline: c.max_seconds for c in configs}
    output: list[BudgetResult] = []
    for r in results:
        if r.pipeline in budget_map:
            output.append(check_budget(r, budget_map[r.pipeline]))
    return output
