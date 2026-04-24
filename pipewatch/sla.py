"""SLA tracking: define expected completion windows and detect violations."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from typing import Optional

from pipewatch.checker import CheckResult, CheckStatus


@dataclass
class SLAConfig:
    """Per-pipeline SLA definition."""
    pipeline: str
    # Latest wall-clock time (UTC) by which a successful run must finish
    deadline: time  # e.g. time(6, 0) => must finish by 06:00 UTC
    # Optional: only enforce on these ISO weekdays (1=Mon … 7=Sun); empty = every day
    days: list[int] = field(default_factory=list)

    def applies_today(self, now: datetime) -> bool:
        """Return True if the SLA should be evaluated for *now*'s date."""
        if not self.days:
            return True
        return now.isoweekday() in self.days


@dataclass
class SLAViolation:
    pipeline: str
    deadline: time
    last_success: Optional[datetime]
    evaluated_at: datetime

    def __str__(self) -> str:
        last = self.last_success.isoformat() if self.last_success else "never"
        return (
            f"{self.pipeline}: SLA violated — deadline {self.deadline.isoformat()} UTC, "
            f"last success {last}"
        )


@dataclass
class SLAReport:
    violations: list[SLAViolation] = field(default_factory=list)
    passed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return len(self.violations) == 0

    def summary(self) -> str:
        v, p, s = len(self.violations), len(self.passed), len(self.skipped)
        return f"SLA: {p} passed, {v} violated, {s} skipped"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def check_sla(
    configs: list[SLAConfig],
    results: list[CheckResult],
    now: Optional[datetime] = None,
) -> SLAReport:
    """Evaluate SLA configs against the latest check results."""
    now = now or _utcnow()
    result_map = {r.pipeline: r for r in results}
    report = SLAReport()

    for cfg in configs:
        if not cfg.applies_today(now):
            report.skipped.append(cfg.pipeline)
            continue

        # Deadline for *today* in UTC
        deadline_dt = datetime(
            now.year, now.month, now.day,
            cfg.deadline.hour, cfg.deadline.minute, cfg.deadline.second,
            tzinfo=timezone.utc,
        )

        # Only evaluate after the deadline has passed
        if now < deadline_dt:
            report.skipped.append(cfg.pipeline)
            continue

        res = result_map.get(cfg.pipeline)
        last_success: Optional[datetime] = None
        if res and res.status == CheckStatus.OK and res.last_run:
            last_success = res.last_run

        # Violation: no successful run recorded on or after today's deadline window start
        # (i.e. last success must be within today and before or at deadline)
        ok_today = (
            last_success is not None
            and last_success.date() == now.date()
            and last_success <= deadline_dt
        )
        # Also accept a run that happened before deadline but still today
        ok_today = (
            last_success is not None
            and last_success.date() == now.date()
        )

        if ok_today:
            report.passed.append(cfg.pipeline)
        else:
            report.violations.append(
                SLAViolation(
                    pipeline=cfg.pipeline,
                    deadline=cfg.deadline,
                    last_success=last_success,
                    evaluated_at=now,
                )
            )

    return report
