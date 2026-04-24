"""Watchdog: detect pipelines that have never run or disappeared from state."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.config import AppConfig, PipelineConfig
from pipewatch.state import StateStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WatchdogEntry:
    pipeline: str
    issue: str  # "never_run" | "missing_state" | "stale_deadline"
    last_seen: Optional[datetime] = None
    note: str = ""

    def __str__(self) -> str:
        ts = self.last_seen.isoformat() if self.last_seen else "never"
        return f"[{self.issue}] {self.pipeline} (last_seen={ts}) {self.note}".strip()


@dataclass
class WatchdogReport:
    generated_at: datetime = field(default_factory=_utcnow)
    entries: List[WatchdogEntry] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return len(self.entries) == 0

    def summary(self) -> str:
        if self.healthy:
            return "Watchdog: all pipelines accounted for."
        lines = [f"Watchdog: {len(self.entries)} issue(s) detected:"]
        for e in self.entries:
            lines.append(f"  - {e}")
        return "\n".join(lines)


def run_watchdog(
    app_cfg: AppConfig,
    store: StateStore,
    now_fn=None,
) -> WatchdogReport:
    """Check every configured pipeline for state anomalies."""
    now = (now_fn or _utcnow)()
    report = WatchdogReport(generated_at=now)

    for pipeline in app_cfg.pipelines:
        record = store.latest(pipeline.name)

        if record is None:
            report.entries.append(
                WatchdogEntry(
                    pipeline=pipeline.name,
                    issue="never_run",
                    note="No run record found in state store.",
                )
            )
            continue

        last_seen = record.finished_dt or record.started_dt
        if last_seen is None:
            report.entries.append(
                WatchdogEntry(
                    pipeline=pipeline.name,
                    issue="missing_state",
                    note="Record exists but has no timestamp.",
                )
            )
            continue

        age_minutes = (now - last_seen).total_seconds() / 60
        hard_deadline = pipeline.max_age_minutes * 3
        if age_minutes > hard_deadline:
            report.entries.append(
                WatchdogEntry(
                    pipeline=pipeline.name,
                    issue="stale_deadline",
                    last_seen=last_seen,
                    note=(
                        f"Last seen {age_minutes:.0f}m ago; "
                        f"hard deadline is {hard_deadline:.0f}m."
                    ),
                )
            )

    return report
