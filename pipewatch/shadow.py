"""Shadow mode: run checks without emitting alerts, for dry-run validation."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.checker import CheckResult, CheckStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ShadowEntry:
    pipeline: str
    status: str
    checked_at: str
    would_alert: bool
    reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status,
            "checked_at": self.checked_at,
            "would_alert": self.would_alert,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ShadowEntry":
        return cls(
            pipeline=d["pipeline"],
            status=d["status"],
            checked_at=d["checked_at"],
            would_alert=d["would_alert"],
            reason=d.get("reason"),
        )


@dataclass
class ShadowReport:
    entries: List[ShadowEntry] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: _utcnow().isoformat())

    @property
    def would_alert_count(self) -> int:
        return sum(1 for e in self.entries if e.would_alert)

    @property
    def summary(self) -> str:
        total = len(self.entries)
        alerts = self.would_alert_count
        return f"Shadow: {total} checked, {alerts} would alert"


def run_shadow(results: List[CheckResult], now_fn=_utcnow) -> ShadowReport:
    """Simulate alerting logic without sending any notifications."""
    entries = []
    ts = now_fn().isoformat()
    for result in results:
        would_alert = result.status in (CheckStatus.STALE, CheckStatus.FAILED)
        reason = result.message if would_alert else None
        entries.append(
            ShadowEntry(
                pipeline=result.pipeline,
                status=result.status.value,
                checked_at=ts,
                would_alert=would_alert,
                reason=reason,
            )
        )
    return ShadowReport(entries=entries, generated_at=ts)


def save_shadow_report(report: ShadowReport, path: Path) -> None:
    data = {
        "generated_at": report.generated_at,
        "entries": [e.to_dict() for e in report.entries],
    }
    path.write_text(json.dumps(data, indent=2))
