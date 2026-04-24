"""Escalation policy: re-alert after repeated failures beyond a threshold."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class EscalationEntry:
    pipeline: str
    failure_count: int = 0
    first_failed_at: Optional[str] = None
    last_escalated_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "failure_count": self.failure_count,
            "first_failed_at": self.first_failed_at,
            "last_escalated_at": self.last_escalated_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "EscalationEntry":
        return cls(
            pipeline=d["pipeline"],
            failure_count=d.get("failure_count", 0),
            first_failed_at=d.get("first_failed_at"),
            last_escalated_at=d.get("last_escalated_at"),
        )


@dataclass
class EscalationResult:
    pipeline: str
    should_escalate: bool
    failure_count: int
    threshold: int

    @property
    def summary(self) -> str:
        if self.should_escalate:
            return (
                f"{self.pipeline}: ESCALATE after {self.failure_count} failures "
                f"(threshold={self.threshold})"
            )
        return (
            f"{self.pipeline}: ok ({self.failure_count}/{self.threshold} failures)"
        )


class EscalationStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: Dict[str, EscalationEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {
                k: EscalationEntry.from_dict(v) for k, v in raw.items()
            }

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2)
        )

    def get(self, pipeline: str) -> EscalationEntry:
        return self._data.get(pipeline, EscalationEntry(pipeline=pipeline))

    def record_failure(self, pipeline: str) -> EscalationEntry:
        entry = self.get(pipeline)
        now_str = _utcnow().isoformat()
        if entry.failure_count == 0:
            entry.first_failed_at = now_str
        entry.failure_count += 1
        self._data[pipeline] = entry
        self._save()
        return entry

    def mark_escalated(self, pipeline: str) -> None:
        entry = self.get(pipeline)
        entry.last_escalated_at = _utcnow().isoformat()
        self._data[pipeline] = entry
        self._save()

    def reset(self, pipeline: str) -> None:
        self._data.pop(pipeline, None)
        self._save()


def check_escalation(
    pipeline: str, store: EscalationStore, threshold: int
) -> EscalationResult:
    """Return an EscalationResult indicating whether escalation is warranted."""
    entry = store.get(pipeline)
    should = entry.failure_count >= threshold
    return EscalationResult(
        pipeline=pipeline,
        should_escalate=should,
        failure_count=entry.failure_count,
        threshold=threshold,
    )
