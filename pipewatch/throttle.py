"""Alert throttling: suppress repeated alerts within a cooldown window."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ThrottleEntry:
    pipeline: str
    last_alerted: datetime
    alert_count: int = 1

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_alerted": self.last_alerted.isoformat(),
            "alert_count": self.alert_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ThrottleEntry":
        return cls(
            pipeline=d["pipeline"],
            last_alerted=datetime.fromisoformat(d["last_alerted"]),
            alert_count=d.get("alert_count", 1),
        )


@dataclass
class ThrottleStore:
    path: Path
    _entries: Dict[str, ThrottleEntry] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            data = json.loads(self.path.read_text())
            self._entries = {
                k: ThrottleEntry.from_dict(v) for k, v in data.items()
            }

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def is_throttled(self, pipeline: str, cooldown_seconds: int) -> bool:
        """Return True if an alert was sent within the cooldown window."""
        entry = self._entries.get(pipeline)
        if entry is None:
            return False
        elapsed = (_utcnow() - entry.last_alerted).total_seconds()
        return elapsed < cooldown_seconds

    def record_alert(self, pipeline: str) -> ThrottleEntry:
        """Record that an alert was just sent for *pipeline*."""
        existing = self._entries.get(pipeline)
        count = (existing.alert_count + 1) if existing else 1
        entry = ThrottleEntry(
            pipeline=pipeline,
            last_alerted=_utcnow(),
            alert_count=count,
        )
        self._entries[pipeline] = entry
        self._save()
        return entry

    def reset(self, pipeline: str) -> None:
        """Clear throttle state for *pipeline* (e.g. after it recovers)."""
        self._entries.pop(pipeline, None)
        self._save()

    def get(self, pipeline: str) -> Optional[ThrottleEntry]:
        return self._entries.get(pipeline)

    def all_entries(self) -> list[ThrottleEntry]:
        return list(self._entries.values())
