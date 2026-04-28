"""Exponential back-off tracker for pipeline alert suppression.

When a pipeline keeps failing, repeated alerts become noise.  This module
tracks how many consecutive failures have occurred and computes a
cooldown window that grows exponentially, capping at a configurable
maximum so operators are still notified periodically.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BackoffEntry:
    pipeline: str
    consecutive_failures: int = 0
    last_alerted_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "consecutive_failures": self.consecutive_failures,
            "last_alerted_at": self.last_alerted_at.isoformat() if self.last_alerted_at else None,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "BackoffEntry":
        la = d.get("last_alerted_at")
        return cls(
            pipeline=d["pipeline"],
            consecutive_failures=d.get("consecutive_failures", 0),
            last_alerted_at=datetime.fromisoformat(la) if la else None,
        )


def cooldown_seconds(entry: BackoffEntry, base: int = 60, max_seconds: int = 3600) -> int:
    """Return the required cooldown in seconds given consecutive failures."""
    if entry.consecutive_failures <= 1:
        return base
    raw = base * int(math.pow(2, entry.consecutive_failures - 1))
    return min(raw, max_seconds)


def should_alert(entry: BackoffEntry, base: int = 60, max_seconds: int = 3600,
                 now_fn=_utcnow) -> bool:
    """Return True if enough time has passed since the last alert."""
    if entry.last_alerted_at is None:
        return True
    elapsed = (now_fn() - entry.last_alerted_at).total_seconds()
    return elapsed >= cooldown_seconds(entry, base=base, max_seconds=max_seconds)


class BackoffStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, BackoffEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {k: BackoffEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2))

    def get(self, pipeline: str) -> BackoffEntry:
        return self._data.get(pipeline, BackoffEntry(pipeline=pipeline))

    def record_failure(self, pipeline: str, now_fn=_utcnow) -> BackoffEntry:
        entry = self.get(pipeline)
        entry.consecutive_failures += 1
        entry.last_alerted_at = now_fn()
        self._data[pipeline] = entry
        self._save()
        return entry

    def reset(self, pipeline: str) -> None:
        self._data.pop(pipeline, None)
        self._save()
