"""Quota tracking: enforce max failure counts over a rolling window."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class QuotaEntry:
    pipeline: str
    max_failures: int
    window_hours: float
    failure_timestamps: list[str] = field(default_factory=list)

    def record_failure(self, now_fn=_utcnow) -> None:
        self.failure_timestamps.append(now_fn().isoformat())
        self._prune(now_fn)

    def _prune(self, now_fn=_utcnow) -> None:
        cutoff = now_fn() - timedelta(hours=self.window_hours)
        self.failure_timestamps = [
            ts for ts in self.failure_timestamps
            if datetime.fromisoformat(ts) >= cutoff
        ]

    def failures_in_window(self, now_fn=_utcnow) -> int:
        self._prune(now_fn)
        return len(self.failure_timestamps)

    def exceeded(self, now_fn=_utcnow) -> bool:
        return self.failures_in_window(now_fn) >= self.max_failures

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "max_failures": self.max_failures,
            "window_hours": self.window_hours,
            "failure_timestamps": self.failure_timestamps,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "QuotaEntry":
        return cls(
            pipeline=data["pipeline"],
            max_failures=data["max_failures"],
            window_hours=data["window_hours"],
            failure_timestamps=data.get("failure_timestamps", []),
        )


class QuotaStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, QuotaEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {k: QuotaEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2))

    def get(self, pipeline: str) -> Optional[QuotaEntry]:
        return self._data.get(pipeline)

    def configure(self, pipeline: str, max_failures: int, window_hours: float) -> QuotaEntry:
        entry = self._data.get(pipeline)
        if entry is None:
            entry = QuotaEntry(pipeline=pipeline, max_failures=max_failures, window_hours=window_hours)
            self._data[pipeline] = entry
        else:
            entry.max_failures = max_failures
            entry.window_hours = window_hours
        self._save()
        return entry

    def record_failure(self, pipeline: str, now_fn=_utcnow) -> Optional[QuotaEntry]:
        entry = self._data.get(pipeline)
        if entry is None:
            return None
        entry.record_failure(now_fn)
        self._save()
        return entry

    def reset(self, pipeline: str) -> bool:
        entry = self._data.get(pipeline)
        if entry is None:
            return False
        entry.failure_timestamps = []
        self._save()
        return True

    def all(self) -> list[QuotaEntry]:
        return list(self._data.values())
