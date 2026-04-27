"""Rate limiting for pipeline check frequency — prevents hammering slow state stores."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


def _utcnow() -> float:
    return time.time()


@dataclass
class RateLimitEntry:
    pipeline: str
    last_checked: float
    check_count: int = 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_checked": self.last_checked,
            "check_count": self.check_count,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RateLimitEntry":
        return cls(
            pipeline=d["pipeline"],
            last_checked=d["last_checked"],
            check_count=d.get("check_count", 0),
        )

    def seconds_since_last_check(self) -> float:
        """Return the number of seconds elapsed since this entry was last checked."""
        return _utcnow() - self.last_checked


@dataclass
class RateLimitStore:
    path: Path
    _entries: Dict[str, RateLimitEntry] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text())
                self._entries = {
                    k: RateLimitEntry.from_dict(v) for k, v in raw.items()
                }
            except (json.JSONDecodeError, KeyError):
                # If the store file is corrupt or missing required fields, start fresh.
                self._entries = {}

    def _save(self) -> None:
        self.path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def is_rate_limited(self, pipeline: str, min_interval_seconds: float) -> bool:
        entry = self._entries.get(pipeline)
        if entry is None:
            return False
        return (_utcnow() - entry.last_checked) < min_interval_seconds

    def record_check(self, pipeline: str) -> None:
        existing = self._entries.get(pipeline)
        count = (existing.check_count + 1) if existing else 1
        self._entries[pipeline] = RateLimitEntry(
            pipeline=pipeline,
            last_checked=_utcnow(),
            check_count=count,
        )
        self._save()

    def get(self, pipeline: str) -> Optional[RateLimitEntry]:
        return self._entries.get(pipeline)

    def reset(self, pipeline: str) -> None:
        self._entries.pop(pipeline, None)
        self._save()

    def all_entries(self) -> list:
        return list(self._entries.values())
