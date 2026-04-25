"""Deduplication guard: suppress duplicate alerts within a cooldown window."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DedupEntry:
    pipeline: str
    status: str          # e.g. "STALE" or "FAILED"
    first_seen: datetime
    last_seen: datetime
    count: int = 1

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "count": self.count,
        }

    @staticmethod
    def from_dict(d: dict) -> "DedupEntry":
        return DedupEntry(
            pipeline=d["pipeline"],
            status=d["status"],
            first_seen=datetime.fromisoformat(d["first_seen"]),
            last_seen=datetime.fromisoformat(d["last_seen"]),
            count=d.get("count", 1),
        )


class DedupStore:
    """Persist dedup state to a JSON file."""

    def __init__(self, path: str, cooldown_minutes: int = 60) -> None:
        self._path = path
        self._cooldown_minutes = cooldown_minutes
        self._entries: Dict[str, DedupEntry] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path) as fh:
            raw = json.load(fh)
        self._entries = {
            k: DedupEntry.from_dict(v) for k, v in raw.items()
        }

    def _save(self) -> None:
        with open(self._path, "w") as fh:
            json.dump({k: v.to_dict() for k, v in self._entries.items()}, fh, indent=2)

    def _key(self, pipeline: str, status: str) -> str:
        return f"{pipeline}:{status}"

    def is_duplicate(self, pipeline: str, status: str, now: Optional[datetime] = None) -> bool:
        """Return True if this alert was already sent within the cooldown window."""
        now = now or _utcnow()
        key = self._key(pipeline, status)
        entry = self._entries.get(key)
        if entry is None:
            return False
        elapsed = (now - entry.last_seen).total_seconds() / 60
        return elapsed < self._cooldown_minutes

    def record(self, pipeline: str, status: str, now: Optional[datetime] = None) -> DedupEntry:
        """Record an alert event; create or update the entry."""
        now = now or _utcnow()
        key = self._key(pipeline, status)
        if key in self._entries:
            entry = self._entries[key]
            entry.last_seen = now
            entry.count += 1
        else:
            entry = DedupEntry(
                pipeline=pipeline,
                status=status,
                first_seen=now,
                last_seen=now,
            )
            self._entries[key] = entry
        self._save()
        return entry

    def reset(self, pipeline: str, status: str) -> None:
        """Clear dedup state for a pipeline/status pair (e.g. after recovery)."""
        key = self._key(pipeline, status)
        self._entries.pop(key, None)
        self._save()

    def all_entries(self) -> list:
        return list(self._entries.values())
