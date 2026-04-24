"""Dead-letter queue: persist and inspect runs that exceeded max retries."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DeadLetterEntry:
    pipeline: str
    status: str
    last_run: Optional[str]
    attempts: int
    first_failed_at: str
    added_at: str = field(default_factory=lambda: _utcnow().isoformat())
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status,
            "last_run": self.last_run,
            "attempts": self.attempts,
            "first_failed_at": self.first_failed_at,
            "added_at": self.added_at,
            "note": self.note,
        }

    @staticmethod
    def from_dict(d: dict) -> "DeadLetterEntry":
        return DeadLetterEntry(
            pipeline=d["pipeline"],
            status=d["status"],
            last_run=d.get("last_run"),
            attempts=d["attempts"],
            first_failed_at=d["first_failed_at"],
            added_at=d["added_at"],
            note=d.get("note", ""),
        )

    def summary(self) -> str:
        ts = self.last_run or "never"
        return (
            f"{self.pipeline}: {self.status} | attempts={self.attempts} "
            f"| last_run={ts} | failed_since={self.first_failed_at}"
        )


class DeadLetterStore:
    def __init__(self, path: str) -> None:
        self._path = path
        self._entries: dict[str, DeadLetterEntry] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path) as fh:
            raw = json.load(fh)
        self._entries = {
            k: DeadLetterEntry.from_dict(v) for k, v in raw.items()
        }

    def _save(self) -> None:
        with open(self._path, "w") as fh:
            json.dump({k: v.to_dict() for k, v in self._entries.items()}, fh, indent=2)

    def add(self, entry: DeadLetterEntry) -> None:
        self._entries[entry.pipeline] = entry
        self._save()

    def get(self, pipeline: str) -> Optional[DeadLetterEntry]:
        return self._entries.get(pipeline)

    def remove(self, pipeline: str) -> bool:
        if pipeline in self._entries:
            del self._entries[pipeline]
            self._save()
            return True
        return False

    def all(self) -> List[DeadLetterEntry]:
        return list(self._entries.values())

    def clear(self) -> None:
        self._entries.clear()
        self._save()
