"""fence.py — Concurrency fence: prevent a pipeline from running if another
instance is already active (based on state store started/finished times)."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class FenceEntry:
    pipeline: str
    locked_at: datetime
    owner: str  # e.g. hostname or pid string

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "locked_at": self.locked_at.isoformat(),
            "owner": self.owner,
        }

    @staticmethod
    def from_dict(d: dict) -> "FenceEntry":
        return FenceEntry(
            pipeline=d["pipeline"],
            locked_at=datetime.fromisoformat(d["locked_at"]),
            owner=d["owner"],
        )


@dataclass
class FenceResult:
    pipeline: str
    locked: bool
    owner: Optional[str] = None
    locked_at: Optional[datetime] = None

    def summary(self) -> str:
        if not self.locked:
            return f"{self.pipeline}: no active fence"
        return (
            f"{self.pipeline}: LOCKED by {self.owner} "
            f"since {self.locked_at.isoformat() if self.locked_at else 'unknown'}"
        )


class FenceStore:
    def __init__(self, path: str) -> None:
        self._path = path
        self._data: dict[str, FenceEntry] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path) as f:
            raw = json.load(f)
        self._data = {k: FenceEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        with open(self._path, "w") as f:
            json.dump({k: v.to_dict() for k, v in self._data.items()}, f, indent=2)

    def acquire(self, pipeline: str, owner: str) -> bool:
        """Acquire the fence. Returns True if acquired, False if already locked."""
        if pipeline in self._data:
            return False
        self._data[pipeline] = FenceEntry(
            pipeline=pipeline, locked_at=_utcnow(), owner=owner
        )
        self._save()
        return True

    def release(self, pipeline: str) -> bool:
        """Release the fence. Returns True if it was held."""
        if pipeline not in self._data:
            return False
        del self._data[pipeline]
        self._save()
        return True

    def check(self, pipeline: str) -> FenceResult:
        entry = self._data.get(pipeline)
        if entry is None:
            return FenceResult(pipeline=pipeline, locked=False)
        return FenceResult(
            pipeline=pipeline,
            locked=True,
            owner=entry.owner,
            locked_at=entry.locked_at,
        )

    def all_entries(self) -> list[FenceEntry]:
        return list(self._data.values())
