"""Retry policy tracking for pipeline runs."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
import json
from pathlib import Path


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RetryEntry:
    pipeline: str
    attempts: int = 0
    last_attempt: Optional[datetime] = None
    last_error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "attempts": self.attempts,
            "last_attempt": self.last_attempt.isoformat() if self.last_attempt else None,
            "last_error": self.last_error,
        }

    @staticmethod
    def from_dict(d: dict) -> "RetryEntry":
        la = d.get("last_attempt")
        return RetryEntry(
            pipeline=d["pipeline"],
            attempts=d.get("attempts", 0),
            last_attempt=datetime.fromisoformat(la) if la else None,
            last_error=d.get("last_error"),
        )


class RetryStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: Dict[str, RetryEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            raw = json.loads(self._path.read_text())
            self._data = {k: RetryEntry.from_dict(v) for k, v in raw.items()}

    def _save(self) -> None:
        self._path.write_text(json.dumps({k: v.to_dict() for k, v in self._data.items()}, indent=2))

    def record_attempt(self, pipeline: str, error: Optional[str] = None) -> RetryEntry:
        entry = self._data.get(pipeline, RetryEntry(pipeline=pipeline))
        entry.attempts += 1
        entry.last_attempt = _utcnow()
        entry.last_error = error
        self._data[pipeline] = entry
        self._save()
        return entry

    def reset(self, pipeline: str) -> None:
        if pipeline in self._data:
            del self._data[pipeline]
            self._save()

    def get(self, pipeline: str) -> Optional[RetryEntry]:
        return self._data.get(pipeline)

    def all_entries(self) -> List[RetryEntry]:
        return list(self._data.values())


def should_retry(entry: Optional[RetryEntry], max_retries: int) -> bool:
    if entry is None:
        return True
    return entry.attempts < max_retries
