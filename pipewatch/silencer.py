"""Silence (suppress) alerts for specific pipelines for a duration."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SilenceEntry:
    pipeline: str
    until: str  # ISO format UTC
    reason: str = ""

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        until_dt = datetime.fromisoformat(self.until)
        if until_dt.tzinfo is None:
            until_dt = until_dt.replace(tzinfo=timezone.utc)
        return now < until_dt

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "SilenceEntry":
        return cls(**d)


class SilenceStore:
    def __init__(self, path: Path):
        self.path = path
        self._entries: list[SilenceEntry] = []
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self._entries = []
            return
        data = json.loads(self.path.read_text())
        self._entries = [SilenceEntry.from_dict(d) for d in data]

    def _save(self) -> None:
        self.path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def silence(self, pipeline: str, until: datetime, reason: str = "") -> SilenceEntry:
        entry = SilenceEntry(pipeline=pipeline, until=until.isoformat(), reason=reason)
        self._entries = [e for e in self._entries if e.pipeline != pipeline]
        self._entries.append(entry)
        self._save()
        return entry

    def unsilence(self, pipeline: str) -> bool:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.pipeline != pipeline]
        self._save()
        return len(self._entries) < before

    def is_silenced(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        for e in self._entries:
            if e.pipeline == pipeline and e.is_active(now):
                return True
        return False

    def active_entries(self, now: Optional[datetime] = None) -> list[SilenceEntry]:
        return [e for e in self._entries if e.is_active(now)]

    def prune(self, now: Optional[datetime] = None) -> int:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.is_active(now)]
        self._save()
        return before - len(self._entries)
