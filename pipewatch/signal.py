"""Signal module: emit and query named signals for pipeline events."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SignalEntry:
    pipeline: str
    name: str
    message: str
    emitted_at: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "name": self.name,
            "message": self.message,
            "emitted_at": self.emitted_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SignalEntry":
        return cls(
            pipeline=d["pipeline"],
            name=d["name"],
            message=d["message"],
            emitted_at=datetime.fromisoformat(d["emitted_at"]),
        )

    def summary(self) -> str:
        ts = self.emitted_at.strftime("%Y-%m-%d %H:%M:%S")
        return f"[{ts}] {self.pipeline} / {self.name}: {self.message}"


class SignalStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: List[SignalEntry] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            self._entries = []
            return
        raw = json.loads(self._path.read_text())
        self._entries = [SignalEntry.from_dict(r) for r in raw]

    def _save(self) -> None:
        self._path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def emit(self, pipeline: str, name: str, message: str, now_fn=_utcnow) -> SignalEntry:
        entry = SignalEntry(
            pipeline=pipeline,
            name=name,
            message=message,
            emitted_at=now_fn(),
        )
        self._entries.append(entry)
        self._save()
        return entry

    def get(self, pipeline: str) -> List[SignalEntry]:
        return [e for e in self._entries if e.pipeline == pipeline]

    def get_by_name(self, pipeline: str, name: str) -> List[SignalEntry]:
        return [e for e in self._entries if e.pipeline == pipeline and e.name == name]

    def all(self) -> List[SignalEntry]:
        return list(self._entries)

    def clear(self, pipeline: str) -> int:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.pipeline != pipeline]
        self._save()
        return before - len(self._entries)
