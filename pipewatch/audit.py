"""Audit log: record state-changing actions taken by pipewatch."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class AuditEntry:
    action: str          # e.g. "silence.add", "retry.reset", "baseline.update"
    pipeline: str
    actor: str           # "scheduler", "cli", or a user-supplied label
    detail: str          # human-readable description
    timestamp: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict:
        return {
            "action": self.action,
            "pipeline": self.pipeline,
            "actor": self.actor,
            "detail": self.detail,
            "timestamp": self.timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict) -> "AuditEntry":
        return AuditEntry(
            action=data["action"],
            pipeline=data["pipeline"],
            actor=data["actor"],
            detail=data["detail"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )


class AuditStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def _load(self) -> List[dict]:
        if not self._path.exists():
            return []
        return json.loads(self._path.read_text())

    def _save(self, entries: List[dict]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(entries, indent=2))

    def record(self, entry: AuditEntry) -> None:
        entries = self._load()
        entries.append(entry.to_dict())
        self._save(entries)

    def get(self, pipeline: Optional[str] = None) -> List[AuditEntry]:
        raw = self._load()
        results = [AuditEntry.from_dict(r) for r in raw]
        if pipeline:
            results = [e for e in results if e.pipeline == pipeline]
        return results

    def clear(self, pipeline: Optional[str] = None) -> int:
        entries = self._load()
        if pipeline:
            kept = [e for e in entries if e["pipeline"] != pipeline]
        else:
            kept = []
        removed = len(entries) - len(kept)
        self._save(kept)
        return removed
