"""Pipeline run annotations — attach notes to pipeline check results."""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Annotation:
    pipeline: str
    note: str
    author: str
    created_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "note": self.note,
            "author": self.author,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Annotation":
        return cls(
            pipeline=d["pipeline"],
            note=d["note"],
            author=d["author"],
            created_at=datetime.fromisoformat(d["created_at"]),
        )


class AnnotationStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: list[Annotation] = []
        if path.exists():
            raw = json.loads(path.read_text())
            self._entries = [Annotation.from_dict(r) for r in raw]

    def _save(self) -> None:
        self._path.write_text(json.dumps([e.to_dict() for e in self._entries], indent=2))

    def add(self, pipeline: str, note: str, author: str) -> Annotation:
        ann = Annotation(pipeline=pipeline, note=note, author=author)
        self._entries.append(ann)
        self._save()
        return ann

    def get(self, pipeline: str) -> list[Annotation]:
        return [e for e in self._entries if e.pipeline == pipeline]

    def all(self) -> list[Annotation]:
        return list(self._entries)

    def clear(self, pipeline: str) -> int:
        before = len(self._entries)
        self._entries = [e for e in self._entries if e.pipeline != pipeline]
        self._save()
        return before - len(self._entries)
