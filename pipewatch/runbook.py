"""Runbook links: attach remediation URLs/notes to pipelines."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional


@dataclass
class RunbookEntry:
    pipeline: str
    url: Optional[str]
    note: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "RunbookEntry":
        return RunbookEntry(
            pipeline=d["pipeline"],
            url=d.get("url"),
            note=d.get("note"),
        )

    def summary(self) -> str:
        parts = [f"[{self.pipeline}]"]
        if self.url:
            parts.append(self.url)
        if self.note:
            parts.append(f"({self.note})")
        return " ".join(parts)


class RunbookStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: dict[str, RunbookEntry] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text())
                self._entries = {
                    k: RunbookEntry.from_dict(v) for k, v in data.items()
                }
            except (json.JSONDecodeError, KeyError) as exc:
                raise ValueError(
                    f"Failed to load runbook store from {self._path}: {exc}"
                ) from exc

    def _save(self) -> None:
        self._path.write_text(
            json.dumps({k: v.to_dict() for k, v in self._entries.items()}, indent=2)
        )

    def set(self, pipeline: str, url: Optional[str] = None, note: Optional[str] = None) -> RunbookEntry:
        entry = RunbookEntry(pipeline=pipeline, url=url, note=note)
        self._entries[pipeline] = entry
        self._save()
        return entry

    def get(self, pipeline: str) -> Optional[RunbookEntry]:
        return self._entries.get(pipeline)

    def remove(self, pipeline: str) -> bool:
        if pipeline in self._entries:
            del self._entries[pipeline]
            self._save()
            return True
        return False

    def all(self) -> list[RunbookEntry]:
        return list(self._entries.values())
