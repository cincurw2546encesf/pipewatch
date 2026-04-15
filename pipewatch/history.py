"""Run history tracking: retain and query past CheckResult records per pipeline."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.checker import CheckResult, CheckStatus

_DEFAULT_HISTORY_FILE = Path(".pipewatch_history.json")
_DEFAULT_MAX_ENTRIES = 100


@dataclass
class HistoryEntry:
    pipeline: str
    status: str
    checked_at: str
    last_run: Optional[str]
    message: str

    @classmethod
    def from_result(cls, result: CheckResult) -> "HistoryEntry":
        return cls(
            pipeline=result.pipeline,
            status=result.status.value,
            checked_at=datetime.now(timezone.utc).isoformat(),
            last_run=result.last_run.isoformat() if result.last_run else None,
            message=str(result),
        )

    def to_dict(self) -> dict:
        return asdict(self)


class HistoryStore:
    def __init__(
        self,
        path: Path = _DEFAULT_HISTORY_FILE,
        max_entries: int = _DEFAULT_MAX_ENTRIES,
    ) -> None:
        self.path = path
        self.max_entries = max_entries

    def _load_raw(self) -> List[dict]:
        if not self.path.exists():
            return []
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return []

    def record(self, result: CheckResult) -> None:
        entries = self._load_raw()
        entries.append(HistoryEntry.from_result(result).to_dict())
        # keep only the most recent entries across all pipelines
        entries = entries[-self.max_entries :]
        self.path.write_text(json.dumps(entries, indent=2))

    def for_pipeline(self, pipeline: str) -> List[HistoryEntry]:
        return [
            HistoryEntry(**e)
            for e in self._load_raw()
            if e.get("pipeline") == pipeline
        ]

    def all_entries(self) -> List[HistoryEntry]:
        return [HistoryEntry(**e) for e in self._load_raw()]

    def clear(self, pipeline: Optional[str] = None) -> int:
        if pipeline is None:
            count = len(self._load_raw())
            self.path.write_text(json.dumps([]))
            return count
        entries = self._load_raw()
        kept = [e for e in entries if e.get("pipeline") != pipeline]
        self.path.write_text(json.dumps(kept, indent=2))
        return len(entries) - len(kept)
