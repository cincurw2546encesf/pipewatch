"""Pipeline run state tracking — reads, writes, and queries pipeline run records."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_STATE_FILE = Path(os.environ.get("PIPEWATCH_STATE_FILE", "~/.pipewatch/state.json")).expanduser()


@dataclass
class RunRecord:
    pipeline: str
    status: str  # "success" | "failed" | "running"
    started_at: str
    finished_at: Optional[str] = None
    message: Optional[str] = None

    @property
    def started_dt(self) -> datetime:
        return datetime.fromisoformat(self.started_at)

    @property
    def finished_dt(self) -> Optional[datetime]:
        return datetime.fromisoformat(self.finished_at) if self.finished_at else None

    @classmethod
    def now(cls, pipeline: str, status: str, message: Optional[str] = None) -> "RunRecord":
        ts = datetime.now(timezone.utc).isoformat()
        return cls(pipeline=pipeline, status=status, started_at=ts, finished_at=ts, message=message)


@dataclass
class StateStore:
    path: Path = field(default_factory=lambda: DEFAULT_STATE_FILE)
    _records: List[RunRecord] = field(default_factory=list, init=False, repr=False)

    def load(self) -> None:
        """Load records from disk. Raises ValueError if the file contains invalid JSON or malformed records."""
        if not self.path.exists():
            self._records = []
            return
        try:
            with self.path.open() as fh:
                raw = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError(f"State file {self.path} contains invalid JSON: {exc}") from exc
        try:
            self._records = [RunRecord(**r) for r in raw]
        except (TypeError, KeyError) as exc:
            raise ValueError(f"State file {self.path} contains malformed records: {exc}") from exc

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w") as fh:
            json.dump([asdict(r) for r in self._records], fh, indent=2)

    def record_run(self, record: RunRecord) -> None:
        self._records.append(record)
        self.save()

    def latest(self, pipeline: str) -> Optional[RunRecord]:
        matches = [r for r in self._records if r.pipeline == pipeline]
        return matches[-1] if matches else None

    def all_for(self, pipeline: str) -> List[RunRecord]:
        return [r for r in self._records if r.pipeline == pipeline]

    def all_pipelines(self) -> List[str]:
        seen: Dict[str, None] = {}
        for r in self._records:
            seen.setdefault(r.pipeline, None)
        return list(seen.keys())
