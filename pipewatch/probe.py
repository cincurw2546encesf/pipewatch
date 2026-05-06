"""Probe module: lightweight liveness/readiness checks for pipeline endpoints."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ProbeResult:
    pipeline: str
    probe_type: str  # 'liveness' | 'readiness'
    reachable: bool
    latency_ms: Optional[float]
    checked_at: datetime = field(default_factory=_utcnow)
    error: Optional[str] = None

    def summary(self) -> str:
        status = "OK" if self.reachable else "UNREACHABLE"
        lat = f"{self.latency_ms:.1f}ms" if self.latency_ms is not None else "n/a"
        return f"[{status}] {self.pipeline} ({self.probe_type}) latency={lat}"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "probe_type": self.probe_type,
            "reachable": self.reachable,
            "latency_ms": self.latency_ms,
            "checked_at": self.checked_at.isoformat(),
            "error": self.error,
        }


class ProbeStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: dict[str, dict] = self._load()

    def _load(self) -> dict:
        if self._path.exists():
            return json.loads(self._path.read_text())
        return {}

    def _save(self) -> None:
        self._path.write_text(json.dumps(self._entries, indent=2))

    def record(self, result: ProbeResult) -> None:
        self._entries[result.pipeline] = result.to_dict()
        self._save()

    def get(self, pipeline: str) -> Optional[ProbeResult]:
        entry = self._entries.get(pipeline)
        if entry is None:
            return None
        return ProbeResult(
            pipeline=entry["pipeline"],
            probe_type=entry["probe_type"],
            reachable=entry["reachable"],
            latency_ms=entry["latency_ms"],
            checked_at=datetime.fromisoformat(entry["checked_at"]),
            error=entry.get("error"),
        )

    def all(self) -> list[ProbeResult]:
        return [self.get(p) for p in self._entries]


def run_probe(pipeline: str, url: str, probe_type: str = "liveness", timeout: float = 5.0) -> ProbeResult:
    """Perform an HTTP GET probe against *url* and return a ProbeResult."""
    try:
        import urllib.request
        start = time.monotonic()
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            _ = resp.read()
        latency_ms = (time.monotonic() - start) * 1000
        return ProbeResult(pipeline=pipeline, probe_type=probe_type, reachable=True, latency_ms=latency_ms)
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(pipeline=pipeline, probe_type=probe_type, reachable=False, latency_ms=None, error=str(exc))
