"""Tests for pipewatch.probe."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.probe import ProbeResult, ProbeStore, run_probe


@pytest.fixture()
def store(tmp_path: Path) -> ProbeStore:
    return ProbeStore(tmp_path / "probes.json")


def _result(pipeline: str = "my_pipe", reachable: bool = True) -> ProbeResult:
    return ProbeResult(
        pipeline=pipeline,
        probe_type="liveness",
        reachable=reachable,
        latency_ms=42.5 if reachable else None,
        checked_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        error=None if reachable else "connection refused",
    )


def test_empty_store_returns_none(store: ProbeStore) -> None:
    assert store.get("missing") is None


def test_empty_store_all_returns_empty(store: ProbeStore) -> None:
    assert store.all() == []


def test_record_and_retrieve(store: ProbeStore) -> None:
    r = _result()
    store.record(r)
    got = store.get("my_pipe")
    assert got is not None
    assert got.pipeline == "my_pipe"
    assert got.reachable is True
    assert got.latency_ms == pytest.approx(42.5)


def test_record_persists_across_reload(tmp_path: Path) -> None:
    path = tmp_path / "probes.json"
    s1 = ProbeStore(path)
    s1.record(_result())
    s2 = ProbeStore(path)
    assert s2.get("my_pipe") is not None


def test_record_overwrite_latest(store: ProbeStore) -> None:
    store.record(_result(reachable=True))
    store.record(_result(reachable=False))
    got = store.get("my_pipe")
    assert got is not None
    assert got.reachable is False


def test_summary_ok(store: ProbeStore) -> None:
    r = _result(reachable=True)
    assert "OK" in r.summary()
    assert "my_pipe" in r.summary()
    assert "42.5ms" in r.summary()


def test_summary_unreachable() -> None:
    r = _result(reachable=False)
    assert "UNREACHABLE" in r.summary()
    assert "n/a" in r.summary()


def test_to_dict_contains_fields() -> None:
    r = _result()
    d = r.to_dict()
    assert d["pipeline"] == "my_pipe"
    assert d["reachable"] is True
    assert d["probe_type"] == "liveness"
    assert "checked_at" in d


def test_run_probe_success() -> None:
    mock_resp = MagicMock()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_resp.read.return_value = b"ok"
    with patch("urllib.request.urlopen", return_value=mock_resp):
        result = run_probe("pipe", "http://localhost/health")
    assert result.reachable is True
    assert result.latency_ms is not None
    assert result.error is None


def test_run_probe_failure() -> None:
    with patch("urllib.request.urlopen", side_effect=OSError("refused")):
        result = run_probe("pipe", "http://localhost/health")
    assert result.reachable is False
    assert result.latency_ms is None
    assert "refused" in (result.error or "")
