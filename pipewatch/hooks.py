"""Lifecycle hooks: run custom shell commands on pipeline check events."""
from __future__ import annotations

import logging
import shlex
import subprocess
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.checker import CheckResult, CheckStatus

logger = logging.getLogger(__name__)

# Mapping from status name to list of shell commands
HookMap = Dict[str, List[str]]


@dataclass
class HookConfig:
    on_stale: List[str] = field(default_factory=list)
    on_failed: List[str] = field(default_factory=list)
    on_ok: List[str] = field(default_factory=list)
    timeout: int = 30

    def commands_for(self, status: CheckStatus) -> List[str]:
        return {
            CheckStatus.STALE: self.on_stale,
            CheckStatus.FAILED: self.on_failed,
            CheckStatus.OK: self.on_ok,
        }.get(status, [])


@dataclass
class HookRunResult:
    command: str
    returncode: int
    stdout: str
    stderr: str

    @property
    def success(self) -> bool:
        return self.returncode == 0


def run_hook(command: str, result: CheckResult, timeout: int = 30) -> HookRunResult:
    """Execute a single hook command, injecting pipeline context via env."""
    env_extra = {
        "PIPEWATCH_PIPELINE": result.pipeline,
        "PIPEWATCH_STATUS": result.status.value,
        "PIPEWATCH_MESSAGE": result.message or "",
        "PIPEWATCH_LAST_RUN": result.last_run.isoformat() if result.last_run else "",
    }
    import os
    env = {**os.environ, **env_extra}

    try:
        proc = subprocess.run(
            shlex.split(command),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        return HookRunResult(
            command=command,
            returncode=proc.returncode,
            stdout=proc.stdout.strip(),
            stderr=proc.stderr.strip(),
        )
    except subprocess.TimeoutExpired:
        logger.warning("Hook timed out after %ds: %s", timeout, command)
        return HookRunResult(command=command, returncode=-1, stdout="", stderr="timeout")
    except Exception as exc:  # noqa: BLE001
        logger.error("Hook error (%s): %s", command, exc)
        return HookRunResult(command=command, returncode=-1, stdout="", stderr=str(exc))


def run_hooks(result: CheckResult, hook_cfg: Optional[HookConfig]) -> List[HookRunResult]:
    """Run all hooks registered for a result's status."""
    if hook_cfg is None:
        return []
    commands = hook_cfg.commands_for(result.status)
    outcomes: List[HookRunResult] = []
    for cmd in commands:
        logger.debug("Running hook for %s/%s: %s", result.pipeline, result.status.value, cmd)
        outcomes.append(run_hook(cmd, result, timeout=hook_cfg.timeout))
    return outcomes
