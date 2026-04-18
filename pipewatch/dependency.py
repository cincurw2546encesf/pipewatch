"""Dependency checking: verify upstream pipelines ran successfully before downstream ones."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.checker import CheckResult, CheckStatus


@dataclass
class DependencyViolation:
    pipeline: str
    depends_on: str
    reason: str

    def __str__(self) -> str:
        return f"{self.pipeline} depends on {self.depends_on}: {self.reason}"


@dataclass
class DependencyReport:
    violations: List[DependencyViolation] = field(default_factory=list)
    checked: int = 0

    @property
    def healthy(self) -> bool:
        return len(self.violations) == 0

    def summary(self) -> str:
        if self.healthy:
            return f"All {self.checked} dependency checks passed."
        lines = [f"{len(self.violations)} dependency violation(s):"]
        for v in self.violations:
            lines.append(f"  - {v}")
        return "\n".join(lines)


def check_dependencies(
    results: List[CheckResult],
    dependency_map: dict,  # {pipeline_name: [dep_name, ...]}
) -> DependencyReport:
    """Check that each pipeline's declared dependencies are in an OK state."""
    by_name = {r.pipeline_name: r for r in results}
    violations: List[DependencyViolation] = []
    checked = 0

    for pipeline_name, deps in dependency_map.items():
        for dep in deps:
            checked += 1
            dep_result: Optional[CheckResult] = by_name.get(dep)
            if dep_result is None:
                violations.append(
                    DependencyViolation(
                        pipeline=pipeline_name,
                        depends_on=dep,
                        reason="dependency not found in results",
                    )
                )
            elif dep_result.status != CheckStatus.OK:
                violations.append(
                    DependencyViolation(
                        pipeline=pipeline_name,
                        depends_on=dep,
                        reason=f"dependency status is {dep_result.status.value}",
                    )
                )

    return DependencyReport(violations=violations, checked=checked)
