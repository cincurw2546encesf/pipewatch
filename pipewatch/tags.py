"""Tag-based filtering and grouping for pipeline results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from pipewatch.checker import CheckResult


@dataclass
class TagIndex:
    """Maps tags to lists of CheckResult objects."""
    _index: Dict[str, List[CheckResult]] = field(default_factory=dict, init=False, repr=False)

    def build(self, results: Iterable[CheckResult]) -> "TagIndex":
        """Populate index from an iterable of CheckResult objects."""
        self._index.clear()
        for result in results:
            for tag in result.pipeline.tags if hasattr(result.pipeline, "tags") else []:
                self._index.setdefault(tag, []).append(result)
        return self

    def get(self, tag: str) -> List[CheckResult]:
        """Return results associated with *tag*, or empty list."""
        return self._index.get(tag, [])

    def all_tags(self) -> List[str]:
        """Sorted list of all known tags."""
        return sorted(self._index.keys())


def filter_by_tags(
    results: Iterable[CheckResult],
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
) -> List[CheckResult]:
    """Return results whose pipeline tags match the include/exclude rules.

    - *include*: keep only results that have **at least one** of these tags.
    - *exclude*: drop results that have **any** of these tags.
    Both filters may be combined; exclusion is applied after inclusion.
    """
    out: List[CheckResult] = []
    for result in results:
        tags: List[str] = getattr(result.pipeline, "tags", []) or []
        tag_set = set(tags)

        if include and not tag_set.intersection(include):
            continue
        if exclude and tag_set.intersection(exclude):
            continue
        out.append(result)
    return out


def group_by_tag(results: Iterable[CheckResult]) -> Dict[str, List[CheckResult]]:
    """Return a dict mapping each tag to the results that carry it."""
    index: Dict[str, List[CheckResult]] = {}
    for result in results:
        tags: List[str] = getattr(result.pipeline, "tags", []) or []
        for tag in tags:
            index.setdefault(tag, []).append(result)
    return index
