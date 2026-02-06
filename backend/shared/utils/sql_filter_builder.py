from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List


@dataclass
class SqlFilterBuilder:
    clauses: List[str] = field(default_factory=list)
    params: List[Any] = field(default_factory=list)

    def add(self, condition_template: str, value: Any) -> None:
        self.params.append(value)
        self.clauses.append(condition_template.replace("$X", f"${len(self.params)}"))

    def where(self, *, joiner: str = " AND ", prefix: str = " WHERE ") -> str:
        return f"{prefix}{joiner.join(self.clauses)}" if self.clauses else ""

    def join(self, *, joiner: str = " AND ") -> str:
        return joiner.join(self.clauses)
