from __future__ import annotations

from typing import Any


class QueryOperatorApplicabilityMixin:
    applies_to: list[Any]

    def can_apply_to(self, data_type: Any) -> bool:
        return data_type in self.applies_to
