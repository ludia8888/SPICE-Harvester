"""Foundry-style compute backend routing helpers for ontology runtime paths."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ComputeBackend = Literal["index_pruning", "spark_on_demand"]
ComputeRoute = Literal["search_around", "writeback"]


def _normalize_threshold(value: int) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        normalized = 1
    return max(1, normalized)


def _normalize_count(value: int) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        normalized = 0
    return max(0, normalized)


@dataclass(frozen=True)
class ComputeRoutingDecision:
    route: ComputeRoute
    estimated_count: int
    threshold: int
    selected_backend: ComputeBackend

    @property
    def spark_routed(self) -> bool:
        return self.selected_backend == "spark_on_demand"

    def as_metadata(
        self,
        *,
        execution_backend: ComputeBackend | None = None,
        fallback_reason: str | None = None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "route": self.route,
            "estimated_count": int(self.estimated_count),
            "threshold": int(self.threshold),
            "selected_backend": self.selected_backend,
            "spark_routed": self.spark_routed,
        }
        if execution_backend is not None:
            metadata["execution_backend"] = execution_backend
        if fallback_reason:
            metadata["fallback_reason"] = fallback_reason
        return metadata


def select_backend(*, route: ComputeRoute, estimated_count: int, threshold: int) -> ComputeRoutingDecision:
    normalized_threshold = _normalize_threshold(threshold)
    normalized_count = _normalize_count(estimated_count)
    selected_backend: ComputeBackend = (
        "spark_on_demand" if normalized_count > normalized_threshold else "index_pruning"
    )
    return ComputeRoutingDecision(
        route=route,
        estimated_count=normalized_count,
        threshold=normalized_threshold,
        selected_backend=selected_backend,
    )


def choose_search_around_backend(*, estimated_count: int, threshold: int) -> ComputeRoutingDecision:
    return select_backend(route="search_around", estimated_count=estimated_count, threshold=threshold)


def choose_writeback_backend(*, estimated_count: int, threshold: int) -> ComputeRoutingDecision:
    return select_backend(route="writeback", estimated_count=estimated_count, threshold=threshold)
