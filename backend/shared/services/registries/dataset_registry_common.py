from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shared.services.registries.dataset_registry import DatasetRegistry


def require_dataset_registry_pool(registry: "DatasetRegistry") -> Any:
    if not registry._pool:
        raise RuntimeError("DatasetRegistry not connected")
    return registry._pool
