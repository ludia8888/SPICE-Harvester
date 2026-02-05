"""
ProcessedEventRegistry factory (Factory Method).

Centralizes validation + connection logic so Kafka workers and other runtime
components don't duplicate bootstrap code.
"""

from __future__ import annotations

from typing import Optional

from shared.services.registries.processed_event_registry import (
    ProcessedEventRegistry,
    validate_lease_settings,
    validate_registry_enabled,
)


async def create_processed_event_registry(
    *,
    lease_timeout_seconds: Optional[int] = None,
    dsn: Optional[str] = None,
    schema: str = "spice_event_registry",
    validate: bool = True,
) -> ProcessedEventRegistry:
    if validate:
        validate_registry_enabled()
        validate_lease_settings(lease_timeout_seconds=lease_timeout_seconds)

    registry = ProcessedEventRegistry(
        dsn=dsn,
        schema=schema,
        lease_timeout_seconds=lease_timeout_seconds,
    )
    await registry.connect()
    return registry

