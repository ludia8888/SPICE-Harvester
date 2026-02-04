from __future__ import annotations

from shared.utils.deterministic_ids import deterministic_uuid5_str


def spice_event_id(*, command_id: str, event_type: str, aggregate_id: str) -> str:
    """
    Deterministic domain event id derived from a command id.

    This keeps Event Sourcing idempotent under at-least-once delivery by ensuring
    repeated processing of the same command produces the same event id.
    """

    return deterministic_uuid5_str(f"spice:{command_id}:{event_type}:{aggregate_id}")
