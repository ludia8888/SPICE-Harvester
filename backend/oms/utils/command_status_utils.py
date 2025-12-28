from __future__ import annotations

from shared.models.commands import CommandStatus


def map_registry_status(status_value: str) -> CommandStatus:
    status_value = (status_value or "").lower()
    if status_value == "processing":
        return CommandStatus.PROCESSING
    if status_value == "done":
        return CommandStatus.COMPLETED
    if status_value == "failed":
        return CommandStatus.FAILED
    if status_value == "skipped_stale":
        return CommandStatus.FAILED
    return CommandStatus.PENDING
