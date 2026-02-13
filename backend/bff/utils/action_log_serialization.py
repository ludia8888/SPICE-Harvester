"""Action log serialization helpers (BFF).

Centralizes ActionLogRecord -> dict conversion used by multiple BFF modules.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from shared.services.registries.action_log_registry import ActionLogRecord
import logging

ACTION_LOG_CLASS_ID = "ActionLog"


def dt_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.isoformat()
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/utils/action_log_serialization.py:20", exc_info=True)
        return str(value)


def serialize_action_log_record(record: ActionLogRecord) -> Dict[str, Any]:
    return {
        "class_id": ACTION_LOG_CLASS_ID,
        "instance_id": record.action_log_id,
        "rid": f"action_log:{record.action_log_id}",
        "action_log_id": record.action_log_id,
        "db_name": record.db_name,
        "action_type_id": record.action_type_id,
        "action_type_rid": record.action_type_rid,
        "resource_rid": record.resource_rid,
        "ontology_commit_id": record.ontology_commit_id,
        "input": record.input,
        "status": record.status,
        "result": record.result,
        "correlation_id": record.correlation_id,
        "submitted_by": record.submitted_by,
        "submitted_at": dt_iso(record.submitted_at),
        "finished_at": dt_iso(record.finished_at),
        "writeback_target": record.writeback_target,
        "writeback_commit_id": record.writeback_commit_id,
        "action_applied_event_id": record.action_applied_event_id,
        "action_applied_seq": record.action_applied_seq,
        "metadata": record.metadata,
        "updated_at": dt_iso(record.updated_at),
    }

