from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

AUTHORITATIVE_STATE_COMMITTED = "committed"
FOLLOWUP_STATUS_COMPLETED = "completed"
FOLLOWUP_STATUS_DEGRADED = "degraded"
FOLLOWUP_STATUS_SKIPPED = "skipped"


def build_followup_status(
    name: str,
    *,
    status: str,
    error: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    followup: Dict[str, Any] = {
        "name": str(name),
        "status": str(status),
    }
    if error:
        followup["error"] = str(error)
    if isinstance(details, dict) and details:
        followup["details"] = dict(details)
    return followup


def followup_completed(name: str, *, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return build_followup_status(name, status=FOLLOWUP_STATUS_COMPLETED, details=details)


def followup_degraded(
    name: str,
    *,
    error: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return build_followup_status(
        name,
        status=FOLLOWUP_STATUS_DEGRADED,
        error=error,
        details=details,
    )


def followup_skipped(
    name: str,
    *,
    error: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return build_followup_status(
        name,
        status=FOLLOWUP_STATUS_SKIPPED,
        error=error,
        details=details,
    )


def build_write_path_contract(
    *,
    authoritative_write: str,
    followups: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "authoritative_state": AUTHORITATIVE_STATE_COMMITTED,
        "authoritative_write": str(authoritative_write),
        "derived_side_effects": list(followups or []),
    }


def emit_write_path_contract(
    logger_obj: logging.Logger,
    *,
    authoritative_write: str,
    followups: Optional[List[Dict[str, Any]]] = None,
    level: str = "info",
    message_prefix: str = "Write path contract",
) -> Dict[str, Any]:
    contract = build_write_path_contract(
        authoritative_write=authoritative_write,
        followups=followups,
    )
    normalized_level = str(level or "info").strip().lower()
    log_fn = logger_obj.warning if normalized_level == "warning" else logger_obj.info
    log_fn("%s: %s", message_prefix, contract)
    return contract
