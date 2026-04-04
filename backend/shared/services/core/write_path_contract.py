from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional

AUTHORITATIVE_STATE_COMMITTED = "committed"
FOLLOWUP_STATUS_COMPLETED = "completed"
FOLLOWUP_STATUS_DEGRADED = "degraded"
FOLLOWUP_STATUS_SKIPPED = "skipped"
RECOVERY_STRATEGY_RETRY = "retry"
RECOVERY_STRATEGY_RECONCILE = "reconcile"
RECOVERY_STRATEGY_NOOP = "noop"

_FOLLOWUP_STATUSES = {
    FOLLOWUP_STATUS_COMPLETED,
    FOLLOWUP_STATUS_DEGRADED,
    FOLLOWUP_STATUS_SKIPPED,
}
_RECOVERY_STRATEGIES = {
    RECOVERY_STRATEGY_RETRY,
    RECOVERY_STRATEGY_RECONCILE,
    RECOVERY_STRATEGY_NOOP,
}

_WRITE_PATH_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "action_batch_submission": {
        "authoritative_store": "event_store",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "oms.action_async",
            "reference": "action_batch_submission",
        },
    },
    "dataset_ingest_commit": {
        "authoritative_store": "postgres.dataset_registry",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "connector_sync_worker",
            "reference": "dataset_ingest_commit",
        },
    },
    "dataset_ingest_publish": {
        "authoritative_store": "postgres.dataset_registry",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "dataset_registry_publish",
            "reference": "dataset_ingest_publish",
        },
    },
    "dataset_version_publish": {
        "authoritative_store": "lakefs",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "pipeline_worker",
            "reference": "dataset_version_publish",
        },
    },
    "instance_create_event": {
        "authoritative_store": "event_store",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "instance_worker",
            "reference": "instance_create_event",
        },
    },
    "instance_delete_event": {
        "authoritative_store": "event_store",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "instance_worker",
            "reference": "instance_delete_event",
        },
    },
    "instance_update_event": {
        "authoritative_store": "event_store",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "instance_worker",
            "reference": "instance_update_event",
        },
    },
    "objectify_job_commit": {
        "authoritative_store": "postgres.objectify_registry",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "objectify_worker",
            "reference": "objectify_job_commit",
        },
    },
    "ontology_resource_mutation": {
        "authoritative_store": "postgres.ontology_resources",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "ontology_worker",
            "reference": "ontology_resource_mutation",
        },
    },
    "pipeline_create": {
        "authoritative_store": "postgres.pipeline_registry",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "bff.pipeline_catalog",
            "reference": "pipeline_create",
        },
    },
    "pipeline_update": {
        "authoritative_store": "postgres.pipeline_registry",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "bff.pipeline_detail",
            "reference": "pipeline_update",
        },
    },
    "relay_checkpoint_advance": {
        "authoritative_store": "postgres.processed_event_registry",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "message_relay",
            "reference": "relay_checkpoint_advance",
        },
    },
    "relay_kafka_publish": {
        "authoritative_store": "event_store",
        "recovery_path": {
            "strategy": RECOVERY_STRATEGY_RECONCILE,
            "owner": "message_relay",
            "reference": "relay_kafka_publish",
        },
    },
}


class WritePathContractError(ValueError):
    """Raised when a write-path contract violates the shared platform schema."""


def _normalize_non_empty_token(value: Any, *, field_name: str) -> str:
    token = str(value or "").strip()
    if not token:
        raise WritePathContractError(f"{field_name} is required")
    return token


def _normalize_authoritative_store(value: Any) -> str:
    store = _normalize_non_empty_token(value, field_name="authoritative_store")
    if store in {"event_store", "lakefs"}:
        return store
    if store.startswith("postgres."):
        suffix = store.split(".", 1)[1].strip()
        if suffix:
            return store
    if store.startswith("elasticsearch"):
        raise WritePathContractError("elasticsearch is derived-only and cannot be authoritative")
    raise WritePathContractError(
        "authoritative_store must be one of 'event_store', 'lakefs', or 'postgres.<registry_name>'"
    )


def _normalize_recovery_path(value: Mapping[str, Any] | None) -> Dict[str, str]:
    if not isinstance(value, Mapping):
        raise WritePathContractError("recovery_path is required")
    strategy = _normalize_non_empty_token(value.get("strategy"), field_name="recovery_path.strategy").lower()
    if strategy not in _RECOVERY_STRATEGIES:
        raise WritePathContractError(
            "recovery_path.strategy must be one of 'retry', 'reconcile', or 'noop'"
        )
    owner = _normalize_non_empty_token(value.get("owner"), field_name="recovery_path.owner")
    reference = _normalize_non_empty_token(value.get("reference"), field_name="recovery_path.reference")
    return {
        "strategy": strategy,
        "owner": owner,
        "reference": reference,
    }


def _normalize_followup(value: Mapping[str, Any]) -> Dict[str, Any]:
    name = _normalize_non_empty_token(value.get("name"), field_name="derived_side_effects[].name")
    status = _normalize_non_empty_token(value.get("status"), field_name="derived_side_effects[].status").lower()
    if status not in _FOLLOWUP_STATUSES:
        raise WritePathContractError(
            "derived_side_effects[].status must be one of 'completed', 'degraded', or 'skipped'"
        )
    followup: Dict[str, Any] = {
        "name": name,
        "status": status,
    }
    if value.get("error") is not None:
        followup["error"] = str(value.get("error"))
    details = value.get("details")
    if isinstance(details, Mapping) and details:
        followup["details"] = dict(details)
    return followup


def _resolve_write_path_defaults(authoritative_write: str) -> Dict[str, Any]:
    definition = _WRITE_PATH_DEFINITIONS.get(authoritative_write)
    if definition is None:
        raise WritePathContractError(
            f"authoritative_write '{authoritative_write}' is not registered in write_path_contract.py"
        )
    return definition


def build_followup_status(
    name: str,
    *,
    status: str,
    error: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    followup: Dict[str, Any] = {
        "name": _normalize_non_empty_token(name, field_name="derived_side_effects[].name"),
        "status": _normalize_non_empty_token(
            status,
            field_name="derived_side_effects[].status",
        ).lower(),
    }
    if followup["status"] not in _FOLLOWUP_STATUSES:
        raise WritePathContractError(
            "derived_side_effects[].status must be one of 'completed', 'degraded', or 'skipped'"
        )
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
    authoritative_store: Optional[str] = None,
    followups: Optional[List[Dict[str, Any]]] = None,
    recovery_path: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_write = _normalize_non_empty_token(authoritative_write, field_name="authoritative_write")
    defaults = _resolve_write_path_defaults(normalized_write)
    resolved_store = _normalize_authoritative_store(
        authoritative_store or defaults.get("authoritative_store")
    )
    resolved_recovery_input = dict(defaults.get("recovery_path") or {})
    if isinstance(recovery_path, Mapping):
        resolved_recovery_input.update(dict(recovery_path))
    return {
        "authoritative_state": AUTHORITATIVE_STATE_COMMITTED,
        "authoritative_store": resolved_store,
        "authoritative_write": normalized_write,
        "derived_side_effects": [_normalize_followup(item) for item in list(followups or [])],
        "recovery_path": _normalize_recovery_path(resolved_recovery_input),
    }


def emit_write_path_contract(
    logger_obj: logging.Logger,
    *,
    authoritative_write: str,
    authoritative_store: Optional[str] = None,
    followups: Optional[List[Dict[str, Any]]] = None,
    recovery_path: Optional[Mapping[str, Any]] = None,
    level: str = "info",
    message_prefix: str = "Write path contract",
) -> Dict[str, Any]:
    contract = build_write_path_contract(
        authoritative_write=authoritative_write,
        authoritative_store=authoritative_store,
        followups=followups,
        recovery_path=recovery_path,
    )
    normalized_level = str(level or "info").strip().lower()
    log_fn = logger_obj.warning if normalized_level == "warning" else logger_obj.info
    log_fn("%s: %s", message_prefix, contract)
    return contract
