from __future__ import annotations

import inspect
import logging
from typing import Any, Dict, Iterable, List, Mapping, Optional

from shared.observability.log_taxonomy import log_taxonomy_event

RUNTIME_STATE_READY = "ready"
RUNTIME_STATE_DEGRADED = "degraded"
RUNTIME_STATE_HARD_DOWN = "hard_down"
_RUNTIME_STATES = {
    RUNTIME_STATE_READY,
    RUNTIME_STATE_DEGRADED,
    RUNTIME_STATE_HARD_DOWN,
}

_DEFAULT_RUNTIME_STATUS_ATTRS = (
    "runtime_status",
)

logger = logging.getLogger(__name__)


def _state_rank(value: str) -> int:
    if value == RUNTIME_STATE_HARD_DOWN:
        return 2
    if value == RUNTIME_STATE_DEGRADED:
        return 1
    return 0


def normalize_runtime_state(value: Any, *, default: str = RUNTIME_STATE_DEGRADED) -> str:
    raw = str(value or "").strip().lower()
    if raw in _RUNTIME_STATES:
        return raw
    if raw in {"ok", "healthy"}:
        return RUNTIME_STATE_READY
    if raw in {"unready", "failed", "unhealthy", "down", "unavailable"}:
        return RUNTIME_STATE_HARD_DOWN
    return default


def normalize_affected_features(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        token = value.strip()
        return [token] if token else []
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, Mapping)):
        features: List[str] = []
        seen: set[str] = set()
        for item in value:
            token = str(item or "").strip()
            if not token or token in seen:
                continue
            seen.add(token)
            features.append(token)
        return features
    token = str(value).strip()
    return [token] if token else []


def _ordered_unique_strings(values: Iterable[Any]) -> List[str]:
    items: List[str] = []
    seen: set[str] = set()
    for raw in values:
        token = str(raw or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        items.append(token)
    return items


def build_runtime_issue(
    *,
    component: str,
    message: str,
    dependency: Optional[str] = None,
    state: str = RUNTIME_STATE_DEGRADED,
    classification: str = "unavailable",
    affected_features: Optional[Iterable[str]] = None,
    affects_readiness: Optional[bool] = None,
) -> Dict[str, Any]:
    resolved_state = normalize_runtime_state(
        RUNTIME_STATE_HARD_DOWN if affects_readiness else state,
        default=RUNTIME_STATE_DEGRADED,
    )
    features = normalize_affected_features(affected_features)
    dependency_name = str(dependency or component or "").strip() or "runtime"
    issue_component = str(component or dependency_name).strip() or dependency_name
    if not features:
        features = [dependency_name]
    return {
        "component": issue_component,
        "dependency": dependency_name,
        "state": resolved_state,
        "classification": str(classification or "unavailable").strip().lower() or "unavailable",
        "message": str(message or "").strip() or issue_component,
        "affects_readiness": bool(affects_readiness if affects_readiness is not None else resolved_state == RUNTIME_STATE_HARD_DOWN),
        "affected_features": features,
    }


def normalize_runtime_issue(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        component = str(value.get("component") or value.get("dependency") or value.get("name") or "runtime").strip()
        dependency = str(value.get("dependency") or component).strip() or component
        affects_readiness_raw = value.get("affects_readiness")
        if isinstance(affects_readiness_raw, bool):
            affects_readiness = affects_readiness_raw
        else:
            affects_readiness = None
        return build_runtime_issue(
            component=component,
            dependency=dependency,
            message=str(value.get("message") or value.get("error") or component).strip() or component,
            state=str(value.get("state") or value.get("status") or ""),
            classification=str(value.get("classification") or "unavailable"),
            affected_features=normalize_affected_features(
                value.get("affected_features") or value.get("features") or value.get("feature")
            ),
            affects_readiness=affects_readiness,
        )
    token = str(value or "").strip() or "runtime"
    return build_runtime_issue(component=token, dependency=token, message=token)


def default_runtime_status() -> Dict[str, Any]:
    return {
        "ready": True,
        "degraded": False,
        "issues": [],
        "background_tasks": {},
        "dependency_status": {},
    }


def _resolve_state_holder(holder: Any) -> Any:
    state = getattr(holder, "state", None)
    return state if state is not None else holder


def _candidate_attr_names(additional: Optional[Iterable[str]] = None) -> List[str]:
    names: List[str] = []
    seen: set[str] = set()
    for item in list(additional or []) + list(_DEFAULT_RUNTIME_STATUS_ATTRS):
        token = str(item or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        names.append(token)
    return names


def get_runtime_status(holder: Any, *, attr_names: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    state_holder = _resolve_state_holder(holder)
    for attr in _candidate_attr_names(attr_names):
        value = getattr(state_holder, attr, None)
        if isinstance(value, Mapping):
            return normalize_runtime_status(value)
    return normalize_runtime_status({})


def set_runtime_status(holder: Any, status: Mapping[str, Any], *, attr_names: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    state_holder = _resolve_state_holder(holder)
    normalized = normalize_runtime_status(status)
    names = _candidate_attr_names(attr_names)
    for attr in names:
        setattr(state_holder, attr, dict(normalized))
    return normalized


def ensure_runtime_status(holder: Any, *, attr_names: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    normalized = get_runtime_status(holder, attr_names=attr_names)
    return set_runtime_status(holder, normalized, attr_names=attr_names)


def record_runtime_issue(
    holder: Any,
    *,
    component: str,
    message: str,
    dependency: Optional[str] = None,
    state: str = RUNTIME_STATE_DEGRADED,
    classification: str = "unavailable",
    affected_features: Optional[Iterable[str]] = None,
    affects_readiness: Optional[bool] = None,
    attr_names: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    runtime_status = ensure_runtime_status(holder, attr_names=attr_names)
    issue = build_runtime_issue(
        component=component,
        dependency=dependency,
        message=message,
        state=state,
        classification=classification,
        affected_features=affected_features,
        affects_readiness=affects_readiness,
    )
    issues = [normalize_runtime_issue(item) for item in runtime_status.get("issues") or []]
    if not any(
        existing.get("component") == issue["component"]
        and existing.get("message") == issue["message"]
        and existing.get("state") == issue["state"]
        for existing in issues
    ):
        issues.append(issue)
    runtime_status["issues"] = issues
    dependency_status = dict(runtime_status.get("dependency_status") or {})
    dependency_name = issue["dependency"]
    previous = normalize_runtime_state(dependency_status.get(dependency_name), default=RUNTIME_STATE_READY)
    if _state_rank(issue["state"]) >= _state_rank(previous):
        dependency_status[dependency_name] = issue["state"]
    runtime_status["dependency_status"] = dependency_status
    runtime_status["degraded"] = bool(issues or runtime_status.get("degraded"))
    if issue["state"] == RUNTIME_STATE_HARD_DOWN:
        runtime_status["ready"] = False
    normalized = set_runtime_status(holder, runtime_status, attr_names=attr_names)
    _emit_runtime_issue_log(holder, issue=issue)
    return normalized


def normalize_background_tasks(value: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, raw in value.items():
        task_name = str(key or "").strip()
        if not task_name:
            continue
        if isinstance(raw, Mapping):
            payload = dict(raw)
        else:
            payload = {"status": str(raw or "").strip() or "unknown"}
        status_value = str(payload.get("status") or "unknown").strip() or "unknown"
        payload["status"] = status_value
        normalized[task_name] = payload
    return normalized


def normalize_runtime_status(value: Mapping[str, Any] | None) -> Dict[str, Any]:
    payload = dict(value or {})
    issues = [normalize_runtime_issue(item) for item in payload.get("issues") or []]
    explicit_ready = bool(payload.get("ready", True))
    explicit_degraded = bool(payload.get("degraded", False))
    if not explicit_ready and issues and not any(issue["state"] == RUNTIME_STATE_HARD_DOWN for issue in issues):
        issues = [
            {
                **issue,
                "state": RUNTIME_STATE_HARD_DOWN,
                "affects_readiness": True,
            }
            for issue in issues
        ]
    dependency_status: Dict[str, str] = {}
    raw_dependency_status = payload.get("dependency_status")
    if isinstance(raw_dependency_status, Mapping):
        for key, raw in raw_dependency_status.items():
            token = str(key or "").strip()
            if not token:
                continue
            dependency_status[token] = normalize_runtime_state(raw, default=RUNTIME_STATE_DEGRADED)
    for issue in issues:
        dependency_name = issue["dependency"]
        previous = dependency_status.get(dependency_name, RUNTIME_STATE_READY)
        if _state_rank(issue["state"]) >= _state_rank(previous):
            dependency_status[dependency_name] = issue["state"]
    background_tasks = normalize_background_tasks(payload.get("background_tasks"))
    hard_down = (not explicit_ready) or any(issue["state"] == RUNTIME_STATE_HARD_DOWN for issue in issues)
    degraded = (not hard_down) and (
        explicit_degraded
        or any(issue["state"] == RUNTIME_STATE_DEGRADED for issue in issues)
        or any(state == RUNTIME_STATE_DEGRADED for state in dependency_status.values())
    )
    ready = not hard_down
    return {
        "ready": ready,
        "degraded": degraded,
        "issues": issues,
        "background_tasks": background_tasks,
        "dependency_status": dependency_status,
    }


def _dependency_details(
    *,
    issues: List[Dict[str, Any]],
    dependency_status: Mapping[str, str],
    unhealthy_services: int = 0,
) -> Dict[str, Dict[str, Any]]:
    details: Dict[str, Dict[str, Any]] = {}

    def _ensure_detail(dependency_name: str) -> Dict[str, Any]:
        state = normalize_runtime_state(
            dependency_status.get(dependency_name),
            default=RUNTIME_STATE_READY,
        )
        payload = details.get(dependency_name)
        if payload is None:
            payload = {
                "dependency": dependency_name,
                "state": state,
                "classification": "healthy" if state == RUNTIME_STATE_READY else "unknown",
                "classifications": [],
                "message": "" if state == RUNTIME_STATE_READY else f"{dependency_name} is {state}",
                "messages": [],
                "components": [],
                "affected_features": [],
                "affects_readiness": state == RUNTIME_STATE_HARD_DOWN,
                "issue_count": 0,
                "source": "derived",
            }
            details[dependency_name] = payload
        else:
            if _state_rank(state) > _state_rank(str(payload.get("state") or RUNTIME_STATE_READY)):
                payload["state"] = state
            if state == RUNTIME_STATE_HARD_DOWN:
                payload["affects_readiness"] = True
        return payload

    for dependency_name in dependency_status:
        _ensure_detail(str(dependency_name))

    for issue in issues:
        dependency_name = str(issue.get("dependency") or issue.get("component") or "runtime").strip() or "runtime"
        detail = _ensure_detail(dependency_name)
        issue_state = normalize_runtime_state(issue.get("state"), default=RUNTIME_STATE_DEGRADED)
        if _state_rank(issue_state) >= _state_rank(str(detail.get("state") or RUNTIME_STATE_READY)):
            detail["state"] = issue_state
        classifications = _ordered_unique_strings(
            list(detail.get("classifications") or []) + [issue.get("classification")]
        )
        messages = _ordered_unique_strings(list(detail.get("messages") or []) + [issue.get("message")])
        components = _ordered_unique_strings(list(detail.get("components") or []) + [issue.get("component")])
        affected_features = _ordered_unique_strings(
            list(detail.get("affected_features") or []) + list(issue.get("affected_features") or [])
        )
        detail["classifications"] = classifications
        detail["classification"] = classifications[0] if classifications else detail.get("classification") or "unknown"
        detail["messages"] = messages
        detail["message"] = messages[0] if messages else detail.get("message") or f"{dependency_name} is {detail['state']}"
        detail["components"] = components
        detail["affected_features"] = affected_features
        detail["affects_readiness"] = bool(detail.get("affects_readiness") or issue.get("affects_readiness"))
        detail["issue_count"] = int(detail.get("issue_count") or 0) + 1
        detail["source"] = "issue"

    if unhealthy_services > 0:
        detail = _ensure_detail("services")
        detail["state"] = RUNTIME_STATE_DEGRADED
        detail["classification"] = "internal"
        detail["classifications"] = _ordered_unique_strings(list(detail.get("classifications") or []) + ["internal"])
        detail["messages"] = _ordered_unique_strings(
            list(detail.get("messages") or []) + ["One or more initialized services failed health checks"]
        )
        detail["message"] = detail["messages"][0]
        detail["components"] = _ordered_unique_strings(list(detail.get("components") or []) + ["service_health_checks"])
        detail["affected_features"] = _ordered_unique_strings(
            list(detail.get("affected_features") or []) + ["service_health_checks"]
        )
        detail["source"] = "derived"

    return dict(sorted(details.items(), key=lambda item: item[0]))


def _root_cause_summary(details: Mapping[str, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    root_causes: List[Dict[str, Any]] = []
    for dependency_name, detail in details.items():
        state = normalize_runtime_state(detail.get("state"), default=RUNTIME_STATE_READY)
        if state == RUNTIME_STATE_READY:
            continue
        root_causes.append(
            {
                "dependency": dependency_name,
                "state": state,
                "classification": str(detail.get("classification") or "unknown"),
                "message": str(detail.get("message") or f"{dependency_name} is {state}"),
                "affected_features": list(detail.get("affected_features") or []),
                "components": list(detail.get("components") or []),
                "affects_readiness": bool(detail.get("affects_readiness")),
                "issue_count": int(detail.get("issue_count") or 0),
                "source": str(detail.get("source") or "derived"),
            }
        )
    root_causes.sort(
        key=lambda item: (
            -_state_rank(str(item.get("state") or RUNTIME_STATE_READY)),
            -(1 if int(item.get("issue_count") or 0) > 0 else 0),
            not bool(item.get("affects_readiness")),
            str(item.get("dependency") or ""),
        )
    )
    return root_causes


def _service_name_from_holder(holder: Any) -> str:
    state_holder = _resolve_state_holder(holder)
    for candidate in (getattr(state_holder, "service_name", None), getattr(getattr(state_holder, "state", None), "service_name", None)):
        token = str(candidate or "").strip()
        if token:
            return token
    return "runtime"


def _emit_runtime_issue_log(holder: Any, *, issue: Mapping[str, Any]) -> None:
    state = normalize_runtime_state(issue.get("state"), default=RUNTIME_STATE_DEGRADED)
    classification = str(issue.get("classification") or "unavailable").strip().lower() or "unavailable"
    level = logging.ERROR if state == RUNTIME_STATE_HARD_DOWN else logging.WARNING
    retryable = classification in {"unavailable", "retryable"}
    log_taxonomy_event(
        logger,
        level,
        "Runtime dependency issue recorded",
        event_name="runtime.issue",
        event_family="availability",
        failure_class=classification,
        retryable=retryable,
        extra={
            "runtime_issue": {
                "service": _service_name_from_holder(holder),
                "component": issue.get("component"),
                "dependency": issue.get("dependency"),
                "state": state,
                "classification": classification,
                "message": issue.get("message"),
                "affected_features": list(issue.get("affected_features") or []),
                "affects_readiness": bool(issue.get("affects_readiness")),
            }
        },
    )


async def probe_service_runtime_state(service: object) -> str:
    if hasattr(service, "health_check"):
        result = service.health_check()
        if inspect.isawaitable(result):
            result = await result
        ok = bool(getattr(result, "status", result)) if not isinstance(result, bool) else result
        return RUNTIME_STATE_READY if bool(ok) else RUNTIME_STATE_HARD_DOWN
    if hasattr(service, "ping"):
        result = service.ping()
        if inspect.isawaitable(result):
            result = await result
        return RUNTIME_STATE_READY if bool(result) else RUNTIME_STATE_HARD_DOWN
    if hasattr(service, "check_connection"):
        result = service.check_connection()
        if inspect.isawaitable(result):
            result = await result
        return RUNTIME_STATE_READY if bool(result) else RUNTIME_STATE_HARD_DOWN
    return RUNTIME_STATE_DEGRADED


def availability_surface(
    *,
    service: str,
    container_ready: bool,
    runtime_status: Mapping[str, Any] | None,
    unhealthy_services: int = 0,
    dependency_status_overrides: Mapping[str, Any] | None = None,
    status_reason_override: Optional[str] = None,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    normalized = normalize_runtime_status(runtime_status or {})
    dependency_status = dict(normalized.get("dependency_status") or {})
    dependency_status["container"] = RUNTIME_STATE_READY if container_ready else RUNTIME_STATE_HARD_DOWN
    runtime_dependency_state = (
        RUNTIME_STATE_HARD_DOWN
        if not normalized["ready"]
        else RUNTIME_STATE_DEGRADED if normalized["degraded"] else RUNTIME_STATE_READY
    )
    dependency_status["runtime"] = runtime_dependency_state
    if unhealthy_services > 0:
        dependency_status["services"] = RUNTIME_STATE_DEGRADED
    if isinstance(dependency_status_overrides, Mapping):
        for name, value in dependency_status_overrides.items():
            token = str(name or "").strip()
            if not token:
                continue
            dependency_status[token] = normalize_runtime_state(value, default=RUNTIME_STATE_DEGRADED)
    has_hard_down_dependency = any(value == RUNTIME_STATE_HARD_DOWN for value in dependency_status.values())
    has_degraded_dependency = any(value == RUNTIME_STATE_DEGRADED for value in dependency_status.values())
    state = (
        RUNTIME_STATE_HARD_DOWN
        if (not container_ready or not normalized["ready"] or has_hard_down_dependency)
        else RUNTIME_STATE_DEGRADED
        if (normalized["degraded"] or unhealthy_services > 0 or has_degraded_dependency)
        else RUNTIME_STATE_READY
    )
    affected_features: List[str] = []
    for issue in normalized["issues"]:
        for feature in issue.get("affected_features") or []:
            token = str(feature or "").strip()
            if token and token not in affected_features:
                affected_features.append(token)
    hard_down_dependencies = sorted([name for name, value in dependency_status.items() if value == RUNTIME_STATE_HARD_DOWN])
    degraded_dependencies = sorted([name for name, value in dependency_status.items() if value == RUNTIME_STATE_DEGRADED])
    dependency_details = _dependency_details(
        issues=normalized["issues"],
        dependency_status=dependency_status,
        unhealthy_services=unhealthy_services,
    )
    root_causes = _root_cause_summary(dependency_details)
    if status_reason_override:
        status_reason = str(status_reason_override).strip()
    elif root_causes:
        status_reason = root_causes[0]["message"]
    elif state == RUNTIME_STATE_READY:
        status_reason = "All tracked dependencies are ready"
    elif not container_ready:
        status_reason = "Service container is not initialized"
    else:
        status_reason = f"Service is {state}"
    classifications: Dict[str, int] = {}
    for item in root_causes:
        classification = str(item.get("classification") or "unknown")
        if str(item.get("source") or "derived") != "issue":
            continue
        if classification in {"unknown", "healthy"}:
            continue
        classifications[classification] = int(classifications.get(classification, 0)) + 1
    payload = {
        "status": state,
        "alive": True,
        "ready": state != RUNTIME_STATE_HARD_DOWN,
        "accepting_traffic": state != RUNTIME_STATE_HARD_DOWN,
        "degraded": state == RUNTIME_STATE_DEGRADED,
        "hard_down": state == RUNTIME_STATE_HARD_DOWN,
        "service": service,
        "status_reason": status_reason,
        "issues": normalized["issues"],
        "background_tasks": normalized["background_tasks"],
        "dependency_status": dependency_status,
        "dependency_details": dependency_details,
        "affected_features": affected_features,
        "root_causes": root_causes,
        "impact_summary": {
            "hard_down_dependencies": hard_down_dependencies,
            "degraded_dependencies": degraded_dependencies,
            "affected_features": affected_features,
            "root_cause_dependencies": [item["dependency"] for item in root_causes],
            "root_cause_messages": [item["message"] for item in root_causes],
            "classifications": classifications,
        },
    }
    if message is not None:
        payload["message"] = str(message)
    return payload
