from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from shared.services.agent_tool_registry import AgentToolRegistry


def _derive_resource_scopes(path: str) -> list[str]:
    raw = str(path or "")
    scopes: list[str] = []
    cursor = 0
    while True:
        start = raw.find("{", cursor)
        if start < 0:
            break
        end = raw.find("}", start + 1)
        if end < 0:
            break
        token = raw[start + 1 : end].strip()
        if token:
            scopes.append(token)
        cursor = end + 1
    return [s for s in scopes if s]


def _derive_tool_type(*, tool_id: str, method: str, risk_level: str) -> str:
    tool_id = str(tool_id or "").strip().lower()
    method = str(method or "").strip().upper()
    risk_level = str(risk_level or "").strip().lower()

    if tool_id.startswith("graph.") or tool_id.startswith("query."):
        return "object_query"
    if tool_id.startswith("commands."):
        return "command"
    if tool_id.startswith("clarification."):
        return "clarification"
    if tool_id.startswith("session.") or tool_id.startswith("agent_session."):
        return "session_variable_update"
    if tool_id.startswith("functions.") or tool_id.startswith("ontology.functions."):
        return "function"
    if method == "GET" and risk_level == "read":
        return "object_query"
    if tool_id.startswith("actions."):
        return "action"
    if tool_id.startswith("pipelines.") or tool_id.startswith("objectify.") or tool_id.startswith("ontology."):
        return "action"
    return "action" if risk_level != "read" else "object_query"


def _default_allowlist_bundle_path() -> Path:
    return Path(__file__).resolve().parents[1] / "policies" / "agent_tool_allowlist.json"


def load_agent_tool_allowlist_bundle(*, bundle_path: Optional[str] = None) -> list[dict[str, Any]]:
    resolved = Path(bundle_path).expanduser() if bundle_path else _default_allowlist_bundle_path()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("agent tool allowlist bundle must be a JSON list")
    bundle: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        tool_id = str(item.get("tool_id") or "").strip()
        method = str(item.get("method") or "").strip().upper()
        path = str(item.get("path") or "").strip()
        if not tool_id or not method or not path:
            continue
        bundle.append(dict(item))
    if not bundle:
        raise ValueError("agent tool allowlist bundle is empty")
    return bundle


async def bootstrap_agent_tool_allowlist(
    *,
    tool_registry: AgentToolRegistry,
    bundle_path: Optional[str] = None,
    only_if_empty: bool = True,
) -> dict[str, Any]:
    if only_if_empty:
        existing = await tool_registry.list_tool_policies(status=None, limit=1)
        if existing:
            return {
                "status": "skipped",
                "reason": "existing_policies_present",
                "existing_count": len(existing),
            }

    bundle = load_agent_tool_allowlist_bundle(bundle_path=bundle_path)

    upserted: list[str] = []
    for item in bundle:
        max_payload_raw = item.get("max_payload_bytes")
        try:
            max_payload_bytes = int(max_payload_raw) if max_payload_raw is not None else None
        except (TypeError, ValueError):
            max_payload_bytes = None
        tool_id = str(item.get("tool_id") or "").strip()
        method = str(item.get("method") or "").strip().upper()
        path = str(item.get("path") or "").strip()
        risk_level = str(item.get("risk_level") or "read").strip().lower()
        tool_type = str(item.get("tool_type") or "").strip() or _derive_tool_type(
            tool_id=tool_id, method=method, risk_level=risk_level
        )
        version = str(item.get("version") or "").strip() or "v1"
        resource_scopes = list(item.get("resource_scopes") or []) or _derive_resource_scopes(path)
        timeout_seconds = None
        if item.get("timeout_seconds") is not None:
            try:
                timeout_seconds = float(item.get("timeout_seconds"))
            except (TypeError, ValueError):
                timeout_seconds = None

        record = await tool_registry.upsert_tool_policy(
            tool_id=tool_id,
            method=method,
            path=path,
            risk_level=risk_level,
            requires_approval=bool(item.get("requires_approval") or False),
            requires_idempotency_key=bool(item.get("requires_idempotency_key") or False),
            status=str(item.get("status") or "ACTIVE").strip().upper(),
            roles=list(item.get("roles") or []),
            max_payload_bytes=max_payload_bytes,
            version=version,
            tool_type=tool_type,
            input_schema=item.get("input_schema") if isinstance(item.get("input_schema"), dict) else {},
            output_schema=item.get("output_schema") if isinstance(item.get("output_schema"), dict) else {},
            timeout_seconds=timeout_seconds,
            retry_policy=item.get("retry_policy") if isinstance(item.get("retry_policy"), dict) else {},
            resource_scopes=[str(s).strip() for s in resource_scopes if str(s).strip()],
            metadata=item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
        )
        upserted.append(record.tool_id)

    return {
        "status": "success",
        "bundle_path": str(Path(bundle_path).expanduser()) if bundle_path else str(_default_allowlist_bundle_path()),
        "upserted_count": len(upserted),
        "upserted_tool_ids": sorted(upserted),
    }
