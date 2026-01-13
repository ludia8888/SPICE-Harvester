from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from shared.services.agent_tool_registry import AgentToolRegistry


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
        record = await tool_registry.upsert_tool_policy(
            tool_id=str(item.get("tool_id") or "").strip(),
            method=str(item.get("method") or "").strip().upper(),
            path=str(item.get("path") or "").strip(),
            risk_level=str(item.get("risk_level") or "read").strip().lower(),
            requires_approval=bool(item.get("requires_approval") or False),
            requires_idempotency_key=bool(item.get("requires_idempotency_key") or False),
            status=str(item.get("status") or "ACTIVE").strip().upper(),
            roles=list(item.get("roles") or []),
            max_payload_bytes=max_payload_bytes,
        )
        upserted.append(record.tool_id)

    return {
        "status": "success",
        "bundle_path": str(Path(bundle_path).expanduser()) if bundle_path else str(_default_allowlist_bundle_path()),
        "upserted_count": len(upserted),
        "upserted_tool_ids": sorted(upserted),
    }

