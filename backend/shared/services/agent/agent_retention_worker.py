from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from shared.services.registries.agent_session_registry import AgentSessionRegistry
from shared.services.storage.storage_service import StorageService

logger = logging.getLogger(__name__)

_RETENTION_ACTIONS = {"redact", "delete"}


async def run_agent_session_retention_worker(
    *,
    session_registry: AgentSessionRegistry,
    poll_interval_seconds: int,
    retention_days: int,
    stop_event: asyncio.Event,
    tenant_id: Optional[str] = None,
    action: str = "redact",
    storage_service: Optional[StorageService] = None,
    delete_file_upload_objects: bool = True,
    retention_policy_json: Optional[str] = None,
) -> None:
    """
    Background retention worker for agent session data (SEC-005).

    This worker is intentionally best-effort; failures are logged and retried.
    """
    poll_seconds = max(1, int(poll_interval_seconds))
    retention_days = max(0, int(retention_days))
    action_value = str(action or "").strip().lower() or "redact"
    delete_uploads = bool(delete_file_upload_objects)
    policy = _parse_retention_policy(retention_policy_json)

    while not stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)
            if not policy:
                if retention_days > 0:
                    cutoff = now - timedelta(days=retention_days)
                    if delete_uploads and storage_service is not None:
                        try:
                            expired = await session_registry.list_expired_file_uploads(
                                cutoff=cutoff, tenant_id=tenant_id, limit=500
                            )
                            for item in expired:
                                bucket = str(item.get("bucket") or "").strip()
                                key = str(item.get("key") or "").strip()
                                if not bucket or not key:
                                    continue
                                try:
                                    await storage_service.delete_object(bucket=bucket, key=key)
                                except Exception as exc:
                                    logger.warning(
                                        "Agent retention failed to delete upload object bucket=%s key=%s: %s",
                                        bucket,
                                        key,
                                        exc,
                                        exc_info=True,
                                    )
                        except Exception as exc:
                            logger.warning("Agent retention upload cleanup failed: %s", exc)

                    await session_registry.apply_retention(
                        cutoff=cutoff,
                        tenant_id=tenant_id,
                        action=action_value,
                    )
            else:
                await _apply_policy(
                    session_registry=session_registry,
                    storage_service=storage_service,
                    tenant_id=tenant_id,
                    delete_uploads=delete_uploads,
                    default_days=retention_days,
                    default_action=action_value,
                    now=now,
                    policy=policy,
                )
        except Exception as exc:
            logger.warning("Agent retention worker failed: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_seconds)
        except asyncio.TimeoutError:
            continue


def _parse_retention_policy(raw: Optional[str]) -> dict[str, dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/agent/agent_retention_worker.py:93", exc_info=True)
        return {}
    if not isinstance(parsed, dict):
        return {}

    policy: dict[str, dict[str, Any]] = {}
    for key, value in parsed.items():
        name = str(key or "").strip().lower()
        if not name:
            continue
        if isinstance(value, (int, float, str)):
            try:
                days = int(str(value).strip())
            except Exception:
                logging.getLogger(__name__).warning("Broad exception fallback at shared/services/agent/agent_retention_worker.py:106", exc_info=True)
                continue
            policy[name] = {"days": max(0, min(3650, days))}
            continue
        if isinstance(value, dict):
            days_raw = value.get("days")
            action_raw = value.get("action")
            days_value: Optional[int] = None
            if days_raw is not None:
                try:
                    days_value = int(str(days_raw).strip())
                except (TypeError, ValueError):
                    days_value = None
            action_value: Optional[str] = None
            if action_raw is not None:
                candidate = str(action_raw).strip().lower()
                if candidate in _RETENTION_ACTIONS:
                    action_value = candidate
            entry: dict[str, Any] = {}
            if days_value is not None:
                entry["days"] = max(0, min(3650, int(days_value)))
            if action_value is not None:
                entry["action"] = action_value
            if entry:
                policy[name] = entry
    return policy


def _policy_days_action(
    *,
    policy: dict[str, dict[str, Any]],
    key: str,
    default_days: int,
    default_action: str,
) -> tuple[int, str]:
    name = str(key or "").strip().lower()
    entry = policy.get(name) if name else None
    days_value = default_days
    action_value = default_action
    if isinstance(entry, dict):
        if entry.get("days") is not None:
            try:
                days_value = int(entry.get("days"))
            except (TypeError, ValueError):
                days_value = default_days
        if entry.get("action") is not None:
            candidate = str(entry.get("action")).strip().lower()
            if candidate in _RETENTION_ACTIONS:
                action_value = candidate
    return max(0, int(days_value)), str(action_value or default_action).strip().lower() or "redact"


async def _apply_policy(
    *,
    session_registry: AgentSessionRegistry,
    storage_service: Optional[StorageService],
    tenant_id: Optional[str],
    delete_uploads: bool,
    default_days: int,
    default_action: str,
    now: datetime,
    policy: dict[str, dict[str, Any]],
) -> None:
    # Core object types (SEC-005): messages/tool_calls/context_items/ci_results/events/llm_calls.
    msg_days, msg_action = _policy_days_action(policy=policy, key="messages", default_days=default_days, default_action=default_action)
    tool_days, tool_action = _policy_days_action(policy=policy, key="tool_calls", default_days=default_days, default_action=default_action)
    ci_days, _ci_action = _policy_days_action(policy=policy, key="ci_results", default_days=default_days, default_action=default_action)
    evt_days, _evt_action = _policy_days_action(policy=policy, key="events", default_days=default_days, default_action=default_action)
    llm_days, _llm_action = _policy_days_action(policy=policy, key="llm_calls", default_days=default_days, default_action=default_action)

    # Context items support per-type overrides via keys like: "context_items:file_upload"
    ctx_default_days, ctx_default_action = _policy_days_action(
        policy=policy,
        key="context_items",
        default_days=default_days,
        default_action=default_action,
    )
    ctx_overrides: dict[str, tuple[int, str]] = {}
    for key in policy.keys():
        if not key.startswith("context_items:"):
            continue
        item_type = key.split(":", 1)[1].strip()
        if not item_type:
            continue
        days_value, action_value = _policy_days_action(
            policy=policy,
            key=key,
            default_days=ctx_default_days,
            default_action=ctx_default_action,
        )
        ctx_overrides[item_type] = (days_value, action_value)

    overridden_types = sorted({t for t in ctx_overrides.keys() if t})

    file_upload_days, _file_upload_action = ctx_overrides.get("file_upload", (ctx_default_days, ctx_default_action))
    if file_upload_days > 0 and delete_uploads and storage_service is not None:
        cutoff = now - timedelta(days=file_upload_days)
        try:
            expired = await session_registry.list_expired_file_uploads(cutoff=cutoff, tenant_id=tenant_id, limit=500)
            for item in expired:
                bucket = str(item.get("bucket") or "").strip()
                key = str(item.get("key") or "").strip()
                if not bucket or not key:
                    continue
                try:
                    await storage_service.delete_object(bucket=bucket, key=key)
                except Exception as exc:
                    logger.warning(
                        "Agent retention failed to delete upload object bucket=%s key=%s: %s",
                        bucket,
                        key,
                        exc,
                        exc_info=True,
                    )
        except Exception as exc:
            logger.warning("Agent retention upload cleanup failed: %s", exc)

    # Apply retention per object type. Each call is category-scoped to prevent drift across differing cutoffs.
    if msg_days > 0:
        cutoff = now - timedelta(days=msg_days)
        await session_registry.apply_retention(
            cutoff=cutoff,
            tenant_id=tenant_id,
            action=msg_action,
            include_messages=True,
            include_tool_calls=False,
            include_context_items=False,
            include_ci_results=False,
            include_events=False,
            include_llm_calls=False,
        )
    if tool_days > 0:
        cutoff = now - timedelta(days=tool_days)
        await session_registry.apply_retention(
            cutoff=cutoff,
            tenant_id=tenant_id,
            action=tool_action,
            include_messages=False,
            include_tool_calls=True,
            include_context_items=False,
            include_ci_results=False,
            include_events=False,
            include_llm_calls=False,
        )
    if ci_days > 0:
        cutoff = now - timedelta(days=ci_days)
        await session_registry.apply_retention(
            cutoff=cutoff,
            tenant_id=tenant_id,
            action="delete",
            include_messages=False,
            include_tool_calls=False,
            include_context_items=False,
            include_ci_results=True,
            include_events=False,
            include_llm_calls=False,
        )
    if evt_days > 0:
        cutoff = now - timedelta(days=evt_days)
        await session_registry.apply_retention(
            cutoff=cutoff,
            tenant_id=tenant_id,
            action="delete",
            include_messages=False,
            include_tool_calls=False,
            include_context_items=False,
            include_ci_results=False,
            include_events=True,
            include_llm_calls=False,
        )
    if llm_days > 0:
        cutoff = now - timedelta(days=llm_days)
        await session_registry.apply_retention(
            cutoff=cutoff,
            tenant_id=tenant_id,
            action="delete",
            include_messages=False,
            include_tool_calls=False,
            include_context_items=False,
            include_ci_results=False,
            include_events=False,
            include_llm_calls=True,
        )

    # Per-context-item-type overrides first.
    for item_type, (days_value, action_value) in sorted(ctx_overrides.items()):
        if days_value <= 0:
            continue
        cutoff = now - timedelta(days=days_value)
        await session_registry.apply_retention(
            cutoff=cutoff,
            tenant_id=tenant_id,
            action=action_value,
            include_messages=False,
            include_tool_calls=False,
            include_context_items=True,
            include_ci_results=False,
            include_events=False,
            include_llm_calls=False,
            context_item_type=item_type,
        )

    # Default context items (excluding any explicitly overridden types).
    if ctx_default_days > 0:
        cutoff = now - timedelta(days=ctx_default_days)
        await session_registry.apply_retention(
            cutoff=cutoff,
            tenant_id=tenant_id,
            action=ctx_default_action,
            include_messages=False,
            include_tool_calls=False,
            include_context_items=True,
            include_ci_results=False,
            include_events=False,
            include_llm_calls=False,
            exclude_context_item_types=overridden_types,
        )
