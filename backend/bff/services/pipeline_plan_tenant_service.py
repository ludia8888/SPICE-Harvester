"""Pipeline plan tenant helpers (BFF).

Centralizes tenant/actor resolution and tenant policy lookup used by:
- pipeline plan compilation
- pipeline agent endpoints
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import Request

from bff.routers.registry_deps import get_agent_policy_registry


def resolve_tenant_id(request: Request) -> str:
    user = getattr(request.state, "user", None)
    candidate = (
        getattr(user, "tenant_id", None)
        or getattr(user, "org_id", None)
        or request.headers.get("X-Tenant-ID")
        or request.headers.get("X-Org-ID")
        or "default"
    )
    return str(candidate).strip() or "default"


def resolve_actor(request: Request) -> str:
    return request.headers.get("X-User-ID") or request.headers.get("X-User") or request.headers.get("X-Actor") or "system"


async def resolve_tenant_policy(request: Request) -> tuple[Optional[str], Optional[list[str]], Optional[Dict[str, Any]]]:
    tenant_id = resolve_tenant_id(request)
    try:
        policy_registry = await get_agent_policy_registry()
        policy = await policy_registry.get_tenant_policy(tenant_id=tenant_id)
    except Exception:
        return None, None, None

    if not policy:
        return None, None, None

    allowed_models = [str(m).strip() for m in (policy.allowed_models or []) if str(m).strip()]
    if policy.default_model:
        allowed_models.append(str(policy.default_model).strip())
    allowed_models = [m for m in allowed_models if m]
    allowed_models_final = list(dict.fromkeys(allowed_models)) or None

    selected_model: Optional[str] = None
    if policy.default_model:
        selected_model = str(policy.default_model).strip() or None
    if selected_model is None and allowed_models_final:
        selected_model = str(allowed_models_final[0]).strip() or None

    data_policies = dict(getattr(policy, "data_policies", None) or {})
    return selected_model, allowed_models_final, data_policies
