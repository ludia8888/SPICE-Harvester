from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional, Set


class PrincipalPolicyError(ValueError):
    pass


def build_principal_tags(
    *,
    principal_type: Optional[str] = None,
    principal_id: Optional[str] = None,
    user_id: Optional[str] = None,
    role: Optional[str] = None,
) -> Set[str]:
    # Back-compat: legacy callers pass user_id.
    if principal_id is None:
        principal_id = user_id
    if principal_type is None and user_id is not None:
        principal_type = "user"

    ptype = str(principal_type or "").strip().lower()
    pid = str(principal_id or "").strip()
    tags: Set[str] = set()
    if pid:
        tags.add(f"{ptype or 'user'}:{pid}")
    r = str(role or "").strip()
    if r:
        tags.add(f"role:{r}")
    return tags


def policy_allows(*, policy: Any, principal_tags: Set[str]) -> bool:
    """
    Evaluate a minimal Foundry-style principal policy:

      { "effect": "ALLOW"|"DENY", "principals": ["user:alice", "role:DomainModeler"] }
    """
    if not isinstance(policy, dict) or not policy:
        return True

    effect = str(policy.get("effect") or "ALLOW").strip().upper()
    if effect not in {"ALLOW", "DENY"}:
        effect = "ALLOW"

    principals = policy.get("principals")
    if principals is None:
        principals = []
    if isinstance(principals, str):
        principals = [p.strip() for p in principals.split(",") if p.strip()]
    if not isinstance(principals, list):
        principals = []
    principal_set = {str(p).strip() for p in principals if str(p).strip()}

    if not principal_set:
        return effect != "ALLOW"

    matches = bool(principal_tags & principal_set)
    if effect == "DENY":
        return not matches
    return matches
