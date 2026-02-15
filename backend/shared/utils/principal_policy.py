from __future__ import annotations

from typing import Any, Optional, Set


class PrincipalPolicyError(ValueError):
    pass


_SUPPORTED_PRINCIPAL_PREFIXES = {"user", "group", "role", "service"}


def build_principal_tags(
    *,
    principal_type: Optional[str] = None,
    principal_id: Optional[str] = None,
    role: Optional[str] = None,
) -> Set[str]:
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
    Evaluate a minimal principal policy:

      { "effect": "ALLOW"|"DENY", "principals": ["user:alice", "role:DomainModeler"] }
    """
    if not isinstance(policy, dict) or not policy:
        return True

    effect = str(policy.get("effect") or "ALLOW").strip().upper()
    if effect not in {"ALLOW", "DENY"}:
        effect = "ALLOW"

    def _normalize_policy_principals(raw: Any) -> Set[str]:
        values: list[str] = []
        if isinstance(raw, str):
            values = [part.strip() for part in raw.split(",")]
        elif isinstance(raw, list):
            values = [str(part).strip() for part in raw]
        else:
            return set()

        normalized: Set[str] = set()
        for value in values:
            if not value:
                continue
            if ":" in value:
                prefix, remainder = value.split(":", 1)
                pfx = str(prefix).strip().lower()
                rem = str(remainder).strip()
                if pfx in _SUPPORTED_PRINCIPAL_PREFIXES and rem:
                    normalized.add(f"{pfx}:{rem}")
        return normalized

    principal_set: Set[str] = set()
    principal_set.update(_normalize_policy_principals(policy.get("principals")))

    if any(
        policy.get(key) not in (None, "", [], {})
        for key in ("roles", "users", "groups", "services", "scopes", "rules", "policy")
    ):
        # Runtime supports principal/tag-based policy only.
        return False

    if not principal_set:
        return effect != "ALLOW"

    matches = bool(principal_tags & principal_set)
    if effect == "DENY":
        return not matches
    return matches
