from __future__ import annotations

import logging
from typing import Iterable, Optional

from shared.config.settings import ApplicationSettings, get_settings

logger = logging.getLogger(__name__)

_SENSITIVE_TOKEN_FIELDS: tuple[tuple[str, str], ...] = (
    ("ADMIN_TOKEN", "admin_token"),
    ("ADMIN_API_KEY", "admin_api_key"),
    ("BFF_ADMIN_TOKEN", "bff_admin_token"),
    ("BFF_WRITE_TOKEN", "bff_write_token"),
    ("BFF_AGENT_TOKEN", "bff_agent_token"),
    ("OMS_ADMIN_TOKEN", "oms_admin_token"),
    ("OMS_WRITE_TOKEN", "oms_write_token"),
    ("USER_JWT_HS256_SECRET", "user_jwt_hs256_secret"),
)

_WEAK_EXACT_VALUES = {
    "",
    "change_me",
    "changeme",
    "secret",
    "default",
    "token",
    "admin",
    "password",
    "ci_admin_token",
    "spice-dev-agent-token",
    "spice-dev-user-jwt-secret",
}

_WEAK_PREFIXES = (
    "spice-dev-",
    "example",
    "dummy",
)

_MIN_SECRET_LENGTH = 12


def _split_secret_values(raw: Optional[str]) -> tuple[str, ...]:
    value = str(raw or "").strip()
    if not value:
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _is_weak_secret(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return True
    if normalized in _WEAK_EXACT_VALUES:
        return True
    if any(normalized.startswith(prefix) for prefix in _WEAK_PREFIXES):
        return True
    if len(normalized) < _MIN_SECRET_LENGTH:
        return True
    return False


def _collect_weak_secret_issues(settings: ApplicationSettings) -> list[str]:
    auth = settings.auth
    issues: list[str] = []
    for env_key, field_name in _SENSITIVE_TOKEN_FIELDS:
        raw = getattr(auth, field_name, None)
        for token in _split_secret_values(raw):
            if _is_weak_secret(token):
                issues.append(env_key)
                break

    agent_bff_tokens = _split_secret_values(settings.agent.bff_token)
    if agent_bff_tokens and any(_is_weak_secret(token) for token in agent_bff_tokens):
        issues.append("AGENT_BFF_TOKEN")

    deduped = sorted(set(issues))
    return deduped


def _format_issue_message(*, service_name: str, issues: Iterable[str]) -> str:
    keys = ", ".join(sorted(set(str(item) for item in issues if str(item).strip())))
    return (
        f"{service_name} startup blocked: weak/default secrets detected for [{keys}]. "
        "In production, set strong non-default credentials/tokens before startup."
    )


def ensure_startup_security(service_name: str, settings: Optional[ApplicationSettings] = None) -> None:
    cfg = settings or get_settings()
    issues = _collect_weak_secret_issues(cfg)
    if not issues:
        return

    if cfg.is_production:
        raise RuntimeError(_format_issue_message(service_name=service_name, issues=issues))

    logger.warning(
        "%s startup security warning (non-production): weak/default secrets detected for %s",
        service_name,
        ", ".join(issues),
    )
