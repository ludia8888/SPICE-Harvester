from __future__ import annotations

import os
from typing import Any, Optional

from shared.utils.bool_utils import parse_boolish


def _is_docker_environment() -> bool:
    parsed = parse_boolish(os.getenv("DOCKER_CONTAINER"))
    if parsed is not None:
        return parsed
    return os.path.exists("/.dockerenv")


def _clamp_int(raw: Any, *, default: int, min_value: int = 0, max_value: int = 1_000_000) -> int:
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(min_value, min(max_value, value))


def _parse_boolish(raw: Any) -> Optional[bool]:
    return parse_boolish(raw)


def _strip_optional_text(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _strip_text_if_not_none(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    return str(raw).strip()


def _normalize_base_branch(raw: Any) -> str:
    return str(raw or "").strip() or "main"


def _clamp_flush_timeout_seconds(raw: Any, *, default: float = 10.0) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    return max(0.1, min(value, 600.0))


def _env_truthy(name: str) -> bool:
    parsed = _parse_boolish(os.getenv(name))
    return parsed is True


def _should_load_dotenv() -> bool:
    if not _env_truthy("SPICE_LOAD_DOTENV"):
        return False
    if _is_docker_environment():
        return False
    return True


_ENV_FILE = ".env" if _should_load_dotenv() else None
