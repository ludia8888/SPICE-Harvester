#!/usr/bin/env python3
"""
Dev-only cleanup wrapper for test/smoke TerminusDB databases.

Runs shared.tools.registry_cleanup with repo-local .env hydration.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "backend"))


def _load_repo_dotenv() -> dict[str, str]:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def _hydrate_env_from_dotenv() -> None:
    dotenv = _load_repo_dotenv()
    for key in ("ADMIN_TOKEN", "BFF_ADMIN_TOKEN", "BFF_WRITE_TOKEN", "ADMIN_API_KEY", "ENVIRONMENT"):
        if key not in os.environ and key in dotenv:
            os.environ[key] = dotenv[key]


if __name__ == "__main__":
    _hydrate_env_from_dotenv()
    from shared.tools.registry_cleanup import main  # noqa: E402

    raise SystemExit(main())
