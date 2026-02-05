from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional


def load_repo_dotenv(*, keys: Optional[Iterable[str]] = None) -> dict[str, str]:
    """
    Load key/value pairs from the repo-root `.env`.

    Notes
    - This is a dev/test helper only. Production settings are controlled via real
      environment variables (see `shared.config.settings`), and `.env` loading is
      intentionally opt-in for security.
    - `keys` can be used to restrict which variables are parsed/returned.
    """

    env_path = Path(__file__).resolve().parents[3] / ".env"
    if not env_path.exists():
        return {}

    allowlist = {str(k).strip() for k in keys} if keys is not None else None
    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if allowlist is not None and key not in allowlist:
            continue
        parsed = value.strip().strip("'").strip('"')
        values[key] = parsed

    return values

