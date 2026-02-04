"""S3 client configuration helpers (shared).

Centralizes S3 addressing-style heuristics so services don't duplicate the same
endpoint hostname checks.
"""

from __future__ import annotations

from typing import Optional, Set
from urllib.parse import urlparse

try:
    from botocore.config import Config
except ImportError:  # pragma: no cover
    Config = None  # type: ignore[assignment]


_DEFAULT_PATH_STYLE_HOSTS: Set[str] = {
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "minio",
    "spice-minio",
    "spice_minio",
}


def build_s3_client_config(
    endpoint_url: str,
    *,
    addressing_style: Optional[str] = None,
    extra_path_style_hosts: Optional[Set[str]] = None,
):
    """Return a botocore Config for S3 addressing style, or None when not needed."""
    if Config is None:
        return None

    style = str(addressing_style or "").strip().lower()
    if style in {"path", "virtual"}:
        return Config(s3={"addressing_style": style})

    host = (urlparse(str(endpoint_url or "")).hostname or "").lower()
    hosts = set(_DEFAULT_PATH_STYLE_HOSTS)
    if extra_path_style_hosts:
        hosts.update({str(item).lower() for item in extra_path_style_hosts if item})
    use_path_style = host in hosts or host.endswith(".localhost")
    return Config(s3={"addressing_style": "path"}) if use_path_style else None

