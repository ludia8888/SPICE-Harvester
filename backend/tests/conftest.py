from __future__ import annotations

import os
import socket
import sys
from pathlib import Path

import pytest

from shared.utils.repo_dotenv import load_repo_dotenv


def _load_repo_dotenv() -> dict[str, str]:
    return load_repo_dotenv()


def _ensure_repo_root_on_sys_path() -> None:
    """
    Ensure tests can import using either:
    - `mcp_servers.*` (backend-root PYTHONPATH style)
    - `backend.mcp_servers.*` (repo-root package style)
    """

    repo_root = Path(__file__).resolve().parents[2]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


def _env_or_dotenv(dotenv: dict[str, str], key: str, default: str) -> str:
    return (os.getenv(key) or dotenv.get(key) or default).strip()


def _configure_host_run_defaults() -> None:
    """
    Host-run integration defaults for the `backend/tests` suite.

    This test suite has its own `pytest.ini`, so repo-level/`backend/` conftest
    files are not loaded when running individual tests. Normalize env defaults
    here so "run one test file" behaves the same as the full suite run.
    """

    if os.path.exists("/.dockerenv"):
        return

    _ensure_repo_root_on_sys_path()

    docker_env = (os.getenv("DOCKER_CONTAINER") or "").strip().lower()
    if docker_env in {"1", "true", "yes", "on"}:
        os.environ["DOCKER_CONTAINER"] = "false"

    dotenv = _load_repo_dotenv()
    es_host = _env_or_dotenv(dotenv, "ELASTICSEARCH_HOST", "127.0.0.1")
    es_port = _env_or_dotenv(dotenv, "ELASTICSEARCH_PORT", _env_or_dotenv(dotenv, "ELASTICSEARCH_PORT_HOST", "9200"))
    os.environ.setdefault("ELASTICSEARCH_HOST", es_host)
    os.environ.setdefault("ELASTICSEARCH_PORT", es_port)
    os.environ.setdefault("ELASTICSEARCH_URL", f"http://{es_host}:{es_port}")
    kafka_port = _env_or_dotenv(dotenv, "KAFKA_PORT_HOST", "39092")
    # Use docker-compose.yml defaults for local dev infrastructure
    postgres_port = _env_or_dotenv(dotenv, "POSTGRES_PORT_HOST", "5433")
    redis_port = _env_or_dotenv(dotenv, "REDIS_PORT_HOST", "6379")
    minio_port = _env_or_dotenv(dotenv, "MINIO_PORT_HOST", "9000")
    lakefs_port = _env_or_dotenv(dotenv, "LAKEFS_PORT_HOST", "48080")

    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", f"127.0.0.1:{kafka_port}")
    os.environ.setdefault(
        "POSTGRES_URL",
        f"postgresql://spiceadmin:spicepass123@localhost:{postgres_port}/spicedb",
    )
    os.environ.setdefault("REDIS_URL", f"redis://:spicepass123@localhost:{redis_port}/0")
    os.environ.setdefault("REDIS_HOST", "localhost")
    os.environ.setdefault("REDIS_PORT", redis_port)
    os.environ.setdefault("MINIO_ENDPOINT_URL", f"http://localhost:{minio_port}")
    os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")
    os.environ.setdefault("LAKEFS_PORT_HOST", lakefs_port)
    os.environ.setdefault("LAKEFS_API_PORT", lakefs_port)

    # Never pull LakeFS credentials from the repo `.env` (frequently contains unrelated AWS keys).
    os.environ.setdefault("LAKEFS_ACCESS_KEY_ID", "spice-lakefs-admin")
    os.environ.setdefault("LAKEFS_SECRET_ACCESS_KEY", "spice-lakefs-admin-secret")
    os.environ.setdefault("LAKEFS_INSTALLATION_ACCESS_KEY_ID", os.environ["LAKEFS_ACCESS_KEY_ID"])
    os.environ.setdefault("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY", os.environ["LAKEFS_SECRET_ACCESS_KEY"])
    if "LAKEFS_API_URL" not in os.environ:
        os.environ["LAKEFS_API_URL"] = f"http://127.0.0.1:{lakefs_port}"
    os.environ.setdefault("PIPELINE_JOB_QUEUE_FLUSH_TIMEOUT_SECONDS", "20")


_configure_host_run_defaults()


def pytest_configure() -> None:
    _configure_host_run_defaults()


def _is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config, items) -> None:  # type: ignore[no-untyped-def]
    """Auto-skip ``requires_infra`` tests when local services are not reachable."""
    postgres_port = int(os.environ.get("POSTGRES_PORT_HOST", "5433"))
    infra_available = _is_port_open("localhost", postgres_port)
    if infra_available:
        return
    skip_marker = pytest.mark.skip(reason="local infrastructure not available (requires_infra)")
    for item in items:
        if item.get_closest_marker("requires_infra"):
            item.add_marker(skip_marker)
