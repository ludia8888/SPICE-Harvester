from __future__ import annotations

import os
from pathlib import Path


def _load_repo_dotenv() -> dict[str, str]:
    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / ".env"
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


def _env_or_dotenv(dotenv: dict[str, str], key: str, default: str) -> str:
    return (os.getenv(key) or dotenv.get(key) or default).strip()


def _ensure_test_env() -> None:
    dotenv = _load_repo_dotenv()
    if not Path("/.dockerenv").exists():
        os.environ.setdefault("DOCKER_CONTAINER", "false")
    candidate_keys = (
        "ADMIN_TOKEN",
        "BFF_ADMIN_TOKEN",
        "OMS_ADMIN_TOKEN",
        "SMOKE_ADMIN_TOKEN",
        "ADMIN_API_KEY",
    )
    token = None
    for key in candidate_keys:
        value = (os.getenv(key) or "").strip()
        if value:
            token = value
            break
    if not token:
        token = "test-token"

    for key in ("ADMIN_TOKEN", "BFF_ADMIN_TOKEN", "OMS_ADMIN_TOKEN", "SMOKE_ADMIN_TOKEN"):
        os.environ.setdefault(key, token)

    os.environ.setdefault("RUN_LIVE_OMS_SMOKE", "true")
    os.environ.setdefault("RUN_LIVE_BRANCH_VIRTUALIZATION", "true")

    kafka_port = _env_or_dotenv(dotenv, "KAFKA_PORT_HOST", "39092")
    postgres_port = _env_or_dotenv(dotenv, "POSTGRES_PORT_HOST", "55433")
    redis_port = _env_or_dotenv(dotenv, "REDIS_PORT_HOST", "6380")
    minio_port = _env_or_dotenv(dotenv, "MINIO_PORT_HOST", "9002")
    es_host = _env_or_dotenv(dotenv, "ELASTICSEARCH_HOST", "localhost")
    es_port = _env_or_dotenv(dotenv, "ELASTICSEARCH_PORT", _env_or_dotenv(dotenv, "ELASTICSEARCH_PORT_HOST", "9200"))
    lakefs_port = _env_or_dotenv(dotenv, "LAKEFS_PORT_HOST", "48080")
    lakefs_access = _env_or_dotenv(dotenv, "LAKEFS_ACCESS_KEY_ID", "")
    lakefs_secret = _env_or_dotenv(dotenv, "LAKEFS_SECRET_ACCESS_KEY", "")

    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", f"127.0.0.1:{kafka_port}")
    os.environ.setdefault(
        "POSTGRES_URL",
        f"postgresql://spiceadmin:spicepass123@localhost:{postgres_port}/spicedb",
    )
    os.environ.setdefault("REDIS_HOST", "localhost")
    os.environ.setdefault("REDIS_PORT", redis_port)
    os.environ.setdefault("REDIS_URL", f"redis://:spicepass123@localhost:{redis_port}/0")
    os.environ.setdefault("MINIO_ENDPOINT_URL", f"http://localhost:{minio_port}")
    os.environ.setdefault("ELASTICSEARCH_HOST", es_host)
    os.environ.setdefault("ELASTICSEARCH_PORT", es_port)
    os.environ.setdefault("LAKEFS_PORT_HOST", lakefs_port)
    os.environ.setdefault("LAKEFS_API_PORT", lakefs_port)
    if lakefs_access:
        os.environ.setdefault("LAKEFS_INSTALLATION_ACCESS_KEY_ID", lakefs_access)
    if lakefs_secret:
        os.environ.setdefault("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY", lakefs_secret)
    if "ELASTICSEARCH_URL" not in os.environ:
        os.environ["ELASTICSEARCH_URL"] = f"http://{es_host}:{es_port}"
    if "LAKEFS_API_URL" not in os.environ:
        os.environ["LAKEFS_API_URL"] = f"http://127.0.0.1:{lakefs_port}"
    os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")


_ensure_test_env()
