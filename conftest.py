from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "backend"

if BACKEND_DIR.exists():
    backend_path = str(BACKEND_DIR)
    if backend_path not in sys.path:
        sys.path.insert(0, backend_path)


def _load_repo_dotenv() -> dict[str, str]:
    env_path = ROOT_DIR / ".env"
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


def _ensure_test_tokens() -> None:
    if not Path("/.dockerenv").exists():
        os.environ.setdefault("DOCKER_CONTAINER", "false")

    dotenv = _load_repo_dotenv()
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
        if not (os.getenv(key) or "").strip():
            os.environ[key] = token

    os.environ.setdefault("RUN_LIVE_OMS_SMOKE", "true")
    os.environ.setdefault("RUN_LIVE_BRANCH_VIRTUALIZATION", "true")

    kafka_port = _env_or_dotenv(dotenv, "KAFKA_PORT_HOST", "39092")
    postgres_port = _env_or_dotenv(dotenv, "POSTGRES_PORT_HOST", "55433")
    redis_port = _env_or_dotenv(dotenv, "REDIS_PORT_HOST", "6380")
    minio_port = _env_or_dotenv(dotenv, "MINIO_PORT_HOST", "9002")
    es_host = _env_or_dotenv(dotenv, "ELASTICSEARCH_HOST", "localhost")
    es_port = _env_or_dotenv(dotenv, "ELASTICSEARCH_PORT", _env_or_dotenv(dotenv, "ELASTICSEARCH_PORT_HOST", "9200"))

    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", f"127.0.0.1:{kafka_port}")
    os.environ.setdefault(
        "POSTGRES_URL",
        f"postgresql://spiceadmin:spicepass123@localhost:{postgres_port}/spicedb",
    )
    os.environ.setdefault("REDIS_URL", f"redis://:spicepass123@localhost:{redis_port}/0")
    os.environ.setdefault("MINIO_ENDPOINT_URL", f"http://localhost:{minio_port}")
    if "ELASTICSEARCH_URL" not in os.environ:
        os.environ["ELASTICSEARCH_URL"] = f"http://{es_host}:{es_port}"
    os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")


_ensure_test_tokens()
