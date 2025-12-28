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


def _ensure_test_tokens() -> None:
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
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
    os.environ.setdefault("POSTGRES_URL", "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb")
    os.environ.setdefault("REDIS_URL", "redis://:spicepass123@localhost:6380/0")
    os.environ.setdefault("MINIO_ENDPOINT_URL", "http://localhost:9002")
    os.environ.setdefault("ELASTICSEARCH_URL", "http://localhost:9200")
    os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")


_ensure_test_tokens()
