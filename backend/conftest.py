from __future__ import annotations

import os
from pathlib import Path

from shared.config.settings import get_settings, reload_settings
from shared.utils.repo_dotenv import load_repo_dotenv

def _ensure_test_env() -> None:
    if not Path("/.dockerenv").exists():
        docker_env = str(os.environ.get("DOCKER_CONTAINER") or "").strip().lower()
        if docker_env in {"1", "true", "yes", "on"}:
            os.environ["DOCKER_CONTAINER"] = "false"
        else:
            os.environ.setdefault("DOCKER_CONTAINER", "false")
    candidate_keys = (
        "ADMIN_TOKEN",
        "BFF_ADMIN_TOKEN",
        "OMS_ADMIN_TOKEN",
        "SMOKE_ADMIN_TOKEN",
        "ADMIN_API_KEY",
    )
    dotenv = load_repo_dotenv(keys=candidate_keys)
    token = None
    for key in candidate_keys:
        value = str(os.environ.get(key, "")).strip()
        if value:
            token = value
            break
    if not token:
        for key in candidate_keys:
            value = str(dotenv.get(key, "")).strip()
            if value:
                token = value
                break
    if not token:
        token = "test-token"

    for key in ("ADMIN_TOKEN", "BFF_ADMIN_TOKEN", "OMS_ADMIN_TOKEN", "SMOKE_ADMIN_TOKEN"):
        if str(os.environ.get(key, "")).strip():
            continue
        value = str(dotenv.get(key, "")).strip()
        os.environ[key] = value or token

    os.environ.setdefault("RUN_LIVE_OMS_SMOKE", "true")
    os.environ.setdefault("RUN_LIVE_BRANCH_VIRTUALIZATION", "true")

    # `shared.config.settings` instantiates a module-level settings object at import
    # time, so we must reload it after normalizing DOCKER_CONTAINER on host runs.
    reload_settings()
    settings = get_settings()

    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", settings.database.kafka_servers)
    os.environ.setdefault("POSTGRES_URL", settings.database.postgres_url)
    os.environ.setdefault("REDIS_HOST", settings.database.redis_host)
    os.environ.setdefault("REDIS_PORT", str(settings.database.redis_port))
    os.environ.setdefault("REDIS_URL", settings.database.redis_url)
    os.environ.setdefault("MINIO_ENDPOINT_URL", settings.storage.minio_endpoint_url)
    os.environ.setdefault("MINIO_ACCESS_KEY", settings.storage.minio_access_key)
    os.environ.setdefault("MINIO_SECRET_KEY", settings.storage.minio_secret_key)
    os.environ.setdefault("ELASTICSEARCH_HOST", settings.database.elasticsearch_host)
    os.environ.setdefault("ELASTICSEARCH_PORT", str(settings.database.elasticsearch_port))

    lakefs_port = str(settings.storage.lakefs_api_port)
    os.environ.setdefault("LAKEFS_API_PORT", lakefs_port)
    os.environ.setdefault("LAKEFS_PORT_HOST", lakefs_port)
    if settings.storage.lakefs_access_key_id:
        os.environ.setdefault("LAKEFS_INSTALLATION_ACCESS_KEY_ID", settings.storage.lakefs_access_key_id)
    if settings.storage.lakefs_secret_access_key:
        os.environ.setdefault("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY", settings.storage.lakefs_secret_access_key)

    if "ELASTICSEARCH_URL" not in os.environ:
        os.environ["ELASTICSEARCH_URL"] = settings.database.elasticsearch_url
    if "LAKEFS_API_URL" not in os.environ:
        os.environ["LAKEFS_API_URL"] = settings.storage.lakefs_api_url or f"http://127.0.0.1:{lakefs_port}"


_ensure_test_env()
