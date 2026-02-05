from __future__ import annotations

import os

from shared.utils.repo_dotenv import load_repo_dotenv


def _load_repo_dotenv() -> dict[str, str]:
    return load_repo_dotenv()


def _env_or_dotenv(dotenv: dict[str, str], key: str, default: str) -> str:
    return (os.getenv(key) or dotenv.get(key) or default).strip()


def pytest_configure() -> None:
    """
    Host-run integration defaults for the `backend/tests` suite.

    This test suite has its own `pytest.ini`, so repo-level/`backend/` conftest
    files are not loaded when running individual tests. Normalize env defaults
    here so "run one test file" behaves the same as the full suite run.
    """

    if os.path.exists("/.dockerenv"):
        return

    docker_env = (os.getenv("DOCKER_CONTAINER") or "").strip().lower()
    if docker_env in {"1", "true", "yes", "on"}:
        os.environ["DOCKER_CONTAINER"] = "false"

    dotenv = _load_repo_dotenv()
    if "ELASTICSEARCH_PORT" not in os.environ:
        os.environ["ELASTICSEARCH_PORT"] = os.getenv("ELASTICSEARCH_PORT_HOST", "9200")
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
