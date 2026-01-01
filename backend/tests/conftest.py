from __future__ import annotations

import os


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

    if "ELASTICSEARCH_PORT" not in os.environ:
        os.environ["ELASTICSEARCH_PORT"] = os.getenv("ELASTICSEARCH_PORT_HOST", "9200")
    kafka_port = os.getenv("KAFKA_PORT_HOST", "39092").strip()
    postgres_port = os.getenv("POSTGRES_PORT_HOST", "55433").strip()
    redis_port = os.getenv("REDIS_PORT_HOST", "6380").strip()

    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", f"localhost:{kafka_port}")
    os.environ.setdefault(
        "POSTGRES_URL",
        f"postgresql://spiceadmin:spicepass123@localhost:{postgres_port}/spicedb",
    )
    os.environ.setdefault("REDIS_URL", f"redis://:spicepass123@localhost:{redis_port}/0")
    os.environ.setdefault("REDIS_HOST", "localhost")
    os.environ.setdefault("REDIS_PORT", redis_port)
    # MinIO host port is intentionally not 9000 to avoid clashing with any local MinIO.
    os.environ.setdefault("MINIO_ENDPOINT_URL", "http://localhost:9002")
    os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")
