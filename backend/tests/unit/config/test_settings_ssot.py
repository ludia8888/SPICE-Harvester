from __future__ import annotations

import pytest

from shared.config.settings import (
    AgentRuntimeSettings,
    AuthSettings,
    ClientSettings,
    DatabaseSettings,
    FeatureFlagsSettings,
    ObjectifySettings,
    PipelineSettings,
    ServiceSettings,
    StorageSettings,
)


def _disable_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # settings.py reads `.env` only when SPICE_LOAD_DOTENV=true and not in Docker;
    # force-disable dotenv reads for deterministic unit tests.
    monkeypatch.setenv("DOCKER_CONTAINER", "1")


def test_pipeline_publish_lock_timeout_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.delenv("PIPELINE_PUBLISH_LOCK_ACQUIRE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS", raising=False)

    defaults = PipelineSettings()
    assert defaults.publish_lock_acquire_timeout_seconds == 30

    monkeypatch.setenv("PIPELINE_LOCK_ACQUIRE_TIMEOUT_SECONDS", "123")
    legacy = PipelineSettings()
    assert legacy.publish_lock_acquire_timeout_seconds == 123

    monkeypatch.setenv("PIPELINE_PUBLISH_LOCK_ACQUIRE_TIMEOUT_SECONDS", "10")
    explicit = PipelineSettings()
    assert explicit.publish_lock_acquire_timeout_seconds == 10


def test_pipeline_spark_driver_memory_default_and_blank(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.delenv("PIPELINE_SPARK_DRIVER_MEMORY", raising=False)

    defaults = PipelineSettings()
    assert defaults.spark_driver_memory == "512m"

    monkeypatch.setenv("PIPELINE_SPARK_DRIVER_MEMORY", "   ")
    blank = PipelineSettings()
    assert blank.spark_driver_memory == "512m"

    monkeypatch.setenv("PIPELINE_SPARK_DRIVER_MEMORY", "384m")
    explicit = PipelineSettings()
    assert explicit.spark_driver_memory == "384m"


def test_agent_bff_token_and_command_timeout_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.delenv("AGENT_BFF_TOKEN", raising=False)
    monkeypatch.setenv("BFF_AGENT_TOKEN", "abc123")

    settings = AgentRuntimeSettings()
    assert settings.bff_token == "abc123"

    monkeypatch.delenv("AGENT_COMMAND_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("PIPELINE_RUN_TIMEOUT_SECONDS", "42")
    settings = AgentRuntimeSettings()
    assert settings.command_timeout_seconds == 42.0

    monkeypatch.setenv("AGENT_COMMAND_TIMEOUT_SECONDS", "13")
    settings = AgentRuntimeSettings()
    assert settings.command_timeout_seconds == 13.0


def test_auth_settings_agent_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.delenv("AGENT_REQUIRE_AUTH", raising=False)
    monkeypatch.delenv("AGENT_AUTH_EXEMPT_PATHS", raising=False)
    auth = AuthSettings()
    assert auth.is_agent_auth_required(default_required=True) is True
    assert auth.resolve_agent_exempt_paths(defaults=("/health",)) == {"/health"}

    monkeypatch.setenv("AGENT_REQUIRE_AUTH", "false")
    monkeypatch.setenv("AGENT_AUTH_EXEMPT_PATHS", "/health,/metrics")
    auth = AuthSettings()
    assert auth.is_agent_auth_required(default_required=True) is False
    assert auth.resolve_agent_exempt_paths(defaults=("/health",)) == {"/health", "/metrics"}


def test_client_token_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.delenv("OMS_CLIENT_TOKEN", raising=False)
    monkeypatch.delenv("OMS_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    monkeypatch.setenv("ADMIN_API_KEY", "adm")

    settings = ClientSettings()
    assert settings.oms_client_token == "adm"

    monkeypatch.delenv("BFF_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("BFF_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setenv("ADMIN_TOKEN", "bffadm")

    settings = ClientSettings()
    assert settings.bff_admin_token == "bffadm"


def test_oms_grpc_service_settings_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.setenv("OMS_GRPC_ENABLED", "false")
    monkeypatch.setenv("OMS_GRPC_BIND_HOST", "127.0.0.9")
    monkeypatch.setenv("OMS_GRPC_SERVER_USE_TLS", "true")
    monkeypatch.setenv("OMS_GRPC_REQUIRE_MTLS", "true")
    monkeypatch.setenv("OMS_GRPC_CLIENT_CA_PATH", "/tmp/client-ca.pem")
    monkeypatch.setenv("OMS_GRPC_CLIENT_CERT_PATH", "/tmp/client.crt")
    monkeypatch.setenv("OMS_GRPC_CLIENT_KEY_PATH", "/tmp/client.key")
    monkeypatch.setenv("OMS_GRPC_SERVER_CERT_PATH", "/tmp/server.crt")
    monkeypatch.setenv("OMS_GRPC_SERVER_KEY_PATH", "/tmp/server.key")
    monkeypatch.setenv("OMS_GRPC_SERVER_CA_PATH", "/tmp/server-ca.pem")

    services = ServiceSettings()
    assert services.oms_grpc_enabled is False
    assert services.oms_grpc_bind_host == "127.0.0.9"
    assert services.oms_grpc_server_use_tls is True
    assert services.oms_grpc_require_mtls is True
    assert services.oms_grpc_client_ca_path == "/tmp/client-ca.pem"
    assert services.oms_grpc_client_cert_path == "/tmp/client.crt"
    assert services.oms_grpc_client_key_path == "/tmp/client.key"
    assert services.oms_grpc_server_cert_path == "/tmp/server.crt"
    assert services.oms_grpc_server_key_path == "/tmp/server.key"
    assert services.oms_grpc_server_ca_path == "/tmp/server-ca.pem"


def test_auth_settings_oms_grpc_service_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.setenv("OMS_GRPC_SERVICE_TOKEN", "svc-client")
    monkeypatch.setenv("OMS_GRPC_SERVICE_TOKENS", "svc-a,svc-b")

    auth = AuthSettings()
    assert auth.oms_grpc_service_token_effective == "svc-client"
    assert auth.oms_grpc_expected_service_tokens == ("svc-a", "svc-b")


def test_lakefs_repository_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    settings = StorageSettings()
    assert settings.lakefs_raw_repository == "raw-datasets"
    assert settings.lakefs_artifacts_repository == "pipeline-artifacts"


def test_local_port_host_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate local (non-Docker) scripts relying on docker-compose port overrides.
    monkeypatch.setenv("DOCKER_CONTAINER", "false")

    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    monkeypatch.setenv("POSTGRES_PORT_HOST", "55433")
    assert DatabaseSettings().postgres_port == 55433

    monkeypatch.delenv("ELASTICSEARCH_PORT", raising=False)
    monkeypatch.setenv("ELASTICSEARCH_PORT_HOST", "19200")
    assert DatabaseSettings().elasticsearch_port == 19200

    monkeypatch.delenv("REDIS_PORT", raising=False)
    monkeypatch.setenv("REDIS_PORT_HOST", "6380")
    assert DatabaseSettings().redis_port == 6380

    monkeypatch.delenv("MINIO_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("MINIO_PORT_HOST", "9002")
    assert StorageSettings().minio_endpoint_url == "http://127.0.0.1:9002"


def test_objectify_dataset_primary_chunk_size_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.delenv("OBJECTIFY_DATASET_PRIMARY_INDEX_CHUNK_SIZE", raising=False)
    settings = ObjectifySettings()
    assert settings.dataset_primary_index_chunk_size == 500


def test_objectify_dataset_primary_chunk_size_clamps_lower_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    monkeypatch.setenv("OBJECTIFY_DATASET_PRIMARY_INDEX_CHUNK_SIZE", "0")
    settings = ObjectifySettings()
    assert settings.dataset_primary_index_chunk_size == 1


def test_feature_flags_no_strict_compat_toggles(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    flags = FeatureFlagsSettings()
    assert not hasattr(flags, "enable_foundry_v2_strict_compat")
    assert not hasattr(flags, "foundry_v2_strict_compat_db_allowlist")


def test_service_settings_no_external_funnel_address_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    services = ServiceSettings()
    assert not hasattr(services, "funnel_host")
    assert not hasattr(services, "funnel_port")
    assert not hasattr(services, "funnel_base_url_override")
    assert not hasattr(services, "funnel_base_url")
