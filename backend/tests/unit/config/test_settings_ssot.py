from __future__ import annotations

import pytest

from shared.config.settings import AgentRuntimeSettings, ClientSettings, PipelineSettings, StorageSettings


def _disable_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # settings.py loads `.env` unless DOCKER_CONTAINER is set.
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


def test_lakefs_repository_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)
    settings = StorageSettings()
    assert settings.lakefs_raw_repository == "raw-datasets"
    assert settings.lakefs_artifacts_repository == "pipeline-artifacts"
