from __future__ import annotations

from pathlib import Path

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def test_secure_default_compose_hides_internal_and_data_plane_ports() -> None:
    root = _repo_root()
    base_compose = _load_yaml(root / "backend" / "docker-compose.yml")
    services = base_compose.get("services") or {}

    internal_services = {
        "postgres",
        "redis",
        "elasticsearch",
        "kafka",
        "zookeeper",
        "minio",
        "lakefs",
        "oms",
        "agent",
        "ingest-reconciler-worker",
    }
    for service_name in sorted(internal_services):
        service = services.get(service_name)
        assert isinstance(service, dict), f"missing service in compose: {service_name}"
        assert "ports" not in service, f"{service_name} must not publish host ports in secure-default compose"

    bff_service = services.get("bff") or {}
    assert "ports" in bff_service, "BFF must remain the only public entrypoint"


def test_full_compose_does_not_republish_oms_port() -> None:
    root = _repo_root()
    full_compose = _load_yaml(root / "docker-compose.full.yml")
    services = full_compose.get("services") or {}
    internal_services = {
        "postgres",
        "redis",
        "elasticsearch",
        "kafka",
        "zookeeper",
        "minio",
        "lakefs",
        "oms",
        "agent",
        "ingest-reconciler-worker",
    }
    for service_name in sorted(internal_services):
        service = services.get(service_name)
        assert isinstance(service, dict), f"missing service in full compose: {service_name}"
        assert "ports" not in service, f"{service_name} must stay internal in docker-compose.full.yml"


def test_debug_overlay_explicitly_exposes_required_gate_ports() -> None:
    root = _repo_root()
    debug_overlay = _load_yaml(root / "backend" / "docker-compose.debug-ports.yml")
    services = debug_overlay.get("services") or {}

    required = {
        "postgres",
        "redis",
        "elasticsearch",
        "kafka",
        "zookeeper",
        "minio",
        "lakefs",
        "oms",
        "agent",
        "ingest-reconciler-worker",
    }
    for service_name in sorted(required):
        service = services.get(service_name) or {}
        assert "ports" in service, f"{service_name} must be exposed in debug overlay for Gate workflows"
