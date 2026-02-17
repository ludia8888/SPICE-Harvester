from __future__ import annotations

import re
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _extract_service_block(compose_yaml: str, service_name: str) -> str:
    pattern = rf"(?ms)^  {re.escape(service_name)}:\n(.*?)(?=^  [a-zA-Z0-9_-]+:|\Z)"
    match = re.search(pattern, compose_yaml)
    if not match:
        raise AssertionError(f"Service block not found: {service_name}")
    return match.group(1)


def test_gateway_does_not_expose_funnel_public_routes() -> None:
    root = _repo_root()
    prod_nginx = (root / "backend" / "nginx.conf").read_text(encoding="utf-8")
    dev_nginx = (root / "backend" / "nginx-dev.conf").read_text(encoding="utf-8")

    assert "/api/funnel/" not in prod_nginx
    assert "/health/funnel" not in prod_nginx
    assert "upstream funnel_backend" not in prod_nginx

    assert "location /funnel/" not in dev_nginx
    assert "upstream funnel_backend" not in dev_nginx


def test_default_compose_does_not_publish_funnel_host_port() -> None:
    root = _repo_root()
    compose = (root / "backend" / "docker-compose.yml").read_text(encoding="utf-8")
    assert "\n  funnel:\n" not in compose
    assert "${FUNNEL_PORT_HOST:-8003}:8003" not in compose
    assert "funnel:8003" not in compose


def test_full_compose_bff_does_not_depend_on_funnel_service() -> None:
    root = _repo_root()
    compose = (root / "docker-compose.full.yml").read_text(encoding="utf-8")
    bff_block = _extract_service_block(compose, "bff")

    assert "\n      - funnel\n" not in bff_block
    assert "\n  funnel:\n" not in compose


def test_full_compose_does_not_enable_legacy_pull_request_surface() -> None:
    root = _repo_root()
    compose = (root / "docker-compose.full.yml").read_text(encoding="utf-8")
    assert "ENABLE_PULL_REQUESTS" not in compose


def test_settings_do_not_expose_legacy_pull_request_feature_flag() -> None:
    root = _repo_root()
    settings_text = (root / "backend" / "shared" / "config" / "settings.py").read_text(encoding="utf-8")
    assert "enable_pull_requests" not in settings_text
    assert "ENABLE_PULL_REQUESTS" not in settings_text


def test_funnel_service_absent_in_compose_variants() -> None:
    root = _repo_root()
    https_compose = (root / "backend" / "docker-compose-https.yml").read_text(encoding="utf-8")
    assert "\n  funnel:\n" not in https_compose
    assert "funnel:8003" not in https_compose


def test_prometheus_default_targets_do_not_assume_funnel_service() -> None:
    root = _repo_root()
    prometheus = (root / "backend" / "monitoring" / "prometheus.yml").read_text(encoding="utf-8")
    assert '"funnel:8003"' not in prometheus


def test_debug_ports_overlay_does_not_expose_funnel_port() -> None:
    root = _repo_root()
    debug_overlay = (root / "backend" / "docker-compose.debug-ports.yml").read_text(encoding="utf-8")
    assert "\n  funnel:\n" not in debug_overlay
    assert "8003:8003" not in debug_overlay


def test_deploy_scripts_do_not_reference_external_funnel_health() -> None:
    root = _repo_root()
    deploy_sh = (root / "backend" / "deploy.sh").read_text(encoding="utf-8")
    production_tests_sh = (root / "backend" / "run_production_tests.sh").read_text(encoding="utf-8")

    assert "FUNNEL_RUNTIME_MODE" not in deploy_sh
    assert 'wait_for_url "Funnel"' not in deploy_sh
    assert "FUNNEL_URL" not in deploy_sh
    assert "FUNNEL_RUNTIME_MODE" not in production_tests_sh
    assert 'wait_for_url "Funnel"' not in production_tests_sh
    assert "FUNNEL_URL" not in production_tests_sh


def test_ssl_cert_generator_does_not_assume_external_funnel_service() -> None:
    root = _repo_root()
    ssl_script = (root / "backend" / "generate_ssl_certs.sh").read_text(encoding="utf-8")

    assert 'SERVICES=("oms" "bff" "funnel")' not in ssl_script
    assert "DNS.5 = funnel" not in ssl_script
    assert "- **Funnel**:" not in ssl_script


def test_local_start_script_does_not_start_funnel_process() -> None:
    root = _repo_root()
    start_services_py = (root / "backend" / "scripts" / "start_services.py").read_text(encoding="utf-8")
    assert "uvicorn main:app --host 0.0.0.0 --port 8003" not in start_services_py
    assert "Funnel process startup" not in start_services_py
    assert "Funnel runtime is internal (in-process)" in start_services_py


def test_env_templates_do_not_expose_external_funnel_toggles() -> None:
    root = _repo_root()
    repo_env = (root / ".env.example").read_text(encoding="utf-8")
    backend_env = (root / "backend" / ".env.example").read_text(encoding="utf-8")

    assert "FUNNEL_BASE_URL" not in repo_env
    assert "FUNNEL_RUNTIME_MODE" not in repo_env
    assert "FUNNEL_PORT" not in backend_env
    assert "FUNNEL_RUNTIME_MODE" not in backend_env
