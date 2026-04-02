from __future__ import annotations

import ast
from pathlib import Path

import pytest

from shared.config.app_config import AppConfig


def _disable_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # settings.py reads `.env` only when SPICE_LOAD_DOTENV=true and not in Docker;
    # force-disable dotenv reads for deterministic unit tests.
    monkeypatch.setenv("DOCKER_CONTAINER", "1")


def _settings_ssot_allowed_files(repo_backend: Path) -> set[Path]:
    return {
        repo_backend / "shared" / "config" / "settings.py",
        repo_backend / "shared" / "config" / "settings_support.py",
        repo_backend / "shared" / "config" / "settings_agent.py",
        repo_backend / "shared" / "config" / "settings_infra.py",
        repo_backend / "shared" / "config" / "settings_observability.py",
        repo_backend / "shared" / "config" / "settings_security.py",
        repo_backend / "shared" / "config" / "settings_workers.py",
    }


def _iter_runtime_python_files(repo_backend: Path) -> list[Path]:
    roots: list[Path] = [
        repo_backend / "shared",
        repo_backend / "oms",
        repo_backend / "bff",
        repo_backend / "funnel",
        repo_backend / "agent",
        repo_backend / "message_relay",
        repo_backend / "data_connector",
        repo_backend / "monitoring",
        repo_backend / "perf",
        repo_backend / "scripts",
        repo_backend / "examples",
    ]

    for child in repo_backend.iterdir():
        if not child.is_dir():
            continue
        if child.name.endswith("_worker") or child.name.endswith("_service"):
            roots.append(child)

    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        files.extend(sorted(root.rglob("*.py")))

    # Include backend root-level scripts (but intentionally exclude backend/tests/**).
    files.extend(sorted(repo_backend.glob("*.py")))

    # Ensure stable output + no duplicates.
    return sorted(set(files))


def test_app_config_reflects_current_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)

    monkeypatch.setenv("INSTANCE_EVENTS_TOPIC", "instance_events_v1")
    assert AppConfig.INSTANCE_EVENTS_TOPIC == "instance_events_v1"

    monkeypatch.setenv("INSTANCE_EVENTS_TOPIC", "instance_events_v2")
    assert AppConfig.INSTANCE_EVENTS_TOPIC == "instance_events_v2"


def test_no_os_getenv_calls_outside_settings_module() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    allowed = _settings_ssot_allowed_files(repo_backend)

    offenders: list[str] = []
    for path in _iter_runtime_python_files(repo_backend):
        if path in allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr != "getenv":
                continue
            if isinstance(func.value, ast.Name) and func.value.id == "os":
                offenders.append(f"{path.relative_to(repo_backend)}:{node.lineno}")

    assert not offenders, "os.getenv() usage must live in settings SSoT only:\n" + "\n".join(sorted(offenders))


def test_no_application_settings_instantiation_outside_settings_module() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    allowed = _settings_ssot_allowed_files(repo_backend)

    offenders: list[str] = []
    for path in _iter_runtime_python_files(repo_backend):
        if path in allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Name) and func.id == "ApplicationSettings":
                offenders.append(f"{path.relative_to(repo_backend)}:{node.lineno}")
            elif isinstance(func, ast.Attribute) and func.attr == "ApplicationSettings":
                offenders.append(f"{path.relative_to(repo_backend)}:{node.lineno}")

    assert not offenders, "Instantiate settings via get_settings()/DI only:\n" + "\n".join(sorted(offenders))


def test_no_import_global_settings_symbol_outside_settings_module() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    allowed = _settings_ssot_allowed_files(repo_backend)

    offenders: list[str] = []
    for path in _iter_runtime_python_files(repo_backend):
        if path in allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module != "shared.config.settings":
                continue
            for alias in node.names:
                if alias.name == "settings":
                    offenders.append(f"{path.relative_to(repo_backend)}:{node.lineno}")

    assert not offenders, "Import `get_settings` (not the global `settings`) to avoid drift:\n" + "\n".join(
        sorted(offenders)
    )


def test_no_foundry_strict_opt_out_env_keys_reintroduced() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    repo_root = repo_backend.parent
    forbidden = (
        "ENABLE_FOUNDRY_V2_STRICT_COMPAT",
        "FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST",
    )

    offenders: list[str] = []
    files_to_scan = _iter_runtime_python_files(repo_backend) + [
        repo_backend / ".env.example",
        repo_root / ".env.example",
    ]
    for path in files_to_scan:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                offenders.append(f"{path.relative_to(repo_root)} contains {token}")

    assert not offenders, "Strict compat opt-out keys must not be reintroduced:\n" + "\n".join(sorted(offenders))


def test_no_external_funnel_address_env_keys_reintroduced() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    repo_root = repo_backend.parent
    forbidden = (
        "FUNNEL_BASE_URL",
        "FUNNEL_URL",
        "FUNNEL_RUNTIME_MODE",
        "FUNNEL_PORT",
    )

    offenders: list[str] = []
    files_to_scan = _iter_runtime_python_files(repo_backend) + [
        repo_backend / ".env.example",
        repo_root / ".env.example",
    ]
    for path in files_to_scan:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                offenders.append(f"{path.relative_to(repo_root)} contains {token}")

    assert not offenders, "External Funnel address/env mode keys must not be reintroduced:\n" + "\n".join(
        sorted(offenders)
    )
