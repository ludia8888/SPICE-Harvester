from __future__ import annotations

import ast
from pathlib import Path

import pytest

from shared.config.app_config import AppConfig


def _disable_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    # settings.py loads `.env` unless DOCKER_CONTAINER is set.
    monkeypatch.setenv("DOCKER_CONTAINER", "1")


def _iter_runtime_python_files(repo_backend: Path) -> list[Path]:
    roots: list[Path] = [
        repo_backend / "shared",
        repo_backend / "oms",
        repo_backend / "bff",
        repo_backend / "funnel",
        repo_backend / "agent",
        repo_backend / "message_relay",
        repo_backend / "data_connector",
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
    return files


def test_app_config_reflects_current_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_env_file(monkeypatch)

    monkeypatch.setenv("INSTANCE_EVENTS_TOPIC", "instance_events_v1")
    assert AppConfig.INSTANCE_EVENTS_TOPIC == "instance_events_v1"

    monkeypatch.setenv("INSTANCE_EVENTS_TOPIC", "instance_events_v2")
    assert AppConfig.INSTANCE_EVENTS_TOPIC == "instance_events_v2"


def test_no_os_getenv_calls_outside_settings_module() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    allowed = {
        repo_backend / "shared" / "config" / "settings.py",
        # Test-only helper (unit-tested) – not used by services/workers directly.
        repo_backend / "shared" / "utils" / "env_utils.py",
    }

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
    allowed = {
        repo_backend / "shared" / "config" / "settings.py",
    }

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

