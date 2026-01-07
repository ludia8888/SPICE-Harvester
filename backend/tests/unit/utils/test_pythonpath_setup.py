from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from shared.utils import pythonpath_setup


def test_detect_backend_directory_with_markers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    backend_dir = tmp_path / "backend"
    (backend_dir / "shared").mkdir(parents=True)
    (backend_dir / "bff").mkdir()
    (backend_dir / "oms").mkdir()
    (backend_dir / "funnel").mkdir()
    (backend_dir / "pyproject.toml").write_text("[build-system]\n", encoding="utf-8")

    fake_file = backend_dir / "shared" / "utils" / "pythonpath_setup.py"
    fake_file.parent.mkdir(parents=True, exist_ok=True)
    fake_file.write_text("", encoding="utf-8")

    monkeypatch.setattr(pythonpath_setup, "__file__", str(fake_file))

    detected = pythonpath_setup.detect_backend_directory()
    assert detected == backend_dir


def test_setup_pythonpath_updates_env(monkeypatch: pytest.MonkeyPatch) -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    original_sys_path = list(sys.path)
    monkeypatch.setenv("PYTHONPATH", "")

    try:
        assert pythonpath_setup.setup_pythonpath(backend_dir=backend_dir) is True
        assert str(backend_dir) in sys.path
        assert str(backend_dir) in os.environ.get("PYTHONPATH", "")
    finally:
        sys.path[:] = original_sys_path


def test_setup_pythonpath_invalid_directory(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    missing = tmp_path / "missing"
    assert pythonpath_setup.setup_pythonpath(backend_dir=missing) is False
    captured = capsys.readouterr()
    assert "does not exist" in captured.err


def test_configure_python_environment_success(monkeypatch: pytest.MonkeyPatch) -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    original_sys_path = list(sys.path)
    monkeypatch.setenv("PYTHONPATH", "")

    try:
        assert pythonpath_setup.configure_python_environment(backend_dir=backend_dir, verbose=False) is True
    finally:
        sys.path[:] = original_sys_path
