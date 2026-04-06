from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from scripts import platform_contract_audit as audit_module


@pytest.mark.unit
def test_platform_contract_audit_guard() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    repo_root = backend_dir.parent
    script_path = backend_dir / "scripts" / "platform_contract_audit.py"
    result = subprocess.run(
        [sys.executable, str(script_path), "--repo-root", str(repo_root)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    output = (result.stdout or "") + ("\n" if result.stdout and result.stderr else "") + (result.stderr or "")
    raise AssertionError(f"Platform contract audit failed:\n{output}")


@pytest.mark.unit
def test_runtime_vocabulary_audit_flags_minified_nginx_health_payload(tmp_path: Path) -> None:
    backend_dir = tmp_path / "backend"
    backend_dir.mkdir()
    (backend_dir / "nginx.conf").write_text(
        'location /health { return 200 \'{"status":"healthy"}\'; }\n',
        encoding="utf-8",
    )
    (tmp_path / "docs").mkdir()

    violations = audit_module.audit_runtime_vocabulary(tmp_path)

    assert any("backend/nginx.conf" in violation for violation in violations)


@pytest.mark.unit
def test_runtime_vocabulary_audit_scans_docs_examples(tmp_path: Path) -> None:
    backend_dir = tmp_path / "backend"
    backend_dir.mkdir()
    (backend_dir / "placeholder.py").write_text("STATUS = 'ready'\n", encoding="utf-8")
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "example.md").write_text('{"status": "healthy"}\n', encoding="utf-8")

    violations = audit_module.audit_runtime_vocabulary(tmp_path)

    assert any("docs/example.md" in violation for violation in violations)
