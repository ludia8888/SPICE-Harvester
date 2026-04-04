from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


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
