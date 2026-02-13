from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.unit
def test_error_taxonomy_audit_guard() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    script_path = backend_dir / "shared" / "tools" / "error_taxonomy_audit.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--backend-root",
        str(backend_dir),
        "--fail-on-raw-http-without-code",
        "--fail-on-raw-code",
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    output = (result.stdout or "") + ("\n" if result.stdout and result.stderr else "") + (result.stderr or "")
    raise AssertionError(f"Error taxonomy audit failed:\n{output}")
