from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_runtime_python_code_does_not_use_datetime_utcnow() -> None:
    current_file = Path(__file__).resolve()
    backend_root = current_file.parents[3]
    excluded_paths = {
        current_file,
        backend_root / "scripts" / "migrations" / "fix_datetime_deprecation.py",
    }

    offenders: list[str] = []
    for path in backend_root.rglob("*.py"):
        if path in excluded_paths:
            continue
        text = path.read_text(encoding="utf-8")
        if "datetime.utcnow" not in text and "datetime.datetime.utcnow" not in text:
            continue
        offenders.append(str(path.resolve()))

    assert not offenders, "Replace datetime.utcnow() with shared.utils.time_utils.utcnow():\n" + "\n".join(sorted(offenders))
