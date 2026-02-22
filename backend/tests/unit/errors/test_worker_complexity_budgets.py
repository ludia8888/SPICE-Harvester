from __future__ import annotations

import ast
from pathlib import Path

import pytest


def _count_broad_exception_handlers(path: Path) -> int:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    count = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        handler_type = node.type
        if handler_type is None:
            continue
        if isinstance(handler_type, ast.Name) and handler_type.id == "Exception":
            count += 1
            continue
        if isinstance(handler_type, ast.Tuple):
            for item in handler_type.elts:
                if isinstance(item, ast.Name) and item.id == "Exception":
                    count += 1
                    break
    return count


@pytest.mark.unit
def test_action_worker_broad_exception_budget() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    action_worker_path = backend_dir / "action_worker" / "main.py"
    broad_exception_count = _count_broad_exception_handlers(action_worker_path)
    assert broad_exception_count <= 8, (
        f"action_worker/main.py has {broad_exception_count} 'except Exception' handlers; "
        "budget is 8. Replace catch-alls with typed exceptions or explicit failure propagation."
    )


@pytest.mark.unit
def test_pipeline_worker_file_size_budget() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    pipeline_worker_path = backend_dir / "pipeline_worker" / "main.py"
    line_count = len(pipeline_worker_path.read_text(encoding="utf-8").splitlines())
    assert line_count <= 6803, (
        f"pipeline_worker/main.py has {line_count} lines; budget is 6803. "
        "Move cohesive execution concerns to pipeline_worker/* modules before adding more logic."
    )
