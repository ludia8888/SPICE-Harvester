from __future__ import annotations

import re
from pathlib import Path


_BROAD_EXCEPTION_PATTERN = re.compile(r"except\s+Exception(?:\s+as\s+\w+)?\s*:")
_BACKEND_DIR = Path(__file__).resolve().parents[3]
_ROUTER_FILES = (
    _BACKEND_DIR / "bff" / "routers" / "foundry_datasets_v2.py",
    _BACKEND_DIR / "bff" / "routers" / "foundry_ontology_v2.py",
)


def test_foundry_v2_routers_do_not_use_broad_exception_handlers() -> None:
    for router_path in _ROUTER_FILES:
        source = router_path.read_text(encoding="utf-8")
        assert _BROAD_EXCEPTION_PATTERN.search(source) is None, f"broad catch must be removed: {router_path}"
