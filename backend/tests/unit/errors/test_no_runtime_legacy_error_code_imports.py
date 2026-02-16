from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_ROOTS = (
    PROJECT_ROOT / "bff",
    PROJECT_ROOT / "oms",
    PROJECT_ROOT / "shared",
    PROJECT_ROOT / "mcp_servers",
    PROJECT_ROOT / "ontology_worker",
    PROJECT_ROOT / "action_worker",
)
ALLOWED_FILES = {
    PROJECT_ROOT / "shared" / "errors" / "legacy_codes.py",
    PROJECT_ROOT / "shared" / "errors" / "external_codes.py",
}
PATTERNS = (
    "from shared.errors.legacy_codes import",
    "import shared.errors.legacy_codes",
)


def _iter_python_files(root: Path):
    for path in root.rglob("*.py"):
        if "/tests/" in str(path):
            continue
        yield path


def test_runtime_modules_do_not_import_legacy_error_codes_directly() -> None:
    offenders: list[str] = []
    for root in RUNTIME_ROOTS:
        if not root.exists():
            continue
        for path in _iter_python_files(root):
            if path in ALLOWED_FILES:
                continue
            text = path.read_text(encoding="utf-8")
            if any(pattern in text for pattern in PATTERNS):
                offenders.append(str(path.relative_to(PROJECT_ROOT)))

    assert not offenders, (
        "Runtime modules must import ExternalErrorCode from shared.errors.external_codes "
        f"instead of shared.errors.legacy_codes: {sorted(offenders)}"
    )
