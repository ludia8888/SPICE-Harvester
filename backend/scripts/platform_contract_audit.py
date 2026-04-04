from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[2]

FACADE_LINE_CEILINGS = {
    "backend/bff/routers/foundry_ontology_v2.py": 3021,
    "backend/pipeline_worker/main.py": 3071,
    "backend/objectify_worker/main.py": 774,
    "backend/shared/services/registries/dataset_registry.py": 121,
}

FACADE_REQUIRED_MARKERS = {
    "backend/bff/routers/foundry_ontology_v2.py": [
        "foundry_ontology_v2_metadata as _metadata",
        "foundry_ontology_v2_object_rows as _object_rows",
        "foundry_ontology_v2_queries as _query_routes",
        "foundry_ontology_v2_media as _media",
        "foundry_ontology_v2_read_routes as _read_routes",
        "foundry_ontology_v2_models import",
    ],
    "backend/pipeline_worker/main.py": [
        "PipelineIngestDomain",
        "PipelineOutputDomain",
        "PipelineRunDomain",
        "PipelineValidationDomain",
        "PipelineWorkerRuntimeMixin",
    ],
    "backend/objectify_worker/main.py": [
        "import job_processing as _job_processing",
        "import lineage_helpers as _lineage_helpers",
        "import ontology_runtime as _ontology_runtime",
        "import batch_loading as _batch_loading",
        "runtime_helpers import",
        "ObjectifyWorkerRuntimeMixin",
    ],
    "backend/shared/services/registries/dataset_registry.py": [
        "DatasetRegistryCatalogMixin",
        "DatasetRegistryGovernanceMixin",
        "DatasetRegistryEditsMixin",
        "DatasetRegistryIngestMixin",
    ],
}

DOC_REQUIRED_SNIPPETS = {
    "docs/WRITE_PATH_CONTRACTS.md": [
        "authoritative_store",
        "recovery_path",
        "completed`, `degraded`, `skipped`",
        "`event_store`",
        "`postgres.<registry_name>`",
        "`lakefs`",
    ],
    "docs/SERVICE_BOUNDARIES.md": [
        "backend/bff/routers/foundry_ontology_v2.py",
        "backend/pipeline_worker/main.py",
        "backend/objectify_worker/main.py",
        "backend/shared/services/registries/dataset_registry.py",
    ],
    "docs/REGISTRY_OWNERSHIP.md": [
        "`ObjectifyRegistry`",
        "`PipelineRegistry`",
        "`DatasetRegistry`",
        "`ProcessedEventRegistry`",
    ],
}

BANNED_CLASSIFICATION_LITERALS = (
    '"classification": "healthy"',
    '"classification": "unknown"',
    "'classification': 'healthy'",
    "'classification': 'unknown'",
)


def _iter_non_test_python_files(backend_root: Path) -> Iterable[Path]:
    for path in backend_root.rglob("*.py"):
        if any(part == "tests" for part in path.parts):
            continue
        if path.name.startswith("test_"):
            continue
        yield path


def audit_facade_line_counts(repo_root: Path) -> List[str]:
    violations: List[str] = []
    for relative_path, ceiling in FACADE_LINE_CEILINGS.items():
        path = repo_root / relative_path
        line_count = sum(1 for _ in path.open("r", encoding="utf-8"))
        if line_count > ceiling:
            violations.append(
                f"{relative_path} is {line_count} lines (ceiling {ceiling}); move new logic into helper/domain modules"
            )
    return violations


def audit_facade_markers(repo_root: Path) -> List[str]:
    violations: List[str] = []
    for relative_path, markers in FACADE_REQUIRED_MARKERS.items():
        text = (repo_root / relative_path).read_text(encoding="utf-8")
        missing = [marker for marker in markers if marker not in text]
        if missing:
            violations.append(f"{relative_path} is missing delegation markers: {', '.join(missing)}")
    return violations


def audit_runtime_vocabulary(repo_root: Path) -> List[str]:
    violations: List[str] = []
    backend_root = repo_root / "backend"
    for path in _iter_non_test_python_files(backend_root):
        if path.name == "platform_contract_audit.py":
            continue
        text = path.read_text(encoding="utf-8")
        for banned in BANNED_CLASSIFICATION_LITERALS:
            if banned in text:
                violations.append(
                    f"{path.relative_to(repo_root)} contains banned legacy runtime classification literal {banned}"
                )
    return violations


def audit_docs(repo_root: Path) -> List[str]:
    violations: List[str] = []
    for relative_path, snippets in DOC_REQUIRED_SNIPPETS.items():
        text = (repo_root / relative_path).read_text(encoding="utf-8")
        missing = [snippet for snippet in snippets if snippet not in text]
        if missing:
            violations.append(f"{relative_path} is missing required platform contract snippets: {', '.join(missing)}")
    return violations


def run_audit(repo_root: Path) -> List[str]:
    violations: List[str] = []
    violations.extend(audit_facade_line_counts(repo_root))
    violations.extend(audit_facade_markers(repo_root))
    violations.extend(audit_runtime_vocabulary(repo_root))
    violations.extend(audit_docs(repo_root))
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit platform isolation, vocabulary, and facade contracts.")
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root to audit (defaults to current repo).",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    violations = run_audit(repo_root)
    if not violations:
        print("Platform contract audit passed.")
        return 0

    print("Platform contract audit failed:")
    for violation in violations:
        print(f" - {violation}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
