from __future__ import annotations

import argparse
import re
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

RUNTIME_VOCAB_SCAN_ROOTS = ("backend", "docs")
RUNTIME_VOCAB_TEXT_SUFFIXES = {".py", ".conf", ".md", ".json", ".yaml", ".yml", ".toml"}
RUNTIME_VOCAB_PATTERNS = (
    (
        re.compile(r"""["']classification["']\s*:\s*["'](?:healthy|unknown)["']"""),
        "legacy runtime classification",
    ),
    (
        re.compile(r"""["']status["']\s*:\s*["'](?:healthy|unhealthy)["']"""),
        "legacy runtime status",
    ),
)

ABSENT_RUNTIME_FILES = (
    "backend/shared/services/core/health_check.py",
)

DUPLICATE_SURFACE_GUARDS = {
    "backend/funnel/routers/type_inference_router.py": [
        '@router.get("/health")',
    ],
    "backend/bff/routers/foundry_ontology_v2_errors.py": [
        "def _foundry_error(",
    ],
    "backend/bff/routers/foundry_connectivity_v2.py": [
        "def _feature_flags_settings(",
        "def _jdbc_enabled_for_db(",
        "def _cdc_enabled_for_db(",
    ],
    "backend/bff/routers/foundry_connectivity_v2_common.py": [
        "def _foundry_error(",
        "def _dataset_id_from_rid(",
        "def _dataset_rid(",
    ],
    "backend/bff/routers/lineage.py": [
        "def _freshness_status(",
        "def _status_rank(",
        "def _staleness_reason_with_scope(",
        "def _update_type(",
        "def _chunked(",
        "async def _get_latest_edges_to_batched(",
        "async def _get_latest_edges_from_batched(",
        "async def _get_latest_edges_for_projections_batched(",
        "def _edge_cause_payload(",
        "async def _enrich_artifacts_with_latest_writer(",
        "def _out_of_date_scope(",
    ],
    "backend/bff/services/lineage_out_of_date_service.py": [
        "def _parse_artifact_node_id(",
        "def _suggest_remediation_actions(",
        "def _freshness_status(",
        "def _status_rank(",
        "def _staleness_reason_with_scope(",
        "def _update_type(",
        "def _chunked(",
        "async def _get_latest_edges_to_batched(",
        "async def _get_latest_edges_from_batched(",
        "async def _get_latest_edges_for_projections_batched(",
        "def _edge_cause_payload(",
        "async def _enrich_artifacts_with_latest_writer(",
        "def _out_of_date_scope(",
    ],
    "backend/bff/services/data_connector_pipelining_service.py": [
        "def _row_hash(",
        "def _parse_csv_bytes(",
        "def _align_row_to_columns(",
        "def _align_rows_to_columns(",
        "def _apply_append_mode(",
        "def _apply_update_mode(",
    ],
    "backend/bff/services/pipeline_execution_service.py": [
        "def _build_ontology_ref(",
        "def _parse_optional_bool(",
        "def _extract_deploy_dependencies_raw(",
        "def _parse_deploy_schedule_fields(",
    ],
    "backend/bff/services/pipeline_execution_deploy.py": [
        "def _extract_deploy_dependencies_raw(",
        "def _parse_deploy_schedule_fields(",
        "def _parse_optional_bool(",
    ],
    "backend/bff/services/pipeline_execution_preview_build.py": [
        "def _build_ontology_ref(",
    ],
    "backend/bff/services/ontology_ops_service.py": [
        "def _localized_to_string(",
    ],
    "backend/bff/routers/object_types.py": [
        "def _unwrap_data(",
        "def _extract_resources(",
        "def _extract_resource(",
        "def _localized_text(",
        "def _extract_ontology_properties(",
    ],
    "backend/bff/routers/link_types_read.py": [
        "def _unwrap_data(",
        "def _extract_resources(",
        "def _normalize_object_ref(",
        "def _localized_text(",
    ],
    "backend/bff/routers/foundry_ontology_v2_serialization.py": [
        "def _localized_text(",
    ],
    "backend/bff/routers/ontology_extensions.py": [
        "def _default_expected_head_commit(",
    ],
    "backend/bff/routers/foundry_orchestration_v2.py": [
        "def _foundry_error(",
    ],
    "backend/bff/services/ontology_extensions_service.py": [
        "def _default_expected_head_commit(",
    ],
    "backend/bff/routers/pipeline_datasets_ops_ingest.py": [
        "def _dataset_artifact_prefix(",
    ],
    "backend/bff/routers/foundry_datasets_v2.py": [
        "def _dataset_id_from_rid(",
        "def _dataset_rid(",
    ],
    "backend/shared/services/core/connector_ingest_service.py": [
        "def _dataset_artifact_prefix(",
    ],
    "backend/shared/services/registries/dataset_registry_catalog.py": [
        "def _require_pool(",
    ],
    "backend/shared/services/registries/dataset_registry_governance.py": [
        "def _require_pool(",
    ],
    "backend/shared/services/registries/dataset_registry_ingest.py": [
        "def _require_pool(",
    ],
    "backend/shared/services/registries/dataset_registry_relationships.py": [
        "def _require_pool(",
    ],
    "backend/oms/routers/attachments.py": [
        "def _foundry_error(",
    ],
    "backend/oms/routers/timeseries.py": [
        "def _foundry_error(",
    ],
    "backend/oms/routers/query.py": [
        "def _foundry_error(",
    ],
    "backend/oms/routers/ontology.py": [
        "def _localized_to_string(",
    ],
}

APIRESPONSE_IMPORT_PATTERN = re.compile(r"^from shared\.models\.requests import .*?\bApiResponse\b", re.MULTILINE)


def _iter_non_test_python_files(backend_root: Path) -> Iterable[Path]:
    for path in backend_root.rglob("*.py"):
        if any(part == "tests" for part in path.parts):
            continue
        if path.name.startswith("test_"):
            continue
        yield path


def _iter_runtime_contract_text_files(repo_root: Path) -> Iterable[Path]:
    for root_name in RUNTIME_VOCAB_SCAN_ROOTS:
        root = repo_root / root_name
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in RUNTIME_VOCAB_TEXT_SUFFIXES:
                continue
            if any(part in {"tests", "__pycache__", "_build"} for part in path.parts):
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
    for path in _iter_runtime_contract_text_files(repo_root):
        if path.name == "platform_contract_audit.py":
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern, label in RUNTIME_VOCAB_PATTERNS:
            match = pattern.search(text)
            if match:
                violations.append(
                    f"{path.relative_to(repo_root)} contains banned {label} literal {match.group(0)}"
                )
    return violations


def audit_absent_runtime_files(repo_root: Path) -> List[str]:
    violations: List[str] = []
    for relative_path in ABSENT_RUNTIME_FILES:
        if (repo_root / relative_path).exists():
            violations.append(f"{relative_path} must stay removed; legacy duplicate runtime surface reintroduced")
    return violations


def audit_duplicate_surfaces(repo_root: Path) -> List[str]:
    violations: List[str] = []
    for relative_path, banned_markers in DUPLICATE_SURFACE_GUARDS.items():
        path = repo_root / relative_path
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        present = [marker for marker in banned_markers if marker in text]
        if present:
            violations.append(f"{relative_path} reintroduced duplicate helper/runtime surface markers: {', '.join(present)}")
    return violations


def audit_apiresponse_import_surface(repo_root: Path) -> List[str]:
    violations: List[str] = []
    for path in _iter_non_test_python_files(repo_root / "backend"):
        text = path.read_text(encoding="utf-8")
        if APIRESPONSE_IMPORT_PATTERN.search(text):
            violations.append(
                f"{path.relative_to(repo_root)} imports ApiResponse via requests.py; use shared.models.responses instead"
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
    violations.extend(audit_absent_runtime_files(repo_root))
    violations.extend(audit_duplicate_surfaces(repo_root))
    violations.extend(audit_apiresponse_import_surface(repo_root))
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
