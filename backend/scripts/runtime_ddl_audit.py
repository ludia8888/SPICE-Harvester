#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


DDL_PATTERNS = (
    re.compile(r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS", re.IGNORECASE),
    re.compile(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS", re.IGNORECASE),
    re.compile(r"CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS", re.IGNORECASE),
    re.compile(r"ALTER\s+TABLE", re.IGNORECASE),
)


ALLOWED_RUNTIME_DDL_FILES = {
    "backend/oms/services/ontology_deployment_registry.py",
    "backend/oms/services/ontology_deployment_registry_base.py",
    "backend/oms/services/ontology_deployment_registry_v2.py",
    "backend/oms/services/ontology_resources.py",
    "backend/shared/security/database_access.py",
    "backend/shared/services/core/audit_log_store.py",
    "backend/shared/services/events/aggregate_sequence_allocator.py",
    "backend/shared/services/registries/action_log_registry.py",
    "backend/shared/services/registries/agent_function_registry.py",
    "backend/shared/services/registries/agent_model_registry.py",
    "backend/shared/services/registries/agent_policy_registry.py",
    "backend/shared/services/registries/agent_registry.py",
    "backend/shared/services/registries/agent_session_registry.py",
    "backend/shared/services/registries/agent_tool_registry.py",
    "backend/shared/services/registries/connector_registry.py",
    "backend/shared/services/registries/dataset_profile_registry.py",
    "backend/shared/services/registries/dataset_registry_schema.py",
    "backend/shared/services/registries/lineage_store.py",
    "backend/shared/services/registries/objectify_registry.py",
    "backend/shared/services/registries/ontology_key_spec_registry.py",
    "backend/shared/services/registries/pipeline_plan_registry.py",
    "backend/shared/services/registries/pipeline_registry.py",
    "backend/shared/services/registries/postgres_schema_registry.py",
    "backend/shared/services/registries/processed_event_registry.py",
    "backend/shared/utils/label_mapper.py",
}


def _scan_python_runtime_ddl(repo_root: Path) -> tuple[list[str], list[str]]:
    backend_root = repo_root / "backend"
    found: list[str] = []
    violations: list[str] = []
    for path in sorted(backend_root.rglob("*.py")):
        relative = path.relative_to(repo_root).as_posix()
        if relative.startswith("backend/tests/"):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if not any(pattern.search(text) for pattern in DDL_PATTERNS):
            continue
        found.append(relative)
        if relative not in ALLOWED_RUNTIME_DDL_FILES:
            violations.append(relative)
    return found, violations


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Guardrail: runtime DDL may only live in explicitly approved compatibility fallback modules.",
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Repository root (defaults to auto-detected root).",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    found, violations = _scan_python_runtime_ddl(repo_root)

    print(f"Runtime DDL callsites found: {len(found)}")
    for item in found:
        print(f"  - {item}")

    missing_allowlist_entries = sorted(set(ALLOWED_RUNTIME_DDL_FILES) - set(found))
    if missing_allowlist_entries:
        print("Stale allowlist entries (safe to delete once migration fallback is removed):")
        for item in missing_allowlist_entries:
            print(f"  - {item}")

    if violations:
        print("Unexpected runtime DDL callsites detected:", file=sys.stderr)
        for item in violations:
            print(f"  - {item}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
