#!/usr/bin/env python3
"""
Regenerate auto-managed documentation artifacts for the external docs portal.

This script calls existing generator scripts with portal-specific output paths,
then performs post-processing (MyST→standard Markdown conversion, JSON data
extraction for interactive visualizations).

Use ``--check`` to verify outputs without modifying files (CI / pre-commit).

Architecture
------------
Existing generators accept ``--output`` or ``--out-dir`` flags.  We call them
with paths under ``docs-portal/`` so that the same data extraction logic
produces portal-ready output.  Post-processing converts MyST fences
(``{mermaid}``) to standard Markdown fences (``mermaid``) and injects MDX
frontmatter.

Additionally, we extract structured JSON data from backend modules to feed
the interactive visualization components (ServiceTopology, ErrorTaxonomyHeatmap).

New generators (called as Python functions, not subprocesses):
- generate_portal_api_reference.py  → API endpoint reference + OpenAPI JSON
- generate_portal_config_reference.py → Config reference + config-landscape.json
- generate_portal_mermaid.py → .mmd → MDX conversion
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
PORTAL_ROOT = REPO_ROOT / "docs-portal"
PORTAL_DOCS = PORTAL_ROOT / "docs"
PORTAL_STATIC = PORTAL_ROOT / "static"
PORTAL_GENERATED = PORTAL_STATIC / "generated"

# ---------------------------------------------------------------------------
# Subprocess helper
# ---------------------------------------------------------------------------

def _run(cmd: list[str], *, check_mode: bool = False) -> int:
    """Run a subprocess, returning its exit code."""
    result = subprocess.run(cmd, cwd=str(REPO_ROOT))
    return result.returncode


# ---------------------------------------------------------------------------
# 1. Architecture reference  →  docs-portal/docs/architecture/_auto_architecture.mdx
# ---------------------------------------------------------------------------

def _generate_architecture(check: bool) -> int:
    """Run generate_architecture_reference.py targeting the portal.

    We always regenerate (never pass --check to the subprocess) because
    we apply post-processing (MyST→GFM, ${} escaping) that changes the
    file after the generator writes it.  For --check mode we compare
    the fully post-processed output against what's already on disk.
    """
    import tempfile

    output = PORTAL_DOCS / "architecture" / "auto-architecture.md"
    output.parent.mkdir(parents=True, exist_ok=True)

    if check:
        # Generate into a temp file, post-process, then compare
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".md", delete=False, dir=str(output.parent)
        ) as tmp:
            tmp_path = Path(tmp.name)

        cmd = [
            sys.executable,
            "scripts/generate_architecture_reference.py",
            "--output", str(tmp_path),
        ]
        rc = _run(cmd)
        if rc != 0:
            tmp_path.unlink(missing_ok=True)
            return rc

        _post_process_myst(tmp_path)
        new_content = tmp_path.read_text(encoding="utf-8")
        tmp_path.unlink(missing_ok=True)

        current = output.read_text(encoding="utf-8") if output.exists() else ""
        if current != new_content:
            print(f"  {output} is out of date (post-processed content differs).")
            return 1
        return 0

    # Normal generate mode
    cmd = [
        sys.executable,
        "scripts/generate_architecture_reference.py",
        "--output", str(output),
    ]
    rc = _run(cmd)
    if rc != 0:
        return rc

    _post_process_myst(output)
    _extract_service_topology_json()

    return 0


def _post_process_myst(path: Path) -> None:
    """Convert MyST to standard Markdown and escape MDX-incompatible syntax."""
    if not path.exists():
        return
    content = path.read_text(encoding="utf-8")
    # Convert ```{mermaid} → ```mermaid  (MyST directive → GFM)
    content = re.sub(r"```\{mermaid\}", "```mermaid", content)
    # Remove :caption: directives
    content = re.sub(r"^\s*:caption:.*$", "", content, flags=re.MULTILINE)
    # Avoid MDX parsing errors on raw '<=' inside markdown tables/text.
    content = content.replace("<=", "≤")
    # Escape ${...} shell variable references that MDX tries to parse as JSX
    # Use backtick-wrapped code spans for these values
    content = re.sub(r"\$\{([^}]+)\}", r"`${\1}`", content)
    # Escape bare { and } that aren't inside code fences (MDX treats them as JSX)
    # We wrap the file in .md extension which is safer, but still escape inline braces.
    path.write_text(content, encoding="utf-8")


def _extract_service_topology_json() -> None:
    """
    Parse docker-compose.full.yml to produce service-topology.json
    consumed by the ServiceTopology React component.
    """
    compose_path = REPO_ROOT / "docker-compose.full.yml"
    if not compose_path.exists():
        return

    # Import the architecture generator's parser directly
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from generate_architecture_reference import (
        _load_resolved_services,
        _classify_service_role,
    )

    services = _load_resolved_services()
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, str]] = []

    category_map = {
        "api": "api",
        "workers": "worker",
        "data": "data",
        "observability": "observability",
        "bootstrap": "bootstrap",
        "other": "bootstrap",
    }

    for svc in services:
        role = _classify_service_role(svc.name)
        port = svc.ports[0].split(":")[0] if svc.ports else None
        nodes.append({
            "id": svc.name,
            "label": svc.name,
            "category": category_map.get(role, "bootstrap"),
            "port": int(port) if port and port.isdigit() else None,
        })
        for dep in svc.depends_on:
            edges.append({"source": svc.name, "target": dep})

    PORTAL_GENERATED.mkdir(parents=True, exist_ok=True)
    out = PORTAL_GENERATED / "service-topology.json"
    out.write_text(
        json.dumps({"nodes": nodes, "edges": edges}, indent=2) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# 2. Error taxonomy  →  docs-portal/docs/reference/_auto_error_taxonomy.md
#                    +  docs-portal/static/generated/error-taxonomy.json
# ---------------------------------------------------------------------------

def _generate_error_taxonomy(check: bool) -> int:
    """
    Run generate_error_taxonomy.py targeting the portal, then escape
    MDX-incompatible braces and extract JSON.

    Like architecture, we never pass --check to the subprocess because
    we apply post-processing that changes the output.
    """
    import tempfile

    output = PORTAL_DOCS / "reference" / "auto-error-taxonomy.md"
    output.parent.mkdir(parents=True, exist_ok=True)

    if check:
        if not output.exists():
            print(f"  {output} does not exist yet (first run). Skipping check.")
            return 0

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".md", delete=False, dir=str(output.parent)
        ) as tmp:
            tmp_path = Path(tmp.name)
        tmp_path.write_text("", encoding="utf-8")

        cmd = [
            sys.executable,
            "scripts/generate_error_taxonomy.py",
            "--output", str(tmp_path),
        ]
        rc = _run(cmd)
        if rc != 0:
            tmp_path.unlink(missing_ok=True)
            return rc

        _escape_mdx_braces(tmp_path)
        new_content = tmp_path.read_text(encoding="utf-8")
        tmp_path.unlink(missing_ok=True)

        current = output.read_text(encoding="utf-8")
        if current != new_content:
            print(f"  {output} is out of date (post-processed content differs).")
            return 1
        return 0

    # Normal generate mode
    if not output.exists():
        output.write_text("", encoding="utf-8")

    cmd = [
        sys.executable,
        "scripts/generate_error_taxonomy.py",
        "--output", str(output),
    ]
    rc = _run(cmd)
    if rc != 0:
        return rc

    _escape_mdx_braces(output)
    _extract_error_taxonomy_json()

    return 0


def _escape_mdx_braces(path: Path) -> None:
    """Escape bare {WORD} patterns in .md files that MDX would parse as JSX."""
    if not path.exists():
        return
    content = path.read_text(encoding="utf-8")
    # Escape {SUBSYS}, {subsystem}, etc. — bare curly-braced words outside code
    # Replace {WORD} with `{WORD}` (backtick-wrapped code span)
    content = re.sub(r"(?<![`$])(\{[A-Z_a-z][A-Z_a-z0-9]*\})", r"`\1`", content)
    path.write_text(content, encoding="utf-8")


def _extract_error_taxonomy_json() -> None:
    """
    Load the enterprise error catalog and produce error-taxonomy.json
    consumed by the ErrorTaxonomyHeatmap React component.
    """
    try:
        sys.path.insert(0, str(REPO_ROOT / "backend"))
        import logging

        logging.disable(logging.CRITICAL)
        from shared.errors import enterprise_catalog as catalog  # noqa: WPS433

        specs: List[Dict[str, Any]] = []

        # Helper to safely extract spec fields
        def _spec_to_dict(
            external_code: str, spec: Any, subsystem: str = "{SUBSYS}"
        ) -> Dict[str, Any]:
            return {
                "code": external_code,
                "enterprise_code": spec.code_template.replace(
                    "{subsystem}", subsystem
                ),
                "domain": spec.domain.value,
                "error_class": spec.error_class.value,
                "severity": spec.severity.value,
                "title": spec.title,
                "http_status": catalog._resolve_http_status_hint(spec, 500),
                "retryable": catalog._resolve_retryable(spec),
                "retry_policy": catalog._resolve_default_retry_policy(
                    spec
                ).value,
                "subsystem": subsystem,
            }

        for code_key, spec in catalog._ERROR_CODE_SPECS.items():
            specs.append(_spec_to_dict(code_key.value, spec))

        for code_key, spec in catalog._OBJECTIFY_ERROR_SPECS.items():
            specs.append(_spec_to_dict(code_key, spec, subsystem="OBJ"))

        for code_key, spec in catalog._EXTERNAL_CODE_SPECS.items():
            specs.append(_spec_to_dict(code_key, spec))

        PORTAL_GENERATED.mkdir(parents=True, exist_ok=True)
        out = PORTAL_GENERATED / "error-taxonomy.json"
        out.write_text(
            json.dumps({"errors": specs}, indent=2) + "\n", encoding="utf-8"
        )
    except ImportError:
        print(
            "WARN: Could not import enterprise_catalog — "
            "error-taxonomy.json not generated (backend deps missing).",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# 3. Pipeline tooling  →  docs-portal/docs/reference/_generated/
# ---------------------------------------------------------------------------

def _generate_pipeline_tooling(check: bool) -> int:
    """Run generate_pipeline_tooling_reference.py targeting the portal."""
    out_dir = PORTAL_DOCS / "reference" / "_generated"
    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "scripts/generate_pipeline_tooling_reference.py",
        "--out-dir", str(out_dir),
    ]
    if check:
        cmd.append("--check")
    return _run(cmd)


# ---------------------------------------------------------------------------
# 4. Repo file inventory  →  docs-portal/docs/reference/_auto_repo_inventory.md
# ---------------------------------------------------------------------------

def _generate_repo_inventory(check: bool) -> int:
    """Run generate_repo_file_inventory.py targeting the portal."""
    output = PORTAL_DOCS / "reference" / "auto-repo-inventory.md"
    output.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "scripts/generate_repo_file_inventory.py",
        "--output", str(output),
    ]
    if check:
        cmd.append("--check")
    return _run(cmd)


# ---------------------------------------------------------------------------
# 5. API reference  →  docs-portal/docs/api/_auto_*.mdx
#                   +  docs-portal/static/generated/bff-openapi.json
# ---------------------------------------------------------------------------

def _generate_api_reference(check: bool) -> int:
    """Run generate_portal_api_reference.py (Python call, not subprocess)."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from generate_portal_api_reference import generate as api_gen  # noqa: WPS433

    return api_gen(check=check)


# ---------------------------------------------------------------------------
# 6. Config reference  →  docs-portal/docs/reference/_auto_config_reference.mdx
#                       +  docs-portal/static/generated/config-landscape.json
# ---------------------------------------------------------------------------

def _generate_config_reference(check: bool) -> int:
    """Run generate_portal_config_reference.py (Python call, not subprocess)."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from generate_portal_config_reference import generate as config_gen  # noqa: WPS433

    return config_gen(check=check)


# ---------------------------------------------------------------------------
# 7. Mermaid diagrams  →  docs-portal/docs/architecture/_auto_*.mdx
# ---------------------------------------------------------------------------

def _generate_mermaid_diagrams(check: bool) -> int:
    """Run generate_portal_mermaid.py (Python call, not subprocess)."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from generate_portal_mermaid import generate as mermaid_gen  # noqa: WPS433

    return mermaid_gen(check=check)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

GENERATORS = [
    ("Architecture reference", _generate_architecture),
    ("Error taxonomy", _generate_error_taxonomy),
    ("Pipeline tooling reference", _generate_pipeline_tooling),
    ("Repo file inventory", _generate_repo_inventory),
    ("API reference", _generate_api_reference),
    ("Config reference", _generate_config_reference),
    ("Mermaid diagrams", _generate_mermaid_diagrams),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any generated output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    failures: list[str] = []

    for label, fn in GENERATORS:
        print(f"{'CHECK' if args.check else 'GENERATE'}: {label} ...", flush=True)
        try:
            rc = fn(args.check)
        except SystemExit as exc:
            rc = exc.code if isinstance(exc.code, int) else 1
        if rc != 0:
            failures.append(label)
            print(f"  FAIL: {label}", file=sys.stderr)
        else:
            print(f"  OK: {label}")

    if failures:
        print(
            f"\n{'CHECK' if args.check else 'GENERATE'}: "
            f"{len(failures)} generator(s) failed: {', '.join(failures)}",
            file=sys.stderr,
        )
        return 1

    print(
        f"\n{'CHECK' if args.check else 'GENERATE'}: "
        f"All {len(GENERATORS)} portal content generators completed successfully.",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
