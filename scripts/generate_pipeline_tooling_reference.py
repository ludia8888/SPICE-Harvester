#!/usr/bin/env python3
"""
Generate repo docs that must stay in lockstep with the Agent + MCP tools.

Outputs (auto-managed):
- docs/reference/_generated/PIPELINE_MCP_TOOLS.md
- docs/reference/_generated/PIPELINE_AGENT_ALLOWED_TOOLS.md
- docs/reference/_generated/ONTOLOGY_MCP_TOOLS.md
- docs/reference/_generated/ONTOLOGY_AGENT_ALLOWED_TOOLS.md

Why AST parsing (instead of imports)?
- Avoids import-time side effects (settings/env/optional deps).
- Guarantees we document exactly what the server code exposes/allowlists.
"""

from __future__ import annotations

import argparse
import ast
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]

# Pipeline Agent sources
PIPELINE_MCP_SERVER = REPO_ROOT / "backend" / "mcp" / "pipeline_mcp_server.py"
PIPELINE_AGENT_LOOP = REPO_ROOT / "backend" / "bff" / "services" / "pipeline_agent_autonomous_loop.py"

# Ontology Agent sources
ONTOLOGY_MCP_SERVER = REPO_ROOT / "backend" / "mcp" / "ontology_mcp_server.py"
ONTOLOGY_AGENT_LOOP = REPO_ROOT / "backend" / "bff" / "services" / "ontology_agent_autonomous_loop.py"

OUT_DIR = REPO_ROOT / "docs" / "reference" / "_generated"
OUT_MCP = OUT_DIR / "PIPELINE_MCP_TOOLS.md"
OUT_ALLOWED = OUT_DIR / "PIPELINE_AGENT_ALLOWED_TOOLS.md"
OUT_ONTOLOGY_MCP = OUT_DIR / "ONTOLOGY_MCP_TOOLS.md"
OUT_ONTOLOGY_ALLOWED = OUT_DIR / "ONTOLOGY_AGENT_ALLOWED_TOOLS.md"

BEGIN = "<!-- BEGIN AUTO-GENERATED: pipeline_tooling_reference -->"
END = "<!-- END AUTO-GENERATED: pipeline_tooling_reference -->"
BEGIN_ONTOLOGY = "<!-- BEGIN AUTO-GENERATED: ontology_tooling_reference -->"
END_ONTOLOGY = "<!-- END AUTO-GENERATED: ontology_tooling_reference -->"


def _run_git(args: List[str]) -> str | None:
    try:
        return subprocess.check_output(
            args, cwd=str(REPO_ROOT), text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return None


def _detect_source_revision(paths: Sequence[Path]) -> tuple[str | None, str]:
    """
    Prefer a deterministic timestamp derived from the *source-of-truth* files.

    Important: this must NOT change on unrelated commits, otherwise the generated docs
    will constantly be "out of date" immediately after every commit.
    """
    rels = [str(p.resolve().relative_to(REPO_ROOT)) for p in paths]

    # If the caller provides a ref, constrain the log lookup to that ref.
    ref = (os.environ.get("SPICE_DOCS_GIT_REF") or "").strip()
    if ref:
        out = _run_git(["git", "log", "-1", "--format=%H|%cI", ref, "--", *rels])
    else:
        out = _run_git(["git", "log", "-1", "--format=%H|%cI", "--", *rels])

    if out and "|" in out:
        sha, ts = out.split("|", 1)
        sha = sha.strip()
        ts = ts.strip()
        if sha and ts:
            return sha, ts

    return None, datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _write_if_changed(path: Path, content: str) -> None:
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing == content:
            return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _literal_eval_from_annassign(
    tree: ast.AST, *, target_name: str, expected_node: type
) -> Any:
    """
    Find a top-level or nested AnnAssign like: `name: ... = <literal>` and literal_eval its RHS.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.AnnAssign):
            continue
        target = node.target
        if not isinstance(target, ast.Name):
            continue
        if target.id != target_name:
            continue
        if not isinstance(node.value, expected_node):
            # Still try literal_eval; allow Tuple/List/Dict etc. The expected_node is a sanity hint.
            pass
        return ast.literal_eval(node.value)  # type: ignore[arg-type]
    raise ValueError(f"Could not find AnnAssign for {target_name}")


def _extract_mcp_tool_specs() -> List[Dict[str, Any]]:
    src = _read_text(PIPELINE_MCP_SERVER)
    tree = ast.parse(src, filename=str(PIPELINE_MCP_SERVER))
    tool_specs = _literal_eval_from_annassign(tree, target_name="tool_specs", expected_node=ast.List)
    if not isinstance(tool_specs, list) or not all(isinstance(item, dict) for item in tool_specs):
        raise TypeError("tool_specs is not a list[dict]")
    return tool_specs  # type: ignore[return-value]


def _extract_agent_allowed_tools() -> Tuple[str, ...]:
    src = _read_text(PIPELINE_AGENT_LOOP)
    tree = ast.parse(src, filename=str(PIPELINE_AGENT_LOOP))
    allowed = _literal_eval_from_annassign(
        tree,
        target_name="_PIPELINE_AGENT_ALLOWED_TOOLS",
        expected_node=ast.Tuple,
    )
    if not isinstance(allowed, tuple) or not all(isinstance(item, str) for item in allowed):
        raise TypeError("_PIPELINE_AGENT_ALLOWED_TOOLS is not a tuple[str, ...]")
    return allowed  # type: ignore[return-value]


def _tool_required_fields(tool: Dict[str, Any]) -> List[str]:
    schema = tool.get("inputSchema")
    if not isinstance(schema, dict):
        return []
    required = schema.get("required")
    if not isinstance(required, list):
        return []
    return [str(item) for item in required if str(item or "").strip()]


def _tool_category(name: str) -> str:
    if name.startswith("context_pack_"):
        return "Context Pack (analysis hints)"
    if name.startswith("plan_"):
        return "Plan Builder / Validation"
    if name.startswith("pipeline_"):
        return "Pipeline Control Plane (Spark worker execution)"
    return "Other"


def _ontology_tool_category(name: str) -> str:
    if name in {"ontology_new", "ontology_load", "ontology_reset"}:
        return "Initialization"
    if name in {"ontology_set_class_meta", "ontology_set_abstract"}:
        return "Class Metadata"
    if name.startswith("ontology_add_property") or name.startswith("ontology_update_property") or \
       name.startswith("ontology_remove_property") or name == "ontology_set_primary_key":
        return "Property Management"
    if "relationship" in name:
        return "Relationship Management"
    if name in {"ontology_infer_schema_from_data", "ontology_suggest_mappings"}:
        return "Schema Inference"
    if name.startswith("ontology_validate") or name.startswith("ontology_check"):
        return "Validation"
    if name in {"ontology_list_classes", "ontology_get_class", "ontology_search_classes"}:
        return "Query"
    if name in {"ontology_create", "ontology_update", "ontology_preview"}:
        return "Save / Preview"
    return "Other"


def _extract_ontology_agent_allowed_tools() -> Tuple[str, ...]:
    """Extract ontology agent allowed tools from the autonomous loop."""
    if not ONTOLOGY_AGENT_LOOP.exists():
        return ()
    src = _read_text(ONTOLOGY_AGENT_LOOP)
    tree = ast.parse(src, filename=str(ONTOLOGY_AGENT_LOOP))
    try:
        allowed = _literal_eval_from_annassign(
            tree,
            target_name="_ONTOLOGY_AGENT_ALLOWED_TOOLS",
            expected_node=ast.Tuple,
        )
        if not isinstance(allowed, tuple) or not all(isinstance(item, str) for item in allowed):
            return ()
        return allowed
    except ValueError:
        return ()


def _render_mcp_tools_md(tool_specs: List[Dict[str, Any]], *, updated_at: str, rev: str | None) -> str:
    by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for tool in tool_specs:
        name = str(tool.get("name") or "").strip()
        if not name:
            continue
        by_cat.setdefault(_tool_category(name), []).append(tool)

    lines: List[str] = []
    lines.append("# Pipeline MCP Tool Catalog")
    lines.append("")
    lines.append(BEGIN)
    lines.append(f"> Updated: {updated_at}")
    if rev:
        lines.append(f"> Revision: `{rev}`")
    lines.append(
        "> Source of truth: `backend/mcp/pipeline_mcp_server.py` (parsed from the `tool_specs` literal)."
    )
    lines.append("> Regenerate: `python scripts/generate_pipeline_tooling_reference.py`")
    lines.append("")

    for cat in sorted(by_cat.keys()):
        lines.append(f"## {cat}")
        lines.append("")
        lines.append("| Tool | Required args | Description |")
        lines.append("| --- | --- | --- |")
        for tool in sorted(by_cat[cat], key=lambda t: str(t.get("name") or "")):
            name = str(tool.get("name") or "").strip()
            desc = str(tool.get("description") or "").strip().replace("\n", " ")
            required = ", ".join(_tool_required_fields(tool))
            lines.append(f"| `{name}` | `{required}` | {desc} |")
        lines.append("")

    lines.append(END)
    lines.append("")
    return "\n".join(lines)


def _render_agent_allowed_tools_md(
    allowed: Sequence[str],
    *,
    mcp_tool_names: Sequence[str],
    updated_at: str,
    rev: str | None,
) -> str:
    allowed_set = {str(t).strip() for t in allowed if str(t).strip()}
    mcp_set = {str(t).strip() for t in mcp_tool_names if str(t).strip()}

    missing_specs = sorted(allowed_set - mcp_set)
    extra_specs = sorted(mcp_set - allowed_set)

    lines: List[str] = []
    lines.append("# Pipeline Agent Tool Allowlist")
    lines.append("")
    lines.append(BEGIN)
    lines.append(f"> Updated: {updated_at}")
    if rev:
        lines.append(f"> Revision: `{rev}`")
    lines.append(
        "> Source of truth: `backend/bff/services/pipeline_agent_autonomous_loop.py` (`_PIPELINE_AGENT_ALLOWED_TOOLS`)."
    )
    lines.append("> Regenerate: `python scripts/generate_pipeline_tooling_reference.py`")
    lines.append("")
    lines.append("## Allowed tools (runtime-enforced)")
    lines.append("")
    for name in sorted(allowed_set):
        lines.append(f"- `{name}`")
    lines.append("")

    lines.append("## Consistency checks")
    lines.append("")
    if missing_specs:
        lines.append("### Allowed by agent, but missing from MCP tool specs (should be empty)")
        lines.append("")
        for name in missing_specs:
            lines.append(f"- `{name}`")
        lines.append("")
    else:
        lines.append("- `allowed_minus_mcp_specs`: empty (OK)")

    if extra_specs:
        lines.append("- `mcp_specs_minus_allowed`:")
        for name in extra_specs:
            lines.append(f"  - `{name}`")
    else:
        lines.append("- `mcp_specs_minus_allowed`: empty (OK)")

    lines.append("")
    lines.append(END)
    lines.append("")
    return "\n".join(lines)


def _render_ontology_allowed_tools_md(
    allowed: Sequence[str],
    *,
    updated_at: str,
    rev: str | None,
) -> str:
    """Render the ontology agent allowed tools markdown."""
    allowed_set = {str(t).strip() for t in allowed if str(t).strip()}

    # Group by category
    by_cat: Dict[str, List[str]] = {}
    for name in allowed_set:
        cat = _ontology_tool_category(name)
        by_cat.setdefault(cat, []).append(name)

    lines: List[str] = []
    lines.append("# Ontology Agent Tool Allowlist")
    lines.append("")
    lines.append(BEGIN_ONTOLOGY)
    lines.append(f"> Updated: {updated_at}")
    if rev:
        lines.append(f"> Revision: `{rev}`")
    lines.append(
        "> Source of truth: `backend/bff/services/ontology_agent_autonomous_loop.py` (`_ONTOLOGY_AGENT_ALLOWED_TOOLS`)."
    )
    lines.append("> Regenerate: `python scripts/generate_pipeline_tooling_reference.py`")
    lines.append("")
    lines.append("## Allowed tools by category")
    lines.append("")

    for cat in ["Initialization", "Class Metadata", "Property Management", "Relationship Management",
                "Schema Inference", "Validation", "Query", "Save / Preview", "Other"]:
        if cat not in by_cat:
            continue
        lines.append(f"### {cat}")
        lines.append("")
        for name in sorted(by_cat[cat]):
            lines.append(f"- `{name}`")
        lines.append("")

    lines.append(f"**Total: {len(allowed_set)} tools**")
    lines.append("")
    lines.append(END_ONTOLOGY)
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=OUT_DIR,
        help="Directory to write generated markdown (default: docs/reference/_generated)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    out_mcp = args.out_dir / OUT_MCP.name
    out_allowed = args.out_dir / OUT_ALLOWED.name
    out_ontology_allowed = args.out_dir / OUT_ONTOLOGY_ALLOWED.name

    # Pipeline tools
    tool_specs = _extract_mcp_tool_specs()
    allowed_tools = _extract_agent_allowed_tools()
    source_rev, updated_at = _detect_source_revision([PIPELINE_MCP_SERVER, PIPELINE_AGENT_LOOP])

    mcp_names = [str(t.get("name") or "").strip() for t in tool_specs if isinstance(t, dict)]

    mcp_md = _render_mcp_tools_md(tool_specs, updated_at=updated_at, rev=source_rev)
    allowed_md = _render_agent_allowed_tools_md(
        allowed_tools, mcp_tool_names=mcp_names, updated_at=updated_at, rev=source_rev
    )

    # Ontology tools
    ontology_allowed_tools = _extract_ontology_agent_allowed_tools()
    ontology_rev, ontology_updated_at = _detect_source_revision([ONTOLOGY_AGENT_LOOP])
    ontology_allowed_md = _render_ontology_allowed_tools_md(
        ontology_allowed_tools, updated_at=ontology_updated_at, rev=ontology_rev
    )

    outputs = [
        (out_mcp, mcp_md),
        (out_allowed, allowed_md),
        (out_ontology_allowed, ontology_allowed_md),
    ]

    if args.check:
        missing: List[Path] = []
        for path, content in outputs:
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            if existing != content:
                missing.append(path)
        if missing:
            for path in missing:
                print(f"{path} is out of date. Run: python scripts/generate_pipeline_tooling_reference.py")
            return 1
        return 0

    for path, content in outputs:
        _write_if_changed(path, content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
