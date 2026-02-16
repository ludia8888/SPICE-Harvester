#!/usr/bin/env python3
"""Convert existing Mermaid .mmd files to portal MDX pages.

Reads .mmd files from docs/architecture/ and produces MDX pages under
docs-portal/docs/architecture/ with proper frontmatter, descriptions,
and Mermaid code fences.

Also produces the data-flow and service-interactions sequence diagram
as standalone MDX pages that embed the Mermaid diagrams.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
MMD_DIR = REPO_ROOT / "docs" / "architecture"
PORTAL_ARCH = REPO_ROOT / "docs-portal" / "docs" / "architecture"

# Known Mermaid diagram type keywords (first token on first non-comment line)
_MERMAID_TYPE_KEYWORDS = frozenset({
    "classDiagram", "sequenceDiagram", "graph", "flowchart",
    "erDiagram", "stateDiagram", "stateDiagram-v2",
    "gantt", "pie", "gitGraph", "journey", "mindmap",
    "timeline", "quadrantChart", "sankey-beta", "xychart-beta",
})


def _is_empty_diagram(mmd_content: str) -> bool:
    """Return True if the .mmd content is just a type declaration with no body.

    Strips whitespace and Mermaid comments (lines starting with %%).
    If the remaining content is only a diagram type keyword (optionally
    followed by a direction like TB/LR), the diagram is considered empty.
    """
    meaningful_lines = [
        line.strip()
        for line in mmd_content.strip().splitlines()
        if line.strip() and not line.strip().startswith("%%")
    ]
    if not meaningful_lines:
        return True
    if len(meaningful_lines) == 1:
        tokens = meaningful_lines[0].split()
        if tokens[0] in _MERMAID_TYPE_KEYWORDS:
            return True
    return False


# Map of .mmd filename (stem) → MDX metadata
MMD_CONFIGS: Dict[str, Dict] = {
    "data_flow": {
        "title": "Data Flow Architecture",
        "sidebar_label": "Data Flow",
        "sidebar_position": 2,
        "description": (
            "End-to-end data flow through SPICE Harvester, from client requests "
            "through the API Gateway, Service Layer, and Data Layer."
        ),
        "output_name": "auto-data-flow.mdx",
    },
    "service_interactions": {
        "title": "Service Interactions",
        "sidebar_label": "Service Interactions",
        "sidebar_position": 3,
        "description": (
            "Sequence diagram showing request flows between services: "
            "client → BFF → OMS/Funnel → data stores, including caching "
            "and error handling paths."
        ),
        "output_name": "auto-service-interactions.mdx",
    },
    "backend_classes": {
        "title": "Backend Class Architecture",
        "sidebar_label": "Class Diagrams",
        "sidebar_position": 4,
        "description": (
            "Class diagram of the core backend architecture including "
            "domain models, service layer, type system, and middleware."
        ),
        "output_name": "auto-class-diagrams.mdx",
    },
    "classes_SPICE_HARVESTER": {
        "title": "Full System Class Diagram",
        "sidebar_label": "System Classes",
        "sidebar_position": 5,
        "description": (
            "Comprehensive class diagram of the entire SPICE Harvester "
            "system generated from the Python codebase."
        ),
        "output_name": "auto-system-classes.mdx",
    },
}


def _render_mmd_to_mdx(mmd_content: str, config: Dict) -> str:
    """Wrap a .mmd file's content in an MDX page with frontmatter."""
    lines: List[str] = []

    # Frontmatter
    lines.append("---")
    lines.append(f"title: \"{config['title']}\"")
    lines.append(f"sidebar_label: \"{config['sidebar_label']}\"")
    lines.append(f"sidebar_position: {config['sidebar_position']}")
    lines.append("---")
    lines.append("")

    # Header
    lines.append(f"# {config['title']}")
    lines.append("")

    # Auto-generated notice
    lines.append(":::info Auto-Generated")
    lines.append("This diagram is auto-generated from `docs/architecture/` Mermaid sources.")
    lines.append("Do not edit manually. Run `python scripts/generate_portal_mermaid.py`.")
    lines.append(":::")
    lines.append("")

    # Description
    if config.get("description"):
        lines.append(config["description"])
        lines.append("")

    # Mermaid diagram
    lines.append("```mermaid")

    # Clean up the mmd content - remove any leading/trailing whitespace
    cleaned = mmd_content.strip()

    lines.append(cleaned)
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


def _render_placeholder_mdx(config: Dict) -> str:
    """Render a placeholder MDX page for a .mmd file with no diagram content."""
    lines: List[str] = []

    # Frontmatter
    lines.append("---")
    lines.append(f"title: \"{config['title']}\"")
    lines.append(f"sidebar_label: \"{config['sidebar_label']}\"")
    lines.append(f"sidebar_position: {config['sidebar_position']}")
    lines.append("---")
    lines.append("")

    # Header
    lines.append(f"# {config['title']}")
    lines.append("")

    # Auto-generated notice
    lines.append(":::info Auto-Generated")
    lines.append("This page is auto-generated from `docs/architecture/` Mermaid sources.")
    lines.append("Do not edit manually. Run `python scripts/generate_portal_mermaid.py`.")
    lines.append(":::")
    lines.append("")

    # Description
    if config.get("description"):
        lines.append(config["description"])
        lines.append("")

    # Placeholder notice instead of broken mermaid block
    lines.append(":::caution Diagram Not Yet Available")
    lines.append("This diagram is a placeholder and has not been populated yet.")
    lines.append("Once content is added to the source `.mmd` file, it will render automatically.")
    lines.append(":::")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate(check: bool = False) -> int:
    if not MMD_DIR.exists():
        print(f"WARN: {MMD_DIR} not found. Skipping Mermaid conversion.", file=sys.stderr)
        return 0

    outputs: Dict[Path, str] = {}

    for stem, config in MMD_CONFIGS.items():
        mmd_path = MMD_DIR / f"{stem}.mmd"
        if not mmd_path.exists():
            print(f"  SKIP: {mmd_path} (not found)")
            continue

        mmd_content = mmd_path.read_text(encoding="utf-8")
        if _is_empty_diagram(mmd_content):
            print(f"  PLACEHOLDER: {mmd_path} (empty diagram)")
            mdx_content = _render_placeholder_mdx(config)
        else:
            mdx_content = _render_mmd_to_mdx(mmd_content, config)
        output_path = PORTAL_ARCH / config["output_name"]
        outputs[output_path] = mdx_content

    if not outputs:
        print("WARN: No .mmd files found to convert.", file=sys.stderr)
        return 0

    failures: List[str] = []
    for path, content in outputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if check:
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            if existing != content:
                failures.append(str(path))
                print(f"  OUT OF DATE: {path}")
        else:
            path.write_text(content, encoding="utf-8")
            print(f"  WROTE: {path}")

    if check and failures:
        print(
            f"\n{len(failures)} Mermaid MDX file(s) out of date. "
            f"Run: python scripts/generate_portal_mermaid.py"
        )
        return 1

    if not check:
        print(f"\nConverted {len(outputs)} Mermaid diagrams to portal MDX")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return generate(check=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
