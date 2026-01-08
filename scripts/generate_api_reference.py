#!/usr/bin/env python3
"""Generate the auto-managed Endpoint Index in docs/API_REFERENCE.md from BFF OpenAPI."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

BEGIN_MARKER = "<!-- BEGIN AUTO-GENERATED ENDPOINTS -->"
END_MARKER = "<!-- END AUTO-GENERATED ENDPOINTS -->"
METHOD_ORDER = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


def _method_sort_key(method: str) -> Tuple[int, str]:
    try:
        return (METHOD_ORDER.index(method), method)
    except ValueError:
        return (len(METHOD_ORDER), method)


def _load_openapi() -> Dict:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "backend"))

    # Suppress noisy startup logs from app import.
    logging.disable(logging.CRITICAL)
    from bff import main as bff_main  # noqa: WPS433

    return bff_main.app.openapi()


def _render_endpoint_index(schema: Dict) -> str:
    paths = schema.get("paths", {})
    tag_map: Dict[str, List[Tuple[str, str]]] = {}

    for path, ops in paths.items():
        if not path.startswith("/api/v1"):
            continue
        for method, meta in ops.items():
            method_upper = method.upper()
            if method_upper not in METHOD_ORDER:
                continue
            tags = meta.get("tags") or ["Untagged"]
            tag = tags[0]
            tag_map.setdefault(tag, []).append((method_upper, path))

    lines: List[str] = []
    lines.append(BEGIN_MARKER)
    lines.append("> Generated from OpenAPI by `scripts/generate_api_reference.py`. Do not edit manually.")
    lines.append("")

    for tag in sorted(tag_map, key=lambda value: value.lower()):
        lines.append(f"### {tag}")
        entries = sorted(tag_map[tag], key=lambda item: (item[1], _method_sort_key(item[0])))
        for method, path in entries:
            lines.append(f"- `{method} {path}`")
        lines.append("")

    lines.append(END_MARKER)
    return "\n".join(lines).rstrip() + "\n"


def _replace_block(content: str, replacement: str) -> str:
    if BEGIN_MARKER in content and END_MARKER in content:
        before = content.split(BEGIN_MARKER, 1)[0].rstrip()
        after = content.split(END_MARKER, 1)[1].lstrip()
        return f"{before}\n\n{replacement}\n{after}".rstrip() + "\n"

    if "## Endpoint Index" not in content:
        raise ValueError("docs/API_REFERENCE.md missing '## Endpoint Index' section.")

    head, tail = content.split("## Endpoint Index", 1)
    return f"{head}## Endpoint Index\n\n{replacement}\n{tail.lstrip()}".rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "docs" / "API_REFERENCE.md",
        help="Path to API reference markdown (default: docs/API_REFERENCE.md)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path: Path = args.output

    if not output_path.exists():
        raise SystemExit(f"Missing API reference file: {output_path}")

    schema = _load_openapi()
    replacement = _render_endpoint_index(schema)
    updated = _replace_block(output_path.read_text(encoding="utf-8"), replacement)

    if args.check:
        if output_path.read_text(encoding="utf-8") != updated:
            print(f"{output_path} is out of date. Run: python scripts/generate_api_reference.py")
            return 1
        return 0

    output_path.write_text(updated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
