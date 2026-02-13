#!/usr/bin/env python3
"""Generate fully auto-managed API reference markdown from BFF OpenAPI."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

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


def _collect_tagged_routes(schema: Dict) -> Dict[str, List[Tuple[str, str]]]:
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
    return tag_map


def _render_api_reference(schema: Dict) -> str:
    info = schema.get("info", {})
    title = str(info.get("title") or "API")
    version = str(info.get("version") or "unknown")
    openapi_version = str(schema.get("openapi") or "unknown")
    routes_by_tag = _collect_tagged_routes(schema)

    lines: List[str] = []
    lines.append("# API Reference (Auto-Generated)")
    lines.append("")
    lines.append("> Generated from BFF OpenAPI by `scripts/generate_api_reference.py`.")
    lines.append("> Do not edit manually.")
    lines.append("")
    lines.append("## OpenAPI Metadata")
    lines.append("")
    lines.append(f"- Title: `{title}`")
    lines.append(f"- Version: `{version}`")
    lines.append(f"- OpenAPI: `{openapi_version}`")
    lines.append("")
    lines.append("## Endpoint Index (`/api/v1`)")
    lines.append("")

    for tag in sorted(routes_by_tag, key=lambda value: value.lower()):
        lines.append(f"### {tag}")
        entries = sorted(routes_by_tag[tag], key=lambda item: (item[1], _method_sort_key(item[0])))
        for method, path in entries:
            lines.append(f"- `{method} {path}`")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


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

    schema = _load_openapi()
    updated = _render_api_reference(schema)
    current = output_path.read_text(encoding="utf-8") if output_path.exists() else ""

    if args.check:
        if current != updated:
            print(f"{output_path} is out of date. Run: python scripts/generate_api_reference.py")
            return 1
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(updated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
