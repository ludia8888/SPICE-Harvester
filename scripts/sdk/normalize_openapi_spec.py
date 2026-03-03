#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _is_null_schema(value: Any) -> bool:
    return isinstance(value, dict) and value.get("type") == "null" and len(value) == 1


def _normalize_nullable_composition(node: Any) -> Any:
    if isinstance(node, dict):
        for key in list(node.keys()):
            node[key] = _normalize_nullable_composition(node[key])

        for composition_key in ("anyOf", "oneOf"):
            raw = node.get(composition_key)
            if not isinstance(raw, list):
                continue

            filtered = [item for item in raw if not _is_null_schema(item)]
            if len(filtered) != len(raw):
                node["nullable"] = True
                if filtered:
                    node[composition_key] = filtered
                else:
                    node.pop(composition_key, None)
        return node

    if isinstance(node, list):
        return [_normalize_nullable_composition(item) for item in node]

    return node


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize OpenAPI 3.1 nullable anyOf/oneOf to improve SDK generator compatibility.",
    )
    parser.add_argument("--input", required=True, help="Path to source OpenAPI JSON file")
    parser.add_argument("--output", required=True, help="Path to normalized OpenAPI JSON file")
    args = parser.parse_args()

    source_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    normalized = _normalize_nullable_composition(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
