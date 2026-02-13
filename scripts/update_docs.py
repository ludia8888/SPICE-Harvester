#!/usr/bin/env python3
"""
Regenerate auto-managed documentation artifacts.

Use --check to verify outputs without modifying files.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

GENERATORS = [
    "scripts/generate_docs_index.py",
    "scripts/generate_api_reference.py",
    "scripts/generate_architecture_reference.py",
    "scripts/generate_backend_methods.py",
    "scripts/generate_error_taxonomy.py",
    "scripts/generate_pipeline_tooling_reference.py",
    "scripts/generate_repo_file_inventory.py",
]


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)


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
    for gen in GENERATORS:
        cmd = [sys.executable, gen]
        if args.check:
            cmd.append("--check")
        _run(cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
