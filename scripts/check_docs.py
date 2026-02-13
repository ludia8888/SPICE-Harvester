#!/usr/bin/env python3
"""
Enterprise docs QA: enforce code↔docs lockstep and fail on Sphinx warnings.

What this checks:
- All repo doc generators are up to date (`--check`).
- Sphinx HTML build succeeds with warnings treated as errors.

Optional:
- Linkcheck / doctest / coverage builders.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = REPO_ROOT / "docs"
BUILD_DIR = DOCS_DIR / "_build"
AUTOAPI_DIR = DOCS_DIR / "reference" / "autoapi"


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--linkcheck", action="store_true", help="Run Sphinx linkcheck builder")
    parser.add_argument("--doctest", action="store_true", help="Run Sphinx doctest builder")
    parser.add_argument("--coverage", action="store_true", help="Run Sphinx coverage builder")
    parser.add_argument(
        "--allow-warnings",
        action="store_true",
        help="Do not treat Sphinx warnings as errors (still fails on hard build errors).",
    )
    parser.add_argument(
        "--nitpicky",
        action="store_true",
        help="Enable Sphinx nitpicky mode (-n). This can be very strict for large docs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    generators = [
        ["scripts/generate_docs_index.py", "--check"],
        ["scripts/generate_api_reference.py", "--check"],
        ["scripts/generate_architecture_reference.py", "--check"],
        ["scripts/generate_backend_methods.py", "--check"],
        ["scripts/generate_error_taxonomy.py", "--check"],
        ["scripts/generate_pipeline_tooling_reference.py", "--check"],
        ["scripts/generate_repo_file_inventory.py", "--check"],
    ]
    for gen in generators:
        _run([sys.executable, *gen])

    # We already verified generators are up-to-date; keep the Sphinx build pure.
    env = dict(os.environ)
    env["SPICE_DOCS_SKIP_GENERATION"] = "1"

    # Remove stale AutoAPI artifacts for deleted/renamed modules to avoid build-time
    # reference errors from previously generated pages.
    if AUTOAPI_DIR.exists():
        shutil.rmtree(AUTOAPI_DIR)

    sphinx_base = [sys.executable, "-m", "sphinx"]
    if not args.allow_warnings:
        sphinx_base.append("-W")
    if args.nitpicky:
        sphinx_base.append("-n")

    _run(
        [*sphinx_base, "-b", "html", str(DOCS_DIR), str(BUILD_DIR / "html")],
        env=env,
    )
    if args.linkcheck:
        _run(
            [*sphinx_base, "-b", "linkcheck", str(DOCS_DIR), str(BUILD_DIR / "linkcheck")],
            env=env,
        )
    if args.doctest:
        _run(
            [*sphinx_base, "-b", "doctest", str(DOCS_DIR), str(BUILD_DIR / "doctest")],
            env=env,
        )
    if args.coverage:
        _run(
            [*sphinx_base, "-b", "coverage", str(DOCS_DIR), str(BUILD_DIR / "coverage")],
            env=env,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
