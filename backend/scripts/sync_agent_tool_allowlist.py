"""
Sync the canonical agent tool allowlist bundle into Postgres.

Usage (from repo root, with PYTHONPATH including `backend/`):
  python backend/scripts/sync_agent_tool_allowlist.py --only-if-empty
  python backend/scripts/sync_agent_tool_allowlist.py --bundle backend/shared/policies/agent_tool_allowlist.json
"""

from __future__ import annotations

import argparse
import asyncio
import json

from shared.services.agent_tool_allowlist import bootstrap_agent_tool_allowlist
from shared.services.agent_tool_registry import AgentToolRegistry


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync agent tool allowlist bundle into Postgres")
    parser.add_argument(
        "--bundle",
        default=None,
        help="Path to agent_tool_allowlist.json (defaults to the repo bundle)",
    )
    parser.add_argument(
        "--only-if-empty",
        action="store_true",
        help="Skip syncing if any agent tool policies already exist",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    registry = AgentToolRegistry()
    await registry.initialize()
    try:
        result = await bootstrap_agent_tool_allowlist(
            tool_registry=registry,
            bundle_path=args.bundle,
            only_if_empty=bool(args.only_if_empty),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        await registry.close()
    return 0


def main() -> None:
    args = _build_parser().parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()

