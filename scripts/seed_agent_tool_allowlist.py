#!/usr/bin/env python3
"""
Seed minimal agent tool allowlist entries.

Usage:
  python scripts/seed_agent_tool_allowlist.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root / "backend"))

from shared.services.agent_tool_registry import AgentToolRegistry  # noqa: E402


SEED_POLICIES = [
    {
        "tool_id": "system.health",
        "method": "GET",
        "path": "/api/v1/health",
        "risk_level": "read",
        "requires_approval": False,
        "requires_idempotency_key": False,
        "status": "ACTIVE",
    },
    {
        "tool_id": "database.list",
        "method": "GET",
        "path": "/api/v1/databases",
        "risk_level": "read",
        "requires_approval": False,
        "requires_idempotency_key": False,
        "status": "ACTIVE",
    },
    {
        "tool_id": "database.get",
        "method": "GET",
        "path": "/api/v1/databases/{db_name}",
        "risk_level": "read",
        "requires_approval": False,
        "requires_idempotency_key": False,
        "status": "ACTIVE",
    },
    {
        "tool_id": "query.label",
        "method": "POST",
        "path": "/api/v1/databases/{db_name}/query",
        "risk_level": "read",
        "requires_approval": False,
        "requires_idempotency_key": False,
        "status": "ACTIVE",
    },
    {
        "tool_id": "graph.query",
        "method": "POST",
        "path": "/api/v1/graph-query/{db_name}",
        "risk_level": "read",
        "requires_approval": False,
        "requires_idempotency_key": False,
        "status": "ACTIVE",
    },
    {
        "tool_id": "graph.health",
        "method": "GET",
        "path": "/api/v1/graph-query/health",
        "risk_level": "read",
        "requires_approval": False,
        "requires_idempotency_key": False,
        "status": "ACTIVE",
    },
]


async def main() -> None:
    registry = AgentToolRegistry()
    await registry.initialize()
    try:
        for policy in SEED_POLICIES:
            record = await registry.upsert_tool_policy(**policy)
            print(f"✅ {record.tool_id} -> {record.method} {record.path}")
    finally:
        await registry.close()


if __name__ == "__main__":
    asyncio.run(main())
