#!/usr/bin/env python3
"""
Cleanup helper for perf-created databases.

Why this exists:
- BFF DB deletion requires `expected_seq` (OCC).
- Older deployed BFF images may not include `/api/v1/databases/{db}/expected-seq` helper yet.
- This script derives `expected_seq` directly from Postgres (`aggregate_versions`) and deletes via BFF.

Safety:
- Requires explicit `--yes` to actually delete.
"""

from __future__ import annotations

import argparse
import asyncio
import re
from typing import Iterable, Optional

import asyncpg
import httpx

from shared.config.settings import get_settings
from shared.tools.bff_admin_api import delete_database, list_databases, normalize_base_url


def _postgres_dsn_candidates() -> list[str]:
    cfg = get_settings()
    return [cfg.database.postgres_url]


def _bff_base_url() -> str:
    return normalize_base_url(get_settings().services.bff_base_url)


def _admin_token() -> str:
    cfg = get_settings()
    token = str(cfg.clients.bff_admin_token or cfg.clients.oms_client_token or "").strip()
    return token or "change_me"


async def _connect_postgres() -> asyncpg.Connection:
    last_error: Optional[Exception] = None
    for dsn in _postgres_dsn_candidates():
        try:
            conn = await asyncpg.connect(dsn)
            return conn
        except Exception as exc:
            last_error = exc
            continue
    raise RuntimeError(f"Could not connect to Postgres (candidates={_postgres_dsn_candidates()}): {last_error}")


async def _fetch_db_expected_seq(conn: asyncpg.Connection, *, db_name: str) -> Optional[int]:
    schema = str(get_settings().event_sourcing.event_store_sequence_schema or "spice_event_registry").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError(f"Invalid EVENT_STORE_SEQUENCE_SCHEMA: {schema!r}")

    prefix = str(get_settings().event_sourcing.event_store_sequence_handler_prefix or "write_side").strip() or "write_side"
    handler = f"{prefix}:Database"

    row = await conn.fetchrow(
        f"SELECT last_sequence FROM {schema}.aggregate_versions WHERE handler=$1 AND aggregate_id=$2",
        handler,
        db_name,
    )
    if not row:
        return None
    return int(row["last_sequence"] or 0)


def _matches_any_prefix(name: str, prefixes: Iterable[str]) -> bool:
    for prefix in prefixes:
        if prefix and name.startswith(prefix):
            return True
    return False


async def main() -> int:
    parser = argparse.ArgumentParser(description="Delete perf-created databases via BFF (OCC-safe).")
    parser.add_argument("--prefix", action="append", default=["perf_"], help="DB name prefix to target (repeatable)")
    parser.add_argument("--db", action="append", default=[], help="Explicit DB name to delete (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="Only print targets; do not delete")
    parser.add_argument("--yes", action="store_true", help="Actually delete (required)")
    args = parser.parse_args()

    prefixes = [p for p in (args.prefix or []) if p]
    explicit_dbs = [d for d in (args.db or []) if d]

    base_url = _bff_base_url()
    token = _admin_token()
    headers = {"X-Admin-Token": token, "Accept-Language": "en"}

    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        if explicit_dbs:
            targets = explicit_dbs
        else:
            names = await list_databases(client, base_url=base_url)
            targets = [n for n in names if _matches_any_prefix(n, prefixes)]

        targets = sorted(set(targets))
        if not targets:
            print("No matching databases found.")
            return 0

        print("Targets:")
        for name in targets:
            print(f"- {name}")

        if args.dry_run or not args.yes:
            print("Dry run: not deleting. Re-run with --yes to delete.")
            return 0

        conn = await _connect_postgres()
        try:
            for name in targets:
                expected_seq = await _fetch_db_expected_seq(conn, db_name=name)
                if expected_seq is None:
                    print(f"Skipping {name}: no aggregate_versions row found.")
                    continue
                print(f"Deleting {name} (expected_seq={expected_seq})...")
                result = await delete_database(
                    client,
                    base_url=base_url,
                    db_name=name,
                    expected_seq=expected_seq,
                )
                print(f"  -> {result.get('status')}")
        finally:
            await conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
