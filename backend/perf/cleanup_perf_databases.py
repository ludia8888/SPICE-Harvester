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
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, Optional
from urllib.parse import urlparse

import asyncpg
import httpx


def _load_repo_dotenv() -> Dict[str, str]:
    try:
        repo_root = Path(__file__).resolve().parents[2]
    except Exception:
        return {}

    env_path = repo_root / ".env"
    if not env_path.exists():
        return {}

    values: Dict[str, str] = {}
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def _postgres_dsn_candidates() -> list[str]:
    explicit = (os.getenv("POSTGRES_URL") or "").strip()
    if explicit:
        return [explicit]

    dotenv = _load_repo_dotenv()
    port_override = (os.getenv("POSTGRES_PORT_HOST") or "").strip() or (dotenv.get("POSTGRES_PORT_HOST") or "").strip()

    ports: list[int] = []
    if port_override:
        try:
            ports.append(int(port_override))
        except ValueError:
            pass

    for p in (15433, 5433, 5432):
        if p not in ports:
            ports.append(p)

    return [f"postgresql://spiceadmin:spicepass123@localhost:{p}/spicedb" for p in ports]


def _bff_base_url() -> str:
    base = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
    if base.endswith("/api/v1"):
        return base
    return f"{base}/api/v1"


def _admin_token() -> str:
    for key in ("SMOKE_ADMIN_TOKEN", "BFF_ADMIN_TOKEN", "ADMIN_TOKEN"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return "change_me"


def _extract_command_id(payload: object) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        cmd = data.get("command_id")
        if isinstance(cmd, str) and cmd:
            return cmd
    cmd = payload.get("command_id")
    if isinstance(cmd, str) and cmd:
        return cmd
    return None


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
    schema = (os.getenv("EVENT_STORE_SEQUENCE_SCHEMA") or "spice_event_registry").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError(f"Invalid EVENT_STORE_SEQUENCE_SCHEMA: {schema!r}")

    prefix = (os.getenv("EVENT_STORE_SEQUENCE_HANDLER_PREFIX") or "write_side").strip() or "write_side"
    handler = f"{prefix}:Database"

    row = await conn.fetchrow(
        f"SELECT last_sequence FROM {schema}.aggregate_versions WHERE handler=$1 AND aggregate_id=$2",
        handler,
        db_name,
    )
    if not row:
        return None
    return int(row["last_sequence"] or 0)


async def _wait_for_command(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    command_id: str,
    timeout_seconds: int = 180,
) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last: dict = {}
    while time.monotonic() < deadline:
        resp = await client.get(f"{base_url}/commands/{command_id}/status")
        if resp.status_code == 404:
            await asyncio.sleep(0.5)
            continue
        resp.raise_for_status()
        last = resp.json()
        status = last.get("status")
        if status in {"COMPLETED", "FAILED", "CANCELLED"}:
            return last
        await asyncio.sleep(0.5)
    raise TimeoutError(last)


async def _list_databases(client: httpx.AsyncClient, *, base_url: str) -> list[str]:
    resp = await client.get(f"{base_url}/databases")
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict) and isinstance(data.get("databases"), list):
        items = data.get("databases") or []
    elif isinstance(payload, dict) and isinstance(payload.get("databases"), list):
        items = payload.get("databases") or []
    else:
        items = []

    names: list[str] = []
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("name"), str):
            names.append(item["name"])
        elif isinstance(item, str):
            names.append(item)
    return names


async def _delete_database(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    db_name: str,
    expected_seq: int,
) -> dict:
    resp = await client.delete(f"{base_url}/databases/{db_name}", params={"expected_seq": expected_seq})
    if resp.status_code == 409:
        raise RuntimeError(f"OCC conflict deleting {db_name} (expected_seq={expected_seq})")
    resp.raise_for_status()

    payload = resp.json()
    cmd = _extract_command_id(payload)
    if cmd:
        return await _wait_for_command(client, base_url=base_url, command_id=cmd, timeout_seconds=180)
    return payload if isinstance(payload, dict) else {"raw": payload}


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
            names = await _list_databases(client, base_url=base_url)
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
                result = await _delete_database(client, base_url=base_url, db_name=name, expected_seq=expected_seq)
                print(f"  -> {result.get('status')}")
        finally:
            await conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

