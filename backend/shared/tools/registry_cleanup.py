#!/usr/bin/env python3
"""
Dev-only cleanup for TerminusDB databases created by tests/smoke runs.

This script deletes databases by name prefix via the BFF API.
Optionally filters by "last updated" age using Postgres.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import urlparse

import asyncpg
import httpx

from shared.config.settings import get_settings


DEFAULT_TEST_PREFIXES = [
    "agent_progress_",
    "agentprog",
    "openapi_smoke_",
    "test_",
    "full_api_test_",
    "lightweight_test_",
    "e2e_",
    "redis_down_",
    "dual_outage_",
    "palantir_test_",
    "palantir_schema_test_",
    "palantir_debug_",
    "tmp_",
    "debug_",
    "debugfix_",
    "agentdemo_",
    "cardinality_",
    "chaos_db_",
    "test_branch_",
    "test_vc_",
]

DEFAULT_TEST_NAMES = [
    "demo",
    "demo_db",
    "testdb",
    "test_db",
    "core-db",
    "nonexistent",
]


def _env_list(key: str) -> list[str]:
    raw = (os.environ.get(key) or "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _is_safe_endpoint(base_url: str) -> bool:
    host = (urlparse(base_url).hostname or "").lower()
    return host in {
        "localhost",
        "127.0.0.1",
        "0.0.0.0",
        "bff",
    }


def _ensure_dev_only(base_url: str) -> None:
    if get_settings().is_production:
        raise RuntimeError("Refusing to run registry cleanup in production environment.")
    if not _is_safe_endpoint(base_url):
        raise RuntimeError(f"Refusing to run against unsafe BFF endpoint: {base_url}")


def _normalize_base_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/api/v1"):
        return base
    return f"{base}/api/v1"


def _resolve_bff_base_url() -> str:
    base = get_settings().services.bff_base_url
    return _normalize_base_url(base)


def _resolve_admin_token() -> str:
    cfg = get_settings()
    token = str(cfg.clients.bff_admin_token or cfg.clients.oms_client_token or cfg.auth.admin_token or "").strip()
    if token:
        return token.split(",")[0].strip()
    return ""


def _matches_any_prefix(name: str, prefixes: Iterable[str]) -> bool:
    return any(prefix and name.startswith(prefix) for prefix in prefixes)


def _matches_any_name(name: str, names: Iterable[str]) -> bool:
    return any(candidate and name == candidate for candidate in names)


def _extract_command_id(payload: object) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    direct = payload.get("command_id") or payload.get("commandId")
    if isinstance(direct, str) and direct:
        return direct
    data = payload.get("data")
    if isinstance(data, dict):
        direct = data.get("command_id") or data.get("commandId")
        if isinstance(direct, str) and direct:
            return direct
        write = data.get("write")
        if isinstance(write, dict):
            commands = write.get("commands")
            if isinstance(commands, list) and commands:
                first = commands[0]
                if isinstance(first, dict):
                    direct = first.get("command_id") or first.get("commandId")
                    if isinstance(direct, str) and direct:
                        return direct
    return None


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
        status = last.get("status") or last.get("state")
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


async def _connect_postgres() -> asyncpg.Connection:
    return await asyncpg.connect(get_settings().database.postgres_url)


def _sequence_schema() -> str:
    schema = str(get_settings().event_sourcing.event_store_sequence_schema or "spice_event_registry").strip()
    if not schema.replace("_", "").isalnum() or schema[0].isdigit():
        raise ValueError(f"Invalid EVENT_STORE_SEQUENCE_SCHEMA: {schema!r}")
    return schema


def _database_handler() -> str:
    prefix = str(get_settings().event_sourcing.event_store_sequence_handler_prefix or "write_side").strip() or "write_side"
    return f"{prefix}:Database"


async def _fetch_updated_at_map(
    conn: asyncpg.Connection,
    *,
    db_names: list[str],
) -> dict[str, datetime]:
    if not db_names:
        return {}
    schema = _sequence_schema()
    handler = _database_handler()
    rows = await conn.fetch(
        f"SELECT aggregate_id, updated_at FROM {schema}.aggregate_versions WHERE handler=$1 AND aggregate_id = ANY($2)",
        handler,
        db_names,
    )
    result: dict[str, datetime] = {}
    for row in rows:
        updated = row.get("updated_at")
        if isinstance(updated, datetime):
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            result[str(row.get("aggregate_id"))] = updated
    return result


async def _delete_database(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    db_name: str,
) -> dict:
    resp = await client.delete(f"{base_url}/databases/{db_name}")
    if resp.status_code == 404:
        return {"status": "missing"}
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    cmd = _extract_command_id(payload)
    if cmd:
        return await _wait_for_command(client, base_url=base_url, command_id=cmd, timeout_seconds=180)
    return payload if isinstance(payload, dict) else {"raw": payload}


@dataclass(frozen=True)
class CleanupPlan:
    prefixes: list[str]
    names: list[str]
    explicit: list[str]
    min_age_seconds: int
    interval_seconds: int
    dry_run: bool
    loop: bool
    quiet: bool


def _build_plan(args: argparse.Namespace) -> CleanupPlan:
    prefixes = [p for p in (args.prefix or []) if p]
    names = [n for n in (args.name or []) if n]
    explicit = [d for d in (args.db or []) if d]

    if not prefixes:
        prefixes = _env_list("REGISTRY_CLEANUP_PREFIXES") or list(DEFAULT_TEST_PREFIXES)
    if not names:
        names = _env_list("REGISTRY_CLEANUP_NAMES") or list(DEFAULT_TEST_NAMES)

    min_age_seconds = args.min_age_seconds
    if min_age_seconds is None:
        raw = (os.environ.get("REGISTRY_CLEANUP_MIN_AGE_SECONDS") or "").strip()
        min_age_seconds = int(raw) if raw.isdigit() else 300

    interval_seconds = args.interval_seconds
    if interval_seconds is None:
        raw = (os.environ.get("REGISTRY_CLEANUP_INTERVAL_SECONDS") or "").strip()
        interval_seconds = int(raw) if raw.isdigit() else 600

    return CleanupPlan(
        prefixes=prefixes,
        names=names,
        explicit=explicit,
        min_age_seconds=max(0, int(min_age_seconds)),
        interval_seconds=max(30, int(interval_seconds)),
        dry_run=bool(args.dry_run) or not bool(args.yes),
        loop=bool(args.loop),
        quiet=bool(args.quiet),
    )


async def _run_once(plan: CleanupPlan, *, base_url: str, token: str) -> int:
    headers = {"Authorization": f"Bearer {token}", "Accept-Language": "en"}
    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        if plan.explicit:
            targets = plan.explicit
        else:
            names = await _list_databases(client, base_url=base_url)
            targets = [
                name
                for name in names
                if _matches_any_prefix(name, plan.prefixes) or _matches_any_name(name, plan.names)
            ]

        targets = sorted(set(targets))
        if not targets:
            if not plan.quiet:
                print("No matching databases found.")
            return 0

        if not plan.quiet:
            print("Targets:")
            for name in targets:
                print(f"- {name}")

        if plan.dry_run:
            if not plan.quiet:
                print("Dry run: not deleting. Re-run with --yes to delete.")
            return 0

        updated_map: dict[str, datetime] = {}
        if plan.min_age_seconds > 0:
            try:
                conn = await _connect_postgres()
            except Exception as exc:
                if not plan.quiet:
                    print(f"Skipping cleanup: cannot connect to Postgres ({exc})")
                return 0
            try:
                updated_map = await _fetch_updated_at_map(conn, db_names=targets)
            finally:
                await conn.close()

        now = datetime.now(timezone.utc)
        deleted = 0
        for name in targets:
            if plan.min_age_seconds > 0:
                updated_at = updated_map.get(name)
                if not updated_at:
                    if not plan.quiet:
                        print(f"Skipping {name}: no updated_at found.")
                    continue
                age_seconds = (now - updated_at).total_seconds()
                if age_seconds < plan.min_age_seconds:
                    if not plan.quiet:
                        print(f"Skipping {name}: age {int(age_seconds)}s < min {plan.min_age_seconds}s")
                    continue

            if not plan.quiet:
                print(f"Deleting {name}...")
            try:
                result = await _delete_database(client, base_url=base_url, db_name=name)
            except Exception as exc:
                if not plan.quiet:
                    print(f"Failed to delete {name}: {exc}")
                continue
            deleted += 1
            if not plan.quiet:
                status = result.get("status") or result.get("state") or "ok"
                print(f"  -> {status}")

        if not plan.quiet:
            print(f"Deleted {deleted} database(s).")
        return deleted


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dev-only cleanup of test/smoke TerminusDB databases.")
    parser.add_argument("--prefix", action="append", default=[], help="DB name prefix to target (repeatable)")
    parser.add_argument("--name", action="append", default=[], help="Explicit DB name to target (repeatable)")
    parser.add_argument("--db", action="append", default=[], help="Explicit DB name to delete (repeatable)")
    parser.add_argument("--min-age-seconds", type=int, default=None, help="Skip DBs updated within this window")
    parser.add_argument("--interval-seconds", type=int, default=None, help="Loop interval (when --loop)")
    parser.add_argument("--loop", action="store_true", help="Run continuously on an interval")
    parser.add_argument("--dry-run", action="store_true", help="Only print targets; do not delete")
    parser.add_argument("--yes", action="store_true", help="Actually delete (required)")
    parser.add_argument("--quiet", action="store_true", help="Reduce log output")
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    if (os.environ.get("REGISTRY_CLEANUP_ENABLED") or "true").strip().lower() in {"0", "false", "no"}:
        return 0

    plan = _build_plan(args)
    base_url = _resolve_bff_base_url()
    _ensure_dev_only(base_url)

    token = _resolve_admin_token()
    if not token:
        raise RuntimeError("Missing admin token (set ADMIN_TOKEN or BFF_ADMIN_TOKEN).")

    if plan.loop:
        while True:
            try:
                await _run_once(plan, base_url=base_url, token=token)
            except Exception as exc:
                if not plan.quiet:
                    print(f"Cleanup loop error: {exc}")
            await asyncio.sleep(plan.interval_seconds)
    else:
        await _run_once(plan, base_url=base_url, token=token)
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
