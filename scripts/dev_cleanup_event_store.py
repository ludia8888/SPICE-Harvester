#!/usr/bin/env python3
"""
Dev-only cleanup for event store objects + aggregate sequence rows.

This script is destructive and must never run in production.
It removes:
- Event store objects for matching aggregates (events + indexes + snapshots)
- Postgres aggregate_versions rows for matching aggregate_ids
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple

import aioboto3
import asyncpg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "backend"))

from shared.config.service_config import ServiceConfig  # noqa: E402
from shared.services.event_store import EventStore  # noqa: E402


def _ensure_dev_only() -> None:
    env = os.getenv("ENVIRONMENT", "development").strip().lower()
    if env in {"production", "prod"}:
        raise RuntimeError("Refusing to run dev cleanup in production environment.")


def _compile_matcher(
    *,
    names: Iterable[str],
    prefixes: Iterable[str],
    pattern: Optional[str],
) -> callable:
    name_set = {n for n in (name.strip() for name in names) if n}
    prefix_list = [p for p in (p.strip() for p in prefixes) if p]
    regex = re.compile(pattern) if pattern else None

    def matches(value: str) -> bool:
        if value in name_set:
            return True
        if any(value.startswith(prefix) for prefix in prefix_list):
            return True
        if regex and regex.search(value):
            return True
        return False

    return matches


async def _list_keys(s3, *, bucket: str, prefix: str) -> Iterable[str]:
    paginator = s3.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []) or []:
            key = item.get("Key")
            if key:
                yield key


def _date_index_key(occurred_at: str, event_id: str) -> Optional[str]:
    try:
        if occurred_at.endswith("Z"):
            occurred_at = occurred_at.replace("Z", "+00:00")
        dt = datetime.fromisoformat(occurred_at)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        ts_ms = int(dt.timestamp() * 1000)
        return (
            f"indexes/by-date/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
            f"{ts_ms:013d}_{event_id}.json"
        )
    except Exception:
        return None


async def _collect_aggregate_ids(
    s3,
    *,
    bucket: str,
    matcher,
) -> Set[Tuple[str, str]]:
    aggregates: Set[Tuple[str, str]] = set()
    async for key in _list_keys(s3, bucket=bucket, prefix="indexes/by-aggregate/"):
        parts = key.split("/", 4)
        if len(parts) < 4:
            continue
        _, _, aggregate_type, aggregate_id = parts[:4]
        if matcher(aggregate_id):
            aggregates.add((aggregate_type, aggregate_id))
    return aggregates


async def _collect_keys_for_aggregate(
    s3,
    *,
    bucket: str,
    aggregate_type: str,
    aggregate_id: str,
) -> Set[str]:
    keys: Set[str] = set()
    index_prefix = f"indexes/by-aggregate/{aggregate_type}/{aggregate_id}/"
    async for idx_key in _list_keys(s3, bucket=bucket, prefix=index_prefix):
        keys.add(idx_key)
        try:
            obj = await s3.get_object(Bucket=bucket, Key=idx_key)
            body = await obj["Body"].read()
            entry = json.loads(body)
        except Exception:
            continue

        event_id = entry.get("event_id")
        s3_key = entry.get("s3_key")
        occurred_at = entry.get("occurred_at")

        if isinstance(s3_key, str) and s3_key:
            keys.add(s3_key)
        if isinstance(event_id, str) and event_id:
            keys.add(f"indexes/by-event-id/{event_id}.json")
            if isinstance(occurred_at, str) and occurred_at:
                date_key = _date_index_key(occurred_at, event_id)
                if date_key:
                    keys.add(date_key)

    snapshot_prefix = f"snapshots/{aggregate_type}/{aggregate_id}/"
    async for snap_key in _list_keys(s3, bucket=bucket, prefix=snapshot_prefix):
        keys.add(snap_key)

    return keys


async def _delete_keys(s3, *, bucket: str, keys: Iterable[str], dry_run: bool) -> int:
    key_list = list(keys)
    if dry_run:
        return len(key_list)

    deleted = 0
    for key in key_list:
        try:
            await s3.delete_object(Bucket=bucket, Key=key)
            deleted += 1
        except Exception:
            continue
    return deleted


async def _cleanup_postgres(aggregate_ids: Iterable[str], dry_run: bool) -> int:
    aggregate_ids = [agg for agg in aggregate_ids if agg]
    if not aggregate_ids:
        return 0

    dsn = os.getenv("DEV_CLEANUP_POSTGRES_URL") or ServiceConfig.get_postgres_url()
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            "SELECT schemaname FROM pg_tables WHERE tablename='aggregate_versions'"
        )
        schemas = sorted({row["schemaname"] for row in rows})
        deleted = 0
        for schema in schemas:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
                continue
            if dry_run:
                deleted += len(aggregate_ids)
                continue
            result = await conn.execute(
                f"DELETE FROM {schema}.aggregate_versions WHERE aggregate_id = ANY($1)",
                aggregate_ids,
            )
            try:
                deleted += int(result.split()[-1])
            except Exception:
                pass
        return deleted
    finally:
        await conn.close()


async def _collect_postgres_matches(matcher) -> Set[str]:
    dsn = os.getenv("DEV_CLEANUP_POSTGRES_URL") or ServiceConfig.get_postgres_url()
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            "SELECT schemaname FROM pg_tables WHERE tablename='aggregate_versions'"
        )
        schemas = sorted({row["schemaname"] for row in rows})
        matches: Set[str] = set()
        for schema in schemas:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
                continue
            records = await conn.fetch(f"SELECT DISTINCT aggregate_id FROM {schema}.aggregate_versions")
            for record in records:
                aggregate_id = record["aggregate_id"]
                if isinstance(aggregate_id, str) and matcher(aggregate_id):
                    matches.add(aggregate_id)
        return matches
    finally:
        await conn.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Dev-only event store cleanup.")
    parser.add_argument("--db", action="append", default=[], help="Exact database name to match.")
    parser.add_argument("--prefix", action="append", default=[], help="Prefix match for database names.")
    parser.add_argument("--pattern", help="Regex pattern for database names.")
    parser.add_argument("--minio-endpoint", help="Override MinIO endpoint (e.g. http://127.0.0.1:9000).")
    parser.add_argument("--minio-access-key", help="Override MinIO access key.")
    parser.add_argument("--minio-secret-key", help="Override MinIO secret key.")
    parser.add_argument(
        "--postgres-url",
        help="Override Postgres URL (e.g. postgresql://user:pass@127.0.0.1:5432/db).",
    )
    parser.add_argument("--yes", action="store_true", help="Actually delete (otherwise dry-run).")
    parser.add_argument("--skip-postgres", action="store_true", help="Skip aggregate_versions cleanup.")
    args = parser.parse_args()

    _ensure_dev_only()

    matcher = _compile_matcher(names=args.db, prefixes=args.prefix, pattern=args.pattern)
    if not (args.db or args.prefix or args.pattern):
        raise SystemExit("Provide --db and/or --prefix/--pattern to match targets.")

    event_store = EventStore()
    if args.minio_endpoint:
        event_store.endpoint_url = args.minio_endpoint
    if args.minio_access_key:
        event_store.access_key = args.minio_access_key
    if args.minio_secret_key:
        event_store.secret_key = args.minio_secret_key
    bucket = event_store.bucket_name
    session = aioboto3.Session()

    async with session.client(**event_store._s3_client_kwargs()) as s3:  # noqa: SLF001
        aggregates = await _collect_aggregate_ids(s3, bucket=bucket, matcher=matcher)
        if not aggregates:
            print("No matching aggregates found in event store.")
        else:
            all_keys: Set[str] = set()
            for aggregate_type, aggregate_id in sorted(aggregates):
                keys = await _collect_keys_for_aggregate(
                    s3,
                    bucket=bucket,
                    aggregate_type=aggregate_type,
                    aggregate_id=aggregate_id,
                )
                all_keys.update(keys)

            deleted = await _delete_keys(s3, bucket=bucket, keys=all_keys, dry_run=not args.yes)
            print(
                f"{'Would delete' if not args.yes else 'Deleted'} "
                f"{deleted} event store objects from bucket '{bucket}'."
            )

    if not args.skip_postgres:
        if args.postgres_url:
            os.environ["DEV_CLEANUP_POSTGRES_URL"] = args.postgres_url
        aggregate_ids = {agg_id for _agg_type, agg_id in aggregates}
        aggregate_ids.update({name for name in args.db if name})
        aggregate_ids.update(await _collect_postgres_matches(matcher))
        deleted_rows = await _cleanup_postgres(sorted(aggregate_ids), dry_run=not args.yes)
        print(
            f"{'Would delete' if not args.yes else 'Deleted'} "
            f"{deleted_rows} aggregate_versions rows."
        )


if __name__ == "__main__":
    asyncio.run(main())
