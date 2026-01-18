#!/usr/bin/env python3
"""
Dev-only cleanup for MinIO/S3 object storage artifacts created by tests.

This script is destructive and must never run in production.
It removes test objects from:
- Event store bucket (events/indexes/snapshots) by aggregate-id matcher
- Instance event bucket (db_name prefix)
- lakeFS bucket (datasets/pipelines/writeback prefixes)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple
from urllib.parse import urlparse

import aioboto3
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT / "backend"))

from shared.config.settings import get_settings, reload_settings  # noqa: E402
from shared.services.event_store import EventStore  # noqa: E402


DEFAULT_TEST_PREFIXES = [
    "agent_progress_",
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
]


def _ensure_dev_only() -> None:
    env = os.getenv("ENVIRONMENT", "development").strip().lower()
    if env in {"production", "prod"}:
        raise RuntimeError("Refusing to run object store cleanup in production environment.")


def _parse_env_list(key: str) -> list[str]:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _load_repo_dotenv() -> dict[str, str]:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def _env_or_dotenv(dotenv: dict[str, str], key: str, default: str) -> str:
    return (os.getenv(key) or dotenv.get(key) or default).strip()


def _hydrate_env_from_dotenv() -> None:
    dotenv = _load_repo_dotenv()
    os.environ.setdefault("DOCKER_CONTAINER", "false")
    if "MINIO_ENDPOINT_URL" not in os.environ:
        port = _env_or_dotenv(dotenv, "MINIO_PORT_HOST", "9000")
        os.environ["MINIO_ENDPOINT_URL"] = f"http://localhost:{port}"


def _is_safe_endpoint(endpoint_url: str) -> bool:
    host = (urlparse(endpoint_url).hostname or "").lower()
    if not host:
        return False
    return host in {
        "localhost",
        "127.0.0.1",
        "0.0.0.0",
        "minio",
        "spice-minio",
        "spice_minio",
    } or host.endswith(".localhost")


@dataclass(frozen=True)
class Matcher:
    names: tuple[str, ...]
    prefixes: tuple[str, ...]
    regex: Optional[re.Pattern]

    def matches(self, value: str) -> bool:
        if not value:
            return False
        if value in self.names:
            return True
        if any(value.startswith(prefix) for prefix in self.prefixes):
            return True
        if self.regex and self.regex.search(value):
            return True
        return False

    def any(self) -> bool:
        return bool(self.names or self.prefixes or self.regex)


def _compile_matcher(
    *,
    names: Iterable[str],
    prefixes: Iterable[str],
    pattern: Optional[str],
) -> Matcher:
    name_set = tuple({n for n in (n.strip() for n in names) if n})
    prefix_list = tuple({p for p in (p.strip() for p in prefixes) if p})
    regex = re.compile(pattern) if pattern else None
    return Matcher(names=name_set, prefixes=prefix_list, regex=regex)


async def _list_keys(s3, *, bucket: str, prefix: str = "") -> Iterable[str]:
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


async def _collect_aggregate_ids(s3, *, bucket: str, matcher: Matcher) -> Set[Tuple[str, str]]:
    aggregates: Set[Tuple[str, str]] = set()
    async for key in _list_keys(s3, bucket=bucket, prefix="indexes/by-aggregate/"):
        parts = key.split("/", 4)
        if len(parts) < 4:
            continue
        _, _, aggregate_type, aggregate_id = parts[:4]
        if matcher.matches(aggregate_id):
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
    if dry_run or not key_list:
        return len(key_list)

    deleted = 0
    for chunk_start in range(0, len(key_list), 1000):
        chunk = key_list[chunk_start : chunk_start + 1000]
        try:
            resp = await s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
            )
            deleted += len(chunk) - len((resp.get("Errors") or []))
        except Exception:
            for key in chunk:
                try:
                    await s3.delete_object(Bucket=bucket, Key=key)
                    deleted += 1
                except Exception:
                    continue
    return deleted


def _match_instance_key(key: str, matcher: Matcher) -> bool:
    if not key:
        return False
    segment = key.split("/", 1)[0]
    return matcher.matches(segment)


def _match_lakefs_key(key: str, matcher: Matcher) -> bool:
    if not key:
        return False
    parts = [part for part in key.split("/") if part]
    for marker in ("datasets", "pipelines"):
        if marker in parts:
            idx = parts.index(marker)
            if idx + 1 < len(parts) and matcher.matches(parts[idx + 1]):
                return True
    for part in parts:
        if part.startswith("writeback-") and matcher.matches(part[len("writeback-") :]):
            return True
    return False


async def _cleanup_bucket(
    s3,
    *,
    bucket: str,
    matcher: Matcher,
    key_matcher,
    dry_run: bool,
) -> int:
    if not matcher.any():
        return 0

    try:
        await s3.head_bucket(Bucket=bucket)
    except ClientError:
        return 0

    to_delete: list[str] = []
    deleted = 0
    async for key in _list_keys(s3, bucket=bucket):
        if key_matcher(key, matcher):
            to_delete.append(key)
        if len(to_delete) >= 1000:
            deleted += await _delete_keys(s3, bucket=bucket, keys=to_delete, dry_run=dry_run)
            to_delete = []
    if to_delete:
        deleted += await _delete_keys(s3, bucket=bucket, keys=to_delete, dry_run=dry_run)
    return deleted


async def _cleanup_event_store(
    *,
    matcher: Matcher,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket_name: str,
    dry_run: bool,
) -> int:
    if not matcher.any():
        return 0
    session = aioboto3.Session()
    event_store = EventStore()
    event_store.endpoint_url = endpoint_url
    event_store.access_key = access_key
    event_store.secret_key = secret_key
    event_store.bucket_name = bucket_name
    async with session.client(**event_store._s3_client_kwargs()) as s3:  # noqa: SLF001
        aggregates = await _collect_aggregate_ids(s3, bucket=bucket_name, matcher=matcher)
        if not aggregates:
            return 0
        all_keys: Set[str] = set()
        for aggregate_type, aggregate_id in sorted(aggregates):
            keys = await _collect_keys_for_aggregate(
                s3,
                bucket=bucket_name,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
            )
            all_keys.update(keys)
        deleted = await _delete_keys(s3, bucket=bucket_name, keys=all_keys, dry_run=dry_run)
        return deleted


async def main() -> int:
    parser = argparse.ArgumentParser(description="Dev-only object store cleanup.")
    parser.add_argument("--db", action="append", default=[], help="Exact database name to match.")
    parser.add_argument("--prefix", action="append", default=[], help="Prefix match for database names.")
    parser.add_argument("--pattern", help="Regex pattern for database names.")
    parser.add_argument("--minio-endpoint", help="Override MinIO endpoint (e.g. http://127.0.0.1:9000).")
    parser.add_argument("--minio-access-key", help="Override MinIO access key.")
    parser.add_argument("--minio-secret-key", help="Override MinIO secret key.")
    parser.add_argument("--no-defaults", action="store_true", help="Skip built-in test prefixes/names.")
    parser.add_argument("--skip-event-store", action="store_true", help="Skip event store cleanup.")
    parser.add_argument("--skip-instance-bucket", action="store_true", help="Skip instance bucket cleanup.")
    parser.add_argument("--skip-lakefs", action="store_true", help="Skip lakeFS bucket cleanup.")
    parser.add_argument("--skip-dataset-bucket", action="store_true", help="Skip dataset artifact bucket cleanup.")
    parser.add_argument("--yes", action="store_true", help="Actually delete (otherwise dry-run).")
    parser.add_argument("--quiet", action="store_true", help="Reduce output.")
    parser.add_argument("--force", action="store_true", help="Allow non-local endpoints.")
    args = parser.parse_args()

    _ensure_dev_only()
    _hydrate_env_from_dotenv()
    reload_settings()

    extra_names = _parse_env_list("TEST_DB_NAMES")
    extra_prefixes = _parse_env_list("TEST_DB_PREFIXES")

    names = list(args.db)
    prefixes = list(args.prefix)
    if not args.no_defaults:
        names.extend(DEFAULT_TEST_NAMES)
        prefixes.extend(DEFAULT_TEST_PREFIXES)
    names.extend(extra_names)
    prefixes.extend(extra_prefixes)

    matcher = _compile_matcher(names=names, prefixes=prefixes, pattern=args.pattern)
    if not matcher.any():
        raise SystemExit("Provide --db and/or --prefix/--pattern or set TEST_DB_PREFIXES/TEST_DB_NAMES.")

    settings = get_settings().storage
    endpoint_url = args.minio_endpoint or settings.minio_endpoint_url
    access_key = args.minio_access_key or settings.minio_access_key
    secret_key = args.minio_secret_key or settings.minio_secret_key

    if not args.force and not _is_safe_endpoint(endpoint_url):
        raise RuntimeError(
            f"Refusing to delete from non-local endpoint: {endpoint_url}. "
            "Use --force to override."
        )

    dry_run = not args.yes
    session = aioboto3.Session()

    total_deleted = 0
    event_store_bucket = get_settings().storage.event_store_bucket
    instance_bucket = get_settings().storage.instance_bucket
    lakefs_bucket = os.getenv("LAKEFS_STORAGE_BUCKET") or "lakefs"
    dataset_bucket = os.getenv("DATASET_ARTIFACTS_BUCKET") or "dataset-artifacts"

    if not args.quiet:
        print(f"MinIO endpoint: {endpoint_url}")
        print(f"Dry run: {dry_run}")

    async with session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    ) as s3:
        if not args.skip_instance_bucket:
            deleted = await _cleanup_bucket(
                s3,
                bucket=instance_bucket,
                matcher=matcher,
                key_matcher=_match_instance_key,
                dry_run=dry_run,
            )
            total_deleted += deleted
            if not args.quiet:
                print(
                    f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects from instance bucket '{instance_bucket}'."
                )

        if not args.skip_lakefs:
            deleted = await _cleanup_bucket(
                s3,
                bucket=lakefs_bucket,
                matcher=matcher,
                key_matcher=_match_lakefs_key,
                dry_run=dry_run,
            )
            total_deleted += deleted
            if not args.quiet:
                print(
                    f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects from lakeFS bucket '{lakefs_bucket}'."
                )

        if not args.skip_dataset_bucket and dataset_bucket != lakefs_bucket:
            deleted = await _cleanup_bucket(
                s3,
                bucket=dataset_bucket,
                matcher=matcher,
                key_matcher=_match_lakefs_key,
                dry_run=dry_run,
            )
            total_deleted += deleted
            if not args.quiet:
                print(
                    f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects from dataset bucket '{dataset_bucket}'."
                )

    if not args.skip_event_store:
        deleted = await _cleanup_event_store(
            matcher=matcher,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=event_store_bucket,
            dry_run=dry_run,
        )
        total_deleted += deleted
        if not args.quiet:
            print(
                f"{'Would delete' if dry_run else 'Deleted'} {deleted} objects from event store bucket '{event_store_bucket}'."
            )

    if not args.quiet:
        print(f"Total {'eligible' if dry_run else 'deleted'} objects: {total_deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
