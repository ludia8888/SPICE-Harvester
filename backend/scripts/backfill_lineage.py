"""
Lineage backfill utilities.

Goals:
- Eventual recovery when lineage recording is fail-open (ENABLE_LINEAGE=true but store can be unavailable).
- Two operational modes:
  1) queue: drain `spice_lineage.lineage_backfill_queue`
  2) replay: scan event store for a time range and rebuild lineage
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from typing import Optional

from oms.services.event_store import EventStore
from shared.services.lineage_store import LineageStore


def _parse_dt(value: str) -> datetime:
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def _run_queue(*, limit: int, db_name: Optional[str]) -> None:
    lineage = LineageStore()
    await lineage.initialize()

    event_store = EventStore()
    await event_store.connect()

    batch = await lineage.claim_backfill_batch(limit=limit, db_name=db_name)
    if not batch:
        print("No pending lineage backfills.")
        return

    ok = 0
    failed = 0
    for item in batch:
        event_id = str(item.get("event_id"))
        s3_key = item.get("s3_key")
        bucket = item.get("s3_bucket") or event_store.bucket_name

        try:
            if not s3_key:
                s3_key = await event_store.get_event_object_key(event_id=event_id)
            if not s3_key:
                raise RuntimeError("Unable to resolve s3_key for event_id")

            env = await event_store.read_event_by_key(key=str(s3_key))
            await lineage.record_event_envelope(env, s3_bucket=str(bucket), s3_key=str(s3_key))
            await lineage.mark_backfill_done(event_id=event_id)
            ok += 1
        except Exception as e:
            await lineage.mark_backfill_failed(event_id=event_id, error=str(e))
            failed += 1

    print(f"Queue backfill finished: ok={ok} failed={failed} total={len(batch)}")


async def _run_replay(*, from_dt: datetime, to_dt: Optional[datetime], limit: Optional[int]) -> None:
    lineage = LineageStore()
    await lineage.initialize()

    event_store = EventStore()
    await event_store.connect()

    processed = 0
    ok = 0
    failed = 0

    async for env in event_store.replay_events(from_dt, to_dt):
        try:
            s3_key = await event_store.get_event_object_key(event_id=str(env.event_id))
            await lineage.record_event_envelope(env, s3_bucket=event_store.bucket_name, s3_key=s3_key)
            ok += 1
        except Exception as e:
            failed += 1
            try:
                await lineage.enqueue_backfill(
                    envelope=env,
                    s3_bucket=event_store.bucket_name,
                    s3_key=None,
                    error=str(e),
                )
            except Exception:
                pass

        processed += 1
        if limit is not None and processed >= limit:
            break

    print(f"Replay backfill finished: ok={ok} failed={failed} total={processed}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill lineage from event store or queue.")
    parser.add_argument("--mode", choices=["queue", "replay"], default="queue")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--db-name", type=str, default=None)
    parser.add_argument("--from", dest="from_ts", type=str, default=None, help="ISO timestamp (required for replay)")
    parser.add_argument("--to", dest="to_ts", type=str, default=None, help="ISO timestamp (optional for replay)")

    args = parser.parse_args()

    if args.mode == "queue":
        await _run_queue(limit=int(args.limit), db_name=args.db_name)
        return

    if not args.from_ts:
        raise SystemExit("--from is required for replay mode")

    from_dt = _parse_dt(args.from_ts)
    to_dt = _parse_dt(args.to_ts) if args.to_ts else None
    await _run_replay(from_dt=from_dt, to_dt=to_dt, limit=(None if args.limit <= 0 else int(args.limit)))


if __name__ == "__main__":
    asyncio.run(main())

