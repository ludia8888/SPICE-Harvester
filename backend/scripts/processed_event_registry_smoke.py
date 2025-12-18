"""
Smoke tests for ProcessedEventRegistry (Postgres).

Requires a reachable Postgres (POSTGRES_URL or POSTGRES_* env vars).
This is intended as an operator/developer sanity check, not a unit test.
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import suppress

from shared.services.processed_event_registry import ClaimDecision, ProcessedEventRegistry


async def _main() -> int:
    # Make tests run fast.
    os.environ.setdefault("PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS", "2")
    os.environ.setdefault("PROCESSED_EVENT_PG_POOL_MIN", "1")
    os.environ.setdefault("PROCESSED_EVENT_PG_POOL_MAX", "2")

    async def make_registry(owner: str) -> ProcessedEventRegistry:
        os.environ["PROCESSED_EVENT_OWNER"] = owner
        reg = ProcessedEventRegistry()
        await reg.connect()
        return reg

    reg1 = await make_registry("smoke-1")
    reg2 = await make_registry("smoke-2")

    try:
        # A) Lease reclaim after crash (no mark_done)
        r1 = await reg1.claim(handler="smoke", event_id="e1", aggregate_id="agg1", sequence_number=1)
        assert r1.decision == ClaimDecision.CLAIMED, r1

        r2 = await reg2.claim(handler="smoke", event_id="e1", aggregate_id="agg1", sequence_number=1)
        assert r2.decision == ClaimDecision.IN_PROGRESS, r2

        await asyncio.sleep(3)
        r2b = await reg2.claim(handler="smoke", event_id="e1", aggregate_id="agg1", sequence_number=1)
        assert r2b.decision == ClaimDecision.CLAIMED, r2b
        await reg2.mark_done(handler="smoke", event_id="e1", aggregate_id="agg1", sequence_number=1)

        # A) Heartbeat keeps lease alive
        r3 = await reg1.claim(handler="smoke", event_id="e2", aggregate_id="agg2", sequence_number=1)
        assert r3.decision == ClaimDecision.CLAIMED, r3

        async def heartbeats():
            while True:
                await asyncio.sleep(0.5)
                ok = await reg1.heartbeat(handler="smoke", event_id="e2")
                if not ok:
                    return

        hb = asyncio.create_task(heartbeats())
        await asyncio.sleep(3)
        r4 = await reg2.claim(handler="smoke", event_id="e2", aggregate_id="agg2", sequence_number=1)
        assert r4.decision == ClaimDecision.IN_PROGRESS, r4
        hb.cancel()
        with suppress(asyncio.CancelledError):
            await hb

        await asyncio.sleep(3)
        r5 = await reg2.claim(handler="smoke", event_id="e2", aggregate_id="agg2", sequence_number=1)
        assert r5.decision == ClaimDecision.CLAIMED, r5
        await reg2.mark_done(handler="smoke", event_id="e2", aggregate_id="agg2", sequence_number=1)

        # B) Stale sequence is ignored
        r6 = await reg1.claim(handler="smoke", event_id="e3", aggregate_id="agg1", sequence_number=1)
        assert r6.decision == ClaimDecision.STALE, r6

        print("ProcessedEventRegistry smoke tests passed")
        return 0
    finally:
        await reg1.close()
        await reg2.close()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(_main()))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except AssertionError as e:
        print(f"Assertion failed: {e}", file=sys.stderr)
        raise SystemExit(2)
    except Exception as e:
        print(f"Smoke test failed: {e}", file=sys.stderr)
        raise SystemExit(1)
