#!/usr/bin/env python3
# ruff: noqa: E402
"""
Verify projection consistency between Event Store (durable source) and Elasticsearch.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

# Ensure backend/ is importable when the script is executed directly.
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from shared.config.search_config import get_instances_index_name
from shared.config.settings import get_settings
from shared.services.core.projection_consistency import (
    INSTANCE_EVENT_TYPES,
    build_instance_expectations,
    evaluate_projection_document,
)
from shared.services.storage.elasticsearch_service import create_elasticsearch_service
from shared.services.storage.event_store import EventStore


def _parse_ts(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


async def _collect_instance_events(
    *,
    event_store: EventStore,
    from_ts: datetime,
    to_ts: Optional[datetime],
    max_events: int,
) -> List[Any]:
    events: List[Any] = []
    async for envelope in event_store.replay_events(
        from_timestamp=from_ts,
        to_timestamp=to_ts,
        event_types=list(INSTANCE_EVENT_TYPES),
    ):
        if str(envelope.event_type) not in INSTANCE_EVENT_TYPES:
            continue
        events.append(envelope)
        if len(events) >= max_events:
            break
    return events


async def _run(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    from_ts = _parse_ts(args.from_ts) if args.from_ts else now - timedelta(minutes=args.from_minutes_ago)
    to_ts = _parse_ts(args.to_ts) if args.to_ts else None

    event_store = EventStore()
    await event_store.connect()

    settings = get_settings()
    es = create_elasticsearch_service(settings)
    await es.connect()

    try:
        events = await _collect_instance_events(
            event_store=event_store,
            from_ts=from_ts,
            to_ts=to_ts,
            max_events=int(args.max_events),
        )
        build_result = build_instance_expectations(
            events,
            db_name=args.db_name,
            branch=args.branch,
        )

        mismatches: List[Dict[str, Any]] = []
        mismatch_reasons: Counter[str] = Counter()

        for expectation in sorted(
            build_result.expectations.values(),
            key=lambda x: (
                x.key.db_name,
                x.key.branch,
                x.key.class_id,
                x.key.instance_id,
            ),
        ):
            index_name = get_instances_index_name(expectation.key.db_name, branch=expectation.key.branch)
            try:
                doc = await es.get_document(index=index_name, doc_id=expectation.key.instance_id)
            except Exception as exc:
                reason = "es_read_error"
                mismatch_reasons[reason] += 1
                mismatches.append(
                    {
                        "reason": reason,
                        "error": str(exc),
                        "index": index_name,
                        "db_name": expectation.key.db_name,
                        "branch": expectation.key.branch,
                        "class_id": expectation.key.class_id,
                        "instance_id": expectation.key.instance_id,
                        "latest_event_id": expectation.latest_event_id,
                        "latest_event_type": expectation.latest_event_type,
                    }
                )
                continue

            reason = evaluate_projection_document(expectation, doc)
            if reason is None:
                continue

            mismatch_reasons[reason] += 1
            mismatches.append(
                {
                    "reason": reason,
                    "index": index_name,
                    "db_name": expectation.key.db_name,
                    "branch": expectation.key.branch,
                    "class_id": expectation.key.class_id,
                    "instance_id": expectation.key.instance_id,
                    "latest_event_id": expectation.latest_event_id,
                    "latest_event_type": expectation.latest_event_type,
                    "latest_event_sequence": expectation.sequence_number,
                    "latest_event_occurred_at": _to_iso(expectation.occurred_at),
                    "expected_state": "exists" if expectation.should_exist else "deleted",
                }
            )

        payload = {
            "from": _to_iso(from_ts),
            "to": _to_iso(to_ts) if to_ts else None,
            "event_types": sorted(INSTANCE_EVENT_TYPES),
            "scanned_events": len(events),
            "skipped_events": len(build_result.skipped_event_ids),
            "expected_instances": len(build_result.expectations),
            "mismatch_count": len(mismatches),
            "mismatch_reasons": dict(mismatch_reasons),
            "mismatches": mismatches[: int(args.max_mismatch_details)],
            "truncated_mismatches": max(0, len(mismatches) - int(args.max_mismatch_details)),
        }

        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(
                "Projection consistency check: "
                f"events={payload['scanned_events']} expected_instances={payload['expected_instances']} "
                f"mismatches={payload['mismatch_count']}"
            )
            if payload["mismatch_reasons"]:
                print(f"Mismatch reasons: {payload['mismatch_reasons']}")
            if payload["mismatches"]:
                print("Sample mismatches:")
                for row in payload["mismatches"]:
                    print(
                        f"- {row['reason']} "
                        f"{row['db_name']}/{row['branch']}/{row['class_id']}/{row['instance_id']} "
                        f"(event={row['latest_event_type']}:{row['latest_event_id']})"
                    )

        if len(build_result.expectations) == 0 and args.fail_on_empty:
            return 2
        return 1 if mismatches else 0
    finally:
        await es.disconnect()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-ts", type=str, default=None, help="UTC ISO timestamp (e.g. 2026-01-01T00:00:00Z)")
    parser.add_argument("--to-ts", type=str, default=None, help="UTC ISO timestamp upper bound")
    parser.add_argument(
        "--from-minutes-ago",
        type=int,
        default=180,
        help="When --from-ts is omitted, use now - N minutes (default: 180)",
    )
    parser.add_argument("--max-events", type=int, default=5000, help="Maximum replayed events")
    parser.add_argument("--db-name", type=str, default=None, help="Optional db_name filter")
    parser.add_argument("--branch", type=str, default=None, help="Optional branch filter")
    parser.add_argument(
        "--max-mismatch-details",
        type=int,
        default=100,
        help="Max mismatch rows included in output",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON payload")
    parser.add_argument(
        "--fail-on-empty",
        action="store_true",
        help="Exit with code 2 when no expectation rows are produced",
    )
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
