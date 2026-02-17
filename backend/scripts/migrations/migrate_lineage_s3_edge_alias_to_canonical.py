"""
One-time migration: normalize compatibility S3 lineage edge alias to canonical name.

Usage:
  python backend/scripts/migrations/migrate_lineage_s3_edge_alias_to_canonical.py --schema spice_lineage
"""

from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize compatibility S3 lineage edge alias in Postgres")
    parser.add_argument("--dsn", default=None, help="Postgres DSN; defaults to POSTGRES_URL env")
    parser.add_argument("--schema", default="spice_lineage", help="Target schema (default: spice_lineage)")
    return parser


async def _run(*, dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"""
            UPDATE {schema}.lineage_edges
            SET edge_type = 'event_stored_in_object_store',
                metadata = metadata || jsonb_build_object('compat_edge_type', 'event_wrote_s3_object')
            WHERE edge_type = 'event_wrote_s3_object'
            """
        )
        print("lineage S3 edge alias migration completed")
    finally:
        await conn.close()


def main() -> None:
    args = _build_parser().parse_args()
    dsn = args.dsn or os.environ.get("POSTGRES_URL")
    if not dsn:
        raise RuntimeError("Postgres DSN is required (use --dsn or POSTGRES_URL env)")
    asyncio.run(_run(dsn=dsn, schema=args.schema))


if __name__ == "__main__":
    main()
