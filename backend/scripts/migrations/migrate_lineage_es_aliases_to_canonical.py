"""
One-time migration: normalize compatibility ES lineage aliases to canonical names.

Usage:
  python backend/scripts/migrations/migrate_lineage_es_aliases_to_canonical.py --schema spice_lineage
"""

from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize compatibility ES lineage aliases in Postgres")
    parser.add_argument("--dsn", default=None, help="Postgres DSN; defaults to POSTGRES_URL env")
    parser.add_argument("--schema", default="spice_lineage", help="Target schema (default: spice_lineage)")
    return parser


async def _run(*, dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"""
            INSERT INTO {schema}.lineage_nodes (
                node_id, node_type, label, created_at, recorded_at,
                event_id, aggregate_type, aggregate_id, artifact_kind, artifact_key,
                db_name, branch, run_id, code_sha, schema_version, metadata
            )
            SELECT
                regexp_replace(node_id, '^artifact:elasticsearch:', 'artifact:es:') AS node_id,
                node_type,
                label,
                created_at,
                recorded_at,
                event_id,
                aggregate_type,
                aggregate_id,
                'es' AS artifact_kind,
                COALESCE(NULLIF(artifact_key, ''), regexp_replace(node_id, '^artifact:elasticsearch:', '')) AS artifact_key,
                db_name,
                branch,
                run_id,
                code_sha,
                schema_version,
                metadata || jsonb_build_object('compat_node_id', node_id)
            FROM {schema}.lineage_nodes
            WHERE node_id LIKE 'artifact:elasticsearch:%'
            ON CONFLICT (node_id) DO UPDATE
            SET label = COALESCE(EXCLUDED.label, {schema}.lineage_nodes.label),
                recorded_at = GREATEST({schema}.lineage_nodes.recorded_at, EXCLUDED.recorded_at),
                db_name = COALESCE(EXCLUDED.db_name, {schema}.lineage_nodes.db_name),
                branch = COALESCE(EXCLUDED.branch, {schema}.lineage_nodes.branch),
                run_id = COALESCE(EXCLUDED.run_id, {schema}.lineage_nodes.run_id),
                code_sha = COALESCE(EXCLUDED.code_sha, {schema}.lineage_nodes.code_sha),
                schema_version = COALESCE(EXCLUDED.schema_version, {schema}.lineage_nodes.schema_version),
                artifact_kind = 'es',
                artifact_key = COALESCE(EXCLUDED.artifact_key, {schema}.lineage_nodes.artifact_key),
                metadata = {schema}.lineage_nodes.metadata || EXCLUDED.metadata
            """
        )
        await conn.execute(
            f"""
            UPDATE {schema}.lineage_edges
            SET from_node_id = regexp_replace(from_node_id, '^artifact:elasticsearch:', 'artifact:es:')
            WHERE from_node_id LIKE 'artifact:elasticsearch:%'
            """
        )
        await conn.execute(
            f"""
            UPDATE {schema}.lineage_edges
            SET to_node_id = regexp_replace(to_node_id, '^artifact:elasticsearch:', 'artifact:es:')
            WHERE to_node_id LIKE 'artifact:elasticsearch:%'
            """
        )
        await conn.execute(
            f"""
            UPDATE {schema}.lineage_edges
            SET edge_type = 'event_materialized_es_document'
            WHERE edge_type = 'event_wrote_es_document'
            """
        )
        await conn.execute(
            f"""
            UPDATE {schema}.lineage_nodes
            SET artifact_kind = 'es',
                artifact_key = COALESCE(NULLIF(artifact_key, ''), regexp_replace(node_id, '^artifact:es:', ''))
            WHERE node_id LIKE 'artifact:es:%'
            """
        )
        await conn.execute(
            f"""
            DELETE FROM {schema}.lineage_nodes
            WHERE node_id LIKE 'artifact:elasticsearch:%'
            """
        )
        print("lineage ES alias migration completed")
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
