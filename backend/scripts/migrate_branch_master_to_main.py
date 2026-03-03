#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import asyncpg
import httpx

from shared.config.settings import get_settings
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConflictError, LakeFSError

BRANCH_VALUE_FROM = "master"
BRANCH_VALUE_TO = "main"

BRANCH_KEYS = {
    "branch",
    "base_branch",
    "source_branch",
    "view_branch",
    "default_branch",
    "target_branch",
    "branch_name",
    "branchName",
    "sourceBranchName",
    "targetBranchName",
    "onto_branch",
}
BRANCH_LIST_KEYS = {"branches", "protected_branches", "fallback_branches"}


@dataclass
class ColumnChange:
    table: str
    column: str
    kind: str
    candidates: int
    updated: int = 0
    skipped_reason: Optional[str] = None


@dataclass
class LakeFSChange:
    repository: str
    action: str
    detail: str


@dataclass
class MigrationAudit:
    mode: str
    generated_at: str
    text_columns: List[ColumnChange] = field(default_factory=list)
    json_columns: List[ColumnChange] = field(default_factory=list)
    lakefs: List[LakeFSChange] = field(default_factory=list)

    def render_markdown(self) -> str:
        text_candidates = sum(item.candidates for item in self.text_columns)
        text_updated = sum(item.updated for item in self.text_columns)
        json_candidates = sum(item.candidates for item in self.json_columns)
        json_updated = sum(item.updated for item in self.json_columns)
        lines: List[str] = [
            "# Branch `master` → `main` Data Migration Report (2026-03-03)",
            "",
            f"- Generated at: `{self.generated_at}`",
            f"- Mode: `{self.mode}`",
            "",
            "## Summary",
            f"- Text/Varchar candidates: `{text_candidates}`",
            f"- Text/Varchar updated: `{text_updated}`",
            f"- JSON/JSONB candidate rows: `{json_candidates}`",
            f"- JSON/JSONB updated rows: `{json_updated}`",
            f"- lakeFS actions: `{len(self.lakefs)}`",
            "",
            "## Text/Varchar Columns",
        ]
        if not self.text_columns:
            lines.append("- None")
        else:
            for item in self.text_columns:
                suffix = f" (skipped: {item.skipped_reason})" if item.skipped_reason else ""
                lines.append(
                    f"- `{item.table}.{item.column}`: candidates={item.candidates}, updated={item.updated}{suffix}"
                )
        lines.extend(["", "## JSON/JSONB Columns"])
        if not self.json_columns:
            lines.append("- None")
        else:
            for item in self.json_columns:
                suffix = f" (skipped: {item.skipped_reason})" if item.skipped_reason else ""
                lines.append(
                    f"- `{item.table}.{item.column}`: candidates={item.candidates}, updated={item.updated}{suffix}"
                )
        lines.extend(["", "## lakeFS"])
        if not self.lakefs:
            lines.append("- None")
        else:
            for item in self.lakefs:
                lines.append(f"- `{item.repository}`: {item.action} ({item.detail})")
        return "\n".join(lines) + "\n"


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _table_name(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _decode_json_value(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    return raw


def _replace_branch_values(payload: Any, *, parent_key: Optional[str] = None) -> Tuple[Any, bool]:
    changed = False
    if isinstance(payload, dict):
        out: Dict[str, Any] = {}
        for key, value in payload.items():
            if key in BRANCH_KEYS and isinstance(value, str) and value.strip().lower() == BRANCH_VALUE_FROM:
                out[key] = BRANCH_VALUE_TO
                changed = True
                continue
            child, child_changed = _replace_branch_values(value, parent_key=key)
            out[key] = child
            changed = changed or child_changed
        return out, changed
    if isinstance(payload, list):
        out_list: List[Any] = []
        for item in payload:
            if (
                parent_key in BRANCH_LIST_KEYS
                and isinstance(item, str)
                and item.strip().lower() == BRANCH_VALUE_FROM
            ):
                out_list.append(BRANCH_VALUE_TO)
                changed = True
                continue
            child, child_changed = _replace_branch_values(item, parent_key=None)
            out_list.append(child)
            changed = changed or child_changed
        return out_list, changed
    return payload, False


async def _list_primary_key_columns(
    conn: asyncpg.Connection,
    *,
    table_schema: str,
    table_name: str,
) -> List[str]:
    rows = await conn.fetch(
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
        ORDER BY kcu.ordinal_position
        """,
        table_schema,
        table_name,
    )
    return [str(row["column_name"]) for row in rows]


async def _list_unique_key_columns_including(
    conn: asyncpg.Connection,
    *,
    table_schema: str,
    table_name: str,
    target_column: str,
) -> List[List[str]]:
    rows = await conn.fetch(
        """
        SELECT tc.constraint_name, kcu.column_name, kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.table_schema = $1
          AND tc.table_name = $2
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY tc.constraint_name, kcu.ordinal_position
        """,
        table_schema,
        table_name,
    )
    grouped: Dict[str, List[str]] = {}
    for row in rows:
        name = str(row["constraint_name"])
        grouped.setdefault(name, []).append(str(row["column_name"]))
    return [cols for cols in grouped.values() if target_column in cols]


async def _prune_conflicting_master_rows(
    conn: asyncpg.Connection,
    *,
    table_schema: str,
    table_name: str,
    branch_column: str,
    unique_sets: Sequence[Sequence[str]],
) -> int:
    table_ref = _table_name(table_schema, table_name)
    branch_ref = _quote_ident(branch_column)
    deleted_total = 0
    for unique_cols in unique_sets:
        compare_cols = [col for col in unique_cols if col != branch_column]
        if not compare_cols:
            continue
        compare_sql = " AND ".join(
            f"m.{_quote_ident(col)} IS NOT DISTINCT FROM p.{_quote_ident(col)}"
            for col in compare_cols
        )
        delete_sql = f"""
            WITH doomed AS (
                SELECT m.ctid
                FROM {table_ref} m
                JOIN {table_ref} p
                  ON {compare_sql}
                WHERE m.{branch_ref} = $1
                  AND p.{branch_ref} = $2
            )
            DELETE FROM {table_ref} t
            USING doomed d
            WHERE t.ctid = d.ctid
        """
        result = await conn.execute(delete_sql, BRANCH_VALUE_FROM, BRANCH_VALUE_TO)
        deleted_total += int(str(result).split()[-1])
    return deleted_total


async def _rowwise_update_text_column(
    conn: asyncpg.Connection,
    *,
    table_schema: str,
    table_name: str,
    column_name: str,
    pk_columns: Sequence[str],
) -> Tuple[int, int]:
    if not pk_columns:
        return 0, 0
    table_ref = _table_name(table_schema, table_name)
    column_ref = _quote_ident(column_name)
    pk_select = ", ".join(_quote_ident(name) for name in pk_columns)
    rows = await conn.fetch(
        f"SELECT {pk_select} FROM {table_ref} WHERE {column_ref} = $1",
        BRANCH_VALUE_FROM,
    )
    updated = 0
    conflicts = 0
    where_clause = " AND ".join(
        f"{_quote_ident(pk)} = ${idx + 2}" for idx, pk in enumerate(pk_columns)
    )
    branch_param_index = len(pk_columns) + 2
    update_sql = (
        f"UPDATE {table_ref} SET {column_ref} = $1 "
        f"WHERE {where_clause} AND {column_ref} = ${branch_param_index}"
    )
    for row in rows:
        params: List[Any] = [BRANCH_VALUE_TO]
        params.extend(row[pk] for pk in pk_columns)
        params.append(BRANCH_VALUE_FROM)
        try:
            result = await conn.execute(update_sql, *params)
            updated += int(str(result).split()[-1])
        except asyncpg.UniqueViolationError:
            conflicts += 1
    return updated, conflicts


async def _migrate_text_columns(
    conn: asyncpg.Connection,
    *,
    apply: bool,
    audit: MigrationAudit,
) -> None:
    columns = await conn.fetch(
        """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND data_type IN ('text', 'character varying', 'character')
        ORDER BY table_schema, table_name, column_name
        """
    )
    unique_cache: Dict[Tuple[str, str, str], List[List[str]]] = {}
    pk_cache: Dict[Tuple[str, str], List[str]] = {}
    for row in columns:
        schema = str(row["table_schema"])
        table = str(row["table_name"])
        column = str(row["column_name"])
        table_ref = _table_name(schema, table)
        column_ref = _quote_ident(column)
        count_query = (
            f"SELECT COUNT(*)::bigint AS count FROM {table_ref} "
            f"WHERE {column_ref} = $1"
        )
        count = int((await conn.fetchrow(count_query, BRANCH_VALUE_FROM))["count"])
        if count <= 0:
            continue
        updated = 0
        skipped_reason: Optional[str] = None
        pruned_conflicts = 0
        if apply:
            unique_key = (schema, table, column)
            unique_sets = unique_cache.get(unique_key)
            if unique_sets is None:
                unique_sets = await _list_unique_key_columns_including(
                    conn,
                    table_schema=schema,
                    table_name=table,
                    target_column=column,
                )
                unique_cache[unique_key] = unique_sets
            if unique_sets:
                pruned_conflicts = await _prune_conflicting_master_rows(
                    conn,
                    table_schema=schema,
                    table_name=table,
                    branch_column=column,
                    unique_sets=unique_sets,
                )
            update_query = (
                f"UPDATE {table_ref} SET {column_ref} = $1 "
                f"WHERE {column_ref} = $2"
            )
            try:
                result = await conn.execute(update_query, BRANCH_VALUE_TO, BRANCH_VALUE_FROM)
                updated = int(str(result).split()[-1])
            except asyncpg.UniqueViolationError:
                pk_key = (schema, table)
                pk_columns = pk_cache.get(pk_key)
                if pk_columns is None:
                    pk_columns = await _list_primary_key_columns(
                        conn,
                        table_schema=schema,
                        table_name=table,
                    )
                    pk_cache[pk_key] = pk_columns
                rowwise_updated, rowwise_conflicts = await _rowwise_update_text_column(
                    conn,
                    table_schema=schema,
                    table_name=table,
                    column_name=column,
                    pk_columns=pk_columns,
                )
                updated += rowwise_updated
                skipped_reason = (
                    f"rowwise_fallback_conflicts={rowwise_conflicts}"
                    if rowwise_conflicts > 0
                    else "rowwise_fallback_applied"
                )
        if apply and pruned_conflicts > 0:
            prune_note = f"pruned_conflicts={pruned_conflicts}"
            skipped_reason = (
                f"{skipped_reason}; {prune_note}" if skipped_reason else prune_note
            )
        audit.text_columns.append(
            ColumnChange(
                table=f"{schema}.{table}",
                column=column,
                kind="text",
                candidates=count,
                updated=updated,
                skipped_reason=skipped_reason,
            )
        )


async def _migrate_json_columns(
    conn: asyncpg.Connection,
    *,
    apply: bool,
    audit: MigrationAudit,
) -> None:
    columns = await conn.fetch(
        """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND data_type IN ('json', 'jsonb')
        ORDER BY table_schema, table_name, column_name
        """
    )
    pk_cache: Dict[Tuple[str, str], List[str]] = {}

    for row in columns:
        schema = str(row["table_schema"])
        table = str(row["table_name"])
        column = str(row["column_name"])
        data_type = str(row["data_type"])
        table_ref = _table_name(schema, table)
        column_ref = _quote_ident(column)
        cache_key = (schema, table)
        pk_columns = pk_cache.get(cache_key)
        if pk_columns is None:
            pk_columns = await _list_primary_key_columns(
                conn, table_schema=schema, table_name=table
            )
            pk_cache[cache_key] = pk_columns
        if not pk_columns:
            audit.json_columns.append(
                ColumnChange(
                    table=f"{schema}.{table}",
                    column=column,
                    kind=data_type,
                    candidates=0,
                    updated=0,
                    skipped_reason="no primary key",
                )
            )
            continue

        pk_select = ", ".join(_quote_ident(name) for name in pk_columns)
        rows = await conn.fetch(
            (
                f"SELECT {pk_select}, {column_ref} AS payload FROM {table_ref} "
                f"WHERE {column_ref} IS NOT NULL AND {column_ref}::text LIKE '%\"master\"%'"
            )
        )
        if not rows:
            continue

        candidates = len(rows)
        updated = 0
        for db_row in rows:
            payload = _decode_json_value(db_row["payload"])
            patched, changed = _replace_branch_values(payload)
            if not changed:
                continue
            updated += 1
            if not apply:
                continue
            where_clause = " AND ".join(
                f"{_quote_ident(pk)} = ${idx + 2}" for idx, pk in enumerate(pk_columns)
            )
            update_query = (
                f"UPDATE {table_ref} "
                f"SET {column_ref} = $1::{data_type} "
                f"WHERE {where_clause}"
            )
            pk_values = [db_row[pk] for pk in pk_columns]
            await conn.execute(update_query, json.dumps(patched), *pk_values)

        audit.json_columns.append(
            ColumnChange(
                table=f"{schema}.{table}",
                column=column,
                kind=data_type,
                candidates=candidates,
                updated=updated,
            )
        )


async def _list_lakefs_repositories(
    *,
    api_url: str,
    access_key: str,
    secret_key: str,
    timeout_seconds: float,
) -> List[str]:
    async with httpx.AsyncClient(
        base_url=f"{api_url.rstrip('/')}/api/v1",
        auth=(access_key, secret_key),
        timeout=timeout_seconds,
    ) as client:
        resp = await client.get("/repositories")
    if not resp.is_success:
        raise RuntimeError(f"lakeFS list repositories failed ({resp.status_code}): {resp.text}")
    payload = resp.json()
    items: Iterable[Any]
    if isinstance(payload, dict):
        items = payload.get("results") or payload.get("repositories") or []
    elif isinstance(payload, list):
        items = payload
    else:
        items = []
    names: List[str] = []
    for item in items:
        if isinstance(item, dict):
            candidate = item.get("id") or item.get("name") or item.get("repository")
            if isinstance(candidate, str) and candidate.strip():
                names.append(candidate.strip())
        elif isinstance(item, str):
            names.append(item.strip())
    return sorted({name for name in names if name})


async def _migrate_lakefs_branches(*, apply: bool, audit: MigrationAudit) -> None:
    settings = get_settings()
    storage = settings.storage
    access = str(storage.lakefs_access_key_id or "").strip()
    secret = str(storage.lakefs_secret_access_key or "").strip()
    api_url = str(storage.lakefs_api_url_effective or "").strip()
    timeout_seconds = float(storage.lakefs_client_timeout_seconds)

    if not access or not secret or not api_url:
        audit.lakefs.append(
            LakeFSChange(
                repository="*",
                action="skip",
                detail="missing lakeFS credentials or api url",
            )
        )
        return

    repositories = await _list_lakefs_repositories(
        api_url=api_url,
        access_key=access,
        secret_key=secret,
        timeout_seconds=timeout_seconds,
    )
    if not repositories:
        audit.lakefs.append(
            LakeFSChange(
                repository="*",
                action="noop",
                detail="no repositories returned",
            )
        )
        return

    lake_client = LakeFSClient(timeout_seconds=timeout_seconds)
    for repository in repositories:
        try:
            branches = await lake_client.list_branches(repository=repository, amount=500)
            branch_names = {
                str(item.get("name") or "").strip()
                for item in branches
                if isinstance(item, dict)
            }
            branch_names = {name for name in branch_names if name}
            if BRANCH_VALUE_TO in branch_names:
                audit.lakefs.append(
                    LakeFSChange(
                        repository=repository,
                        action="noop",
                        detail="main already exists",
                    )
                )
                continue
            if BRANCH_VALUE_FROM not in branch_names:
                audit.lakefs.append(
                    LakeFSChange(
                        repository=repository,
                        action="noop",
                        detail="master branch missing",
                    )
                )
                continue
            if apply:
                try:
                    await lake_client.create_branch(
                        repository=repository,
                        name=BRANCH_VALUE_TO,
                        source=BRANCH_VALUE_FROM,
                    )
                    detail = "created main from master"
                except LakeFSConflictError:
                    detail = "already exists (conflict)"
                audit.lakefs.append(
                    LakeFSChange(
                        repository=repository,
                        action="create",
                        detail=detail,
                    )
                )
            else:
                audit.lakefs.append(
                    LakeFSChange(
                        repository=repository,
                        action="plan",
                        detail="will create main from master",
                    )
                )
        except LakeFSError as exc:
            audit.lakefs.append(
                LakeFSChange(
                    repository=repository,
                    action="error",
                    detail=str(exc),
                )
            )


async def run(*, apply: bool, report_path: Path) -> None:
    settings = get_settings()
    mode = "apply" if apply else "dry-run"
    audit = MigrationAudit(
        mode=mode,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
    conn = await asyncpg.connect(settings.database.postgres_url)
    try:
        await _migrate_text_columns(conn, apply=apply, audit=audit)
        await _migrate_json_columns(conn, apply=apply, audit=audit)
    finally:
        await conn.close()
    await _migrate_lakefs_branches(apply=apply, audit=audit)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(audit.render_markdown(), encoding="utf-8")
    print(audit.render_markdown())
    print(f"Report written: {report_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate persisted branch values from master to main (Postgres + lakeFS).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Scan and report only (default).")
    mode.add_argument("--apply", action="store_true", help="Apply changes to Postgres and lakeFS.")
    parser.add_argument(
        "--report-path",
        default="/Users/isihyeon/Desktop/SPICE-Harvester/docs/verification/BRANCH_MAIN_DATA_MIGRATION_2026-03-03.md",
        help="Markdown report output path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    apply = bool(args.apply)
    report_path = Path(str(args.report_path)).expanduser().resolve()
    asyncio.run(run(apply=apply, report_path=report_path))


if __name__ == "__main__":
    main()
