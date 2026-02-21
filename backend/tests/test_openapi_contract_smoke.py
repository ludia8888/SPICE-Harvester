"""
OpenAPI-driven, no-mock contract smoke test for BFF.

Goal
----
- Enumerate BFF OpenAPI operations (currently 99 paths / ~108 ops).
- Exclude **only**:
  - WIP projection endpoints (`/api/v1/projections/*`, tagged "Projections (WIP)", or summary contains 🚧)
  - ops-only endpoints (`/api/v1/admin/*`, `/api/v1/monitoring/*`, `/api/v1/config/*`)
- For every remaining operation, perform at least one real HTTP call against a live stack.

Important
---------
This test is intentionally "enterprise paranoid":
- It fails on any 5xx response (unless explicitly expected for missing external config).
- It fails if an included OpenAPI operation has no runnable recipe.
- It does **not** assume external credentials exist (OpenAI / Google Sheets). Those endpoints must
  degrade gracefully (e.g., 400/422/503), and are reported as "not fully verified" unless
  smoke env vars are provided.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import aiohttp
import pytest
from jose import jwt

from tests.utils.auth import bff_auth_headers

BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")

# Optional external verification
SMOKE_GOOGLE_SHEET_URL = (os.getenv("SMOKE_GOOGLE_SHEET_URL") or "").strip()
SMOKE_GOOGLE_SHEET_API_KEY = (os.getenv("SMOKE_GOOGLE_SHEET_API_KEY") or "").strip()
SMOKE_OPENAI_ENABLED = bool((os.getenv("LLM_API_KEY") or "").strip() or (os.getenv("OPENAI_API_KEY") or "").strip())
_ENV_SMOKE_TOKEN = (
    (os.getenv("SMOKE_ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or os.getenv("ADMIN_TOKEN") or "").strip()
)
BFF_HEADERS = bff_auth_headers()
if _ENV_SMOKE_TOKEN and _ENV_SMOKE_TOKEN != BFF_HEADERS.get("X-Admin-Token"):
    raise AssertionError("SMOKE_ADMIN_TOKEN differs from BFF auth token.")
SMOKE_ADMIN_TOKEN = _ENV_SMOKE_TOKEN or (BFF_HEADERS.get("X-Admin-Token") or "")

def _build_smoke_user_jwt() -> str:
    """
    Build a deterministic HS256 user JWT for integration smoke tests.

    The docker-compose default is `USER_JWT_HS256_SECRET=spice-dev-user-jwt-secret`, so we
    fall back to that when the test runner environment doesn't export a secret.
    """
    secret = (os.getenv("USER_JWT_HS256_SECRET") or os.getenv("SMOKE_USER_JWT_SECRET") or "spice-dev-user-jwt-secret").strip()
    if not secret:
        secret = "spice-dev-user-jwt-secret"

    issuer = (os.getenv("USER_JWT_ISSUER") or "").strip() or None
    audience = (os.getenv("USER_JWT_AUDIENCE") or "").strip() or None

    claims: Dict[str, Any] = {
        "sub": "openapi_smoke_user",
        "email": "openapi_smoke_user@local.test",
        "roles": ["admin"],
        "tenant_id": "openapi_smoke_tenant",
        "org_id": "openapi_smoke_org",
    }
    if issuer:
        claims["iss"] = issuer
    if audience:
        claims["aud"] = audience

    return jwt.encode(claims, secret, algorithm="HS256")


SMOKE_USER_JWT = _build_smoke_user_jwt()


def _load_repo_dotenv() -> Dict[str, str]:
    """
    Best-effort loader for the repo root `.env` used by docker-compose port overrides.

    This keeps the smoke test aligned with the actual local stack without requiring users
    to export every variable into the test runner environment.
    """
    try:
        repo_root = Path(__file__).resolve().parents[2]
    except Exception:  # pragma: no cover (extremely unlikely)
        return {}

    env_path = repo_root / ".env"
    if not env_path.exists():
        return {}

    values: Dict[str, str] = {}
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


def _get_postgres_url_candidates() -> list[str]:
    env_url = (os.getenv("POSTGRES_URL") or "").strip()
    if env_url:
        return [env_url]

    dotenv = _load_repo_dotenv()
    port_override = (
        (os.getenv("POSTGRES_PORT_HOST") or "").strip()
        or (dotenv.get("POSTGRES_PORT_HOST") or "").strip()
    )
    ports: list[int] = []
    if port_override:
        try:
            ports.append(int(port_override))
        except ValueError:
            print(
                f"[config-warning] invalid POSTGRES_PORT_HOST={port_override!r}; "
                "falling back to default local ports"
            )

    # Common defaults across compose variants.
    for p in (5433, 5432, 15433):
        if p not in ports:
            ports.append(p)

    return [
        f"postgresql://spiceadmin:spicepass123@localhost:{p}/spicedb" for p in ports
    ]


async def _get_write_side_last_sequence(*, aggregate_type: str, aggregate_id: str) -> int:
    """
    Fetch current write-side aggregate sequence (OCC expected_seq) from Postgres.

    This is the same correctness layer used by OMS at append time.
    """
    import asyncpg
    import re
    from urllib.parse import urlparse

    schema = os.getenv("EVENT_STORE_SEQUENCE_SCHEMA", "spice_event_registry")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError(f"Invalid EVENT_STORE_SEQUENCE_SCHEMA: {schema!r}")

    prefix = (os.getenv("EVENT_STORE_SEQUENCE_HANDLER_PREFIX", "write_side") or "write_side").strip()
    handler = f"{prefix}:{aggregate_type}"

    conn = None
    last_error: Optional[Exception] = None
    for dsn in _get_postgres_url_candidates():
        parsed = urlparse(dsn)
        try:
            conn = await asyncpg.connect(
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                user=parsed.username or "spiceadmin",
                password=parsed.password or "spicepass123",
                database=(parsed.path or "/spicedb").lstrip("/") or "spicedb",
            )
            break
        except Exception as e:  # pragma: no cover (env-specific)
            last_error = e
            continue

    if conn is None:
        raise AssertionError(f"Postgres unavailable for expected_seq (candidates={_get_postgres_url_candidates()}): {last_error}")

    try:
        value = await conn.fetchval(
            f"""
            SELECT last_sequence
            FROM {schema}.aggregate_versions
            WHERE handler = $1 AND aggregate_id = $2
            """,
            handler,
            aggregate_id,
        )
        return int(value or 0)
    finally:
        await conn.close()

async def _get_ontology_head_commit(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    branch: str = "main",
) -> str:
    _ = session, db_name
    return f"branch:{branch}"


async def _record_deployed_commit(
    *,
    db_name: str,
    target_branch: str,
    ontology_commit_id: str,
) -> None:
    # Import lazily so the test can run without OMS deps in some environments.
    from oms.database.postgres import db as postgres_db
    from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2
    from oms.services.ontology_resources import OntologyResourceService

    await postgres_db.connect()
    try:
        registry = OntologyDeploymentRegistryV2()
        await registry.record_deployment(
            db_name=db_name,
            target_branch=target_branch,
            ontology_commit_id=ontology_commit_id,
            proposal_id=None,
            status="succeeded",
            deployed_by="openapi_smoke",
            metadata={"source": "test_openapi_contract_smoke"},
        )
        resources = OntologyResourceService()
        await resources.materialize_commit_snapshot(
            db_name,
            source_branch=target_branch,
            ontology_commit_id=ontology_commit_id,
        )
    finally:
        await postgres_db.disconnect()


_COMMAND_TERMINAL_STATES = {"COMPLETED", "SUCCEEDED", "FAILED", "CANCELLED", "CANCELED"}
_COMMAND_STATUS_KEYS = ("status", "state", "command_status", "commandState")
_ENVELOPE_GENERIC_STATUS = {"SUCCESS", "ERROR"}


def _extract_command_lifecycle_status(payload: Dict[str, Any]) -> str:
    """
    Resolve command lifecycle status from either direct or envelope-style responses.

    Supported shapes:
    - {"status": "COMPLETED"}
    - {"status": "success", "data": {"status": "COMPLETED"}}
    - {"status": "success", "data": {"state": "FAILED"}}
    """
    if not isinstance(payload, dict):
        return ""

    nested = payload.get("data") if isinstance(payload.get("data"), dict) else None
    for source in (nested, payload):
        if not isinstance(source, dict):
            continue
        for key in _COMMAND_STATUS_KEYS:
            value = source.get(key)
            token = str(value or "").strip().upper()
            if not token:
                continue
            # Ignore generic envelope status when command lifecycle status is not present.
            if source is payload and token in _ENVELOPE_GENERIC_STATUS and nested is not None:
                continue
            return token
    return ""


async def _wait_for_command_completed(
    session: aiohttp.ClientSession,
    *,
    command_id: str,
    timeout_seconds: int = 300,
    poll_interval_seconds: float = 1.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[Dict[str, Any]] = None

    while time.monotonic() < deadline:
        async with session.get(f"{BFF_URL}/api/v1/commands/{command_id}/status") as resp:
            if resp.status != 200:
                last = {"status": resp.status, "body": await resp.text()}
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()

        lifecycle_status = _extract_command_lifecycle_status(last)
        if lifecycle_status in _COMMAND_TERMINAL_STATES:
            if lifecycle_status not in {"COMPLETED", "SUCCEEDED"}:
                raise AssertionError(f"Command {command_id} ended in {lifecycle_status}: {last}")
            await asyncio.sleep(0.5)
            return last

        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for command completion (command_id={command_id}, last={last})")


async def _wait_for_ontology_schema_ready(
    session: aiohttp.ClientSession,
    *,
    schema_url: str,
    branch: str,
    class_id: str,
    include_preview: bool = False,
    timeout_seconds: int = 120,
    poll_interval_seconds: float = 1.0,
) -> None:
    """
    Wait until ontology schema is readable on the target branch.

    In evented/protected-branch setups, command completion may precede read-side
    visibility by a short window. This guard removes flaky ONTOLOGY_NOT_FOUND
    races when immediately creating object-type contracts.
    """
    deadline = time.monotonic() + timeout_seconds
    last_status: Optional[int] = None
    last_text: str = ""

    while time.monotonic() < deadline:
        params: Dict[str, Any] = {"branch": branch}
        if include_preview:
            params["preview"] = "true"
        async with session.get(
            schema_url,
            params=params,
            headers=BFF_HEADERS,
        ) as resp:
            status = resp.status
            body = await resp.text()
            if status == 200:
                return
            last_status = status
            last_text = body[:400]
        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(
        f"Timed out waiting for ontology schema visibility "
        f"(branch={branch}, class_id={class_id}, last_http={last_status}, last_body={last_text})"
    )


async def _wait_for_action_type_ready(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    action_type_id: str,
    branch: str = "main",
    timeout_seconds: int = 120,
    poll_interval_seconds: float = 1.0,
) -> None:
    """
    Wait until action-type resource becomes readable on the target branch.

    Proposal approval/deploy can acknowledge before read-side materialization
    catches up. This guard removes simulate-time ACTION_TYPE_NOT_FOUND flakes.
    """
    deadline = time.monotonic() + timeout_seconds
    last_status: Optional[int] = None
    last_text: str = ""
    url = f"{BFF_URL}/api/v2/ontologies/{db_name}/actionTypes/{action_type_id}"

    while time.monotonic() < deadline:
        async with session.get(url, params={"branch": branch}, headers=BFF_HEADERS) as resp:
            status = resp.status
            body = await resp.text()
            if status == 200:
                return
            last_status = status
            last_text = body[:400]
        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(
        f"Timed out waiting for action type visibility "
        f"(branch={branch}, action_type_id={action_type_id}, last_http={last_status}, last_body={last_text})"
    )


def _xlsx_bytes(*, header: list[str], rows: list[list[Any]]) -> bytes:
    try:
        from openpyxl import Workbook
    except Exception as e:  # pragma: no cover
        raise AssertionError("openpyxl is required for Excel endpoint smoke tests") from e

    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(list(header))
    for row in rows:
        ws.append(list(row))

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _csv_bytes(*, header: list[str], rows: list[list[Any]]) -> bytes:
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


@dataclass(frozen=True)
class Operation:
    method: str
    path: str
    tags: Tuple[str, ...]
    summary: str


REMOVED_V1_COMPAT_OPERATIONS: set[tuple[str, str]] = {
    ("GET", "/api/v1/databases/{db_name}/ontology/object-types"),
    ("GET", "/api/v1/databases/{db_name}/ontology/object-types/{class_id}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types"),
    (
        "GET",
        "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}",
    ),
    ("POST", "/api/v1/databases/{db_name}/ontology/object-types"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/object-types/{class_id}"),
    ("POST", "/api/v1/databases/{db_name}/ontology/validate"),
    ("GET", "/api/v1/databases/{db_name}/ontology/{class_id}/schema"),
    ("POST", "/api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata"),
    ("GET", "/api/v1/databases/{db_name}/ontology/proposals"),
    ("POST", "/api/v1/databases/{db_name}/ontology/proposals"),
    ("POST", "/api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve"),
    ("POST", "/api/v1/databases/{db_name}/ontology/deploy"),
    ("GET", "/api/v1/databases/{db_name}/ontology/health"),
    ("POST", "/api/v1/databases/{db_name}/query"),
    ("GET", "/api/v1/databases/{db_name}/query/builder"),
    ("POST", "/api/v1/databases/{db_name}/actions/{action_type_id}/simulate"),
    ("POST", "/api/v1/databases/{db_name}/actions/{action_type_id}/submit"),
    ("POST", "/api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch"),
    ("POST", "/api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo"),
    ("GET", "/api/v1/databases/{db_name}/actions/logs"),
    ("GET", "/api/v1/databases/{db_name}/actions/logs/{action_log_id}"),
    ("GET", "/api/v1/databases/{db_name}/actions/simulations"),
    ("GET", "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}"),
    ("GET", "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions"),
    ("GET", "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/link-types"),
    ("GET", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}"),
    ("POST", "/api/v1/databases/{db_name}/ontology/link-types"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits"),
    ("POST", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits"),
    ("POST", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex"),
    ("GET", "/api/v1/databases/{db_name}/ontology/action-types"),
    ("GET", "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}"),
    ("POST", "/api/v1/databases/{db_name}/ontology/action-types"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}"),
    ("DELETE", "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/functions"),
    ("GET", "/api/v1/databases/{db_name}/ontology/functions/{resource_id}"),
    ("POST", "/api/v1/databases/{db_name}/ontology/functions"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/functions/{resource_id}"),
    ("DELETE", "/api/v1/databases/{db_name}/ontology/functions/{resource_id}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/groups"),
    ("POST", "/api/v1/databases/{db_name}/ontology/groups"),
    ("GET", "/api/v1/databases/{db_name}/ontology/groups/{resource_id}"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/groups/{resource_id}"),
    ("DELETE", "/api/v1/databases/{db_name}/ontology/groups/{resource_id}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/interfaces"),
    ("GET", "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}"),
    ("POST", "/api/v1/databases/{db_name}/ontology/interfaces"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}"),
    ("DELETE", "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/shared-properties"),
    ("GET", "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}"),
    ("POST", "/api/v1/databases/{db_name}/ontology/shared-properties"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}"),
    ("DELETE", "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}"),
    ("GET", "/api/v1/databases/{db_name}/ontology/value-types"),
    ("GET", "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}"),
    ("POST", "/api/v1/databases/{db_name}/ontology/value-types"),
    ("PUT", "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}"),
    ("DELETE", "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}"),
    ("GET", "/api/v1/databases/{db_name}/classes"),
    ("GET", "/api/v1/databases/{db_name}/classes/{class_id}"),
    ("GET", "/api/v1/databases/{db_name}/class/{class_id}/instances"),
    ("GET", "/api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}"),
    ("POST", "/api/v1/databases/{db_name}/classes"),
    ("GET", "/api/v1/databases/{db_name}/class/{class_id}/sample-values"),
    ("POST", "/api/v1/pipeline-plans/compile"),
    ("GET", "/api/v1/pipeline-plans/{plan_id}"),
    ("POST", "/api/v1/pipeline-plans/{plan_id}/preview"),
    ("POST", "/api/v1/pipeline-plans/{plan_id}/inspect-preview"),
    ("POST", "/api/v1/pipeline-plans/{plan_id}/evaluate-joins"),
    ("POST", "/api/v1/pipelines/simulate-definition"),
    ("GET", "/api/v1/pipelines/branches"),
    ("POST", "/api/v1/pipelines/branches/{branch}/archive"),
    ("POST", "/api/v1/pipelines/branches/{branch}/restore"),
    ("POST", "/api/v1/pipelines/{pipeline_id}/branches"),
    ("POST", "/api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis"),
    ("POST", "/api/v1/databases/{db_name}/suggest-schema-from-data"),
    ("POST", "/api/v1/databases/{db_name}/suggest-mappings"),
    ("POST", "/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets"),
    ("POST", "/api/v1/databases/{db_name}/suggest-mappings-from-excel"),
    ("POST", "/api/v1/databases/{db_name}/suggest-schema-from-google-sheets"),
    ("POST", "/api/v1/databases/{db_name}/suggest-schema-from-excel"),
    ("POST", "/api/v1/databases/{db_name}/import-from-google-sheets/dry-run"),
    ("POST", "/api/v1/databases/{db_name}/import-from-google-sheets/commit"),
    ("POST", "/api/v1/databases/{db_name}/import-from-excel/dry-run"),
    ("POST", "/api/v1/databases/{db_name}/import-from-excel/commit"),
    ("POST", "/api/v1/data-connectors/google-sheets/grid"),
    ("POST", "/api/v1/data-connectors/google-sheets/preview"),
    ("POST", "/api/v1/data-connectors/google-sheets/register"),
    ("GET", "/api/v1/data-connectors/google-sheets/registered"),
    ("GET", "/api/v1/data-connectors/google-sheets/{sheet_id}/preview"),
    ("DELETE", "/api/v1/data-connectors/google-sheets/{sheet_id}"),
    ("POST", "/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining"),
    ("POST", "/api/v1/data-connectors/google-sheets/oauth/start"),
    ("GET", "/api/v1/data-connectors/google-sheets/oauth/callback"),
    ("GET", "/api/v1/data-connectors/google-sheets/drive/spreadsheets"),
    ("GET", "/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets"),
    ("DELETE", "/api/v1/data-connectors/google-sheets/connections/{connection_id}"),
    ("GET", "/api/v1/databases/{db_name}/branches"),
    ("POST", "/api/v1/databases/{db_name}/branches"),
    ("GET", "/api/v1/databases/{db_name}/branches/{branch_name}"),
    ("DELETE", "/api/v1/databases/{db_name}/branches/{branch_name}"),
}


REMOVED_V1_STRICT_ABSENT_PATHS: set[str] = {
    "/api/v1/databases/{db_name}/branches",
    "/api/v1/databases/{db_name}/branches/{branch_name}",
    "/api/v1/databases/{db_name}/versions",
    "/api/v1/databases/{db_name}/ontology/branches",
    "/api/v1/databases/{db_name}/merge/simulate",
    "/api/v1/databases/{db_name}/merge/resolve",
    "/api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo",
    "/api/v1/databases/{db_name}/query/builder",
    "/api/v1/databases/{db_name}/actions/logs",
    "/api/v1/databases/{db_name}/actions/logs/{action_log_id}",
    "/api/v1/databases/{db_name}/actions/simulations",
    "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}",
    "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions",
    "/api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}",
    "/api/v1/databases/{db_name}/ontology/object-types",
    "/api/v1/databases/{db_name}/ontology/object-types/{class_id}",
    "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types",
    "/api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}",
    "/api/v1/databases/{db_name}/ontology/validate",
    "/api/v1/databases/{db_name}/ontology/{class_id}/schema",
    "/api/v1/databases/{db_name}/ontology/{class_id}/mapping-metadata",
    "/api/v1/databases/{db_name}/ontology/proposals",
    "/api/v1/databases/{db_name}/ontology/proposals/{proposal_id}/approve",
    "/api/v1/databases/{db_name}/ontology/deploy",
    "/api/v1/databases/{db_name}/ontology/health",
    "/api/v1/databases/{db_name}/ontology/link-types",
    "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}",
    "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits",
    "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex",
    "/api/v1/databases/{db_name}/ontology/action-types",
    "/api/v1/databases/{db_name}/ontology/action-types/{resource_id}",
    "/api/v1/databases/{db_name}/ontology/functions",
    "/api/v1/databases/{db_name}/ontology/functions/{resource_id}",
    "/api/v1/databases/{db_name}/ontology/groups",
    "/api/v1/databases/{db_name}/ontology/groups/{resource_id}",
    "/api/v1/databases/{db_name}/ontology/interfaces",
    "/api/v1/databases/{db_name}/ontology/interfaces/{resource_id}",
    "/api/v1/databases/{db_name}/ontology/shared-properties",
    "/api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}",
    "/api/v1/databases/{db_name}/ontology/value-types",
    "/api/v1/databases/{db_name}/ontology/value-types/{resource_id}",
    "/api/v1/databases/{db_name}/classes",
    "/api/v1/databases/{db_name}/class/{class_id}/sample-values",
    "/api/v1/pipeline-plans/compile",
    "/api/v1/pipeline-plans/{plan_id}",
    "/api/v1/pipeline-plans/{plan_id}/preview",
    "/api/v1/pipeline-plans/{plan_id}/inspect-preview",
    "/api/v1/pipeline-plans/{plan_id}/evaluate-joins",
    "/api/v1/pipelines/simulate-definition",
    "/api/v1/pipelines/branches",
    "/api/v1/pipelines/branches/{branch}/archive",
    "/api/v1/pipelines/branches/{branch}/restore",
    "/api/v1/pipelines/{pipeline_id}/branches",
    "/api/v1/pipelines/datasets/{dataset_id}/versions/{version_id}/funnel-analysis",
    "/api/v1/databases/{db_name}/suggest-schema-from-data",
    "/api/v1/databases/{db_name}/suggest-mappings",
    "/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets",
    "/api/v1/databases/{db_name}/suggest-mappings-from-excel",
    "/api/v1/databases/{db_name}/suggest-schema-from-google-sheets",
    "/api/v1/databases/{db_name}/suggest-schema-from-excel",
    "/api/v1/databases/{db_name}/import-from-google-sheets/dry-run",
    "/api/v1/databases/{db_name}/import-from-google-sheets/commit",
    "/api/v1/databases/{db_name}/import-from-excel/dry-run",
    "/api/v1/databases/{db_name}/import-from-excel/commit",
    "/api/v1/data-connectors/google-sheets/grid",
    "/api/v1/data-connectors/google-sheets/preview",
    "/api/v1/data-connectors/google-sheets/register",
    "/api/v1/data-connectors/google-sheets/registered",
    "/api/v1/data-connectors/google-sheets/{sheet_id}/preview",
    "/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining",
    "/api/v1/data-connectors/google-sheets/{sheet_id}",
    "/api/v1/data-connectors/google-sheets/connections/{connection_id}",
    "/api/v1/data-connectors/google-sheets/oauth/start",
    "/api/v1/data-connectors/google-sheets/oauth/callback",
    "/api/v1/data-connectors/google-sheets/drive/spreadsheets",
    "/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets",
}

NON_FOUNDRY_V2_EXTENSION_ABSENT_PATHS: set[str] = {
    "/api/v2/ontologies/{ontology}/actions/logs/{actionLogId}/undo",
    "/api/v2/ontologies/{ontology}/actions/logs",
    "/api/v2/ontologies/{ontology}/actions/logs/{actionLogId}",
    "/api/v2/ontologies/{ontology}/actions/simulations",
    "/api/v2/ontologies/{ontology}/actions/simulations/{simulationId}",
    "/api/v2/ontologies/{ontology}/actions/simulations/{simulationId}/versions",
    "/api/v2/ontologies/{ontology}/actions/simulations/{simulationId}/versions/{version}",
}


def _collect_reintroduced_removed_ops(spec_paths_map: Dict[str, Any]) -> list[tuple[str, str]]:
    reintroduced_removed_ops: list[tuple[str, str]] = []
    for method, path in sorted(REMOVED_V1_COMPAT_OPERATIONS):
        methods = spec_paths_map.get(path) or {}
        if isinstance(methods, dict) and method.lower() in methods:
            reintroduced_removed_ops.append((method, path))
    return reintroduced_removed_ops


def _local_source_reintroduced_removed_ops() -> list[tuple[str, str]] | None:
    try:
        from bff.main import app as local_bff_app
    except Exception:
        return None

    local_spec_paths_map = (local_bff_app.openapi() or {}).get("paths") or {}
    if not isinstance(local_spec_paths_map, dict):
        return None
    return _collect_reintroduced_removed_ops(local_spec_paths_map)


def _is_wip(op: Operation) -> bool:
    if op.path.startswith("/api/v1/projections"):
        return True
    if any("Projections" in t or "WIP" in t for t in op.tags):
        return True
    if "🚧" in (op.summary or "") or "WIP" in (op.summary or ""):
        return True
    return False


def _is_ops_only(op: Operation) -> bool:
    if (op.method, op.path) in REMOVED_V1_COMPAT_OPERATIONS:
        return True
    if op.path.startswith("/api/v1/admin") or op.path.startswith("/api/v1/monitoring") or op.path.startswith("/api/v1/config"):
        return True
    if any(t in {"Admin Operations", "Monitoring", "Config Monitoring"} for t in op.tags):
        return True
    return False


@dataclass
class SmokeContext:
    db_name: str
    branch_name: str
    class_id: str
    advanced_class_id: str
    wrapper_class_id: str
    instance_id: str
    command_ids: Dict[str, str]
    udf_id: str
    pipeline_id: Optional[str] = None
    dataset_id: Optional[str] = None
    dataset_version_id: Optional[str] = None
    ingest_request_id: Optional[str] = None
    connection_id: Optional[str] = None
    link_type_id: Optional[str] = None
    action_type_id: Optional[str] = None
    action_log_id: Optional[str] = None
    simulation_id: Optional[str] = None
    simulation_version: Optional[int] = None
    agent_session_id: Optional[str] = None
    agent_plan_id: Optional[str] = None
    agent_run_id: Optional[str] = None
    agent_job_id: Optional[str] = None
    agent_approval_request_id: Optional[str] = None
    agent_context_item_id: Optional[str] = None
    document_bundle_id: Optional[str] = None

    @property
    def instance_aggregate_id(self) -> str:
        return f"{self.db_name}:main:{self.class_id}:{self.instance_id}"

    @property
    def pipeline_name(self) -> str:
        return f"{self.db_name}_pipeline"

    @property
    def dataset_name(self) -> str:
        return f"{self.db_name}_dataset"

@dataclass
class RequestPlan:
    method: str
    path_template: str
    url: str
    expected_statuses: Tuple[int, ...]
    params: Optional[Dict[str, Any]] = None
    json_body: Any = None
    form: Optional[aiohttp.FormData] = None
    headers: Optional[Dict[str, str]] = None
    note: Optional[str] = None
    allow_5xx: bool = False
    auth_mode: str = "admin"  # admin|delegated_user|user


async def _request(
    session: aiohttp.ClientSession,
    plan: RequestPlan,
) -> Tuple[int, str, Optional[Dict[str, Any]]]:
    kwargs: Dict[str, Any] = {}
    if plan.params:
        kwargs["params"] = plan.params
    headers: Dict[str, str] = dict(BFF_HEADERS)
    auth_mode = str(getattr(plan, "auth_mode", "") or "admin").strip().lower()
    if auth_mode == "user":
        headers["X-Admin-Token"] = ""
        headers["Authorization"] = f"Bearer {SMOKE_USER_JWT}"
    elif auth_mode == "delegated_user":
        headers["X-Delegated-Authorization"] = f"Bearer {SMOKE_USER_JWT}"
    if plan.headers:
        headers.update(plan.headers)
    kwargs["headers"] = headers
    if plan.form is not None:
        kwargs["data"] = plan.form
    else:
        kwargs["json"] = plan.json_body

    async with session.request(plan.method, plan.url, **kwargs) as resp:
        text = await resp.text()
        payload = None
        ct = resp.headers.get("Content-Type", "")
        if "application/json" in ct:
            try:
                payload = json.loads(text) if text else None
            except Exception:
                payload = None
        return resp.status, text, payload


def _format_path(template: str, ctx: SmokeContext, *, overrides: Optional[Dict[str, str]] = None) -> str:
    values = {
        "db_name": ctx.db_name,
        "branch": ctx.branch_name,
        "branch_name": ctx.branch_name,
        "class_id": ctx.class_id,
        "class_label": ctx.class_id,
        "instance_id": ctx.instance_id,
        "udf_id": ctx.udf_id,
        "command_id": ctx.command_ids.get("create_database") or next(iter(ctx.command_ids.values()), ""),
        "sheet_id": "0000000000000000000000000000000000000000",
        "task_id": "00000000-0000-0000-0000-000000000000",
        "pipeline_id": ctx.pipeline_id or "pipeline_smoke",
        "artifact_id": "00000000-0000-0000-0000-000000000000",
        "dataset_id": ctx.dataset_id or "dataset_smoke",
        "version_id": ctx.dataset_version_id or "00000000-0000-0000-0000-000000000000",
        "ingest_request_id": ctx.ingest_request_id or "00000000-0000-0000-0000-000000000000",
        "connection_id": ctx.connection_id or "missing_connection",
        "connectionRid": "ri.spice.main.connection.smoke-missing",
        "exportRunRid": "ri.spice.main.export-run.smoke-missing",
        "tableImportRid": "ri.spice.main.table-import.smoke-missing",
        "fileImportRid": "ri.spice.main.file-import.smoke-missing",
        "virtualTableRid": "ri.spice.main.virtual-table.smoke-missing",
        "datasetRid": "ri.spice.main.dataset.00000000-0000-0000-0000-000000000000",
        "transactionRid": "ri.spice.main.transaction.00000000-0000-0000-0000-000000000000",
        "branchName": ctx.branch_name,
        "filePath": "missing.csv",
        "property": "missing_property",
        "attachmentRid": "ri.spice.main.attachment.smoke-missing",
        "buildRid": "ri.spice.main.build.smoke-missing",
        "scheduleRid": "ri.spice.main.schedule.00000000-0000-0000-0000-000000000000",
        "link_type_id": ctx.link_type_id or "missing_link_type",
        # Foundry Ontologies v2 placeholders
        "ontology": ctx.db_name,
        "objectType": ctx.class_id,
        "primaryKey": ctx.instance_id,
        "linkType": ctx.link_type_id or "missing_link_type",
        "linkedObjectPrimaryKey": ctx.instance_id,
        "objectSetRid": "ri.object-set.main.versioned-object-set.openapi-smoke",
        "actionTypeRid": (
            f"ri.spice.main.action-type.{ctx.db_name}.{ctx.action_type_id or 'missing_action_type'}"
        ),
        "actionType": ctx.action_type_id or "missing_action_type",
        "action": ctx.action_type_id or "missing_action_type",
        "queryApiName": "missing_query_type",
        "interfaceType": "BaseInterface",
        "sharedPropertyType": "sharedName",
        "valueType": "string",
        "mapping_spec_id": "00000000-0000-0000-0000-000000000000",
        "resource_id": "missing_resource",
        "proposal_id": "00000000-0000-0000-0000-000000000000",
        "action_type_id": ctx.action_type_id or "missing_action_type",
        "action_log_id": ctx.action_log_id or "00000000-0000-0000-0000-000000000000",
        "actionLogId": ctx.action_log_id or "00000000-0000-0000-0000-000000000000",
        "simulation_id": ctx.simulation_id or "00000000-0000-0000-0000-000000000000",
        "simulationId": ctx.simulation_id or "00000000-0000-0000-0000-000000000000",
        "version": str(ctx.simulation_version or 1),
        "resource_type": "action-types",
        "drift_id": "00000000-0000-0000-0000-000000000000",
        "subscription_id": "00000000-0000-0000-0000-000000000000",
        # Agent control-plane placeholders
        "session_id": ctx.agent_session_id or "00000000-0000-0000-0000-000000000000",
        "plan_id": ctx.agent_plan_id or "00000000-0000-0000-0000-000000000000",
        "job_id": ctx.agent_job_id or "00000000-0000-0000-0000-000000000000",
        "run_id": ctx.agent_run_id or "00000000-0000-0000-0000-000000000000",
        "approval_request_id": ctx.agent_approval_request_id or "00000000-0000-0000-0000-000000000000",
        "item_id": ctx.agent_context_item_id or "00000000-0000-0000-0000-000000000000",
        "function_id": f"smoke_echo_{ctx.db_name}",
        # Context / RAG placeholders
        "bundle_id": ctx.document_bundle_id or "bundle_smoke",
        "entity_id": "entity_smoke",
    }
    if overrides:
        values.update(overrides)
    return template.format(**values)


def _normalize_db_path(path: str) -> str:
    if path == "/api/v1/database":
        return "/api/v1/databases"
    if path.startswith("/api/v1/database/"):
        return path.replace("/api/v1/database/", "/api/v1/databases/", 1)
    return path


def _pick_spec_path(paths: Iterable[str], *candidates: str) -> str:
    path_set = set(paths)
    for candidate in candidates:
        if candidate in path_set:
            return candidate
    return candidates[0]


def _ontology_payload(*, class_id: str, label_en: str, label_ko: str) -> Dict[str, Any]:
    return {
        "id": class_id,
        "label": {"en": label_en, "ko": label_ko},
        "description": {"en": f"{label_en} class (openapi smoke)", "ko": f"{label_ko} (스모크 테스트)"},
        "properties": [
            {
                "name": f"{class_id.lower()}_id",
                "type": "xsd:string",
                "label": {"en": f"{label_en} ID", "ko": f"{label_ko} ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "xsd:string",
                "label": {"en": "Name", "ko": "이름"},
                "required": True,
                "titleKey": True,
            },
        ],
        "relationships": [],
        "metadata": {"source": "openapi_smoke"},
    }


def _object_type_resource_payload(
    *,
    class_id: str,
    backing_dataset_id: str,
    pk_spec: Dict[str, Any],
    status: str = "ACTIVE",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    backing_source: Dict[str, Any] = {"dataset_id": backing_dataset_id}
    return {
        "id": class_id,
        "label": {"en": class_id, "ko": class_id},
        "description": {"en": f"{class_id} object type (openapi smoke)", "ko": f"{class_id} 오브젝트 타입(스모크)"},
        "metadata": metadata or {"source": "openapi_smoke"},
        "spec": {
            "backing_source": backing_source,
            "pk_spec": pk_spec,
            "status": str(status or "ACTIVE").upper(),
        },
    }


def _mapping_file_bytes(ctx: SmokeContext) -> bytes:
    payload = {
        "db_name": ctx.db_name,
        "classes": [
            {"db_name": ctx.db_name, "class_id": ctx.class_id, "label": ctx.class_id, "label_lang": "en"}
        ],
        "properties": [
            {
                "db_name": ctx.db_name,
                "class_id": ctx.class_id,
                "property_id": f"{ctx.class_id.lower()}_id",
                "label": f"{ctx.class_id.lower()}_id",
                "label_lang": "en",
            },
            {"db_name": ctx.db_name, "class_id": ctx.class_id, "property_id": "name", "label": "name", "label_lang": "en"},
        ],
        "relationships": [],
        "exported_at": "openapi_smoke",
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _target_schema_json(ctx: SmokeContext) -> str:
    # Used by Excel mapping/commit endpoints for type hints.
    schema = [
        {"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"},
        {"name": "name", "type": "xsd:string"},
    ]
    return json.dumps(schema, ensure_ascii=False)


def _mappings_json(ctx: SmokeContext) -> str:
    mappings = [
        {"source_field": f"{ctx.class_id.lower()}_id", "target_field": f"{ctx.class_id.lower()}_id"},
        {"source_field": "name", "target_field": "name"},
    ]
    return json.dumps(mappings, ensure_ascii=False)


async def _build_plan(op: Operation, ctx: SmokeContext) -> RequestPlan:
    """
    Return a runnable RequestPlan for every non-WIP/non-ops operation.
    If an operation cannot be meaningfully executed without external secrets, this plan must still
    exercise the endpoint safely (e.g., 422 validation) or report a controlled, non-5xx failure.
    """

    key = (op.method, _normalize_db_path(op.path))

    # ---------- Health ----------
    if key == ("GET", "/api/v1/"):
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (200,))
    if key == ("GET", "/api/v1/health"):
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (200,))
    if key == ("GET", "/api/v1/graph-query/health"):
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (200,))

    # ---------- Agent ----------
    if key == ("POST", "/api/v1/agent/runs"):
        url = f"{BFF_URL}{op.path}"
        # Intentionally invalid (no steps) -> deterministic 400 without running any tools.
        body = {"goal": "openapi smoke run", "steps": [], "context": {"source": "openapi_smoke"}, "request_id": str(uuid.uuid4())}
        return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body, auth_mode="delegated_user")

    if key == ("POST", "/api/v1/agent/pipeline-runs"):
        url = f"{BFF_URL}{op.path}"
        # Intentionally minimal -> clarification_required without building a plan.
        body = {
            "goal": "openapi smoke pipeline run",
            "data_scope": {"db_name": ctx.db_name, "branch": "main", "dataset_ids": []},
        }
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400, 422, 429, 503),
            json_body=body,
            auth_mode="delegated_user",
            allow_5xx=True,
        )

    if key == ("POST", "/api/v1/agent/pipeline-runs/stream"):
        url = f"{BFF_URL}{op.path}"
        # Validation-only: avoid opening a streaming response in the smoke harness.
        # Missing required fields -> 422 (no tool execution / no side effects).
        return RequestPlan(op.method, op.path, url, (422,), json_body={}, auth_mode="delegated_user")

    if key == ("POST", "/api/v1/ontology-agent/runs"):
        url = f"{BFF_URL}{op.path}"
        # Intentionally invalid -> 422 without invoking the autonomous agent / LLM.
        return RequestPlan(op.method, op.path, url, (422,), json_body={})

    # ---------- Context Tools (User JWT required) ----------
    if key == ("POST", "/api/v1/context-tools/datasets/describe"):
        url = f"{BFF_URL}{op.path}"
        body = {"db_name": ctx.db_name, "branch": "main"}
        return RequestPlan(op.method, op.path, url, (200, 400, 401, 403, 422), json_body=body, auth_mode="user")

    if key == ("POST", "/api/v1/context-tools/ontology/snapshot"):
        url = f"{BFF_URL}{op.path}"
        body = {"db_name": ctx.db_name, "branch": "main", "ontology_id": ctx.class_id}
        return RequestPlan(op.method, op.path, url, (200, 400, 401, 403, 404, 422), json_body=body, auth_mode="user")

    # ---------- Document Bundles (User JWT + Context7) ----------
    if key == ("POST", "/api/v1/document-bundles/{bundle_id}/search"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"query": "openapi smoke", "limit": 3}
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400, 401, 403, 404, 422, 503),
            json_body=body,
            auth_mode="user",
            allow_5xx=True,
        )

    # ---------- Context7 (MCP) ----------
    if key == ("POST", "/api/v1/context7/search"):
        url = f"{BFF_URL}{op.path}"
        body = {"query": "openapi smoke", "limit": 3, "filters": {"source": "openapi_smoke"}}
        return RequestPlan(op.method, op.path, url, (200, 400, 422, 503), json_body=body, allow_5xx=True)

    if key == ("GET", "/api/v1/context7/context/{entity_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 503), allow_5xx=True)

    if key == ("POST", "/api/v1/context7/knowledge"):
        url = f"{BFF_URL}{op.path}"
        body = {"title": "OpenAPI smoke", "content": "hello", "metadata": {"source": "openapi_smoke"}, "tags": ["openapi_smoke"]}
        return RequestPlan(op.method, op.path, url, (200, 400, 422, 503), json_body=body, allow_5xx=True)

    if key == ("POST", "/api/v1/context7/link"):
        url = f"{BFF_URL}{op.path}"
        body = {"source_id": "entity_a", "target_id": "entity_b", "relationship": "related_to", "properties": {"source": "openapi_smoke"}}
        return RequestPlan(op.method, op.path, url, (200, 400, 422, 503), json_body=body, allow_5xx=True)

    if key == ("POST", "/api/v1/context7/analyze/ontology"):
        url = f"{BFF_URL}{op.path}"
        body = {"ontology_id": ctx.class_id, "db_name": ctx.db_name, "branch": "main", "include_relationships": False, "include_suggestions": True}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 422, 503), json_body=body, allow_5xx=True)

    # ---------- Database ----------
    if key == ("GET", "/api/v1/databases"):
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (200,))
    if key == ("POST", "/api/v1/databases"):
        body = {"name": ctx.db_name, "description": "openapi contract smoke db"}
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (201, 202), json_body=body)
    if key == ("GET", "/api/v1/databases/{db_name}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))
    if key == ("GET", "/api/v1/databases/{db_name}/versions"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))

    # ---------- Pipelines ----------
    if key == ("GET", "/api/v1/pipelines"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name, "branch": "main"})

    if key == ("GET", "/api/v1/pipelines/datasets"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name})

    if key == ("GET", "/api/v1/pipelines/proposals"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name})

    # ---------- Pipeline UDFs (read-only + validation-only) ----------
    if key == ("GET", "/api/v1/pipelines/udfs"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name})

    if key == ("GET", "/api/v1/pipelines/udfs/{udf_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        # Deterministic missing resource to avoid write side-effects.
        return RequestPlan(op.method, op.path, url, (404,))

    if key == ("GET", "/api/v1/pipelines/udfs/{udf_id}/versions/{version}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'udf_id': 'udf_smoke', 'version': '1'})}"
        return RequestPlan(op.method, op.path, url, (404,))

    if key == ("POST", "/api/v1/pipelines/udfs"):
        url = f"{BFF_URL}{op.path}"
        # Validation-only: missing required fields -> 422 (no writes).
        return RequestPlan(op.method, op.path, url, (422,), params={"db_name": ctx.db_name}, json_body={})

    if key == ("POST", "/api/v1/pipelines/udfs/{udf_id}/versions"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        # Validation-only: missing required fields -> 422 (no writes).
        return RequestPlan(op.method, op.path, url, (422,), json_body={})

    if key == ("POST", "/api/v1/pipelines"):
        url = f"{BFF_URL}{op.path}"
        body = {
            "db_name": ctx.db_name,
            "name": ctx.pipeline_name,
            "description": "openapi smoke pipeline",
            "pipeline_type": "batch",
            "location": f"/projects/{ctx.db_name}/pipelines",
            "branch": "main",
            "definition_json": {"nodes": [], "edges": [], "parameters": []},
        }
        return RequestPlan(op.method, op.path, url, (200, 409, 400), json_body=body)

    if key == ("GET", "/api/v1/pipelines/{pipeline_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 404))

    if key == ("GET", "/api/v1/pipelines/{pipeline_id}/artifacts/{artifact_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 404))

    if key == ("PUT", "/api/v1/pipelines/{pipeline_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "description": "openapi smoke pipeline update",
            "definition_json": {"nodes": [], "edges": []},
            "branch": "main",
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body)

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/build"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"db_name": ctx.db_name, "limit": 5}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409, 503), json_body=body)

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/preview"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "db_name": ctx.db_name,
            "definition_json": {
                "nodes": [
                    {
                        "id": "in1",
                        "type": "input",
                        "metadata": {
                            "datasetId": ctx.dataset_id or "dataset_smoke",
                            "datasetName": ctx.dataset_name,
                        },
                    },
                    {"id": "out1", "type": "output", "metadata": {"datasetName": f"{ctx.db_name}_preview_out"}},
                ],
                "edges": [{"from": "in1", "to": "out1"}],
                "parameters": [],
            },
            "limit": 5,
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/rebase"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"onto_branch": "main"}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body)

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/deploy"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "definition_json": {"nodes": [], "edges": []},
            "output": {"db_name": ctx.db_name, "dataset_name": f"{ctx.db_name}_output"},
            "schedule": {"interval_seconds": 3600},
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/proposals"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"title": "OpenAPI Smoke Proposal", "description": "proposal smoke"}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    if key in {
        ("POST", "/api/v1/pipelines/{pipeline_id}/proposals/approve"),
        ("POST", "/api/v1/pipelines/{pipeline_id}/proposals/reject"),
    }:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"review_comment": "smoke review"}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    if key == ("POST", "/api/v1/pipelines/datasets"):
        url = f"{BFF_URL}{op.path}"
        body = {
            "db_name": ctx.db_name,
            "name": ctx.dataset_name,
            "description": "openapi smoke dataset",
            "source_type": "manual",
            "schema_json": {
                "columns": [
                    {"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"},
                    {"name": "name", "type": "xsd:string"},
                ]
            },
        }
        return RequestPlan(op.method, op.path, url, (200, 409, 400), json_body=body)

    if key == ("POST", "/api/v1/pipelines/datasets/{dataset_id}/versions"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        headers = {"X-DB-Name": ctx.db_name}
        body = {
            "row_count": 1,
            "sample_json": {
                "columns": [
                    {"name": f"{ctx.class_id.lower()}_id", "type": "String"},
                    {"name": "name", "type": "String"},
                ],
                "rows": [
                    {f"{ctx.class_id.lower()}_id": ctx.instance_id, "name": "OpenAPI Smoke"}
                ],
            },
            "schema_json": {
                "columns": [
                    {"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"},
                    {"name": "name", "type": "xsd:string"},
                ]
            },
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body, headers=headers)

    if key == ("GET", "/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 404), headers=headers)

    if key == ("POST", "/api/v1/pipelines/datasets/ingest-requests/{ingest_request_id}/schema/approve"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        headers = {"X-DB-Name": ctx.db_name, "X-User-ID": "openapi-smoke"}
        body = {
            "schema_json": {
                "columns": [
                    {"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"},
                    {"name": "name", "type": "xsd:string"},
                ]
            }
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body, headers=headers)

    if key == ("POST", "/api/v1/objectify/mapping-specs"):
        url = f"{BFF_URL}{op.path}"
        headers = {"X-DB-Name": ctx.db_name}
        body = {
            "dataset_id": ctx.dataset_id or "dataset_smoke",
            "target_class_id": ctx.class_id,
            "mappings": [
                {
                    "source_field": f"{ctx.class_id.lower()}_id",
                    "target_field": f"{ctx.class_id.lower()}_id",
                },
                {"source_field": "name", "target_field": "name"},
            ],
            "auto_sync": False,
        }
        return RequestPlan(op.method, op.path, url, (201, 400, 404, 409), json_body=body, headers=headers)

    if key == ("POST", "/api/v1/objectify/databases/{db_name}/run-dag"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        headers = {"X-DB-Name": ctx.db_name, "X-User-ID": "openapi-smoke", "X-User-Type": "user"}
        # Dry-run only (no writes). Many environments won't have object_type contracts; expect a clean 409.
        body = {"class_ids": [ctx.class_id], "branch": "main", "include_dependencies": True, "dry_run": True}
        return RequestPlan(op.method, op.path, url, (200, 403, 404, 409, 422), json_body=body, headers=headers)

    if key == ("POST", "/api/v1/objectify/databases/{db_name}/datasets/{dataset_id}/detect-relationships"):
        # Use a fresh dataset_id to avoid triggering expensive analysis; the route should fail fast with 404.
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'dataset_id': str(uuid.uuid4())})}"
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (404, 403), json_body={}, headers=headers)

    if key == ("POST", "/api/v1/objectify/mapping-specs/{mapping_spec_id}/trigger-incremental"):
        # Use a fresh mapping_spec_id to avoid enqueueing any jobs; the route must return a clean 404.
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'mapping_spec_id': str(uuid.uuid4())})}"
        body = {"execution_mode": "incremental"}
        return RequestPlan(op.method, op.path, url, (404, 403), json_body=body)

    if key == ("GET", "/api/v1/objectify/mapping-specs/{mapping_spec_id}/watermark"):
        # Watermark lookup is read-only and should never 5xx.
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'mapping_spec_id': str(uuid.uuid4())})}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("POST", "/api/v1/backing-datasources"):
        url = f"{BFF_URL}{op.path}"
        headers = {"X-DB-Name": ctx.db_name}
        body = {"dataset_id": ctx.dataset_id or "00000000-0000-0000-0000-000000000001"}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body, headers=headers)

    if key == ("GET", "/api/v1/backing-datasources"):
        url = f"{BFF_URL}{op.path}"
        params = {"dataset_id": ctx.dataset_id or "00000000-0000-0000-0000-000000000001"}
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params=params, headers=headers)

    if key == ("GET", "/api/v1/backing-datasources/{backing_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'backing_id': '00000000-0000-0000-0000-000000000001'})}"
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 404), headers=headers)

    if key == ("POST", "/api/v1/backing-datasources/{backing_id}/versions"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'backing_id': '00000000-0000-0000-0000-000000000001'})}"
        headers = {"X-DB-Name": ctx.db_name}
        body = {"dataset_version_id": ctx.dataset_version_id or "00000000-0000-0000-0000-000000000001"}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body, headers=headers)

    if key == ("GET", "/api/v1/backing-datasources/{backing_id}/versions"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'backing_id': '00000000-0000-0000-0000-000000000001'})}"
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 404), headers=headers)

    if key == ("GET", "/api/v1/backing-datasource-versions/{version_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'version_id': '00000000-0000-0000-0000-000000000001'})}"
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 404), headers=headers)

    if key == ("POST", "/api/v1/key-specs"):
        url = f"{BFF_URL}{op.path}"
        headers = {"X-DB-Name": ctx.db_name}
        body = {
            "dataset_id": ctx.dataset_id or "00000000-0000-0000-0000-000000000001",
            "primary_key": [f"{ctx.class_id.lower()}_id"],
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body, headers=headers)

    if key == ("GET", "/api/v1/key-specs"):
        url = f"{BFF_URL}{op.path}"
        params = {"dataset_id": ctx.dataset_id or "00000000-0000-0000-0000-000000000001"}
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params=params, headers=headers)

    if key == ("GET", "/api/v1/key-specs/{key_spec_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'key_spec_id': '00000000-0000-0000-0000-000000000001'})}"
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 404), headers=headers)

    if key == ("POST", "/api/v1/gate-policies"):
        url = f"{BFF_URL}{op.path}"
        body = {"scope": "objectify_preflight", "name": "default"}
        return RequestPlan(op.method, op.path, url, (200, 400), json_body=body)

    if key == ("GET", "/api/v1/gate-policies"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/gate-results"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("POST", "/api/v1/access-policies"):
        url = f"{BFF_URL}{op.path}"
        body = {
            "db_name": ctx.db_name,
            "subject_type": "object_type",
            "subject_id": ctx.class_id,
            "policy": {"mask_columns": ["name"]},
        }
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409, 422), json_body=body, headers=headers)

    if key == ("GET", "/api/v1/access-policies"):
        url = f"{BFF_URL}{op.path}"
        headers = {"X-DB-Name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name}, headers=headers)

    if key == ("POST", "/api/v1/objectify/datasets/{dataset_id}/run"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        headers = {"X-DB-Name": ctx.db_name}
        body = {"allow_partial": True}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body, headers=headers)

    if key == ("POST", "/api/v1/pipelines/datasets/excel-upload"):
        url = f"{BFF_URL}{op.path}"
        excel_bytes = _xlsx_bytes(
            header=["id", "name"],
            rows=[["1", "alpha"], ["2", "beta"]],
        )
        form = aiohttp.FormData()
        form.add_field(
            "file",
            excel_bytes,
            filename="openapi_smoke.xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        form.add_field("dataset_name", f"{ctx.dataset_name}_excel")
        form.add_field("description", "openapi smoke excel dataset")
        params = {"db_name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 400, 422, 501), params=params, form=form)

    if key == ("POST", "/api/v1/pipelines/datasets/csv-upload"):
        url = f"{BFF_URL}{op.path}"
        csv_bytes = _csv_bytes(header=["id", "name"], rows=[["1", "alpha"], ["2", "beta"]])
        form = aiohttp.FormData()
        form.add_field("file", csv_bytes, filename="openapi_smoke.csv", content_type="text/csv")
        form.add_field("dataset_name", f"{ctx.dataset_name}_csv")
        form.add_field("description", "openapi smoke csv dataset")
        params = {"db_name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 400, 422), params=params, form=form)

    if key == ("POST", "/api/v1/pipelines/datasets/media-upload"):
        url = f"{BFF_URL}{op.path}"
        form = aiohttp.FormData()
        form.add_field("files", b"hello-world", filename="openapi_smoke.txt", content_type="text/plain")
        form.add_field("dataset_name", f"{ctx.dataset_name}_media")
        form.add_field("description", "openapi smoke media dataset")
        params = {"db_name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 400, 422), params=params, form=form)

    if key == ("DELETE", "/api/v1/databases/{db_name}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        expected_seq = await _get_write_side_last_sequence(aggregate_type="Database", aggregate_id=ctx.db_name)
        return RequestPlan(op.method, op.path, url, (200, 202, 404, 409), params={"expected_seq": expected_seq})

    # ---------- Command status ----------
    if key == ("GET", "/api/v1/commands/{command_id}/status"):
        cmd = ctx.command_ids.get("create_database") or next(iter(ctx.command_ids.values()))
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'command_id': cmd})}"
        return RequestPlan(op.method, op.path, url, (200,))

    # ---------- Ontology ----------
    if key == ("POST", "/api/v1/databases/{db_name}/ontology"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = _ontology_payload(class_id=ctx.class_id, label_en="Product", label_ko="제품")
        return RequestPlan(op.method, op.path, url, (200, 202, 409), json_body=body)

    if key == ("GET", "/api/v2/ontologies"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("GET", "/api/v2/ontologies/{ontology}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("GET", "/api/v2/ontologies/{ontology}/fullMetadata"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/actionTypes/{actionType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/objectTypes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objectTypes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        # Validation-only (apiName required) to avoid mutating contracts in smoke.
        return RequestPlan(op.method, op.path, url, (400, 403, 404, 409, 422), json_body={})

    if key == ("PATCH", "/api/v2/ontologies/{ontology}/objectTypes/{objectType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': 'missing_object_type'})}"
        body = {"metadata": {"source": "openapi_smoke"}}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404, 409, 422), json_body=body)

    if key == ("GET", "/api/v2/ontologies/{ontology}/objectTypes/{objectType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("GET", "/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id})}"
        params = {"pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id, 'linkType': ctx.link_type_id})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("GET", "/api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id})}"
        params = {"branch": "main", "preview": "true"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/interfaceTypes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main", "preview": "true", "pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'interfaceType': 'BaseInterface'})}"
        params = {"branch": "main", "preview": "true"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/sharedPropertyTypes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main", "preview": "true", "pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'sharedPropertyType': 'sharedName'})}"
        params = {"branch": "main", "preview": "true"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/valueTypes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"preview": "true"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/valueTypes/{valueType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'valueType': 'string'})}"
        params = {"preview": "true"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/queryTypes/{queryApiName}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'queryApiName': 'missing_query_type'})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("POST", "/api/v2/ontologies/{ontology}/queries/{queryApiName}/execute"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'queryApiName': 'missing_query_type'})}"
        body = {"parameters": {}}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objects/{objectType}/search"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id})}"
        body = {
            "where": {"type": "eq", "field": "class_id", "value": ctx.class_id},
            "pageSize": 10,
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjects"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main"}
        body = {
            "objectSet": {"objectType": ctx.class_id},
            "select": [f"{ctx.class_id.lower()}_id"],
            "pageSize": 10,
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params, json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadLinks"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main", "preview": "true"}
        body = {
            "objectSet": {"objectType": ctx.class_id},
            "links": [ctx.link_type_id or "missing_link_type"],
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params, json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main", "preview": "true"}
        body = {
            "objectSet": {"objectType": ctx.class_id},
            "select": [f"{ctx.class_id.lower()}_id"],
            "pageSize": 10,
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params, json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main", "preview": "true"}
        body = {
            "objectSet": {"objectType": ctx.class_id},
            "select": [f"{ctx.class_id.lower()}_id"],
            "pageSize": 10,
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params, json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objectSets/aggregate"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        params = {"branch": "main"}
        body = {
            "objectSet": {"objectType": ctx.class_id},
            "aggregation": [{"type": "count", "name": "rowCount"}],
            "groupBy": [],
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params, json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objectSets/createTemporary"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        body = {"objectSet": {"objectType": ctx.class_id}}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), json_body=body)

    if key == ("GET", "/api/v2/ontologies/{ontology}/objectSets/{objectSetRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("GET", "/api/v2/ontologies/{ontology}/objects/{objectType}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id})}"
        params = {"pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}"):
        url = (
            f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id, 'primaryKey': ctx.instance_id})}"
        )
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("GET", "/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}"):
        url = (
            f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id, 'primaryKey': ctx.instance_id, 'linkType': ctx.link_type_id or 'missing_link_type'})}"
        )
        params = {"pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404), params=params)

    if key == ("GET", "/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/links/{linkType}/{linkedObjectPrimaryKey}"):
        url = (
            f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id, 'primaryKey': ctx.instance_id, 'linkType': ctx.link_type_id or 'missing_link_type', 'linkedObjectPrimaryKey': ctx.instance_id})}"
        )
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404))

    if key == ("PUT", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409, 422), json_body=None, note="smoke: validate only")

    if key == ("GET", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409))

    if key == ("POST", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/edits"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409, 422), json_body=None, note="smoke: validate only")

    if key == ("POST", "/api/v1/databases/{db_name}/ontology/link-types/{link_type_id}/reindex"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409))

    # ---------- Ontology Extensions ----------
    if key == ("POST", "/api/v1/databases/{db_name}/ontology/records/deployments"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "target_branch": "main",
            "ontology_commit_id": "branch:main",
            "deployed_by": "openapi_smoke",
            "metadata": {"source": "openapi_smoke"},
        }
        return RequestPlan(op.method, op.path, url, (200, 201, 400, 403, 404, 409, 422), json_body=body)

    ontology_resource_collections = {
        "/api/v1/databases/{db_name}/ontology/link-types",
        "/api/v1/databases/{db_name}/ontology/resources/{resource_type}",
    }
    ontology_resource_items = {f"{p}/{{resource_id}}" for p in ontology_resource_collections}

    if key[0] == "POST" and key[1] in ontology_resource_collections:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        # Intentionally invalid (missing required query params like `branch` and `expected_head_commit`)
        # so we can safely assert contract + wiring without mutating server state.
        return RequestPlan(op.method, op.path, url, (201, 400, 404, 409, 422), json_body=None, note="smoke: validate only")

    if key[0] == "PUT" and key[1] in ontology_resource_items:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409, 422), json_body=None, note="smoke: validate only")

    if key[0] == "DELETE" and key[1] in ontology_resource_items:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 204, 400, 404, 409, 422), json_body=None, note="smoke: validate only")

    # ---------- Async instances (write-side) ----------
    if key == ("POST", "/api/v1/databases/{db_name}/instances/{class_label}/create"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id})}"
        body = {"data": {f"{ctx.class_id.lower()}_id": ctx.instance_id, "name": "OpenAPI Smoke Product"}, "metadata": {}}
        return RequestPlan(op.method, op.path, url, (202, 409, 404), json_body=body)

    if key == ("POST", "/api/v1/databases/{db_name}/instances/{class_label}/bulk-create"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id})}"
        body = {
            "instances": [
                {f"{ctx.class_id.lower()}_id": f"{ctx.instance_id}_bulk1", "name": "Bulk 1"},
                {f"{ctx.class_id.lower()}_id": f"{ctx.instance_id}_bulk2", "name": "Bulk 2"},
            ],
            "metadata": {"source": "openapi_smoke"},
        }
        return RequestPlan(op.method, op.path, url, (202, 404), json_body=body)

    if key == ("PUT", "/api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/update"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id, 'instance_id': ctx.instance_id})}"
        expected_seq = await _get_write_side_last_sequence(aggregate_type="Instance", aggregate_id=ctx.instance_aggregate_id)
        body = {"data": {"name": "OpenAPI Smoke Product (updated)"}, "metadata": {}}
        return RequestPlan(op.method, op.path, url, (202, 404, 409), params={"expected_seq": expected_seq}, json_body=body)

    if key == ("DELETE", "/api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/delete"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id, 'instance_id': ctx.instance_id})}"
        expected_seq = await _get_write_side_last_sequence(aggregate_type="Instance", aggregate_id=ctx.instance_aggregate_id)
        return RequestPlan(op.method, op.path, url, (202, 404, 409), params={"expected_seq": expected_seq})

    # ---------- Graph ----------
    if key == ("GET", "/api/v1/graph-query/{db_name}/paths"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400, 404),
            params={"source_class": ctx.class_id, "target_class": ctx.class_id, "max_depth": 1},
        )

    if key == ("POST", "/api/v1/graph-query/{db_name}/simple"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"class_name": ctx.class_id, "filters": {f"{ctx.class_id.lower()}_id": ctx.instance_id}, "limit": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    if key == ("POST", "/api/v1/graph-query/{db_name}/multi-hop"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"start_class": ctx.class_id, "hops": [], "filters": {f"{ctx.class_id.lower()}_id": ctx.instance_id}, "include_documents": False}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    if key == ("POST", "/api/v1/graph-query/{db_name}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"start_class": ctx.class_id, "hops": [], "filters": {f"{ctx.class_id.lower()}_id": ctx.instance_id}, "include_documents": False, "limit": 10, "offset": 0}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    # ---------- Lineage ----------
    if key == ("GET", "/api/v1/lineage/metrics"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/lineage/timeline"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200, 400), params={"db_name": ctx.db_name, "bucket_minutes": 15})

    if key == ("GET", "/api/v1/lineage/out-of-date"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400),
            params={"db_name": ctx.db_name, "freshness_slo_minutes": 120},
        )

    if key == ("GET", "/api/v1/lineage/runs"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400),
            params={"db_name": ctx.db_name, "run_limit": 50},
        )

    if key == ("GET", "/api/v1/lineage/run-impact"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400),
            params={"db_name": ctx.db_name, "run_id": "openapi-smoke-run"},
        )

    if key in {("GET", "/api/v1/lineage/graph"), ("GET", "/api/v1/lineage/impact")}:
        url = f"{BFF_URL}{op.path}"
        root = ctx.command_ids.get("create_database") or next(iter(ctx.command_ids.values()))
        return RequestPlan(op.method, op.path, url, (200, 400), params={"root": root, "db_name": ctx.db_name})

    if key == ("GET", "/api/v1/lineage/path"):
        url = f"{BFF_URL}{op.path}"
        root = ctx.command_ids.get("create_database") or next(iter(ctx.command_ids.values()))
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400),
            params={"source": root, "target": root, "db_name": ctx.db_name},
        )

    if key == ("GET", "/api/v1/lineage/diff"):
        url = f"{BFF_URL}{op.path}"
        root = ctx.command_ids.get("create_database") or next(iter(ctx.command_ids.values()))
        now = datetime.now(timezone.utc)
        from_as_of = (now - timedelta(minutes=30)).isoformat()
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400),
            params={
                "root": root,
                "db_name": ctx.db_name,
                "from_as_of": from_as_of,
                "to_as_of": now.isoformat(),
            },
        )

    if key == ("GET", "/api/v1/lineage/column-lineage"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 400, 404),
            params={
                "db_name": ctx.db_name,
                "branch": "main",
                "source_field": "name",
                "target_field": "name",
                "pair_limit": 100,
            },
        )

    # ---------- Audit ----------
    if key == ("GET", "/api/v1/audit/logs"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200, 400), params={"partition_key": f"db:{ctx.db_name}", "limit": 5})

    if key == ("GET", "/api/v1/audit/chain-head"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params={"partition_key": f"db:{ctx.db_name}"})

    # ---------- Background tasks ----------
    if key == ("GET", "/api/v1/tasks/"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/tasks/metrics/summary"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key in {("GET", "/api/v1/tasks/{task_id}"), ("GET", "/api/v1/tasks/{task_id}/result"), ("DELETE", "/api/v1/tasks/{task_id}")}:
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'task_id': str(uuid.uuid4())})}"
        # Non-existent task is a valid, expected scenario.
        return RequestPlan(op.method, op.path, url, (404, 200))

    # ---------- Label mappings (file upload) ----------
    if key == ("GET", "/api/v1/databases/{db_name}/mappings/"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("DELETE", "/api/v1/databases/{db_name}/mappings/"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 202, 404))

    if key == ("POST", "/api/v1/databases/{db_name}/mappings/export"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,), note="Export is a JSON file response")

    if key in {("POST", "/api/v1/databases/{db_name}/mappings/import"), ("POST", "/api/v1/databases/{db_name}/mappings/validate")}:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        form = aiohttp.FormData()
        form.add_field(
            "file",
            _mapping_file_bytes(ctx),
            filename=f"{ctx.db_name}_mappings.json",
            content_type="application/json",
        )
        return RequestPlan(op.method, op.path, url, (200,), form=form)

    # ---------- Funnel-backed schema/mapping suggestions ----------
    if key == ("POST", "/api/v1/databases/{db_name}/suggest-schema-from-data"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "class_name": ctx.class_id,
            "columns": [f"{ctx.class_id.lower()}_id", "name"],
            "data": [
                {f"{ctx.class_id.lower()}_id": ctx.instance_id, "name": "OpenAPI Smoke Product"},
            ],
            "include_complex_types": True,
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body)

    if key == ("POST", "/api/v1/databases/{db_name}/suggest-mappings"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "source_schema": [{"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"}, {"name": "name", "type": "xsd:string"}],
            "target_schema": [{"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"}, {"name": "name", "type": "xsd:string"}],
            "sample_data": [{f"{ctx.class_id.lower()}_id": ctx.instance_id, "name": "OpenAPI Smoke Product"}],
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body)

    if key == ("POST", "/api/v1/databases/{db_name}/suggest-schema-from-excel"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        excel_bytes = _xlsx_bytes(
            header=[f"{ctx.class_id.lower()}_id", "name"],
            rows=[[ctx.instance_id, "OpenAPI Smoke Product"], [f"{ctx.instance_id}_2", "OpenAPI Smoke Product 2"]],
        )
        form = aiohttp.FormData()
        form.add_field(
            "file",
            excel_bytes,
            filename="openapi_smoke.xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        return RequestPlan(op.method, op.path, url, (200, 400, 422, 501), form=form)

    if key == ("POST", "/api/v1/databases/{db_name}/suggest-mappings-from-excel"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        excel_bytes = _xlsx_bytes(
            header=[f"{ctx.class_id.lower()}_id", "name"],
            rows=[[ctx.instance_id, "OpenAPI Smoke Product"], [f"{ctx.instance_id}_2", "OpenAPI Smoke Product 2"]],
        )
        form = aiohttp.FormData()
        form.add_field(
            "file",
            excel_bytes,
            filename="openapi_smoke.xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        form.add_field("target_schema_json", _target_schema_json(ctx))
        params = {"target_class_id": ctx.class_id}
        return RequestPlan(op.method, op.path, url, (200, 400, 422), params=params, form=form)

    if key == ("POST", "/api/v1/databases/{db_name}/import-from-excel/dry-run"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        excel_bytes = _xlsx_bytes(
            header=[f"{ctx.class_id.lower()}_id", "name"],
            rows=[[ctx.instance_id, "OpenAPI Smoke Product"], [f"{ctx.instance_id}_2", "OpenAPI Smoke Product 2"]],
        )
        form = aiohttp.FormData()
        form.add_field(
            "file",
            excel_bytes,
            filename="openapi_smoke.xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        form.add_field("target_class_id", ctx.class_id)
        form.add_field("mappings_json", _mappings_json(ctx))
        return RequestPlan(op.method, op.path, url, (200, 400, 422), form=form)

    if key == ("POST", "/api/v1/databases/{db_name}/import-from-excel/commit"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        excel_bytes = _xlsx_bytes(
            header=[f"{ctx.class_id.lower()}_id", "name"],
            rows=[[ctx.instance_id, "OpenAPI Smoke Product"], [f"{ctx.instance_id}_2", "OpenAPI Smoke Product 2"]],
        )
        form = aiohttp.FormData()
        form.add_field(
            "file",
            excel_bytes,
            filename="openapi_smoke.xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        form.add_field("target_class_id", ctx.class_id)
        form.add_field("mappings_json", _mappings_json(ctx))
        form.add_field("target_schema_json", _target_schema_json(ctx))
        # Keep commit small
        form.add_field("batch_size", "10")
        form.add_field("allow_partial", "true")
        return RequestPlan(op.method, op.path, url, (200, 400, 422), form=form)

    # ---------- Google Sheets (no external assumption) ----------
    if key in {
        ("POST", "/api/v1/databases/{db_name}/suggest-schema-from-google-sheets"),
        ("POST", "/api/v1/databases/{db_name}/suggest-mappings-from-google-sheets"),
        ("POST", "/api/v1/databases/{db_name}/import-from-google-sheets/dry-run"),
        ("POST", "/api/v1/databases/{db_name}/import-from-google-sheets/commit"),
    }:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        # If a real sheet URL is provided, attempt a real call; otherwise trigger FastAPI validation (422).
        if SMOKE_GOOGLE_SHEET_URL:
            body = {"sheet_url": SMOKE_GOOGLE_SHEET_URL}
            if "target_class_id" in op.path or "import-from-google-sheets" in op.path or "suggest-" in op.path:
                body.update({"target_class_id": ctx.class_id, "class_name": ctx.class_id})
            if SMOKE_GOOGLE_SHEET_API_KEY:
                body["api_key"] = SMOKE_GOOGLE_SHEET_API_KEY
            return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body)
        return RequestPlan(op.method, op.path, url, (422,), json_body={})

    if key == ("POST", "/api/v1/data-connectors/google-sheets/register"):
        url = f"{BFF_URL}{op.path}"
        # Missing sheet_url must fail fast (400) without external calls.
        return RequestPlan(op.method, op.path, url, (400,), json_body={})

    if key == ("POST", "/api/v1/data-connectors/google-sheets/oauth/start"):
        url = f"{BFF_URL}{op.path}"
        body = {"redirect_uri": "http://localhost:5173/connector-callback", "db_name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (200, 400), json_body=body)

    if key == ("GET", "/api/v1/data-connectors/google-sheets/oauth/callback"):
        url = f"{BFF_URL}{op.path}"
        params = {"code": "smoke-code", "state": "invalid-state"}
        return RequestPlan(op.method, op.path, url, (400, 302), params=params)

    if key == ("GET", "/api/v1/data-connectors/google-sheets/drive/spreadsheets"):
        url = f"{BFF_URL}{op.path}"
        params = {"connection_id": ctx.connection_id or "missing_connection", "limit": 5}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params=params)

    if key == ("GET", "/api/v1/data-connectors/google-sheets/spreadsheets/{sheet_id}/worksheets"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'sheet_id': 'missing_sheet'})}"
        params = {"connection_id": ctx.connection_id or "missing_connection"}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params=params)

    if key == ("POST", "/api/v1/data-connectors/google-sheets/{sheet_id}/start-pipelining"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'sheet_id': 'missing_sheet'})}"
        body = {"db_name": ctx.db_name, "worksheet_name": "Sheet1", "limit": 5}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    if key == ("GET", "/api/v1/data-connectors/google-sheets/registered"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/data-connectors/google-sheets/{sheet_id}/preview"):
        # This endpoint should behave as "preview registered sheet". In smoke, we call with a non-registered ID.
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'sheet_id': 'not_registered_smoke'})}"
        return RequestPlan(op.method, op.path, url, (404, 400, 200))

    # ---------- Schema Changes ----------
    if key == ("GET", "/api/v1/schema-changes/history"):
        url = f"{BFF_URL}{op.path}"
        params = {"db_name": ctx.db_name, "limit": 5, "offset": 0}
        return RequestPlan(op.method, op.path, url, (200,), params=params)

    if key == ("PUT", "/api/v1/schema-changes/drifts/{drift_id}/acknowledge"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'drift_id': str(uuid.uuid4())})}"
        body = {"acknowledged_by": "openapi_smoke"}
        return RequestPlan(op.method, op.path, url, (404, 403), json_body=body)

    if key == ("POST", "/api/v1/schema-changes/subscriptions"):
        url = f"{BFF_URL}{op.path}"
        # Use a deterministic subject_id to keep this idempotent (unique constraint is user_id+subject_type+subject_id).
        headers = {"X-User-Id": "openapi_smoke"}
        body = {
            "subject_type": "object_type",
            "subject_id": ctx.class_id,
            "db_name": ctx.db_name,
            "severity_filter": ["warning", "breaking"],
            "notification_channels": ["websocket"],
        }
        return RequestPlan(op.method, op.path, url, (200, 403, 422), json_body=body, headers=headers)

    if key == ("DELETE", "/api/v1/schema-changes/subscriptions/{subscription_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'subscription_id': str(uuid.uuid4())})}"
        headers = {"X-User-Id": "openapi_smoke"}
        return RequestPlan(op.method, op.path, url, (404, 403, 200), headers=headers)

    if key == ("GET", "/api/v1/schema-changes/mappings/{mapping_spec_id}/compatibility"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'mapping_spec_id': str(uuid.uuid4())})}"
        params = {"db_name": ctx.db_name}
        return RequestPlan(op.method, op.path, url, (404, 200), params=params)

    if key == ("GET", "/api/v1/schema-changes/stats"):
        url = f"{BFF_URL}{op.path}"
        params = {"db_name": ctx.db_name, "days": 30}
        return RequestPlan(op.method, op.path, url, (200,), params=params)

    if key == ("DELETE", "/api/v1/data-connectors/google-sheets/{sheet_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'sheet_id': 'not_registered_smoke'})}"
        return RequestPlan(op.method, op.path, url, (404, 400, 200))

    if key == ("DELETE", "/api/v1/data-connectors/google-sheets/connections/{connection_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 404))

    # ---------- Connectivity v2 ----------
    if key == ("POST", "/api/v2/connectivity/connections"):
        url = f"{BFF_URL}{op.path}"
        body = {
            "displayName": "OpenAPI Smoke Connection",
            "configuration": {
                "type": "GoogleSheetsConnectionConfig",
                "accountEmail": "smoke@local.test",
            },
        }
        return RequestPlan(op.method, op.path, url, (201, 400, 403, 422), params={"preview": "true"}, json_body=body)

    if key == ("GET", "/api/v2/connectivity/connections"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/getConfiguration"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/getConfigurationBatch"):
        url = f"{BFF_URL}{op.path}"
        body = [{"connectionRid": "ri.spice.main.connection.smoke-missing"}]
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params={"preview": "true"}, json_body=body)

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/tableImports"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'tableImportRid': 'ri.spice.main.table-import.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/fileImports"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'fileImportRid': 'ri.spice.main.file-import.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/virtualTables"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'virtualTableRid': 'ri.spice.main.virtual-table.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("DELETE", "/api/v2/connectivity/connections/{connectionRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (204, 404, 400), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/test"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/updateSecrets"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        body = {"secrets": {"password": "smoke-secret"}}
        return RequestPlan(op.method, op.path, url, (204, 404, 400), params={"preview": "true"}, json_body=body)

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/updateExportSettings"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        body = {"exportSettings": {"exportsEnabled": True, "exportEnabledWithoutMarkingsValidation": False}}
        return RequestPlan(op.method, op.path, url, (204, 404, 400), params={"preview": "true"}, json_body=body)

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/exportRuns"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        body = {
            "targetUrl": "https://example.com/openapi-smoke-export",
            "method": "POST",
            "payload": {"source": "openapi_smoke"},
            "dryRun": True,
        }
        return RequestPlan(op.method, op.path, url, (202, 400, 404, 409), params={"preview": "true"}, json_body=body)

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/exportRuns/{exportRunRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'exportRunRid': 'ri.spice.main.export-run.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params={"preview": "true"})

    if key == ("GET", "/api/v2/connectivity/connections/{connectionRid}/exportRuns"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/uploadCustomJdbcDrivers"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        body = {"content": "smoke-driver"}
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 404, 400, 422),
            params={"preview": "true", "fileName": "smoke-driver.jar"},
            json_body=body,
        )

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/tableImports"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        body = {
            "displayName": "smoke-table-import",
            "importMode": "SNAPSHOT",
            "destination": {"ontology": ctx.db_name, "branchName": "main", "objectType": ctx.class_id},
            "config": {"type": "postgreSqlImportConfig", "query": "select 1 as id"},
        }
        return RequestPlan(op.method, op.path, url, (200, 404, 400, 403, 422), params={"preview": "true"}, json_body=body)

    if key == ("PUT", "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'tableImportRid': 'ri.spice.main.table-import.smoke-missing'})}"
        body = {"displayName": "smoke-table-import-updated", "importMode": "SNAPSHOT"}
        return RequestPlan(op.method, op.path, url, (200, 404, 400, 403, 422), params={"preview": "true"}, json_body=body)

    if key == ("DELETE", "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'tableImportRid': 'ri.spice.main.table-import.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (204, 404, 400), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/tableImports/{tableImportRid}/execute"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'tableImportRid': 'ri.spice.main.table-import.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 409, 400, 403, 500), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/fileImports"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        body = {
            "displayName": "smoke-file-import",
            "importMode": "SNAPSHOT",
            "destination": {"ontology": ctx.db_name, "branchName": "main", "objectType": ctx.class_id},
            "config": {"type": "postgreSqlFileImportConfig", "query": "select 1 as id"},
        }
        return RequestPlan(op.method, op.path, url, (200, 404, 400, 403, 422), params={"preview": "true"}, json_body=body)

    if key == ("PUT", "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'fileImportRid': 'ri.spice.main.file-import.smoke-missing'})}"
        body = {"displayName": "smoke-file-import-updated", "importMode": "SNAPSHOT"}
        return RequestPlan(op.method, op.path, url, (200, 404, 400, 403, 422), params={"preview": "true"}, json_body=body)

    if key == ("DELETE", "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'fileImportRid': 'ri.spice.main.file-import.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (204, 404, 400), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/fileImports/{fileImportRid}/execute"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'fileImportRid': 'ri.spice.main.file-import.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 409, 400, 403, 500), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/virtualTables"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing'})}"
        body = {
            "displayName": "smoke-virtual-table",
            "destination": {"ontology": ctx.db_name, "branchName": "main", "objectType": ctx.class_id},
            "config": {"type": "postgreSqlVirtualTableConfig", "query": "select 1 as id"},
        }
        return RequestPlan(op.method, op.path, url, (200, 404, 400, 403, 422), params={"preview": "true"}, json_body=body)

    if key == ("PUT", "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'virtualTableRid': 'ri.spice.main.virtual-table.smoke-missing'})}"
        body = {"displayName": "smoke-virtual-table-updated"}
        return RequestPlan(op.method, op.path, url, (200, 404, 400, 403, 422), params={"preview": "true"}, json_body=body)

    if key == ("DELETE", "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'virtualTableRid': 'ri.spice.main.virtual-table.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (204, 404, 400), params={"preview": "true"})

    if key == ("POST", "/api/v2/connectivity/connections/{connectionRid}/virtualTables/{virtualTableRid}/execute"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'connectionRid': 'ri.spice.main.connection.smoke-missing', 'virtualTableRid': 'ri.spice.main.virtual-table.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 409, 400, 403, 500), params={"preview": "true"})

    # ---------- Datasets v2 ----------
    if key == ("GET", "/api/v2/datasets"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404), params={"dbName": ctx.db_name, "pageSize": 10})

    if key == ("POST", "/api/v2/datasets"):
        url = f"{BFF_URL}{op.path}"
        body = {
            "name": f"openapi-smoke-dataset-{uuid.uuid4().hex[:8]}",
            "parentFolderRid": f"ri.spice.main.folder.{ctx.db_name}",
            "branchName": "main",
        }
        return RequestPlan(op.method, op.path, url, (200, 201, 400, 403, 409, 422), json_body=body)

    if key == ("GET", "/api/v2/datasets/{datasetRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400))

    if key == ("GET", "/api/v2/datasets/{datasetRid}/branches"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400))

    if key == ("POST", "/api/v2/datasets/{datasetRid}/branches"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        body = {"branchName": f"smoke-{uuid.uuid4().hex[:6]}", "sourceBranchName": "main"}
        return RequestPlan(op.method, op.path, url, (200, 201, 400, 404, 409, 422), json_body=body)

    if key == ("GET", "/api/v2/datasets/{datasetRid}/branches/{branchName}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000', 'branchName': 'main'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400))

    if key == ("DELETE", "/api/v2/datasets/{datasetRid}/branches/{branchName}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000', 'branchName': 'smoke-delete'})}"
        return RequestPlan(op.method, op.path, url, (204, 404, 400, 409))

    if key == ("GET", "/api/v2/datasets/{datasetRid}/files"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400))

    if key == ("GET", "/api/v2/datasets/{datasetRid}/files/{filePath}/content"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000', 'filePath': 'missing.csv'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400))

    if key == ("POST", "/api/v2/datasets/{datasetRid}/files:upload"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        form = aiohttp.FormData()
        form.add_field("branchName", "main")
        form.add_field("path", "smoke.csv")
        form.add_field("file", b"id,name\n1,smoke\n", filename="smoke.csv", content_type="text/csv")
        return RequestPlan(op.method, op.path, url, (200, 201, 400, 404, 409, 422), form=form)

    if key == ("POST", "/api/v2/datasets/{datasetRid}/readTable"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        body = {"branchName": "main", "path": "missing.csv", "pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 422), json_body=body)

    if key == ("GET", "/api/v2/datasets/{datasetRid}/schema"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 404, 400))

    if key == ("PUT", "/api/v2/datasets/{datasetRid}/schema"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        body = {"fieldSchemaList": [{"fieldPath": f"{ctx.class_id.lower()}_id", "type": {"type": "string"}}]}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409, 422), json_body=body)

    if key == ("POST", "/api/v2/datasets/{datasetRid}/transactions"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000'})}"
        body = {"transactionType": "APPEND", "branchName": "main"}
        return RequestPlan(op.method, op.path, url, (200, 201, 400, 404, 409, 422), json_body=body)

    if key == ("POST", "/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/abort"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000', 'transactionRid': 'ri.spice.main.transaction.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 204, 400, 404, 409, 422), json_body={})

    if key == ("POST", "/api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'datasetRid': 'ri.spice.main.dataset.00000000-0000-0000-0000-000000000000', 'transactionRid': 'ri.spice.main.transaction.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 204, 400, 404, 409, 422), json_body={})

    # ---------- Orchestration v2 ----------
    if key == ("POST", "/api/v2/orchestration/builds/create"):
        url = f"{BFF_URL}{op.path}"
        body = {"target": {"targetRids": ["ri.spice.main.pipeline.00000000-0000-0000-0000-000000000000"]}, "branchName": "main"}
        return RequestPlan(op.method, op.path, url, (200, 201, 400, 404, 409, 422), json_body=body)

    if key == ("POST", "/api/v2/orchestration/builds/getBatch"):
        url = f"{BFF_URL}{op.path}"
        body = {"buildRids": ["ri.spice.main.build.smoke-missing"]}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 422), json_body=body)

    if key == ("GET", "/api/v2/orchestration/builds/{buildRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'buildRid': 'ri.spice.main.build.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404))

    if key == ("GET", "/api/v2/orchestration/builds/{buildRid}/jobs"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'buildRid': 'ri.spice.main.build.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404))

    if key == ("POST", "/api/v2/orchestration/builds/{buildRid}/cancel"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'buildRid': 'ri.spice.main.build.smoke-missing'})}"
        return RequestPlan(op.method, op.path, url, (200, 204, 400, 404, 409, 422), json_body={})

    if key == ("POST", "/api/v2/orchestration/schedules"):
        url = f"{BFF_URL}{op.path}"
        body = {
            "targetRid": "ri.spice.main.pipeline.00000000-0000-0000-0000-000000000000",
            "schedule": {"type": "cron", "cronExpression": "0 * * * *"},
        }
        return RequestPlan(op.method, op.path, url, (200, 201, 400, 404, 409, 422), json_body=body)

    if key == ("GET", "/api/v2/orchestration/schedules/{scheduleRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'scheduleRid': 'ri.spice.main.schedule.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404))

    if key == ("DELETE", "/api/v2/orchestration/schedules/{scheduleRid}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'scheduleRid': 'ri.spice.main.schedule.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 204, 400, 404, 409))

    if key == ("POST", "/api/v2/orchestration/schedules/{scheduleRid}/pause"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'scheduleRid': 'ri.spice.main.schedule.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 204, 400, 404, 409), json_body={})

    if key == ("POST", "/api/v2/orchestration/schedules/{scheduleRid}/unpause"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'scheduleRid': 'ri.spice.main.schedule.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 204, 400, 404, 409), json_body={})

    if key == ("GET", "/api/v2/orchestration/schedules/{scheduleRid}/runs"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'scheduleRid': 'ri.spice.main.schedule.00000000-0000-0000-0000-000000000000'})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404))

    # ---------- Ontology v2 (additional) ----------
    if key == ("POST", "/api/v2/ontologies/attachments/upload"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 422), json_body={})

    if key == ("POST", "/api/v2/ontologies/{ontology}/objects/{objectType}/count"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id})}"
        body = {"where": {"type": "eq", "field": "class_id", "value": ctx.class_id}}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404, 422), json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objects/{objectType}/aggregate"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id})}"
        body = {"where": {"type": "eq", "field": "class_id", "value": ctx.class_id}, "aggregation": [{"type": "count", "name": "rowCount"}]}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404, 422), json_body=body)

    if key == ("POST", "/api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}/timeseries/{property}/streamPoints"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name, 'objectType': ctx.class_id, 'primaryKey': ctx.instance_id, 'property': 'missing_property'})}"
        body = {"pageSize": 10}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404, 422), json_body=body)

    # ---------- Actions (writeback) ----------
    if key == ("POST", "/api/v2/ontologies/{ontology}/actions/{action}/apply"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        body = {
            "options": {"mode": "VALIDATE_ONLY"},
            "parameters": {
                "product": {"class_id": ctx.class_id, "instance_id": ctx.instance_id},
                "new_name": "OpenAPI Smoke Product (validate)",
            }
        }
        params = {
            "branch": "main",
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404, 409, 422, 503), json_body=body, params=params)

    if key == ("POST", "/api/v2/ontologies/{ontology}/actions/{action}/applyBatch"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'ontology': ctx.db_name})}"
        body = {
            "requests": [
                {
                    "parameters": {
                        "product": {"class_id": ctx.class_id, "instance_id": ctx.instance_id},
                        "new_name": "OpenAPI Smoke Product (batch-v2)",
                    }
                }
            ]
        }
        params = {"branch": "main"}
        return RequestPlan(op.method, op.path, url, (200, 400, 403, 404, 409, 422), json_body=body, params=params)

    # ---------- AI (LLM) ----------
    if key == ("POST", "/api/v1/ai/intent"):
        url = f"{BFF_URL}{op.path}"
        # Intentionally invalid -> 422 without invoking the LLM router.
        return RequestPlan(op.method, op.path, url, (422,), json_body={})

    if key in {("POST", "/api/v1/ai/query/{db_name}"), ("POST", "/api/v1/ai/translate/query-plan/{db_name}")}:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"question": "List Product by product_id", "mode": "auto", "branch": "main", "limit": 5}
        # Without OPENAI_API_KEY, this must degrade to a clear 503 (not 500).
        expected = (200, 503, 400, 422) if not SMOKE_OPENAI_ENABLED else (200, 400, 422)
        return RequestPlan(
            op.method,
            op.path,
            url,
            expected,
            json_body=body,
            allow_5xx=not SMOKE_OPENAI_ENABLED,
        )

    # ---------- Fallback ----------
    # Generic safe defaults:
    # - GET: substitute path params; allow 404 for resource-specific routes.
    # - POST/PUT/DELETE without a recipe is a hard failure (we want full coverage).
    if op.method == "GET":
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        auth_mode = "admin"
        expected_statuses: Tuple[int, ...] = (200, 404)
        allow_5xx = False

        if op.path.startswith("/api/v1/agent"):
            auth_mode = "delegated_user"
        elif op.path.startswith("/api/v1/context-tools") or op.path.startswith("/api/v1/document-bundles"):
            auth_mode = "user"
        elif op.path.startswith("/api/v1/context7"):
            # Context7 depends on external MCP wiring; treat 503 as acceptable degradation.
            expected_statuses = (200, 400, 404, 503)
            allow_5xx = True

        return RequestPlan(
            op.method,
            op.path,
            url,
            expected_statuses,
            auth_mode=auth_mode,
            allow_5xx=allow_5xx,
        )

    raise AssertionError(f"Missing RequestPlan recipe for operation: {op.method} {op.path} (tags={op.tags}, summary={op.summary!r})")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_openapi_stable_contract_smoke():
    # Basic reachability check (no mocks).
    async with aiohttp.ClientSession(headers=BFF_HEADERS) as session:
        try:
            async with session.get(f"{BFF_URL}/api/v1/health") as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"BFF not healthy at {BFF_URL} (status={resp.status})"
                    )
        except Exception as e:  # pragma: no cover
            raise RuntimeError(f"BFF not reachable at {BFF_URL}: {e}")

    async with aiohttp.ClientSession(headers=BFF_HEADERS) as session:
        async with session.get(f"{BFF_URL}/openapi.json") as resp:
            assert resp.status == 200, f"Failed to fetch OpenAPI: {resp.status} {await resp.text()}"
            spec = await resp.json()

    spec_paths = set((spec.get("paths") or {}).keys())
    spec_paths_map = spec.get("paths") or {}

    # Legacy routes removed in Foundry/Postgres runtime must never reappear.
    reintroduced_strict_absent = sorted(path for path in REMOVED_V1_STRICT_ABSENT_PATHS if path in spec_paths)
    assert not reintroduced_strict_absent, f"Reintroduced legacy paths in OpenAPI: {reintroduced_strict_absent}"

    # Non-Foundry v2 extensions that previously leaked into public surface must stay absent.
    reintroduced_non_foundry_v2 = sorted(path for path in NON_FOUNDRY_V2_EXTENSION_ABSENT_PATHS if path in spec_paths)
    assert not reintroduced_non_foundry_v2, f"Reintroduced non-Foundry v2 extension paths in OpenAPI: {reintroduced_non_foundry_v2}"

    reintroduced_removed_ops = _collect_reintroduced_removed_ops(spec_paths_map)
    if reintroduced_removed_ops:
        print(f"[openapi] legacy compat operations still exposed: {reintroduced_removed_ops}")
        strict_removed_ops = str(os.getenv("OPENAPI_STRICT_REMOVED_V1_OPS", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if strict_removed_ops:
            local_reintroduced = _local_source_reintroduced_removed_ops()
            stale_runtime_hint = ""
            if local_reintroduced == []:
                stale_runtime_hint = (
                    " (local source OpenAPI is clean; running BFF instance likely stale. "
                    "Restart/redeploy local stack and retry.)"
                )
            raise AssertionError(
                f"Reintroduced legacy operations in OpenAPI: {reintroduced_removed_ops}{stale_runtime_hint}"
            )

    db_create_path = _pick_spec_path(spec_paths, "/api/v1/databases", "/api/v1/database")
    db_ontology_path = _pick_spec_path(
        spec_paths,
        "/api/v1/databases/{db_name}/ontology",
        "/api/v1/database/{db_name}/ontology",
    )
    object_type_full_metadata_path = _pick_spec_path(
        spec_paths,
        "/api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata",
    )
    db_instance_create_path = _pick_spec_path(
        spec_paths,
        "/api/v1/databases/{db_name}/instances/{class_label}/create",
        "/api/v1/database/{db_name}/instances/{class_label}/create",
    )
    db_mappings_import_path = _pick_spec_path(
        spec_paths,
        "/api/v1/databases/{db_name}/mappings/import",
        "/api/v1/database/{db_name}/mappings/import",
    )
    db_delete_path = _pick_spec_path(
        spec_paths,
        "/api/v1/databases/{db_name}",
        "/api/v1/database/{db_name}",
    )

    operations: list[Operation] = []
    for path, methods in (spec.get("paths") or {}).items():
        if not isinstance(methods, dict):
            continue
        for m, op_spec in methods.items():
            if m.lower() not in {"get", "post", "put", "delete", "patch"}:
                continue
            tags = tuple(op_spec.get("tags") or [])
            summary = str(op_spec.get("summary") or "")
            operations.append(Operation(method=m.upper(), path=path, tags=tags, summary=summary))

    wip = [op for op in operations if _is_wip(op)]
    ops_only = [op for op in operations if _is_ops_only(op)]
    included = [op for op in operations if not _is_wip(op) and not _is_ops_only(op)]

    # Ensure this test stays honest about what's excluded.
    print(f"[openapi] total_ops={len(operations)} included={len(included)} wip={len(wip)} ops_only={len(ops_only)}")
    if wip:
        print("[openapi] excluded_wip:")
        for op in sorted(wip, key=lambda o: (o.path, o.method)):
            print(f"  - {op.method} {op.path} tags={list(op.tags)} summary={op.summary!r}")
        raise AssertionError("WIP projection endpoints must not appear in OpenAPI schema")
    if ops_only:
        print("[openapi] excluded_ops_only:")
        for op in sorted(ops_only, key=lambda o: (o.path, o.method)):
            print(f"  - {op.method} {op.path} tags={list(op.tags)} summary={op.summary!r}")

    # Setup minimal shared state for path params.
    unique = uuid.uuid4().hex[:10]
    ctx = SmokeContext(
        db_name=f"openapi_smoke_{unique}",
        branch_name="feature/openapi-smoke",
        class_id="Product",
        advanced_class_id="Order",
        wrapper_class_id="SmokeWrapperClass",
        instance_id=f"prod_{unique}",
        command_ids={},
        udf_id="udf_smoke",
        action_type_id=f"RenameProduct_{unique}",
    )

    failures: list[str] = []
    executed: set[Tuple[str, str]] = set()

    # NOTE: Use force_close to avoid flaky keep-alive reuse across many sequential requests
    # (observed as occasional aiohttp.ServerDisconnectedError on a single op).
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60),
        headers=BFF_HEADERS,
        connector=aiohttp.TCPConnector(force_close=True),
    ) as session:
        # ---- Deterministic setup (also counts toward coverage) ----
        try:
            # Create DB (async ES mode) and wait
            plan = await _build_plan(
                Operation("POST", db_create_path, ("Database Management",), "Create Database"),
                ctx,
            )
            status, text, payload = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses:
                raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")

            if status == 202 and isinstance(payload, dict):
                command_id = ((payload.get("data") or {}).get("command_id")) or ((payload.get("data") or {}).get("commandId"))
                assert command_id, f"Missing command_id in 202 response: {payload}"
                ctx.command_ids["create_database"] = str(command_id)
                await _wait_for_command_completed(session, command_id=str(command_id))

            # Create ontology classes (Product + Order) on main. If the stack is running with
            # protected branches (e.g., ONTOLOGY_REQUIRE_PROPOSALS=true), writes to main will 409.
            # In that case, write to ctx.branch_name and merge/deploy into main via proposals.
            deployment_recorded = False
            action_type_created = False
            action_branch_for_runtime = "main"

            ontology_ops = [
                (
                    Operation("POST", db_ontology_path, ("Ontology Management",), "Create Ontology"),
                    _ontology_payload(class_id=ctx.class_id, label_en="Product", label_ko="제품"),
                ),
                (
                    Operation(
                        "POST",
                        db_ontology_path,
                        ("Ontology Management",),
                        "Create Ontology",
                    ),
                    _ontology_payload(class_id=ctx.advanced_class_id, label_en="Order", label_ko="주문"),
                ),
            ]
            needs_proposal: list[Tuple[str, Any]] = []
            for op, body in ontology_ops:
                plan = await _build_plan(op, ctx)
                status, text, payload = await _request(session, plan)
                executed.add((plan.method, plan.path_template))

                if status == 409:
                    needs_proposal.append((plan.path_template, body))
                    continue

                if status not in plan.expected_statuses:
                    raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")
                if status == 202 and isinstance(payload, dict):
                    command_id = ((payload.get("data") or {}).get("command_id")) or (
                        (payload.get("data") or {}).get("commandId")
                    )
                    if command_id:
                        await _wait_for_command_completed(session, command_id=str(command_id))

            if needs_proposal:
                # Write missing ontology resources to the feature branch.
                # In Foundry/Postgres profiles the legacy branch-management APIs may be removed.
                # Proposal/write APIs still accept branch refs, so fallback should not require
                # explicit branch-create endpoints to be present.
                for path_template, body in needs_proposal:
                    url = f"{BFF_URL}{_format_path(path_template, ctx)}"
                    write_class_id = str(body.get("id") if isinstance(body, dict) else "").strip() or "unknown"
                    branch_plan = RequestPlan(
                        "POST",
                        path_template,
                        url,
                        (200, 202, 409),
                        params={"branch": ctx.branch_name},
                        json_body=body,
                        note="smoke: proposal fallback write",
                    )
                    status, text, payload = await _request(session, branch_plan)
                    executed.add((branch_plan.method, branch_plan.path_template))
                    if status == 409:
                        raise AssertionError(
                            f"Protected-branch fallback failed for {branch_plan.path_template} (http=409): {text[:800]}"
                        )
                    if status == 200 and isinstance(payload, dict) and str(payload.get("status") or "").lower() == "error":
                        raise AssertionError(
                            f"Proposal fallback ontology write returned logical error "
                            f"(class_id={write_class_id}, http=200): {text[:800]}"
                        )
                    if status == 202 and isinstance(payload, dict):
                        command_id = ((payload.get("data") or {}).get("command_id")) or (
                            (payload.get("data") or {}).get("commandId")
                        )
                        if command_id:
                            await _wait_for_command_completed(session, command_id=str(command_id))

                # Create an action type on the branch so it lands in main via the same proposal.
                branch_head = await _get_ontology_head_commit(session, db_name=ctx.db_name, branch=ctx.branch_name)
                proposal_class_ids: list[str] = []
                proposal_class_meta: dict[str, tuple[str, str]] = {}
                for _, body in needs_proposal:
                    if not isinstance(body, dict):
                        continue
                    class_id = str(body.get("id") or "").strip()
                    if not class_id:
                        continue
                    label = body.get("label") if isinstance(body.get("label"), dict) else {}
                    label_en = str(label.get("en") or class_id).strip() or class_id
                    label_ko = str(label.get("ko") or class_id).strip() or class_id
                    if class_id not in proposal_class_meta:
                        proposal_class_ids.append(class_id)
                        proposal_class_meta[class_id] = (label_en, label_ko)
                if ctx.class_id not in proposal_class_meta:
                    # Action validation later references ctx.class_id (Product). Ensure the same class
                    # exists on the feature branch when only a subset of ontology writes fell back.
                    ensure_product_plan = RequestPlan(
                        "POST",
                        db_ontology_path,
                        f"{BFF_URL}{_format_path(db_ontology_path, ctx)}",
                        (200, 202, 409),
                        params={"branch": ctx.branch_name},
                        json_body=_ontology_payload(class_id=ctx.class_id, label_en="Product", label_ko="제품"),
                        note="smoke: ensure action target class exists on proposal branch",
                    )
                    status, text, payload = await _request(session, ensure_product_plan)
                    executed.add((ensure_product_plan.method, ensure_product_plan.path_template))
                    if status not in ensure_product_plan.expected_statuses:
                        raise AssertionError(
                            f"Failed to ensure action target class on branch (http={status}): {text[:800]}"
                        )
                    if status == 200 and isinstance(payload, dict) and str(payload.get("status") or "").lower() == "error":
                        raise AssertionError(
                            f"Ensure action target class returned logical error (http=200): {text[:800]}"
                        )
                    if status == 202 and isinstance(payload, dict):
                        command_id = ((payload.get("data") or {}).get("command_id")) or (
                            (payload.get("data") or {}).get("commandId")
                        )
                        if command_id:
                            await _wait_for_command_completed(session, command_id=str(command_id))
                    proposal_class_ids.append(ctx.class_id)
                    proposal_class_meta[ctx.class_id] = ("Product", "제품")

                action_target_class = ctx.class_id
                action_type_url = f"{OMS_URL}/api/v1/database/{ctx.db_name}/ontology/resources/action-types"
                action_type_body = {
                    "id": ctx.action_type_id,
                    "label": {"en": "Rename Product", "ko": "상품 이름 변경"},
                    "description": {"en": "Rename an existing Product", "ko": "기존 Product 인스턴스 이름 변경"},
                    "spec": {
                        "input_schema": {
                            "fields": [
                                {
                                    "name": "product",
                                    "type": "object_ref",
                                    "required": True,
                                    "object_type": action_target_class,
                                },
                                {"name": "new_name", "type": "string", "required": True, "max_length": 2000},
                            ],
                            "allow_extra_fields": False,
                        },
                        "permission_policy": {
                            "effect": "ALLOW",
                            "principals": ["role:Owner", "role:Editor", "role:DomainModeler"],
                        },
                        "writeback_target": {
                            "repo": "ontology-writeback",
                            "branch": f"writeback-{ctx.db_name}",
                        },
                        "conflict_policy": "FAIL",
                        "implementation": {
                            "type": "template_v1",
                            "targets": [
                                {
                                    "target": {"from": "input.product"},
                                    "changes": {
                                        "set": {"name": {"$ref": "input.new_name"}},
                                    },
                                }
                            ],
                        },
                    },
                    "metadata": {"source": "openapi_smoke"},
                }
                async with session.post(
                    action_type_url,
                    headers=BFF_HEADERS,
                    params={"branch": ctx.branch_name, "expected_head_commit": branch_head},
                    json=action_type_body,
                ) as resp:
                    if resp.status != 201:
                        raise AssertionError(
                            f"Failed to create action type on branch (http={resp.status}): {await resp.text()}"
                        )
                action_type_created = True
                action_branch_for_runtime = ctx.branch_name

                # Ontology approval gates require object type contracts for any object types
                # introduced on the feature branch (e.g., Product/Order). Bootstrap a minimal
                # backing dataset + contracts so the proposal can be approved deterministically.
                dataset_upload_url = f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload"
                dataset_form = aiohttp.FormData()
                dataset_bytes = _csv_bytes(
                    header=["product_id", "order_id", "name"],
                    rows=[[ctx.instance_id, f"order_{ctx.instance_id}", "OpenAPI Smoke"]],
                )
                dataset_form.add_field(
                    "file",
                    dataset_bytes,
                    filename="openapi_smoke_products_orders.csv",
                    content_type="text/csv",
                )
                dataset_form.add_field("dataset_name", ctx.dataset_name)
                dataset_form.add_field("description", "openapi smoke bootstrap dataset (for object type contracts)")
                dataset_form.add_field("has_header", "true")
                dataset_upload_plan = RequestPlan(
                    "POST",
                    "/api/v1/pipelines/datasets/csv-upload",
                    dataset_upload_url,
                    (200,),
                    params={"db_name": ctx.db_name, "branch": "main"},
                    form=dataset_form,
                    headers={"Idempotency-Key": f"openapi-smoke-{ctx.db_name}-dataset", "X-DB-Name": ctx.db_name},
                    note="smoke: bootstrap dataset for ontology approval gates",
                )
                status, text, payload = await _request(session, dataset_upload_plan)
                executed.add((dataset_upload_plan.method, dataset_upload_plan.path_template))
                if status != 200 or not isinstance(payload, dict):
                    raise AssertionError(f"Failed to bootstrap dataset (http={status}): {text[:800]}")
                data = payload.get("data")
                if isinstance(data, dict):
                    dataset = data.get("dataset") if isinstance(data.get("dataset"), dict) else None
                    if isinstance(dataset, dict):
                        ctx.dataset_id = str(dataset.get("dataset_id") or dataset.get("id") or "").strip() or ctx.dataset_id
                if not ctx.dataset_id:
                    raise AssertionError(f"Missing dataset_id in bootstrap dataset response: {payload}")

                object_type_url = f"{OMS_URL}/api/v1/database/{ctx.db_name}/ontology/resources/object-types"
                object_type_classes = [
                    (class_id, *proposal_class_meta.get(class_id, (class_id, class_id)))
                    for class_id in proposal_class_ids
                ]
                for class_id, label_en, label_ko in object_type_classes:
                    schema_url = f"{BFF_URL}{_format_path(object_type_full_metadata_path, ctx, overrides={'objectType': class_id})}"
                    try:
                        await _wait_for_ontology_schema_ready(
                            session,
                            schema_url=schema_url,
                            branch=ctx.branch_name,
                            class_id=class_id,
                            include_preview=True,
                            timeout_seconds=5,
                        )
                    except AssertionError as schema_exc:
                        # Some protected-branch stacks acknowledge ontology commands but expose
                        # read-side visibility later than expected; re-issue a branch write once.
                        recovery_plan = RequestPlan(
                            "POST",
                            db_ontology_path,
                            f"{BFF_URL}{_format_path(db_ontology_path, ctx)}",
                            (200, 202, 409),
                            params={"branch": ctx.branch_name},
                            json_body=_ontology_payload(class_id=class_id, label_en=label_en, label_ko=label_ko),
                            note="smoke: self-heal ontology class on branch before object-type bootstrap",
                        )
                        status, text, payload = await _request(session, recovery_plan)
                        executed.add((recovery_plan.method, recovery_plan.path_template))
                        if status not in recovery_plan.expected_statuses:
                            raise AssertionError(
                                f"Failed to self-heal ontology class {class_id} on branch "
                                f"(http={status}): {text[:800]}"
                            ) from schema_exc
                        if status == 200 and isinstance(payload, dict) and str(payload.get("status") or "").lower() == "error":
                            raise AssertionError(
                                f"Self-heal ontology write returned logical error "
                                f"(class_id={class_id}, http=200): {text[:800]}"
                            ) from schema_exc
                        if status == 202 and isinstance(payload, dict):
                            command_id = ((payload.get("data") or {}).get("command_id")) or (
                                (payload.get("data") or {}).get("commandId")
                            )
                            if command_id:
                                await _wait_for_command_completed(session, command_id=str(command_id))
                        # Skip a second long polling cycle here; proceed directly to object_type
                        # contract bootstrap, which is the authoritative convergence step.
                    object_type_plan = RequestPlan(
                        "POST",
                        "/api/v1/database/{db_name}/ontology/resources/object-types",
                        object_type_url,
                        (201, 409),
                        params={
                            "branch": ctx.branch_name,
                            "expected_head_commit": await _get_ontology_head_commit(
                                session,
                                db_name=ctx.db_name,
                                branch=ctx.branch_name,
                            ),
                        },
                        json_body=_object_type_resource_payload(
                            class_id=class_id,
                            backing_dataset_id=ctx.dataset_id,
                            pk_spec={"primary_key": [f"{class_id.lower()}_id"], "title_key": ["name"]},
                            status="ACTIVE",
                            metadata={"source": "openapi_smoke"},
                        ),
                        headers={"X-DB-Name": ctx.db_name},
                        note="smoke: bootstrap object type contract for ontology approval gates",
                    )
                    status, text, _ = await _request(session, object_type_plan)
                    executed.add((object_type_plan.method, object_type_plan.path_template))
                    if status == 409:
                        # In Foundry-style profiles, ontology create may already register a minimal
                        # object_type placeholder. Converge it to a fully valid contract via update.
                        update_url = f"{object_type_url}/{class_id}"
                        update_plan = RequestPlan(
                            "PUT",
                            "/api/v1/database/{db_name}/ontology/resources/object-types/{resource_id}",
                            update_url,
                            (200,),
                            params={
                                "branch": ctx.branch_name,
                                "expected_head_commit": await _get_ontology_head_commit(
                                    session,
                                    db_name=ctx.db_name,
                                    branch=ctx.branch_name,
                                ),
                            },
                            json_body=_object_type_resource_payload(
                                class_id=class_id,
                                backing_dataset_id=ctx.dataset_id,
                                pk_spec={"primary_key": [f"{class_id.lower()}_id"], "title_key": ["name"]},
                                status="ACTIVE",
                                metadata={"source": "openapi_smoke"},
                            ),
                            headers={"X-DB-Name": ctx.db_name},
                            note="smoke: converge object type contract when create is already present",
                        )
                        status, text, _ = await _request(session, update_plan)
                        executed.add((update_plan.method, update_plan.path_template))
                        if status not in update_plan.expected_statuses:
                            raise AssertionError(
                                f"Failed to converge object type contract for {class_id} (http={status}): {text[:800]}"
                            )
                        continue
                    if status not in object_type_plan.expected_statuses:
                        raise AssertionError(
                            f"Failed to bootstrap object type contract for {class_id} (http={status}): {text[:800]}"
                        )

                # Legacy BFF proposal/deploy routes are removed. For smoke setup, mark
                # the latest main-head ontology commit as deployed via registry helper.
                main_head = await _get_ontology_head_commit(session, db_name=ctx.db_name, branch="main")
                await _record_deployed_commit(
                    db_name=ctx.db_name,
                    target_branch="main",
                    ontology_commit_id=main_head,
                )
                deployment_recorded = True

            # Create one instance (async) and wait
            plan = await _build_plan(
                Operation(
                    "POST",
                    db_instance_create_path,
                    ("Async Instance Management",),
                    "Create Instance Async",
                ),
                ctx,
            )
            status, text, payload = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses:
                raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")
            if status == 202 and isinstance(payload, dict):
                command_id = payload.get("command_id") or (payload.get("data") or {}).get("command_id")
                assert command_id, f"Missing command_id for async instance create: {payload}"
                ctx.command_ids["create_instance"] = str(command_id)
                await _wait_for_command_completed(session, command_id=str(command_id), timeout_seconds=180)
            action_instance_id_for_runtime = ctx.instance_id

            # Create one action type and mark the current ontology commit as deployed so Action simulation can run.
            if not action_type_created:
                head_commit = await _get_ontology_head_commit(session, db_name=ctx.db_name, branch="main")
                action_type_url = f"{OMS_URL}/api/v1/database/{ctx.db_name}/ontology/resources/action-types"
                action_type_body = {
                    "id": ctx.action_type_id,
                    "label": {"en": "Rename Product", "ko": "상품 이름 변경"},
                    "description": {"en": "Rename an existing Product", "ko": "기존 Product 인스턴스 이름 변경"},
                    "spec": {
                        "input_schema": {
                            "fields": [
                                {"name": "product", "type": "object_ref", "required": True, "object_type": ctx.class_id},
                                {"name": "new_name", "type": "string", "required": True, "max_length": 2000},
                            ],
                            "allow_extra_fields": False,
                        },
                        "permission_policy": {
                            "effect": "ALLOW",
                            "principals": ["role:Owner", "role:Editor", "role:DomainModeler"],
                        },
                        "writeback_target": {
                            "repo": "ontology-writeback",
                            "branch": f"writeback-{ctx.db_name}",
                        },
                        "conflict_policy": "FAIL",
                        "implementation": {
                            "type": "template_v1",
                            "targets": [
                                {
                                    "target": {"from": "input.product"},
                                    "changes": {
                                        "set": {"name": {"$ref": "input.new_name"}},
                                    },
                                }
                            ],
                        },
                    },
                    "metadata": {"source": "openapi_smoke"},
                }

                async with session.post(
                    action_type_url,
                    headers=BFF_HEADERS,
                    params={"branch": "main", "expected_head_commit": head_commit},
                    json=action_type_body,
                ) as resp:
                    status = resp.status
                    text = await resp.text()

                if status == 201:
                    action_type_created = True
                    action_branch_for_runtime = "main"
                elif status == 409:
                    # Some stacks protect ontology resources (including action-types) on main, even if
                    # class creation is allowed (or already happened). In that case, create the action
                    # type on a fresh branch from the current main head and merge+deploy it.
                    action_branch = ctx.branch_name

                    branch_head = await _get_ontology_head_commit(session, db_name=ctx.db_name, branch=action_branch)
                    async with session.post(
                        action_type_url,
                        headers=BFF_HEADERS,
                        params={"branch": action_branch, "expected_head_commit": branch_head},
                        json=action_type_body,
                    ) as resp:
                        status = resp.status
                        text = await resp.text()
                    if status != 201:
                        raise AssertionError(f"Failed to create action type on fallback branch (http={status}): {text[:800]}")
                    action_branch_for_runtime = action_branch

                    # Legacy BFF proposal/deploy routes are removed. For smoke setup, mark
                    # the latest main-head ontology commit as deployed via registry helper.
                    main_head = await _get_ontology_head_commit(session, db_name=ctx.db_name, branch="main")
                    await _record_deployed_commit(
                        db_name=ctx.db_name,
                        target_branch="main",
                        ontology_commit_id=main_head,
                    )
                    deployment_recorded = True
                    action_type_created = True
                else:
                    raise AssertionError(f"Failed to create action type (http={status}): {text[:800]}")

            if not deployment_recorded:
                deployed_commit = await _get_ontology_head_commit(session, db_name=ctx.db_name, branch="main")
                await _record_deployed_commit(db_name=ctx.db_name, target_branch="main", ontology_commit_id=deployed_commit)
                deployment_recorded = True
            if action_branch_for_runtime != "main":
                action_branch_head = await _get_ontology_head_commit(
                    session,
                    db_name=ctx.db_name,
                    branch=action_branch_for_runtime,
                )
                await _record_deployed_commit(
                    db_name=ctx.db_name,
                    target_branch=action_branch_for_runtime,
                    ontology_commit_id=action_branch_head,
                )
                # Ensure validate/apply can resolve base instance state on the same branch
                # where the action type is materialized.
                branch_instance_plan = await _build_plan(
                    Operation(
                        "POST",
                        db_instance_create_path,
                        ("Async Instance Management",),
                        "Create Instance Async",
                    ),
                    ctx,
                )
                branch_instance_params = dict(branch_instance_plan.params or {})
                branch_instance_params["branch"] = action_branch_for_runtime
                branch_instance_plan.params = branch_instance_params
                branch_instance_id = f"{ctx.instance_id}_branch"
                if isinstance(branch_instance_plan.json_body, dict):
                    branch_payload = dict(branch_instance_plan.json_body)
                    data_payload = dict((branch_payload.get("data") or {}))
                    data_payload[f"{ctx.class_id.lower()}_id"] = branch_instance_id
                    branch_payload["data"] = data_payload
                    branch_instance_plan.json_body = branch_payload
                status, text, payload = await _request(session, branch_instance_plan)
                executed.add((branch_instance_plan.method, branch_instance_plan.path_template))
                if status not in branch_instance_plan.expected_statuses:
                    raise AssertionError(
                        f"{branch_instance_plan.method} {branch_instance_plan.path_template} "
                        f"unexpected status on action branch {action_branch_for_runtime}: {status}: {text[:500]}"
                    )
                if status == 202 and isinstance(payload, dict):
                    command_id = payload.get("command_id") or (payload.get("data") or {}).get("command_id")
                    if command_id:
                        ctx.command_ids["create_instance_action_branch"] = str(command_id)
                        await _wait_for_command_completed(session, command_id=str(command_id), timeout_seconds=180)
                if status in {202, 409}:
                    action_instance_id_for_runtime = branch_instance_id

            await _wait_for_action_type_ready(
                session,
                db_name=ctx.db_name,
                action_type_id=ctx.action_type_id,
                branch=action_branch_for_runtime,
            )

            apply_validate_url = f"{BFF_URL}/api/v2/ontologies/{ctx.db_name}/actions/{ctx.action_type_id}/apply"
            apply_validate_body = {
                "options": {"mode": "VALIDATE_ONLY"},
                "parameters": {
                    "product": {"class_id": ctx.class_id, "instance_id": action_instance_id_for_runtime},
                    "new_name": "OpenAPI Smoke Product (validate)",
                }
            }
            apply_validate_params = {"branch": action_branch_for_runtime}
            async with session.post(apply_validate_url, params=apply_validate_params, json=apply_validate_body) as resp:
                status = resp.status
                text = await resp.text()
                executed.add(("POST", "/api/v2/ontologies/{ontology}/actions/{action}/apply"))
                if status != 200:
                    raise AssertionError(f"Action validate apply failed (http={status}): {text[:800]}")

            # Import mappings so label-based query can run deterministically
            plan = await _build_plan(
                Operation(
                    "POST",
                    db_mappings_import_path,
                    ("Label Mappings",),
                    "Import Mappings",
                ),
                ctx,
            )
            status, text, _ = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses:
                raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")

            # Create dataset (Pipeline Builder) for dataset_id-dependent routes
            plan = await _build_plan(
                Operation("POST", "/api/v1/pipelines/datasets", ("Pipeline Builder",), "Create Dataset"),
                ctx,
            )
            status, text, payload = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses:
                raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")
            if isinstance(payload, dict):
                dataset = (payload.get("data") or {}).get("dataset")
                if isinstance(dataset, dict):
                    ctx.dataset_id = dataset.get("dataset_id") or ctx.dataset_id

            # Create dataset version for ingest-request dependent routes.
            if ctx.dataset_id:
                plan = await _build_plan(
                    Operation(
                        "POST",
                        "/api/v1/pipelines/datasets/{dataset_id}/versions",
                        ("Pipeline Builder",),
                        "Create Dataset Version",
                    ),
                    ctx,
                )
                status, text, payload = await _request(session, plan)
                executed.add((plan.method, plan.path_template))
                if status not in plan.expected_statuses:
                    raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")
                if isinstance(payload, dict):
                    version = (payload.get("data") or {}).get("version")
                    if isinstance(version, dict):
                        ctx.dataset_version_id = (
                            version.get("version_id") or version.get("versionId") or ctx.dataset_version_id
                        )
                        ctx.ingest_request_id = (
                            version.get("ingest_request_id") or version.get("ingestRequestId") or ctx.ingest_request_id
                        )

            # Create pipeline for pipeline_id-dependent routes
            plan = await _build_plan(
                Operation("POST", "/api/v1/pipelines", ("Pipeline Builder",), "Create Pipeline"),
                ctx,
            )
            status, text, payload = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses:
                raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")
            if isinstance(payload, dict):
                pipeline = (payload.get("data") or {}).get("pipeline")
                if isinstance(pipeline, dict):
                    ctx.pipeline_id = pipeline.get("pipeline_id") or ctx.pipeline_id

        except Exception:
            # Setup failure is fatal; without it, most OpenAPI ops can't be exercised meaningfully.
            raise

        # ---- Execute remaining included OpenAPI operations (no early-stop) ----
        # Order matters: avoid deleting shared state until the very end.
        included_non_delete = [op for op in included if op.method != "DELETE"]
        included_delete = [op for op in included if op.method == "DELETE" and op.path != db_delete_path]

        for op in sorted(included_non_delete, key=lambda o: (o.path, o.method)):
            op_key = (op.method, op.path)
            if op_key in executed:
                continue

            try:
                plan = await _build_plan(op, ctx)
            except Exception as e:
                failures.append(f"[RECIPE] {op.method} {op.path}: {e}")
                continue

            try:
                status, text, _ = await _request(session, plan)

                # Enterprise rule: 5xx is always a failure unless a plan explicitly expects it.
                if 500 <= status <= 599 and not plan.allow_5xx:
                    failures.append(
                        f"[5XX] {plan.method} {plan.path_template} -> {status} (note={plan.note!r}) body={text[:800]}"
                    )
                elif status not in plan.expected_statuses:
                    failures.append(
                        f"[STATUS] {plan.method} {plan.path_template} expected={plan.expected_statuses} got={status} body={text[:800]}"
                    )
            except Exception as e:
                failures.append(f"[EXC] {plan.method} {plan.path_template}: {e}")
            finally:
                executed.add(op_key)

        for op in sorted(included_delete, key=lambda o: (o.path, o.method)):
            op_key = (op.method, op.path)
            if op_key in executed:
                continue

            try:
                plan = await _build_plan(op, ctx)
            except Exception as e:
                failures.append(f"[RECIPE] {op.method} {op.path}: {e}")
                continue

            try:
                status, text, _ = await _request(session, plan)

                if 500 <= status <= 599 and not plan.allow_5xx:
                    failures.append(
                        f"[5XX] {plan.method} {plan.path_template} -> {status} (note={plan.note!r}) body={text[:800]}"
                    )
                elif status not in plan.expected_statuses:
                    failures.append(
                        f"[STATUS] {plan.method} {plan.path_template} expected={plan.expected_statuses} got={status} body={text[:800]}"
                    )
            except Exception as e:
                failures.append(f"[EXC] {plan.method} {plan.path_template}: {e}")
            finally:
                executed.add(op_key)

        # ---- Always attempt cleanup (delete DB) even if some ops failed ----
        try:
            plan = await _build_plan(
                Operation("DELETE", db_delete_path, ("Database Management",), "Delete Database"),
                ctx,
            )
            status, text, _ = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses and status != 404:
                failures.append(f"[CLEANUP] {plan.method} {plan.path_template} got={status} body={text[:500]}")
        except Exception as e:
            failures.append(f"[CLEANUP_EXC] delete_database: {e}")

    # Coverage: every included operation must have been attempted.
    attempted = {(m, p) for (m, p) in executed}
    missing = {(op.method, op.path) for op in included} - attempted
    if missing:
        failures.append(f"[COVERAGE] Missing attempts for ops: {sorted(missing)}")

    if failures:
        joined = "\n".join(failures)
        raise AssertionError(f"OpenAPI contract smoke failures ({len(failures)}):\n{joined}")
