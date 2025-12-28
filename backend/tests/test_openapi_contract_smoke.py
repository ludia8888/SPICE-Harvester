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
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import aiohttp
import pytest

from tests.utils.auth import bff_auth_headers

BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")

# Optional external verification
SMOKE_GOOGLE_SHEET_URL = (os.getenv("SMOKE_GOOGLE_SHEET_URL") or "").strip()
SMOKE_GOOGLE_SHEET_API_KEY = (os.getenv("SMOKE_GOOGLE_SHEET_API_KEY") or "").strip()
SMOKE_OPENAI_ENABLED = (os.getenv("OPENAI_API_KEY") or "").strip() != ""
_ENV_SMOKE_TOKEN = (
    (os.getenv("SMOKE_ADMIN_TOKEN") or os.getenv("BFF_ADMIN_TOKEN") or os.getenv("ADMIN_TOKEN") or "").strip()
)
BFF_HEADERS = bff_auth_headers()
if _ENV_SMOKE_TOKEN and _ENV_SMOKE_TOKEN != BFF_HEADERS.get("X-Admin-Token"):
    raise AssertionError("SMOKE_ADMIN_TOKEN differs from BFF auth token.")
SMOKE_ADMIN_TOKEN = _ENV_SMOKE_TOKEN or (BFF_HEADERS.get("X-Admin-Token") or "")


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
            pass

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

        status_value = str(last.get("status") or "").upper()
        if status_value in {"COMPLETED", "FAILED", "CANCELLED"}:
            if status_value != "COMPLETED":
                raise AssertionError(f"Command {command_id} ended in {status_value}: {last}")
            await asyncio.sleep(0.5)
            return last

        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for command completion (command_id={command_id}, last={last})")


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


def _is_wip(op: Operation) -> bool:
    if op.path.startswith("/api/v1/projections"):
        return True
    if any("Projections" in t or "WIP" in t for t in op.tags):
        return True
    if "🚧" in (op.summary or "") or "WIP" in (op.summary or ""):
        return True
    return False


def _is_ops_only(op: Operation) -> bool:
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
    legacy_class_id: str
    instance_id: str
    command_ids: Dict[str, str]
    pipeline_id: Optional[str] = None
    dataset_id: Optional[str] = None
    connection_id: Optional[str] = None

    @property
    def ontology_aggregate_id(self) -> str:
        return f"{self.db_name}:main:{self.class_id}"

    @property
    def advanced_ontology_aggregate_id(self) -> str:
        return f"{self.db_name}:main:{self.advanced_class_id}"

    @property
    def instance_aggregate_id(self) -> str:
        return f"{self.db_name}:main:{self.class_id}:{self.instance_id}"

    @property
    def pipeline_name(self) -> str:
        return f"{self.db_name}_pipeline"

    @property
    def dataset_name(self) -> str:
        return f"{self.db_name}_dataset"

def _safe_pipeline_ref(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "main"
    allowed: list[str] = []
    for ch in raw:
        if ch.isalnum() or ch in {"-", "_", "."}:
            allowed.append(ch)
        else:
            allowed.append("-")
    cleaned = "".join(allowed).strip("-")
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned or "main"


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


async def _request(
    session: aiohttp.ClientSession,
    plan: RequestPlan,
) -> Tuple[int, str, Optional[Dict[str, Any]]]:
    kwargs: Dict[str, Any] = {}
    if plan.params:
        kwargs["params"] = plan.params
    headers: Dict[str, str] = dict(BFF_HEADERS)
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
        "command_id": ctx.command_ids.get("create_database") or next(iter(ctx.command_ids.values()), ""),
        "sheet_id": "0000000000000000000000000000000000000000",
        "task_id": "00000000-0000-0000-0000-000000000000",
        "pipeline_id": ctx.pipeline_id or "pipeline_smoke",
        "dataset_id": ctx.dataset_id or "dataset_smoke",
        "connection_id": ctx.connection_id or "missing_connection",
    }
    if overrides:
        values.update(overrides)
    return template.format(**values)


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
            },
        ],
        "relationships": [],
        "metadata": {"source": "openapi_smoke"},
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

    key = (op.method, op.path)

    # ---------- Health ----------
    if key == ("GET", "/api/v1/"):
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (200,))
    if key == ("GET", "/api/v1/health"):
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (200,))
    if key == ("GET", "/api/v1/graph-query/health"):
        return RequestPlan(op.method, op.path, f"{BFF_URL}{op.path}", (200,))

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

    if key == ("GET", "/api/v1/pipelines/branches"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name})

    if key == ("GET", "/api/v1/pipelines/datasets"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name})

    if key == ("GET", "/api/v1/pipelines/proposals"):
        url = f"{BFF_URL}{op.path}"
        return RequestPlan(op.method, op.path, url, (200,), params={"db_name": ctx.db_name})

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

    if key == ("PUT", "/api/v1/pipelines/{pipeline_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "description": "openapi smoke pipeline update",
            "definition_json": {"nodes": [], "edges": []},
            "branch": "main",
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body)

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/branches"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"branch": ctx.branch_name}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), json_body=body)

    if key == ("POST", "/api/v1/pipelines/branches/{branch}/archive"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'branch': _safe_pipeline_ref(ctx.branch_name)})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), params={"db_name": ctx.db_name})

    if key == ("POST", "/api/v1/pipelines/branches/{branch}/restore"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'branch': _safe_pipeline_ref(ctx.branch_name)})}"
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409), params={"db_name": ctx.db_name})

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/build"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"db_name": ctx.db_name, "limit": 5}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 409, 503), json_body=body)

    if key == ("POST", "/api/v1/pipelines/{pipeline_id}/preview"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "db_name": ctx.db_name,
            "definition_json": {"nodes": [], "edges": []},
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
            "schema_json": {"columns": [{"name": "id", "type": "xsd:string"}]},
        }
        return RequestPlan(op.method, op.path, url, (200, 409, 400), json_body=body)

    if key == ("POST", "/api/v1/pipelines/datasets/{dataset_id}/versions"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "row_count": 1,
            "sample_json": {"columns": [{"name": "id", "type": "String"}], "rows": [{"id": "1"}]},
            "schema_json": {"columns": [{"name": "id", "type": "xsd:string"}]},
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

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

    if key == ("GET", "/api/v1/databases/{db_name}/branches"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))
    if key == ("POST", "/api/v1/databases/{db_name}/branches"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"name": ctx.branch_name, "from_branch": "main"}
        return RequestPlan(op.method, op.path, url, (200, 201, 404, 409), json_body=body)
    if key == ("GET", "/api/v1/databases/{db_name}/branches/{branch_name}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))
    if key == ("DELETE", "/api/v1/databases/{db_name}/branches/{branch_name}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 202, 404))

    if key == ("GET", "/api/v1/databases/{db_name}/classes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))
    if key == ("POST", "/api/v1/databases/{db_name}/classes"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        legacy_pk = f"{ctx.legacy_class_id.lower()}_id"
        body = {
            "@id": ctx.legacy_class_id,
            "label": ctx.legacy_class_id,
            "description": "legacy wrapper class (openapi smoke)",
            # Linter requires a stable primary key property.
            "properties": [
                {
                    "name": legacy_pk,
                    "type": "xsd:string",
                    "label": f"{ctx.legacy_class_id} ID",
                    "required": True,
                    "primaryKey": True,
                }
            ],
            "relationships": [],
        }
        return RequestPlan(op.method, op.path, url, (200, 202, 409, 404), json_body=body)
    if key == ("GET", "/api/v1/databases/{db_name}/classes/{class_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_id': ctx.class_id})}"
        return RequestPlan(op.method, op.path, url, (200,))

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
    if key == ("POST", "/api/v1/database/{db_name}/ontology"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = _ontology_payload(class_id=ctx.class_id, label_en="Product", label_ko="제품")
        return RequestPlan(op.method, op.path, url, (200, 202, 409), json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/ontology-advanced"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = _ontology_payload(class_id=ctx.advanced_class_id, label_en="Order", label_ko="주문")
        return RequestPlan(op.method, op.path, url, (200, 202, 409), json_body=body)

    if key == ("GET", "/api/v1/database/{db_name}/ontology/list"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/database/{db_name}/ontology/{class_label}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id})}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/database/{db_name}/ontology/{class_id}/schema"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_id': ctx.class_id})}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("POST", "/api/v1/database/{db_name}/ontology/validate"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = _ontology_payload(class_id=f"Validate{ctx.class_id}", label_en="ValidateClass", label_ko="검증클래스")
        return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/validate-relationships"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = _ontology_payload(class_id=f"RelValidate{ctx.class_id}", label_en="RelValidate", label_ko="관계검증")
        return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/check-circular-references"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        # No body -> checks existing DB schema.
        return RequestPlan(op.method, op.path, url, (200, 404))

    if key == ("GET", "/api/v1/database/{db_name}/relationship-network/analyze"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/database/{db_name}/relationship-paths"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 400), params={"start_entity": ctx.class_id})

    if key == ("PUT", "/api/v1/database/{db_name}/ontology/{class_label}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id})}"
        expected_seq = await _get_write_side_last_sequence(
            aggregate_type="OntologyClass", aggregate_id=ctx.ontology_aggregate_id
        )
        body = {"description": {"en": "Updated (openapi smoke)"}}
        return RequestPlan(op.method, op.path, url, (200, 202, 409, 404), params={"expected_seq": expected_seq}, json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/ontology/{class_label}/validate"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id})}"
        body = {"description": {"en": "Validate update (openapi smoke)"}}
        return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body)

    if key == ("DELETE", "/api/v1/database/{db_name}/ontology/{class_label}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.advanced_class_id})}"
        expected_seq = await _get_write_side_last_sequence(
            aggregate_type="OntologyClass", aggregate_id=ctx.advanced_ontology_aggregate_id
        )
        headers = {"X-Change-Reason": "openapi_smoke"}
        if SMOKE_ADMIN_TOKEN:
            headers["X-Admin-Token"] = SMOKE_ADMIN_TOKEN
        # Protected branch deletes require admin token; without it we expect 403.
        return RequestPlan(
            op.method,
            op.path,
            url,
            (200, 202, 404, 409, 403),
            params={"expected_seq": expected_seq},
            headers=headers,
        )

    if key == ("POST", "/api/v1/database/{db_name}/ontology/{class_id}/mapping-metadata"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_id': ctx.class_id})}"
        body = {"source": "openapi_smoke", "note": "mapping metadata smoke"}
        return RequestPlan(op.method, op.path, url, (200, 400, 404, 422), json_body=body)

    # ---------- Async instances (write-side) ----------
    if key == ("POST", "/api/v1/database/{db_name}/instances/{class_label}/create"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id})}"
        body = {"data": {f"{ctx.class_id.lower()}_id": ctx.instance_id, "name": "OpenAPI Smoke Product"}, "metadata": {}}
        return RequestPlan(op.method, op.path, url, (202, 409, 404), json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/instances/{class_label}/bulk-create"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id})}"
        body = {
            "instances": [
                {f"{ctx.class_id.lower()}_id": f"{ctx.instance_id}_bulk1", "name": "Bulk 1"},
                {f"{ctx.class_id.lower()}_id": f"{ctx.instance_id}_bulk2", "name": "Bulk 2"},
            ],
            "metadata": {"source": "openapi_smoke"},
        }
        return RequestPlan(op.method, op.path, url, (202, 404), json_body=body)

    if key == ("PUT", "/api/v1/database/{db_name}/instances/{class_label}/{instance_id}/update"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id, 'instance_id': ctx.instance_id})}"
        expected_seq = await _get_write_side_last_sequence(aggregate_type="Instance", aggregate_id=ctx.instance_aggregate_id)
        body = {"data": {"name": "OpenAPI Smoke Product (updated)"}, "metadata": {}}
        return RequestPlan(op.method, op.path, url, (202, 404, 409), params={"expected_seq": expected_seq}, json_body=body)

    if key == ("DELETE", "/api/v1/database/{db_name}/instances/{class_label}/{instance_id}/delete"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_label': ctx.class_id, 'instance_id': ctx.instance_id})}"
        expected_seq = await _get_write_side_last_sequence(aggregate_type="Instance", aggregate_id=ctx.instance_aggregate_id)
        return RequestPlan(op.method, op.path, url, (202, 404, 409), params={"expected_seq": expected_seq})

    # ---------- Instances (read-side) ----------
    if key == ("GET", "/api/v1/database/{db_name}/class/{class_id}/instances"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_id': ctx.class_id})}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/database/{db_name}/class/{class_id}/instance/{instance_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_id': ctx.class_id, 'instance_id': ctx.instance_id})}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("GET", "/api/v1/database/{db_name}/class/{class_id}/sample-values"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'class_id': ctx.class_id})}"
        return RequestPlan(op.method, op.path, url, (200,))

    # ---------- Query ----------
    if key == ("GET", "/api/v1/database/{db_name}/query/builder"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("POST", "/api/v1/database/{db_name}/query"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "class_id": ctx.class_id,
            "filters": [{"field": f"{ctx.class_id.lower()}_id", "operator": "eq", "value": ctx.instance_id}],
            "limit": 5,
        }
        return RequestPlan(op.method, op.path, url, (200,), json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/query/raw"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "type": "select",
            "class_id": ctx.class_id,
            "filters": [{"field": f"{ctx.class_id.lower()}_id", "operator": "eq", "value": ctx.instance_id}],
            "limit": 5,
        }
        return RequestPlan(op.method, op.path, url, (200,), json_body=body)

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

    if key in {("GET", "/api/v1/lineage/graph"), ("GET", "/api/v1/lineage/impact")}:
        url = f"{BFF_URL}{op.path}"
        root = ctx.command_ids.get("create_database") or next(iter(ctx.command_ids.values()))
        return RequestPlan(op.method, op.path, url, (200, 400), params={"root": root, "db_name": ctx.db_name})

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
    if key == ("GET", "/api/v1/database/{db_name}/mappings/"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,))

    if key == ("DELETE", "/api/v1/database/{db_name}/mappings/"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 202, 404))

    if key == ("POST", "/api/v1/database/{db_name}/mappings/export"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200,), note="Export is a JSON file response")

    if key in {("POST", "/api/v1/database/{db_name}/mappings/import"), ("POST", "/api/v1/database/{db_name}/mappings/validate")}:
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        form = aiohttp.FormData()
        form.add_field(
            "file",
            _mapping_file_bytes(ctx),
            filename=f"{ctx.db_name}_mappings.json",
            content_type="application/json",
        )
        return RequestPlan(op.method, op.path, url, (200,), form=form)

    # ---------- Merge conflicts ----------
    if key == ("POST", "/api/v1/database/{db_name}/merge/simulate"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"source_branch": ctx.branch_name, "target_branch": "main", "strategy": "merge"}
        return RequestPlan(op.method, op.path, url, (200, 404, 400), json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/merge/resolve"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {"source_branch": ctx.branch_name, "target_branch": "main", "resolutions": []}
        return RequestPlan(op.method, op.path, url, (200, 400, 404), json_body=body)

    # ---------- Funnel-backed schema/mapping suggestions ----------
    if key == ("POST", "/api/v1/database/{db_name}/suggest-schema-from-data"):
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

    if key == ("POST", "/api/v1/database/{db_name}/suggest-mappings"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        body = {
            "source_schema": [{"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"}, {"name": "name", "type": "xsd:string"}],
            "target_schema": [{"name": f"{ctx.class_id.lower()}_id", "type": "xsd:string"}, {"name": "name", "type": "xsd:string"}],
            "sample_data": [{f"{ctx.class_id.lower()}_id": ctx.instance_id, "name": "OpenAPI Smoke Product"}],
        }
        return RequestPlan(op.method, op.path, url, (200, 400, 422), json_body=body)

    if key == ("POST", "/api/v1/database/{db_name}/suggest-schema-from-excel"):
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

    if key == ("POST", "/api/v1/database/{db_name}/suggest-mappings-from-excel"):
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

    if key == ("POST", "/api/v1/database/{db_name}/import-from-excel/dry-run"):
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

    if key == ("POST", "/api/v1/database/{db_name}/import-from-excel/commit"):
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
        ("POST", "/api/v1/database/{db_name}/suggest-schema-from-google-sheets"),
        ("POST", "/api/v1/database/{db_name}/suggest-mappings-from-google-sheets"),
        ("POST", "/api/v1/database/{db_name}/import-from-google-sheets/dry-run"),
        ("POST", "/api/v1/database/{db_name}/import-from-google-sheets/commit"),
        ("POST", "/api/v1/data-connectors/google-sheets/grid"),
        ("POST", "/api/v1/data-connectors/google-sheets/preview"),
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

    if key == ("DELETE", "/api/v1/data-connectors/google-sheets/{sheet_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx, overrides={'sheet_id': 'not_registered_smoke'})}"
        return RequestPlan(op.method, op.path, url, (404, 400, 200))

    if key == ("DELETE", "/api/v1/data-connectors/google-sheets/connections/{connection_id}"):
        url = f"{BFF_URL}{_format_path(op.path, ctx)}"
        return RequestPlan(op.method, op.path, url, (200, 404))

    # ---------- AI (LLM) ----------
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
        return RequestPlan(op.method, op.path, url, (200, 404))

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
        legacy_class_id="LegacySmokeClass",
        instance_id=f"prod_{unique}",
        command_ids={},
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
            plan = await _build_plan(Operation("POST", "/api/v1/databases", ("Database Management",), "Create Database"), ctx)
            status, text, payload = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses:
                raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")

            if status == 202 and isinstance(payload, dict):
                command_id = ((payload.get("data") or {}).get("command_id")) or ((payload.get("data") or {}).get("commandId"))
                assert command_id, f"Missing command_id in 202 response: {payload}"
                ctx.command_ids["create_database"] = str(command_id)
                await _wait_for_command_completed(session, command_id=str(command_id))

            # Create ontology classes (Product + Order)
            for op in [
                Operation("POST", "/api/v1/database/{db_name}/ontology", ("Ontology Management",), "Create Ontology"),
                Operation("POST", "/api/v1/database/{db_name}/ontology-advanced", ("Ontology Management",), "Create Ontology Advanced"),
            ]:
                plan = await _build_plan(op, ctx)
                status, text, payload = await _request(session, plan)
                executed.add((plan.method, plan.path_template))
                if status not in plan.expected_statuses:
                    raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")
                if status == 202 and isinstance(payload, dict):
                    command_id = ((payload.get("data") or {}).get("command_id")) or ((payload.get("data") or {}).get("commandId"))
                    if command_id:
                        await _wait_for_command_completed(session, command_id=str(command_id))

            # Create branch (for merge simulate, etc)
            plan = await _build_plan(
                Operation("POST", "/api/v1/databases/{db_name}/branches", ("Database Management",), "Create Branch"), ctx
            )
            status, text, _ = await _request(session, plan)
            executed.add((plan.method, plan.path_template))
            if status not in plan.expected_statuses:
                raise AssertionError(f"{plan.method} {plan.path_template} unexpected status {status}: {text[:500]}")

            # Create one instance (async) and wait
            plan = await _build_plan(
                Operation("POST", "/api/v1/database/{db_name}/instances/{class_label}/create", ("Async Instance Management",), "Create Instance Async"),
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

            # Import mappings so label-based query can run deterministically
            plan = await _build_plan(
                Operation("POST", "/api/v1/database/{db_name}/mappings/import", ("Label Mappings",), "Import Mappings"),
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

        except Exception as e:
            # Setup failure is fatal; without it, most OpenAPI ops can't be exercised meaningfully.
            raise

        # ---- Execute remaining included OpenAPI operations (no early-stop) ----
        # Order matters: avoid deleting shared state until the very end.
        included_non_delete = [op for op in included if op.method != "DELETE"]
        included_delete = [op for op in included if op.method == "DELETE" and op.path != "/api/v1/databases/{db_name}"]

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
            plan = await _build_plan(Operation("DELETE", "/api/v1/databases/{db_name}", ("Database Management",), "Delete Database"), ctx)
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
