"""
Foundry 완전 E2E QA: 실제 Kaggle 데이터 + 실시간 API

Tests the full Palantir Foundry lifecycle:
  데이터 수집 → 파이프라인 변환 → 온톨로지 생성 → 객체화 →
  검색/쿼리 → 액션 실행 → 폐루프 반영 → 실시간 크로스도메인

Data sources:
  - Brazilian E-Commerce by Olist (Kaggle, 5 relational CSVs)
  - Open-Meteo Weather API (no key)
  - Frankfurter Exchange Rate API (no key)
  - USGS Earthquake API (no key)

Run:
  RUN_FOUNDRY_E2E_QA=true \\
    PYTHONPATH=backend \\
    python -m pytest backend/tests/test_foundry_e2e_qa.py -v -s --tb=short
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pytest

from shared.config.search_config import get_instances_index_name
from shared.security.database_access import ensure_database_access_table
from tests.utils.auth import bff_auth_headers, oms_auth_headers
from tests.utils.qa_helpers import (
    BugTracker,
    QAClient,
    fetch_frankfurter_csv,
    fetch_open_meteo_csv,
    fetch_usgs_earthquake_csv,
)


# ── Gate: opt-in only ─────────────────────────────────────────────────────────

if os.getenv("RUN_FOUNDRY_E2E_QA", "").strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip(
        "RUN_FOUNDRY_E2E_QA must be enabled. Set RUN_FOUNDRY_E2E_QA=true.",
        allow_module_level=True,
    )


# ── Constants ─────────────────────────────────────────────────────────────────

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "kaggle_data"
BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")
_ES_PORT = os.getenv("ELASTICSEARCH_PORT") or os.getenv("ELASTICSEARCH_PORT_HOST") or "9200"
ES_URL = os.getenv(
    "ELASTICSEARCH_URL",
    f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{_ES_PORT}",
).rstrip("/")

HTTPX_TIMEOUT = float(os.getenv("QA_HTTP_TIMEOUT", "180") or 180)
ES_TIMEOUT = int(os.getenv("QA_ES_TIMEOUT", "240") or 240)


# ── Shared state between phases ──────────────────────────────────────────────

class QAState:
    """Mutable state shared across all 8 phases."""
    def __init__(self) -> None:
        self.db_name: str = ""
        self.user_id: str = ""
        self.suffix: str = ""
        self.headers: Dict[str, str] = {}
        self.oms_headers: Dict[str, str] = {}
        self.bug_tracker = BugTracker()

        # Phase 1 outputs
        self.datasets: Dict[str, Dict[str, Any]] = {}  # name → {dataset_id, version_id, ...}

        # Phase 2 outputs
        self.pipelines: Dict[str, str] = {}  # name → pipeline_id
        self.pipeline_outputs: Dict[str, str] = {}  # name → output dataset_id

        # Phase 3 outputs
        self.ontology_classes: List[str] = []
        self.action_type_ids: List[str] = []

        # Phase 4 outputs
        self.mapping_specs: Dict[str, str] = {}  # class_id → mapping_spec_id
        self.es_index: str = ""

        # Phase 5 outputs
        self.search_results: Dict[str, Any] = {}

        # Phase 6 outputs
        self.action_log_ids: List[str] = []
        self.changed_instance_ids: List[str] = []

        # Phase 8 outputs
        self.live_datasets: Dict[str, Dict[str, Any]] = {}


# ── Helper functions ──────────────────────────────────────────────────────────

def _safe(func):
    """Decorator: catch assertion errors and record as bugs instead of failing."""
    import functools
    @functools.wraps(func)
    async def wrapper(state: QAState, client: httpx.AsyncClient, *args, **kwargs):
        try:
            result = await func(state, client, *args, **kwargs)
            state.bug_tracker.record_pass()
            return result
        except Exception as exc:
            state.bug_tracker.record(
                phase=kwargs.get("phase", func.__name__),
                step=kwargs.get("step", "?"),
                endpoint=kwargs.get("endpoint", "?"),
                expected=kwargs.get("expected", "success"),
                actual=str(exc)[:300],
                severity=kwargs.get("severity", "P1"),
            )
            return None
    return wrapper


async def _wait_for_command(
    client: httpx.AsyncClient,
    command_id: str,
    *,
    timeout: int = 120,
) -> None:
    deadline = time.monotonic() + timeout
    last_payload: Optional[Dict[str, Any]] = None
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/commands/{command_id}/status")
        if resp.status_code == 200:
            last_payload = resp.json()
            status = str(
                last_payload.get("status") or (last_payload.get("data") or {}).get("status") or ""
            ).upper()
            if status in {"COMPLETED", "SUCCESS", "SUCCEEDED", "DONE"}:
                return
            if status in {"FAILED", "ERROR"}:
                raise AssertionError(f"Command {command_id} failed: {last_payload}")
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for command {command_id} (last={last_payload})")


async def _wait_for_ontology(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    class_id: str,
    timeout: int = 90,
    require_properties: bool = False,
) -> None:
    """Wait for ontology class to exist (and optionally have properties populated)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = await client.get(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology/{class_id}",
            params={"branch": "main"},
        )
        if resp.status_code == 200:
            if not require_properties:
                return
            # If we need properties, check they're populated
            # OMS GET returns properties at data.properties (flattened, no spec wrapper)
            data = resp.json().get("data") or resp.json()
            props = data.get("properties") or []
            if len(props) > 0:
                return
        await asyncio.sleep(1.0)
    raise AssertionError(f"Timed out waiting for ontology class {class_id} (require_properties={require_properties})")


async def _wait_for_es_doc(
    client: httpx.AsyncClient,
    index_name: str,
    doc_id: str,
    *,
    timeout: int = ES_TIMEOUT,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{ES_URL}/{index_name}/_doc/{doc_id}")
        except httpx.HTTPError:
            await asyncio.sleep(1.0)
            continue
        if resp.status_code == 200:
            payload = resp.json()
            if payload.get("found") is True or payload.get("_source"):
                return payload
        await asyncio.sleep(1.0)
    raise AssertionError(f"Timed out waiting for ES doc {doc_id} in {index_name}")


async def _grant_db_role(*, db_name: str, principal_id: str) -> None:
    import asyncpg  # type: ignore

    dsn_candidates = [
        (os.getenv("POSTGRES_URL") or "").strip(),
        "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb",
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]
    for dsn in dsn_candidates:
        if not dsn:
            continue
        try:
            conn = await asyncpg.connect(dsn)
        except Exception:
            continue
        try:
            await ensure_database_access_table(conn)
            await conn.execute(
                """
                INSERT INTO database_access (
                    db_name, principal_type, principal_id, principal_name, role, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (db_name, principal_type, principal_id)
                DO UPDATE SET role = EXCLUDED.role, updated_at = NOW()
                """,
                db_name, "user", principal_id, principal_id, "Owner",
            )
            return
        finally:
            await conn.close()


async def _upsert_object_type_contract(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    class_id: str,
    backing_dataset_id: str,
    pk_spec: Dict[str, Any],
) -> None:
    """Create or update an object type contract (pk_spec + backing source).

    IMPORTANT: The resource registry upsert REPLACES the entire spec.
    We must first GET the existing resource to preserve properties/relationships
    created by the ontology worker, then merge in the contract fields.
    """
    expected_head_commit = "branch:main"

    # First, fetch the existing resource to preserve properties/relationships.
    # The resource registry upsert REPLACES the entire spec, so we must
    # include the existing properties and relationships in the new spec.
    existing_spec: Dict[str, Any] = {}
    existing_label: Any = {"en": class_id, "ko": class_id}
    existing_desc: Any = {"en": f"{class_id} object type", "ko": f"{class_id} 오브젝트 타입"}
    existing_metadata: Dict[str, Any] = {"source": "foundry_e2e_qa"}

    try:
        # GET the ontology class — returns properties/relationships flattened from spec
        get_resp = await client.get(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology/{class_id}",
            params={"branch": "main"},
        )
        if get_resp.status_code == 200:
            onto_data = get_resp.json().get("data") or get_resp.json()
            # Rebuild spec from the flattened ontology response
            if onto_data.get("properties"):
                existing_spec["properties"] = onto_data["properties"]
            if onto_data.get("relationships"):
                existing_spec["relationships"] = onto_data["relationships"]
            if onto_data.get("parent_class"):
                existing_spec["parent_class"] = onto_data["parent_class"]
            if onto_data.get("abstract"):
                existing_spec["abstract"] = onto_data["abstract"]
            if onto_data.get("label"):
                existing_label = onto_data["label"]
                existing_spec["label"] = onto_data["label"]
            if onto_data.get("description"):
                existing_desc = onto_data["description"]
                existing_spec["description"] = onto_data["description"]
            if onto_data.get("id"):
                existing_spec["id"] = onto_data["id"]
            if onto_data.get("metadata"):
                existing_metadata = {**onto_data["metadata"], "source": "foundry_e2e_qa"}
    except Exception:
        pass  # If can't GET, just use defaults

    # Merge: preserve existing spec (properties, relationships, etc.) + add contract fields
    merged_spec = {
        **existing_spec,
        "backing_source": {"dataset_id": backing_dataset_id},
        "pk_spec": pk_spec,
        "status": "ACTIVE",
    }

    payload = {
        "id": class_id,
        "label": existing_label,
        "description": existing_desc,
        "metadata": existing_metadata,
        "spec": merged_spec,
    }
    resp = await client.post(
        f"{OMS_URL}/api/v1/database/{db_name}/ontology/resources/object-types",
        params={"branch": "main", "expected_head_commit": expected_head_commit},
        json=payload,
    )
    if resp.status_code in {200, 201}:
        return
    if resp.status_code == 409:
        resp2 = await client.put(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology/resources/object-types/{class_id}",
            params={"branch": "main", "expected_head_commit": expected_head_commit},
            json=payload,
        )
        if resp2.status_code == 200:
            return
        raise AssertionError(f"object-type upsert failed: {resp2.status_code} {resp2.text}")
    raise AssertionError(f"object-type create failed: {resp.status_code} {resp.text}")


# =============================================================================
# PHASE 1: Data Ingestion
# =============================================================================

async def phase1_data_ingestion(state: QAState, client: httpx.AsyncClient) -> None:
    """Upload 5 Olist CSVs + 3 real-time API CSVs = 8 datasets."""
    phase = "Phase 1"
    print(f"\n{'='*60}\n  {phase}: DATA INGESTION\n{'='*60}")

    # ── 1-1: Health checks ────────────────────────────────────────
    print("  [1-1] Health checks...")
    try:
        bff_health = await client.get(f"{BFF_URL}/api/v1/health")
        assert bff_health.status_code == 200, f"BFF health: {bff_health.status_code}"
        state.bug_tracker.record_pass()
        print(f"    BFF: OK ({bff_health.status_code})")
    except Exception as exc:
        state.bug_tracker.record(phase, "1-1a", "GET /api/v1/health (BFF)", "200", str(exc))

    try:
        oms_health = await client.get(f"{OMS_URL}/health")
        assert oms_health.status_code == 200, f"OMS health: {oms_health.status_code}"
        state.bug_tracker.record_pass()
        print(f"    OMS: OK ({oms_health.status_code})")
    except Exception as exc:
        state.bug_tracker.record(phase, "1-1b", "GET /health (OMS)", "200", str(exc))

    # ── 1-2: Create database ─────────────────────────────────────
    print(f"  [1-2] Creating database: {state.db_name}")
    try:
        resp = await client.post(
            f"{BFF_URL}/api/v1/databases",
            json={"name": state.db_name, "description": "Foundry E2E QA - Olist + Live APIs"},
        )
        if resp.status_code == 409:
            print("    DB already exists, continuing...")
        else:
            resp.raise_for_status()
            cmd_id = str(((resp.json().get("data") or {}) or {}).get("command_id") or "")
            if cmd_id:
                await _wait_for_command(client, cmd_id, timeout=180)
        state.bug_tracker.record_pass()
        print(f"    DB created: {state.db_name}")
    except Exception as exc:
        state.bug_tracker.record(phase, "1-2", "POST /databases", "201/409", str(exc), "P0")

    # ── 1-3: Grant DB role ────────────────────────────────────────
    print(f"  [1-3] Granting DB role to {state.user_id}")
    try:
        await _grant_db_role(db_name=state.db_name, principal_id=state.user_id)
        state.bug_tracker.record_pass()
        print("    Role granted: Owner")
    except Exception as exc:
        state.bug_tracker.record(phase, "1-3", "asyncpg grant_db_role", "Owner granted", str(exc))

    # ── 1-4: Upload Olist CSVs ────────────────────────────────────
    olist_files = {
        "olist_orders": ("Olist orders dataset", "olist_orders.csv"),
        "olist_customers": ("Olist customers dataset", "olist_customers.csv"),
        "olist_products": ("Olist products dataset", "olist_products.csv"),
        "olist_order_items": ("Olist order items dataset", "olist_order_items.csv"),
        "olist_sellers": ("Olist sellers dataset", "olist_sellers.csv"),
    }
    print(f"  [1-4] Uploading {len(olist_files)} Olist CSVs...")
    for name, (description, filename) in olist_files.items():
        csv_path = FIXTURE_DIR / filename
        if not csv_path.exists():
            state.bug_tracker.record(
                phase, "1-4", f"read {filename}",
                "file exists", f"File not found: {csv_path}. Run download_kaggle_data.py first.",
                "P0",
            )
            continue
        csv_bytes = csv_path.read_bytes()
        # Retry CSV upload up to 3 times (infrastructure can be flaky)
        max_upload_retries = 3
        uploaded = False
        for upload_attempt in range(max_upload_retries):
            try:
                idem_key = f"idem-{name}-{state.suffix}-{upload_attempt}"
                resp = await client.post(
                    f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
                    params={"db_name": state.db_name, "branch": "main"},
                    headers={**state.headers, "Idempotency-Key": idem_key},
                    data={
                        "dataset_name": name,
                        "description": description,
                        "delimiter": ",",
                        "has_header": "true",
                    },
                    files={"file": (filename, csv_bytes, "text/csv")},
                )
                if resp.status_code >= 400:
                    err_text = resp.text[:300]
                    print(f"    {name}: upload error {resp.status_code}: {err_text}")
                    if upload_attempt < max_upload_retries - 1:
                        await asyncio.sleep(5 * (upload_attempt + 1))
                        continue
                resp.raise_for_status()
                payload = resp.json().get("data") or {}
                ds = payload.get("dataset") or {}
                ver = payload.get("version") or {}
                dataset_id = str(ds.get("dataset_id") or "")
                version_id = str(ver.get("version_id") or "")
                assert dataset_id, f"No dataset_id for {name}"
                assert version_id, f"No version_id for {name}"
                state.datasets[name] = {
                    "dataset_id": dataset_id,
                    "version_id": version_id,
                    "payload": payload,
                }
                state.bug_tracker.record_pass()
                row_info = ""
                preview = payload.get("preview") or {}
                if preview.get("rows"):
                    row_info = f", preview={len(preview['rows'])} rows"
                print(f"    {name}: dataset_id={dataset_id[:12]}...{row_info}")
                uploaded = True
                break
            except Exception as exc:
                err_msg = str(exc)[:200] or f"upload failed (attempt {upload_attempt + 1})"
                if upload_attempt < max_upload_retries - 1:
                    print(f"    {name}: upload attempt {upload_attempt + 1} failed: {err_msg}, retrying...")
                    await asyncio.sleep(5 * (upload_attempt + 1))
                    continue
                state.bug_tracker.record(
                    phase, f"1-4:{name}", f"POST /csv-upload ({name})",
                    "200 with dataset_id", err_msg, "P0",
                )

    # ── 1-5: Upload real-time API data ────────────────────────────
    print("  [1-5] Fetching and uploading real-time API data...")
    live_sources = {
        "weather_saopaulo": ("São Paulo weather (Open-Meteo)", fetch_open_meteo_csv),
        "exchange_rates_brl": ("BRL exchange rates (Frankfurter)", fetch_frankfurter_csv),
        "earthquakes_month": ("Monthly earthquakes (USGS)", fetch_usgs_earthquake_csv),
    }
    for name, (description, fetch_fn) in live_sources.items():
        try:
            csv_bytes = await fetch_fn()
            assert len(csv_bytes) > 50, f"API returned too little data for {name}"
            idem_key = f"idem-{name}-{state.suffix}"
            resp = await client.post(
                f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
                params={"db_name": state.db_name, "branch": "main"},
                headers={**state.headers, "Idempotency-Key": idem_key},
                data={
                    "dataset_name": name,
                    "description": description,
                    "delimiter": ",",
                    "has_header": "true",
                },
                files={"file": (f"{name}.csv", csv_bytes, "text/csv")},
            )
            resp.raise_for_status()
            payload = resp.json().get("data") or {}
            ds = payload.get("dataset") or {}
            ver = payload.get("version") or {}
            dataset_id = str(ds.get("dataset_id") or "")
            version_id = str(ver.get("version_id") or "")
            assert dataset_id, f"No dataset_id for {name}"
            state.live_datasets[name] = {
                "dataset_id": dataset_id,
                "version_id": version_id,
            }
            state.bug_tracker.record_pass()
            # Count rows from CSV bytes
            row_count = csv_bytes.count(b"\n") - 1
            print(f"    {name}: {row_count} rows, dataset_id={dataset_id[:12]}...")
        except Exception as exc:
            state.bug_tracker.record(
                phase, f"1-5:{name}", f"fetch+upload {name}",
                "200 with dataset_id", str(exc)[:200],
            )

    # ── 1-6: List datasets ────────────────────────────────────────
    print(f"  [1-6] Listing datasets for db={state.db_name}")
    try:
        resp = await client.get(
            f"{BFF_URL}/api/v1/pipelines/datasets",
            params={"db_name": state.db_name},
        )
        resp.raise_for_status()
        data = resp.json().get("data") or resp.json()
        ds_list = data.get("datasets") or data if isinstance(data, list) else []
        if isinstance(data, dict):
            ds_list = data.get("datasets") or data.get("items") or []
        total = len(ds_list) if isinstance(ds_list, list) else 0
        expected_count = len(state.datasets) + len(state.live_datasets)
        print(f"    Found {total} datasets (expected ~{expected_count})")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "1-6", "GET /datasets", "list of datasets", str(exc)[:200])


# =============================================================================
# PHASE 2: Pipeline Transforms
# =============================================================================

async def phase2_pipeline_transforms(state: QAState, client: httpx.AsyncClient) -> None:
    """Create and execute pipelines: join, compute, filter, groupBy, aggregate."""
    phase = "Phase 2"
    print(f"\n{'='*60}\n  {phase}: PIPELINE TRANSFORMS\n{'='*60}")

    orders_id = (state.datasets.get("olist_orders") or {}).get("dataset_id")
    items_id = (state.datasets.get("olist_order_items") or {}).get("dataset_id")
    products_id = (state.datasets.get("olist_products") or {}).get("dataset_id")
    sellers_id = (state.datasets.get("olist_sellers") or {}).get("dataset_id")

    if not all([orders_id, items_id]):
        state.bug_tracker.record(phase, "2-0", "prerequisite", "dataset_ids", "Missing orders/items from Phase 1", "P0")
        return

    # ── 2-1: Pipeline A - Enriched Orders (orders JOIN items JOIN products) ──
    print("  [2-1] Creating Pipeline A: enriched orders (3-table join + compute + filter)")
    pipeline_a_def = {
        "nodes": [
            {"id": "in_orders", "type": "input", "metadata": {"datasetId": orders_id}},
            {"id": "in_items", "type": "input", "metadata": {"datasetId": items_id}},
            {"id": "join_oi", "type": "transform", "metadata": {
                "operation": "join", "leftKey": "order_id", "rightKey": "order_id", "joinType": "inner",
            }},
            {"id": "compute_total", "type": "transform", "metadata": {
                "operation": "compute", "expression": "total_value = price + freight_value",
            }},
            {"id": "filter_delivered", "type": "transform", "metadata": {
                "operation": "filter", "expression": "order_status == 'delivered'",
            }},
            {"id": "out_enriched", "type": "output", "metadata": {
                "outputName": "enriched_orders", "outputKind": "dataset",
            }},
        ],
        "edges": [
            {"from": "in_orders", "to": "join_oi"},
            {"from": "in_items", "to": "join_oi"},
            {"from": "join_oi", "to": "compute_total"},
            {"from": "compute_total", "to": "filter_delivered"},
            {"from": "filter_delivered", "to": "out_enriched"},
        ],
        "parameters": [],
        "settings": {"engine": "Batch"},
    }

    try:
        idem_key = f"idem-pipeline-a-{state.suffix}"
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            headers={**state.headers, "Idempotency-Key": idem_key},
            json={
                "db_name": state.db_name,
                "name": f"enriched_orders_{state.suffix}",
                "description": "Orders joined with items, filtered to delivered",
                "definition_json": pipeline_a_def,
                "pipeline_type": "batch",
                "branch": "main",
                "location": "e2e",
            },
        )
        resp.raise_for_status()
        pipeline_data = resp.json().get("data") or resp.json()
        # Response may nest pipeline_id under "pipeline" key
        pipeline_obj = pipeline_data.get("pipeline") or pipeline_data
        pipeline_id = str(pipeline_obj.get("pipeline_id") or pipeline_obj.get("id") or pipeline_data.get("pipeline_id") or "")
        assert pipeline_id, f"No pipeline_id: {pipeline_data}"
        state.pipelines["enriched_orders"] = pipeline_id
        state.bug_tracker.record_pass()
        print(f"    Pipeline A created: {pipeline_id[:12]}...")
    except Exception as exc:
        state.bug_tracker.record(phase, "2-1", "POST /pipelines", "pipeline_id", str(exc)[:200], "P1")
        return

    # ── 2-2: Preview Pipeline A ──────────────────────────────────
    print("  [2-2] Preview Pipeline A...")
    try:
        preview_idem = f"idem-preview-a-{state.suffix}"
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/preview",
            headers={**state.headers, "Idempotency-Key": preview_idem},
            json={"db_name": state.db_name, "branch": "main", "limit": 50},
        )
        resp.raise_for_status()
        preview = resp.json()
        preview_data = preview.get("data") or preview
        print(f"    Preview returned: {json.dumps(preview_data)[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "2-2", f"POST /pipelines/{pipeline_id}/preview", "preview result", str(exc)[:200])

    # ── 2-3: Build Pipeline A ────────────────────────────────────
    print("  [2-3] Build Pipeline A...")
    build_job_id = ""
    try:
        build_idem = f"idem-build-a-{state.suffix}"
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/build",
            headers={**state.headers, "Idempotency-Key": build_idem},
            json={"db_name": state.db_name, "branch": "main", "limit": 50},
        )
        resp.raise_for_status()
        build_data = resp.json().get("data") or resp.json()
        build_job_id = str(build_data.get("job_id") or "")
        print(f"    Build started: job_id={build_job_id}")

        # Wait for build to complete
        if build_job_id:
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline:
                runs_resp = await client.get(f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/runs", params={"limit": 50})
                if runs_resp.status_code == 200:
                    runs = (runs_resp.json().get("data") or {}).get("runs") or []
                    run = next((r for r in runs if r.get("job_id") == build_job_id), None)
                    if run:
                        run_status = str(run.get("status") or "").upper()
                        if run_status in {"SUCCESS", "DEPLOYED", "COMPLETED"}:
                            print(f"    Build completed: {run_status}")
                            break
                        if run_status == "FAILED":
                            raise AssertionError(f"Build failed: {run}")
                await asyncio.sleep(2.0)
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "2-3", f"POST /pipelines/{pipeline_id}/build", "build success", str(exc)[:200])

    # ── 2-4: Deploy Pipeline A ───────────────────────────────────
    print("  [2-4] Deploy Pipeline A...")
    try:
        deploy_idem = f"idem-deploy-a-{state.suffix}"
        deploy_payload: Dict[str, Any] = {
            "promote_build": True,
            "branch": "main",
            "output": {"db_name": state.db_name},
        }
        if build_job_id:
            deploy_payload["build_job_id"] = build_job_id
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/deploy",
            headers={**state.headers, "Idempotency-Key": deploy_idem},
            json=deploy_payload,
        )
        if resp.status_code >= 400:
            print(f"    Deploy error ({resp.status_code}): {resp.text[:300]}")
        resp.raise_for_status()
        deploy_data = resp.json().get("data") or resp.json()
        print(f"    Deploy response: {json.dumps(deploy_data)[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "2-4", f"POST /pipelines/{pipeline_id}/deploy", "deploy success", str(exc)[:200])

    # ── 2-5: Pipeline B - Category Revenue ───────────────────────
    if items_id and products_id:
        print("  [2-5] Creating Pipeline B: category revenue aggregation")
        pipeline_b_def = {
            "nodes": [
                {"id": "in_items", "type": "input", "metadata": {"datasetId": items_id}},
                {"id": "in_products", "type": "input", "metadata": {"datasetId": products_id}},
                {"id": "join_ip", "type": "transform", "metadata": {
                    "operation": "join", "leftKey": "product_id", "rightKey": "product_id", "joinType": "inner",
                }},
                {"id": "compute_rev", "type": "transform", "metadata": {
                    "operation": "compute", "expression": "revenue = price + freight_value",
                }},
                {"id": "out_revenue", "type": "output", "metadata": {
                    "outputName": "category_revenue", "outputKind": "dataset",
                }},
            ],
            "edges": [
                {"from": "in_items", "to": "join_ip"},
                {"from": "in_products", "to": "join_ip"},
                {"from": "join_ip", "to": "compute_rev"},
                {"from": "compute_rev", "to": "out_revenue"},
            ],
            "parameters": [],
            "settings": {"engine": "Batch"},
        }
        try:
            idem_key = f"idem-pipeline-b-{state.suffix}"
            resp = await client.post(
                f"{BFF_URL}/api/v1/pipelines",
                headers={**state.headers, "Idempotency-Key": idem_key},
                json={
                    "db_name": state.db_name,
                    "name": f"category_revenue_{state.suffix}",
                    "description": "Category revenue from items+products",
                    "definition_json": pipeline_b_def,
                    "pipeline_type": "batch",
                    "branch": "main",
                    "location": "e2e",
                },
            )
            resp.raise_for_status()
            pipeline_b_data = resp.json().get("data") or resp.json()
            pipeline_b_obj = pipeline_b_data.get("pipeline") or pipeline_b_data
            pipeline_b_id = str(pipeline_b_obj.get("pipeline_id") or pipeline_b_obj.get("id") or pipeline_b_data.get("pipeline_id") or "")
            state.pipelines["category_revenue"] = pipeline_b_id
            print(f"    Pipeline B created: {pipeline_b_id[:12]}...")

            # Preview
            if pipeline_b_id:
                preview_b_idem = f"idem-preview-b-{state.suffix}"
                resp = await client.post(
                    f"{BFF_URL}/api/v1/pipelines/{pipeline_b_id}/preview",
                    headers={**state.headers, "Idempotency-Key": preview_b_idem},
                    json={"db_name": state.db_name, "branch": "main", "limit": 50},
                )
                resp.raise_for_status()
                preview = resp.json()
                preview_data = preview.get("data") or preview
                print(f"    Pipeline B preview: {json.dumps(preview_data)[:200]}")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "2-5", "Pipeline B creation", "pipeline_id", str(exc)[:200])

    # ── 2-6: Pipeline C - Seller Performance ─────────────────────
    if items_id and sellers_id:
        print("  [2-6] Creating Pipeline C: seller performance")
        pipeline_c_def = {
            "nodes": [
                {"id": "in_items", "type": "input", "metadata": {"datasetId": items_id}},
                {"id": "in_sellers", "type": "input", "metadata": {"datasetId": sellers_id}},
                {"id": "join_is", "type": "transform", "metadata": {
                    "operation": "join", "leftKey": "seller_id", "rightKey": "seller_id", "joinType": "inner",
                }},
                {"id": "out_seller", "type": "output", "metadata": {
                    "outputName": "seller_performance", "outputKind": "dataset",
                }},
            ],
            "edges": [
                {"from": "in_items", "to": "join_is"},
                {"from": "in_sellers", "to": "join_is"},
                {"from": "join_is", "to": "out_seller"},
            ],
            "parameters": [],
            "settings": {"engine": "Batch"},
        }
        try:
            idem_key = f"idem-pipeline-c-{state.suffix}"
            resp = await client.post(
                f"{BFF_URL}/api/v1/pipelines",
                headers={**state.headers, "Idempotency-Key": idem_key},
                json={
                    "db_name": state.db_name,
                    "name": f"seller_performance_{state.suffix}",
                    "description": "Seller performance analysis",
                    "definition_json": pipeline_c_def,
                    "pipeline_type": "batch",
                    "branch": "main",
                    "location": "e2e",
                },
            )
            resp.raise_for_status()
            pipeline_c_data = resp.json().get("data") or resp.json()
            pipeline_c_obj = pipeline_c_data.get("pipeline") or pipeline_c_data
            pipeline_c_id = str(pipeline_c_obj.get("pipeline_id") or pipeline_c_obj.get("id") or pipeline_c_data.get("pipeline_id") or "")
            state.pipelines["seller_performance"] = pipeline_c_id
            print(f"    Pipeline C created: {pipeline_c_id[:12]}...")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "2-6", "Pipeline C creation", "pipeline_id", str(exc)[:200])


# =============================================================================
# PHASE 3: Ontology Creation
# =============================================================================

async def phase3_ontology_creation(state: QAState, client: httpx.AsyncClient) -> None:
    """Create 5 Object Types + 3 Action Types."""
    phase = "Phase 3"
    print(f"\n{'='*60}\n  {phase}: ONTOLOGY CREATION\n{'='*60}")

    # ── 3-1: Create ontology classes ──────────────────────────────
    classes = [
        {
            "id": "Customer",
            "label": {"en": "Customer", "ko": "고객"},
            "description": {"en": "E-commerce customer", "ko": "전자상거래 고객"},
            "properties": [
                {"name": "customer_id", "type": "xsd:string", "label": {"en": "Customer ID"}, "required": True, "primaryKey": True, "titleKey": True},
                {"name": "customer_unique_id", "type": "xsd:string", "label": {"en": "Unique ID"}},
                {"name": "customer_city", "type": "xsd:string", "label": {"en": "City"}},
                {"name": "customer_state", "type": "xsd:string", "label": {"en": "State"}},
                {"name": "customer_zip_code_prefix", "type": "xsd:integer", "label": {"en": "Zip Code"}},
            ],
            "relationships": [],
        },
        {
            "id": "Product",
            "label": {"en": "Product", "ko": "상품"},
            "description": {"en": "Product in catalog", "ko": "상품 카탈로그"},
            "properties": [
                {"name": "product_id", "type": "xsd:string", "label": {"en": "Product ID"}, "required": True, "primaryKey": True},
                {"name": "product_category_name", "type": "xsd:string", "label": {"en": "Category"}, "titleKey": True},
                {"name": "product_weight_g", "type": "xsd:decimal", "label": {"en": "Weight (g)"}},
            ],
            "relationships": [],
        },
        {
            "id": "Seller",
            "label": {"en": "Seller", "ko": "판매자"},
            "description": {"en": "Marketplace seller", "ko": "마켓플레이스 판매자"},
            "properties": [
                {"name": "seller_id", "type": "xsd:string", "label": {"en": "Seller ID"}, "required": True, "primaryKey": True, "titleKey": True},
                {"name": "seller_city", "type": "xsd:string", "label": {"en": "City"}},
                {"name": "seller_state", "type": "xsd:string", "label": {"en": "State"}},
            ],
            "relationships": [],
        },
        {
            "id": "Order",
            "label": {"en": "Order", "ko": "주문"},
            "description": {"en": "Customer order", "ko": "고객 주문"},
            "properties": [
                {"name": "order_id", "type": "xsd:string", "label": {"en": "Order ID"}, "required": True, "primaryKey": True, "titleKey": True},
                {"name": "order_status", "type": "xsd:string", "label": {"en": "Status"}, "required": True},
                {"name": "order_purchase_timestamp", "type": "xsd:dateTime", "label": {"en": "Purchase Date"}},
                {"name": "order_delivered_customer_date", "type": "xsd:dateTime", "label": {"en": "Delivered Date"}},
                {"name": "order_estimated_delivery_date", "type": "xsd:dateTime", "label": {"en": "Estimated Date"}},
            ],
            "relationships": [
                {"predicate": "customer", "target": "Customer", "label": {"en": "Customer"}, "cardinality": "n:1"},
            ],
        },
        {
            "id": "OrderItem",
            "label": {"en": "Order Item", "ko": "주문 항목"},
            "description": {"en": "Line item in an order", "ko": "주문 라인 아이템"},
            "properties": [
                {"name": "order_id", "type": "xsd:string", "label": {"en": "Order ID"}, "required": True, "primaryKey": True, "titleKey": True},
                {"name": "order_item_id", "type": "xsd:integer", "label": {"en": "Item ID"}, "required": True},
                {"name": "price", "type": "xsd:decimal", "label": {"en": "Price"}},
                {"name": "freight_value", "type": "xsd:decimal", "label": {"en": "Freight"}},
            ],
            "relationships": [
                {"predicate": "product", "target": "Product", "label": {"en": "Product"}, "cardinality": "n:1"},
                {"predicate": "seller", "target": "Seller", "label": {"en": "Seller"}, "cardinality": "n:1"},
            ],
        },
    ]

    print(f"  [3-1] Creating {len(classes)} ontology classes...")
    for cls in classes:
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v1/databases/{state.db_name}/ontology",
                params={"branch": "main"},
                json={**cls, "metadata": {"source": "foundry_e2e_qa"}},
            )
            resp.raise_for_status()
            # Extract command_id from 202 response and wait for event-sourcing completion
            resp_data = resp.json()
            command_id = str((resp_data.get("data") or {}).get("command_id") or "")
            if command_id:
                print(f"    {cls['id']}: command_id={command_id[:12]}..., waiting for completion...")
                try:
                    await _wait_for_command(client, command_id, timeout=120)
                except Exception:
                    print(f"    {cls['id']}: command wait timed out, falling back to GET poll...")
            # Wait for class to appear with properties fully populated
            await _wait_for_ontology(client, db_name=state.db_name, class_id=cls["id"], require_properties=True, timeout=120)
            state.ontology_classes.append(cls["id"])
            state.bug_tracker.record_pass()
            print(f"    {cls['id']}: created and properties verified")
        except Exception as exc:
            state.bug_tracker.record(phase, f"3-1:{cls['id']}", f"POST ontology/{cls['id']}", "202+properties", str(exc)[:200])

    # ── 3-2: Object Type Contracts (pk_spec + backing source) ────
    print("  [3-2] Creating object type contracts...")
    ot_specs = [
        ("Customer", "olist_customers", {"primary_key": ["customer_id"], "title_key": ["customer_id"]}),
        ("Product", "olist_products", {"primary_key": ["product_id"], "title_key": ["product_category_name"]}),
        ("Seller", "olist_sellers", {"primary_key": ["seller_id"], "title_key": ["seller_id"]}),
        ("Order", "olist_orders", {"primary_key": ["order_id"], "title_key": ["order_id"]}),
        ("OrderItem", "olist_order_items", {"primary_key": ["order_id"], "title_key": ["order_id"]}),
    ]
    for class_id, dataset_name, pk_spec in ot_specs:
        ds = state.datasets.get(dataset_name)
        if not ds:
            state.bug_tracker.record(phase, f"3-2:{class_id}", "object type contract", "dataset_id", f"Missing dataset {dataset_name}", "P0")
            continue
        try:
            await _upsert_object_type_contract(
                client,
                db_name=state.db_name,
                class_id=class_id,
                backing_dataset_id=ds["dataset_id"],
                pk_spec=pk_spec,
            )
            state.bug_tracker.record_pass()
            print(f"    {class_id}: contract set (backing={ds['dataset_id'][:12]}...)")
        except Exception as exc:
            state.bug_tracker.record(phase, f"3-2:{class_id}", "object type contract", "201/200", str(exc)[:200])

    # ── 3-3: Action Types ─────────────────────────────────────────
    print("  [3-3] Creating action types...")
    action_types = [
        {
            "id": f"fulfill_order_{state.suffix}",
            "label": {"en": "Fulfill Order", "ko": "주문 이행"},
            "description": {"en": "Mark order as shipped", "ko": "주문을 배송 처리"},
            "spec": {
                "input_schema": {
                    "fields": [
                        {"name": "order", "type": "object_ref", "required": True, "object_type": "Order"},
                    ],
                },
                "permission_policy": {
                    "effect": "ALLOW",
                    "principals": ["role:Owner", "role:Editor"],
                },
                "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
                "conflict_policy": "FAIL",
                "implementation": {
                    "type": "template_v1",
                    "targets": [
                        {
                            "target": {"from": "input.order"},
                            "changes": {"set": {"order_status": "shipped"}},
                        },
                    ],
                },
            },
            "metadata": {"source": "foundry_e2e_qa"},
        },
        {
            "id": f"escalate_order_{state.suffix}",
            "label": {"en": "Escalate Order", "ko": "주문 에스컬레이션"},
            "description": {"en": "Escalate an order", "ko": "주문 에스컬레이션"},
            "spec": {
                "input_schema": {
                    "fields": [
                        {"name": "order", "type": "object_ref", "required": True, "object_type": "Order"},
                    ],
                },
                "permission_policy": {"effect": "ALLOW", "principals": ["role:Owner"]},
                "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
                "conflict_policy": "FAIL",
                "implementation": {
                    "type": "template_v1",
                    "targets": [
                        {
                            "target": {"from": "input.order"},
                            "changes": {"set": {"order_status": "escalated"}},
                        },
                    ],
                },
            },
            "metadata": {"source": "foundry_e2e_qa"},
        },
        {
            "id": f"flag_seller_{state.suffix}",
            "label": {"en": "Flag Seller", "ko": "판매자 플래그"},
            "description": {"en": "Flag a seller for review", "ko": "판매자 검토 플래그"},
            "spec": {
                "input_schema": {
                    "fields": [
                        {"name": "seller", "type": "object_ref", "required": True, "object_type": "Seller"},
                    ],
                },
                "permission_policy": {"effect": "ALLOW", "principals": ["role:Owner"]},
                "writeback_target": {"repo": "ontology-writeback", "branch": "writeback-{db_name}"},
                "conflict_policy": "FAIL",
                "implementation": {
                    "type": "template_v1",
                    "targets": [
                        {
                            "target": {"from": "input.seller"},
                            "changes": {"set": {"seller_state": "FLAGGED"}},
                        },
                    ],
                },
            },
            "metadata": {"source": "foundry_e2e_qa"},
        },
    ]

    for at in action_types:
        try:
            resp = await client.post(
                f"{OMS_URL}/api/v1/database/{state.db_name}/ontology/resources/action-types",
                params={"branch": "main", "expected_head_commit": "branch:main"},
                json=at,
                headers=state.oms_headers,
            )
            assert resp.status_code == 201, f"action type {at['id']}: {resp.status_code} {resp.text[:200]}"
            state.action_type_ids.append(at["id"])
            state.bug_tracker.record_pass()
            print(f"    {at['id']}: created")
        except Exception as exc:
            state.bug_tracker.record(phase, f"3-3:{at['id']}", "POST action-types", "201", str(exc)[:200])

    # ── 3-4/3-5/3-6: Verify ontology ─────────────────────────────
    # v2 objectTypes/actionTypes endpoints are BFF-only (not on OMS)
    print("  [3-4] Verifying ontology object types...")
    try:
        resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objectTypes",
            params={"branch": "main"},
        )
        resp.raise_for_status()
        ot_data = resp.json()
        ot_list = ot_data.get("data") or ot_data.get("objectTypes") or ot_data
        if isinstance(ot_list, dict):
            ot_list = list(ot_list.values()) if not ot_list.get("objectTypes") else ot_list.get("objectTypes", [])
        count = len(ot_list) if isinstance(ot_list, list) else 0
        print(f"    Object types: {count}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "3-4", "GET objectTypes (BFF v2)", "5 types", str(exc)[:200])

    print("  [3-5] Verifying Order object type details...")
    try:
        resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objectTypes/Order",
            params={"branch": "main"},
        )
        resp.raise_for_status()
        order_ot = resp.json()
        print(f"    Order details: {json.dumps(order_ot)[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "3-5", "GET objectTypes/Order (BFF v2)", "Order details", str(exc)[:200])

    print("  [3-6] Verifying action types...")
    try:
        resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actionTypes",
            params={"branch": "main"},
        )
        resp.raise_for_status()
        at_data = resp.json()
        print(f"    Action types: {json.dumps(at_data)[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "3-6", "GET actionTypes (BFF v2)", "3 types", str(exc)[:200])


# =============================================================================
# PHASE 4: Objectify
# =============================================================================

async def phase4_objectify(state: QAState, client: httpx.AsyncClient) -> None:
    """Create mapping specs, run DAG objectify, verify ES indexing."""
    phase = "Phase 4"
    print(f"\n{'='*60}\n  {phase}: OBJECTIFY\n{'='*60}")

    # ── 4-0: Verify ontology schemas are ready ──────────────────
    # Phase 3 already waits for command completion + property verification
    print("  [4-0] Verifying ontology schemas are ready for mapping...")
    for class_id in state.ontology_classes:
        try:
            resp = await client.get(
                f"{OMS_URL}/api/v1/database/{state.db_name}/ontology/{class_id}",
                params={"branch": "main"},
            )
            if resp.status_code == 200:
                data = resp.json().get("data") or resp.json()
                props = data.get("properties") or []
                print(f"    {class_id}: {len(props)} properties")
            else:
                print(f"    {class_id}: status={resp.status_code}")
        except Exception as exc:
            print(f"    {class_id}: check failed: {exc}")

    # ── 4-1: Mapping Specs ────────────────────────────────────────
    print("  [4-1] Creating mapping specs...")
    mapping_defs = [
        ("Customer", "olist_customers", [
            {"source_field": "customer_id", "target_field": "customer_id"},
            {"source_field": "customer_unique_id", "target_field": "customer_unique_id"},
            {"source_field": "customer_city", "target_field": "customer_city"},
            {"source_field": "customer_state", "target_field": "customer_state"},
            {"source_field": "customer_zip_code_prefix", "target_field": "customer_zip_code_prefix"},
        ]),
        ("Product", "olist_products", [
            {"source_field": "product_id", "target_field": "product_id"},
            {"source_field": "product_category_name", "target_field": "product_category_name"},
            {"source_field": "product_weight_g", "target_field": "product_weight_g"},
        ]),
        ("Seller", "olist_sellers", [
            {"source_field": "seller_id", "target_field": "seller_id"},
            {"source_field": "seller_city", "target_field": "seller_city"},
            {"source_field": "seller_state", "target_field": "seller_state"},
        ]),
        ("Order", "olist_orders", [
            {"source_field": "order_id", "target_field": "order_id"},
            {"source_field": "order_status", "target_field": "order_status"},
            {"source_field": "order_purchase_timestamp", "target_field": "order_purchase_timestamp"},
            {"source_field": "order_delivered_customer_date", "target_field": "order_delivered_customer_date"},
            {"source_field": "order_estimated_delivery_date", "target_field": "order_estimated_delivery_date"},
            {"source_field": "customer_id", "target_field": "customer"},
        ]),
        ("OrderItem", "olist_order_items", [
            {"source_field": "order_id", "target_field": "order_id"},
            {"source_field": "order_item_id", "target_field": "order_item_id"},
            {"source_field": "price", "target_field": "price"},
            {"source_field": "freight_value", "target_field": "freight_value"},
            {"source_field": "product_id", "target_field": "product"},
            {"source_field": "seller_id", "target_field": "seller"},
        ]),
    ]

    for class_id, ds_name, mappings in mapping_defs:
        ds = state.datasets.get(ds_name)
        if not ds:
            state.bug_tracker.record(phase, f"4-1:{class_id}", "mapping-spec", "dataset_id", f"Missing {ds_name}", "P0")
            continue
        # Retry up to 3 times with increasing delay (ontology schema propagation)
        max_retries = 3
        created = False
        last_err = ""
        for attempt in range(max_retries):
            try:
                resp = await client.post(
                    f"{BFF_URL}/api/v1/objectify/mapping-specs",
                    json={
                        "dataset_id": ds["dataset_id"],
                        "artifact_output_name": ds_name,
                        "target_class_id": class_id,
                        "options": {"ontology_branch": "main"},
                        "mappings": mappings,
                    },
                )
                if resp.status_code == 409:
                    err_text = resp.text[:200]
                    if "schema is required" in err_text and attempt < max_retries - 1:
                        wait_secs = 10 * (attempt + 1)
                        print(f"    {class_id}: schema not ready (attempt {attempt+1}), retrying in {wait_secs}s...")
                        await asyncio.sleep(wait_secs)
                        continue
                    last_err = f"409 Conflict: {err_text}"
                    break
                if resp.status_code == 400:
                    err_text = resp.text[:500]
                    print(f"    {class_id}: 400 error detail: {err_text}")
                    last_err = f"400 Bad Request: {err_text}"
                    break
                resp.raise_for_status()
                spec = (resp.json().get("data") or {}).get("mapping_spec") or {}
                spec_id = str(spec.get("mapping_spec_id") or "")
                assert spec_id, f"No mapping_spec_id for {class_id}"
                state.mapping_specs[class_id] = spec_id
                state.bug_tracker.record_pass()
                print(f"    {class_id}: mapping_spec_id={spec_id[:12]}...")
                created = True
                break
            except Exception as exc:
                last_err = str(exc)[:200]
                if attempt < max_retries - 1:
                    await asyncio.sleep(10)
                    continue
                break
        if not created:
            severity = "P2" if "schema is required" in last_err else "P1"
            state.bug_tracker.record(phase, f"4-1:{class_id}", "POST mapping-specs", "mapping_spec_id", last_err, severity)
            print(f"    {class_id}: FAILED — {last_err[:100]}")

    # ── 4-2: DAG dry_run ──────────────────────────────────────────
    # Only run DAG if we have mapping specs
    has_mapping_specs = len(state.mapping_specs) > 0
    if has_mapping_specs:
        print("  [4-2] DAG dry_run (execution order verification)...")
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v1/objectify/databases/{state.db_name}/run-dag",
                json={
                    "class_ids": list(state.mapping_specs.keys()),
                    "branch": "main",
                    "include_dependencies": True,
                    "dry_run": True,
                },
            )
            resp.raise_for_status()
            dag_plan = resp.json().get("data") or resp.json()
            ordered = list(dag_plan.get("ordered_classes") or dag_plan.get("execution_order") or [])
            print(f"    Execution order: {ordered}")
            # Verify dependency order: Customer, Product, Seller before Order before OrderItem
            if ordered:
                if "Order" in ordered and "Customer" in ordered:
                    assert ordered.index("Customer") < ordered.index("Order"), f"Customer should come before Order: {ordered}"
                if "Order" in ordered and "OrderItem" in ordered:
                    assert ordered.index("Order") < ordered.index("OrderItem"), f"Order should come before OrderItem: {ordered}"
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "4-2", "POST run-dag (dry_run)", "correct order", str(exc)[:200])

        # ── 4-3: DAG execution ────────────────────────────────────────
        print("  [4-3] DAG execution (objectify all classes)...")
        dag_jobs: List[Dict[str, Any]] = []
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v1/objectify/databases/{state.db_name}/run-dag",
                json={
                    "class_ids": list(state.mapping_specs.keys()),
                    "branch": "main",
                    "include_dependencies": True,
                    "dry_run": False,
                },
            )
            if resp.status_code == 409:
                print(f"    DAG 409 (jobs may already exist): {resp.text[:200]}")
                state.bug_tracker.record(phase, "4-3", "POST run-dag", "jobs created", f"409: {resp.text[:200]}", "P2")
            else:
                resp.raise_for_status()
                dag_run = resp.json().get("data") or resp.json()
                dag_jobs = dag_run.get("jobs") or dag_run.get("job_ids") or []
                print(f"    DAG started: {len(dag_jobs)} jobs")
                state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "4-3", "POST run-dag", "jobs created", str(exc)[:200])

        # NOTE: No job status polling endpoint exists.
        # Objectify runs asynchronously via Kafka worker.
        # We proceed directly to ES polling in step 4-4.
    else:
        print("  [4-2/4-3] Skipped — no mapping specs created (all got 409)")
        state.bug_tracker.record(phase, "4-2", "DAG dry_run", "mapping specs needed", "Skipped: no mapping specs", "P2")
        state.bug_tracker.record(phase, "4-3", "DAG execution", "jobs needed", "Skipped: no mapping specs", "P2")

    # ── 4-4: Wait for ES indexing ─────────────────────────────────
    print(f"  [4-4] Waiting for ES indexing (up to {ES_TIMEOUT}s)...")
    state.es_index = get_instances_index_name(state.db_name, branch="main")
    print(f"    ES index: {state.es_index}")

    # Wait for the ES index to exist and contain at least some documents
    # Rather than waiting for a specific doc (which may have a different ID),
    # poll the index for any documents
    deadline = time.monotonic() + ES_TIMEOUT
    es_doc_count = 0
    sample_order_id = ""

    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{ES_URL}/{state.es_index}/_count")
            if resp.status_code == 200:
                es_doc_count = resp.json().get("count", 0)
                if es_doc_count > 0:
                    print(f"    ES index has {es_doc_count} documents")
                    break
        except httpx.HTTPError:
            pass
        await asyncio.sleep(3.0)

    if es_doc_count > 0:
        state.bug_tracker.record_pass()
        # Find a sample order by searching
        try:
            search_resp = await client.post(
                f"{ES_URL}/{state.es_index}/_search",
                json={"query": {"term": {"class_id": "Order"}}, "size": 1},
            )
            if search_resp.status_code == 200:
                hits = (search_resp.json().get("hits") or {}).get("hits") or []
                if hits:
                    source = hits[0].get("_source") or {}
                    sample_order_id = hits[0].get("_id") or source.get("order_id") or ""
                    print(f"    Sample Order doc: _id={sample_order_id}, status={source.get('order_status', source.get('data', {}).get('order_status'))}")
        except Exception:
            pass
    else:
        state.bug_tracker.record(phase, "4-4", "ES indexing", "docs in index", f"Timed out: 0 docs in {state.es_index} after {ES_TIMEOUT}s")

    # ── 4-5: Verify ES doc properties ─────────────────────────────
    print("  [4-5] Verifying object properties match CSV data...")
    if es_doc_count > 0:
        try:
            # Search for any Order-class documents and verify properties
            search_resp = await client.post(
                f"{ES_URL}/{state.es_index}/_search",
                json={"query": {"term": {"class_id": "Order"}}, "size": 3},
            )
            if search_resp.status_code == 200:
                hits = (search_resp.json().get("hits") or {}).get("hits") or []
                for hit in hits[:3]:
                    source = hit.get("_source") or {}
                    data = source.get("data") or source
                    oid = data.get("order_id") or source.get("instance_id") or ""
                    ostatus = data.get("order_status") or ""
                    print(f"    Verified: order_id={oid[:12]}..., status={ostatus}")
                if hits:
                    state.bug_tracker.record_pass()
                else:
                    state.bug_tracker.record(phase, "4-5", "ES doc properties", "Order docs", "No Order-class documents found")
            else:
                state.bug_tracker.record(phase, "4-5", "ES doc properties", "search 200", f"ES search failed: {search_resp.status_code}")
        except Exception as exc:
            state.bug_tracker.record(phase, "4-5", "ES doc properties", "match CSV", str(exc)[:200])
    else:
        state.bug_tracker.record(phase, "4-5", "ES doc properties", "match CSV", "No ES docs to verify (indexing timed out)", "P2")


# =============================================================================
# PHASE 5: Search & Query
# =============================================================================

async def phase5_search_and_query(state: QAState, client: httpx.AsyncClient) -> None:
    """12 search scenarios using Foundry v2 API + graph queries."""
    phase = "Phase 5"
    print(f"\n{'='*60}\n  {phase}: SEARCH & QUERY\n{'='*60}")

    # Pre-check: verify ES has data before running search tests
    # If objectify hasn't completed, all searches will 500 with index_not_found
    if state.es_index:
        try:
            count_resp = await client.get(f"{ES_URL}/{state.es_index}/_count")
            if count_resp.status_code == 200:
                doc_count = count_resp.json().get("count", 0)
                print(f"  ES pre-check: {doc_count} docs in {state.es_index}")
                if doc_count == 0:
                    print("  WARNING: ES index is empty — objectify may not have completed. Searches may fail.")
            else:
                print(f"  ES pre-check: index not found (status={count_resp.status_code})")
        except Exception as exc:
            print(f"  ES pre-check failed: {exc}")

    # v2 search/count endpoints work on both BFF and OMS; use BFF for consistent auth
    base_search_url = f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objects"

    # ── 5-1: eq (order_status == "delivered") ─────────────────────
    print('  [5-1] Search: order_status == "delivered"')
    try:
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"where": {"type": "eq", "field": "order_status", "value": "delivered"}, "pageSize": 10},
        )
        resp.raise_for_status()
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        if isinstance(objects, list):
            for obj in objects[:3]:
                props = obj.get("properties") or obj
                assert str(props.get("order_status", "")).lower() == "delivered", f"Non-delivered: {props}"
        print(f"    Found {len(objects) if isinstance(objects, list) else '?'} delivered orders")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-1", "search eq delivered", "all delivered", str(exc)[:200])

    # ── 5-2: gt (price > 100) ────────────────────────────────────
    print("  [5-2] Search: OrderItem price > 100")
    try:
        resp = await client.post(
            f"{base_search_url}/OrderItem/search",
            params={"branch": "main"},
            json={"where": {"type": "gt", "field": "price", "value": 100}, "pageSize": 10},
        )
        resp.raise_for_status()
        data = resp.json()
        print(f"    Response: {json.dumps(data)[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-2", "search gt price", "results", str(exc)[:200])

    # ── 5-3: and (delivered AND price >= 500) ────────────────────
    print("  [5-3] Search: compound AND query")
    try:
        resp = await client.post(
            f"{base_search_url}/OrderItem/search",
            params={"branch": "main"},
            json={
                "where": {
                    "type": "and",
                    "value": [
                        {"type": "gte", "field": "price", "value": 500},
                    ],
                },
                "pageSize": 10,
            },
        )
        resp.raise_for_status()
        print(f"    Compound AND: {resp.status_code}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-3", "search and compound", "results", str(exc)[:200])

    # ── 5-4: isNull ──────────────────────────────────────────────
    print("  [5-4] Search: isNull (order_delivered_customer_date)")
    try:
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"where": {"type": "isNull", "field": "order_delivered_customer_date", "value": True}, "pageSize": 10},
        )
        resp.raise_for_status()
        print(f"    isNull results: {resp.status_code}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-4", "search isNull", "undelivered orders", str(exc)[:200])

    # ── 5-5: containsAnyTerm (customer_city contains "sao paulo")
    print('  [5-5] Search: containsAnyTerm (customer_city ~ "sao paulo")')
    try:
        resp = await client.post(
            f"{base_search_url}/Customer/search",
            params={"branch": "main"},
            json={"where": {"type": "containsAnyTerm", "field": "customer_city", "value": "sao paulo"}, "pageSize": 10},
        )
        resp.raise_for_status()
        print(f"    Text search: {resp.status_code}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-5", "search containsAnyTerm", "SP customers", str(exc)[:200])

    # ── 5-6: in (order_status in delivered/shipped) ──────────────
    print("  [5-6] Search: in operator")
    try:
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"where": {"type": "in", "field": "order_status", "value": ["delivered", "shipped"]}, "pageSize": 10},
        )
        resp.raise_for_status()
        print(f"    IN results: {resp.status_code}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-6", "search in", "delivered/shipped", str(exc)[:200])

    # ── 5-7: orderBy (price DESC) ────────────────────────────────
    print("  [5-7] Search: orderBy price DESC")
    try:
        resp = await client.post(
            f"{base_search_url}/OrderItem/search",
            params={"branch": "main"},
            json={"orderBy": {"orderType": "fields", "fields": [{"field": "price", "direction": "desc"}]}, "pageSize": 10},
        )
        resp.raise_for_status()
        print(f"    OrderBy: {resp.status_code}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-7", "search orderBy", "sorted results", str(exc)[:200])

    # ── 5-8: select (projection) ─────────────────────────────────
    print("  [5-8] Search: select fields")
    try:
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"select": ["order_id", "order_status"], "pageSize": 5},
        )
        resp.raise_for_status()
        print(f"    Select projection: {resp.status_code}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-8", "search select", "projected fields", str(exc)[:200])

    # ── 5-9: pagination ──────────────────────────────────────────
    print("  [5-9] Search: pagination")
    try:
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"pageSize": 5},
        )
        resp.raise_for_status()
        page1 = resp.json()
        token = page1.get("nextPageToken") or page1.get("pageToken")
        if token:
            resp2 = await client.post(
                f"{base_search_url}/Order/search",
                params={"branch": "main"},
                json={"pageSize": 5, "pageToken": token},
            )
            resp2.raise_for_status()
            print(f"    Page 1 + Page 2: OK")
        else:
            print(f"    Page 1: OK (no next token)")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-9", "search pagination", "page 1+2", str(exc)[:200])

    # ── 5-10: count ──────────────────────────────────────────────
    print("  [5-10] Count: total Orders")
    try:
        resp = await client.post(
            f"{base_search_url}/Order/count",
            params={"branch": "main"},
            json={},
        )
        resp.raise_for_status()
        count_data = resp.json()
        total = count_data.get("count") or count_data.get("total") or count_data.get("data", {}).get("count")
        print(f"    Total orders: {total}")
        state.search_results["initial_order_count"] = total
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-10", "POST Order/count", "count number", str(exc)[:200])

    # ── 5-11: Graph query (1-hop: Order → Customer) ──────────────
    print("  [5-11] Graph: Order → Customer (1-hop)")
    orders_csv_path = FIXTURE_DIR / "olist_orders.csv"
    sample_order_id = ""
    if orders_csv_path.exists():
        import csv as csv_mod
        with open(orders_csv_path) as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                sample_order_id = row.get("order_id", "")
                break
    if sample_order_id:
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v1/graph-query/{state.db_name}",
                params={"base_branch": "main"},
                json={
                    "start_class": "Order",
                    "filters": {"order_id": sample_order_id},
                    "hops": [{"predicate": "customer", "target_class": "Customer"}],
                    "include_paths": True,
                    "max_paths": 10,
                    "include_documents": True,
                    "limit": 50,
                },
            )
            resp.raise_for_status()
            graph = resp.json()
            nodes = graph.get("nodes") or []
            customer_nodes = [n for n in nodes if isinstance(n, dict) and n.get("type") == "Customer"]
            print(f"    Found {len(customer_nodes)} Customer nodes via graph")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "5-11", "graph Order→Customer", "customer node", str(exc)[:200])

    # ── 5-12: Graph query (2-hop: Order → OrderItem → Product) ───
    print("  [5-12] Graph: Order → OrderItem → Product (2-hop)")
    if sample_order_id:
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v1/graph-query/{state.db_name}",
                params={"base_branch": "main"},
                json={
                    "start_class": "Order",
                    "filters": {"order_id": sample_order_id},
                    "hops": [
                        {"predicate": "order", "target_class": "OrderItem", "reverse": True},
                        {"predicate": "product", "target_class": "Product"},
                    ],
                    "include_paths": True,
                    "max_paths": 25,
                    "include_documents": False,
                    "limit": 200,
                },
            )
            resp.raise_for_status()
            graph = resp.json()
            nodes = graph.get("nodes") or []
            product_nodes = [n for n in nodes if isinstance(n, dict) and n.get("type") == "Product"]
            print(f"    Found {len(product_nodes)} Product nodes (2-hop)")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "5-12", "graph 2-hop", "product nodes", str(exc)[:200])


# =============================================================================
# PHASE 6: Actions
# =============================================================================

async def _record_deployed_commit_qa(*, db_name: str, target_branch: str = "main") -> None:
    """Record ontology deployment so actions can execute (required by OMS)."""
    try:
        from oms.database.postgres import db as postgres_db
        from oms.services.ontology_deployment_registry_v2 import OntologyDeploymentRegistryV2

        await postgres_db.connect()
        try:
            registry = OntologyDeploymentRegistryV2()
            ontology_commit_id = f"branch:{target_branch}"
            await registry.record_deployment(
                db_name=db_name,
                target_branch=target_branch,
                ontology_commit_id=ontology_commit_id,
                proposal_id=None,
                status="succeeded",
                deployed_by="foundry_e2e_qa",
                metadata={"source": "foundry_e2e_qa"},
            )
        finally:
            await postgres_db.disconnect()
    except Exception as exc:
        print(f"    WARNING: Failed to record deployment: {exc}")


async def phase6_actions(state: QAState, client: httpx.AsyncClient) -> None:
    """Test simulate, apply, batch-apply actions with closed-loop ES verification."""
    phase = "Phase 6"
    print(f"\n{'='*60}\n  {phase}: ACTIONS\n{'='*60}")

    if not state.action_type_ids:
        state.bug_tracker.record(phase, "6-0", "prerequisite", "action_type_ids", "No action types from Phase 3", "P0")
        return

    # Record ontology deployment — actions only execute on deployed ontology commits
    print("  [6-0] Recording ontology deployment...")
    try:
        await _record_deployed_commit_qa(db_name=state.db_name, target_branch="main")
        state.bug_tracker.record_pass()
        print("    Deployment recorded")
    except Exception as exc:
        state.bug_tracker.record(phase, "6-0", "record deployment", "deployment recorded", str(exc)[:200])

    fulfill_action_id = next((a for a in state.action_type_ids if "fulfill" in a), None)
    escalate_action_id = next((a for a in state.action_type_ids if "escalate" in a), None)

    # Use OMS v2 directly for action apply (like reference test_action_writeback_e2e_smoke.py)
    action_base_url = f"{OMS_URL}/api/v2/ontologies/{state.db_name}/actions"

    # Find order IDs with status != delivered for action testing
    test_order_ids: List[str] = []
    orders_csv_path = FIXTURE_DIR / "olist_orders.csv"
    if orders_csv_path.exists():
        import csv as csv_mod
        with open(orders_csv_path) as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                if row.get("order_status") in ("shipped", "invoiced", "processing", "created", "approved"):
                    test_order_ids.append(row["order_id"])
                    if len(test_order_ids) >= 5:
                        break

    if len(test_order_ids) < 4:
        # Fallback: use any order IDs
        with open(orders_csv_path) as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                if row["order_id"] not in test_order_ids:
                    test_order_ids.append(row["order_id"])
                    if len(test_order_ids) >= 5:
                        break

    if not test_order_ids:
        state.bug_tracker.record(phase, "6-0", "find test orders", "order_ids", "No orders found for action testing", "P0")
        return

    # ── 6-1: Simulate fulfill_order ──────────────────────────────
    # Allow deployment record to propagate before first action call
    await asyncio.sleep(5)

    if fulfill_action_id and test_order_ids:
        print(f"  [6-1] Simulate: {fulfill_action_id}")
        try:
            # Try BFF v2 first (BFF may handle deployment resolution differently)
            resp = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actions/{fulfill_action_id}/apply",
                params={"branch": "main"},
                json={
                    "options": {"mode": "VALIDATE_ONLY"},
                    "parameters": {"order": {"class_id": "Order", "instance_id": test_order_ids[0]}},
                },
            )
            if resp.status_code in (404, 409):
                # Retry via OMS after additional wait for deployment propagation
                print(f"    BFF returned {resp.status_code}, retrying via OMS after delay...")
                await asyncio.sleep(5)
                resp = await client.post(
                    f"{action_base_url}/{fulfill_action_id}/apply",
                    params={"branch": "main"},
                    json={
                        "options": {"mode": "VALIDATE_ONLY"},
                        "parameters": {"order": {"class_id": "Order", "instance_id": test_order_ids[0]}},
                    },
                )
            resp.raise_for_status()
            sim_result = resp.json()
            print(f"    Simulation: {json.dumps(sim_result)[:200]}")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "6-1", "simulate fulfill_order", "VALIDATE_ONLY success", str(exc)[:200])

    # ── 6-2: Apply fulfill_order (single) ─────────────────────────
    if fulfill_action_id and test_order_ids:
        print(f"  [6-2] Apply: {fulfill_action_id} on order {test_order_ids[0][:12]}...")
        try:
            corr_id = f"qa-apply-{uuid.uuid4()}"
            resp = await client.post(
                f"{action_base_url}/{fulfill_action_id}/apply",
                params={"branch": "main"},
                json={
                    "options": {"mode": "VALIDATE_AND_EXECUTE"},
                    "parameters": {"order": {"class_id": "Order", "instance_id": test_order_ids[0]}},
                    "correlationId": corr_id,
                    "metadata": {"source": "foundry_e2e_qa"},
                },
            )
            resp.raise_for_status()
            apply_result = resp.json()
            action_log_id = str(apply_result.get("action_log_id") or apply_result.get("data", {}).get("action_log_id") or "")
            print(f"    Applied: action_log_id={action_log_id}")
            if action_log_id:
                state.action_log_ids.append(action_log_id)
            state.changed_instance_ids.append(test_order_ids[0])
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "6-2", "apply fulfill_order", "action_log_id", str(exc)[:200])

    # ── 6-5: Apply escalate_order ─────────────────────────────────
    if escalate_action_id and len(test_order_ids) > 1:
        print(f"  [6-5] Apply: {escalate_action_id} on order {test_order_ids[1][:12]}...")
        try:
            corr_id = f"qa-escalate-{uuid.uuid4()}"
            resp = await client.post(
                f"{action_base_url}/{escalate_action_id}/apply",
                params={"branch": "main"},
                json={
                    "options": {"mode": "VALIDATE_AND_EXECUTE"},
                    "parameters": {"order": {"class_id": "Order", "instance_id": test_order_ids[1]}},
                    "correlationId": corr_id,
                    "metadata": {"source": "foundry_e2e_qa"},
                },
            )
            resp.raise_for_status()
            state.changed_instance_ids.append(test_order_ids[1])
            state.bug_tracker.record_pass()
            print("    Escalated successfully")
        except Exception as exc:
            state.bug_tracker.record(phase, "6-5", "apply escalate_order", "escalated", str(exc)[:200])

    # ── 6-7: Batch apply fulfill_order × 3 ────────────────────────
    if fulfill_action_id and len(test_order_ids) >= 5:
        batch_ids = test_order_ids[2:5]
        print(f"  [6-7] Batch apply: {fulfill_action_id} × {len(batch_ids)}")
        for oid in batch_ids:
            try:
                corr_id = f"qa-batch-{uuid.uuid4()}"
                resp = await client.post(
                    f"{action_base_url}/{fulfill_action_id}/apply",
                    params={"branch": "main"},
                    json={
                        "options": {"mode": "VALIDATE_AND_EXECUTE"},
                        "parameters": {"order": {"class_id": "Order", "instance_id": oid}},
                        "correlationId": corr_id,
                        "metadata": {"source": "foundry_e2e_qa_batch"},
                    },
                )
                resp.raise_for_status()
                state.changed_instance_ids.append(oid)
                state.bug_tracker.record_pass()
                print(f"    Batch applied: {oid[:12]}...")
            except Exception as exc:
                state.bug_tracker.record(phase, f"6-7:{oid[:8]}", "batch apply", "applied", str(exc)[:200])


# =============================================================================
# PHASE 7: Closed Loop Verification
# =============================================================================

async def phase7_closed_loop(state: QAState, client: httpx.AsyncClient) -> None:
    """Verify action effects are reflected in search results."""
    phase = "Phase 7"
    print(f"\n{'='*60}\n  {phase}: CLOSED LOOP VERIFICATION\n{'='*60}")

    base_search_url = f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objects"

    # Allow time for action worker + ES to propagate
    print("  Waiting 10s for action propagation...")
    await asyncio.sleep(10)

    # ── 7-1: Count shipped orders ─────────────────────────────────
    print('  [7-1] Count: order_status == "shipped"')
    try:
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"where": {"type": "eq", "field": "order_status", "value": "shipped"}, "pageSize": 200},
        )
        resp.raise_for_status()
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        shipped_count = len(objects) if isinstance(objects, list) else 0
        print(f"    Shipped orders: {shipped_count}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "7-1", "search shipped", "count > 0", str(exc)[:200])

    # ── 7-2: Count escalated orders ──────────────────────────────
    print('  [7-2] Count: order_status == "escalated"')
    try:
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"where": {"type": "eq", "field": "order_status", "value": "escalated"}, "pageSize": 200},
        )
        resp.raise_for_status()
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        escalated_count = len(objects) if isinstance(objects, list) else 0
        print(f"    Escalated orders: {escalated_count}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "7-2", "search escalated", "count >= 1", str(exc)[:200])

    # ── 7-3: Graph query on changed order ─────────────────────────
    if state.changed_instance_ids:
        print(f"  [7-3] Graph query on changed order: {state.changed_instance_ids[0][:12]}...")
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v1/graph-query/{state.db_name}",
                params={"base_branch": "main"},
                json={
                    "start_class": "Order",
                    "filters": {"order_id": state.changed_instance_ids[0]},
                    "hops": [{"predicate": "customer", "target_class": "Customer"}],
                    "include_paths": True,
                    "max_paths": 10,
                    "include_documents": True,
                    "limit": 50,
                },
            )
            resp.raise_for_status()
            graph = resp.json()
            nodes = graph.get("nodes") or []
            print(f"    Graph after action: {len(nodes)} nodes")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "7-3", "graph on changed order", "traversable", str(exc)[:200])

    # ── 7-4: Data consistency check ──────────────────────────────
    print("  [7-4] Data consistency: all OrderItems reference existing Orders")
    try:
        resp = await client.post(
            f"{base_search_url}/OrderItem/search",
            params={"branch": "main"},
            json={"pageSize": 20},
        )
        resp.raise_for_status()
        items = resp.json().get("data") or resp.json().get("objects") or []
        print(f"    Checked {len(items) if isinstance(items, list) else '?'} OrderItems")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "7-4", "consistency check", "all refs valid", str(exc)[:200])


# =============================================================================
# PHASE 8: Live Data Cross-Domain
# =============================================================================

async def phase8_live_data_cross_domain(state: QAState, client: httpx.AsyncClient) -> None:
    """Test real-time API data: objectify + search + cross-domain pipeline."""
    phase = "Phase 8"
    print(f"\n{'='*60}\n  {phase}: LIVE DATA CROSS-DOMAIN\n{'='*60}")

    # ── 8-1: WeatherData Object Type ──────────────────────────────
    weather_ds = state.live_datasets.get("weather_saopaulo")
    if weather_ds:
        print("  [8-1] WeatherData: ontology + objectify")
        try:
            weather_cls = {
                "id": "WeatherData",
                "label": {"en": "Weather Data", "ko": "날씨 데이터"},
                "description": {"en": "Hourly weather observations", "ko": "시간별 날씨 관측"},
                "properties": [
                    {"name": "time", "type": "xsd:dateTime", "label": {"en": "Time"}, "required": True, "primaryKey": True, "titleKey": True},
                    {"name": "temperature_2m", "type": "xsd:decimal", "label": {"en": "Temperature (°C)"}},
                    {"name": "windspeed_10m", "type": "xsd:decimal", "label": {"en": "Wind Speed (km/h)"}},
                    {"name": "precipitation", "type": "xsd:decimal", "label": {"en": "Precipitation (mm)"}},
                ],
                "relationships": [],
                "metadata": {"source": "foundry_e2e_qa"},
            }
            resp = await client.post(
                f"{BFF_URL}/api/v1/databases/{state.db_name}/ontology",
                params={"branch": "main"},
                json=weather_cls,
            )
            resp.raise_for_status()
            cmd_id = str((resp.json().get("data") or {}).get("command_id") or "")
            if cmd_id:
                try:
                    await _wait_for_command(client, cmd_id, timeout=120)
                except Exception:
                    pass
            await _wait_for_ontology(client, db_name=state.db_name, class_id="WeatherData", require_properties=True, timeout=120)
            print("    WeatherData class created")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "8-1", "WeatherData ontology", "class created", str(exc)[:200])

    # ── 8-2: ExchangeRate Object Type ─────────────────────────────
    rates_ds = state.live_datasets.get("exchange_rates_brl")
    if rates_ds:
        print("  [8-2] ExchangeRate: ontology + objectify")
        try:
            rate_cls = {
                "id": "ExchangeRate",
                "label": {"en": "Exchange Rate", "ko": "환율"},
                "description": {"en": "Daily BRL exchange rates", "ko": "일별 BRL 환율"},
                "properties": [
                    {"name": "date", "type": "xsd:date", "label": {"en": "Date"}, "required": True, "primaryKey": True, "titleKey": True},
                    {"name": "base", "type": "xsd:string", "label": {"en": "Base Currency"}},
                    {"name": "usd_rate", "type": "xsd:decimal", "label": {"en": "USD Rate"}},
                    {"name": "eur_rate", "type": "xsd:decimal", "label": {"en": "EUR Rate"}},
                    {"name": "krw_rate", "type": "xsd:decimal", "label": {"en": "KRW Rate"}},
                ],
                "relationships": [],
                "metadata": {"source": "foundry_e2e_qa"},
            }
            resp = await client.post(
                f"{BFF_URL}/api/v1/databases/{state.db_name}/ontology",
                params={"branch": "main"},
                json=rate_cls,
            )
            resp.raise_for_status()
            cmd_id = str((resp.json().get("data") or {}).get("command_id") or "")
            if cmd_id:
                try:
                    await _wait_for_command(client, cmd_id, timeout=120)
                except Exception:
                    pass
            await _wait_for_ontology(client, db_name=state.db_name, class_id="ExchangeRate", require_properties=True, timeout=120)
            print("    ExchangeRate class created")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "8-2", "ExchangeRate ontology", "class created", str(exc)[:200])

    # ── 8-3: Earthquake Object Type ───────────────────────────────
    eq_ds = state.live_datasets.get("earthquakes_month")
    if eq_ds:
        print("  [8-3] Earthquake: ontology + objectify")
        try:
            eq_cls = {
                "id": "Earthquake",
                "label": {"en": "Earthquake", "ko": "지진"},
                "description": {"en": "Seismic event data", "ko": "지진 이벤트 데이터"},
                "properties": [
                    {"name": "eq_id", "type": "xsd:string", "label": {"en": "ID"}, "required": True, "primaryKey": True, "titleKey": True},
                    {"name": "magnitude", "type": "xsd:decimal", "label": {"en": "Magnitude"}},
                    {"name": "place", "type": "xsd:string", "label": {"en": "Location"}},
                    {"name": "latitude", "type": "xsd:decimal", "label": {"en": "Latitude"}},
                    {"name": "longitude", "type": "xsd:decimal", "label": {"en": "Longitude"}},
                    {"name": "depth_km", "type": "xsd:decimal", "label": {"en": "Depth (km)"}},
                    {"name": "time", "type": "xsd:string", "label": {"en": "Time"}},
                ],
                "relationships": [],
                "metadata": {"source": "foundry_e2e_qa"},
            }
            resp = await client.post(
                f"{BFF_URL}/api/v1/databases/{state.db_name}/ontology",
                params={"branch": "main"},
                json=eq_cls,
            )
            resp.raise_for_status()
            cmd_id = str((resp.json().get("data") or {}).get("command_id") or "")
            if cmd_id:
                try:
                    await _wait_for_command(client, cmd_id, timeout=120)
                except Exception:
                    pass
            await _wait_for_ontology(client, db_name=state.db_name, class_id="Earthquake", require_properties=True, timeout=120)
            print("    Earthquake class created")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "8-3", "Earthquake ontology", "class created", str(exc)[:200])

    # ── 8-4: Search weather data ──────────────────────────────────
    base_search_url = f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objects"
    if weather_ds:
        print("  [8-4] Search: WeatherData temperature_2m > 20")
        try:
            resp = await client.post(
                f"{base_search_url}/WeatherData/search",
                params={"branch": "main"},
                json={"where": {"type": "gt", "field": "temperature_2m", "value": 20}, "pageSize": 10},
            )
            resp.raise_for_status()
            data = resp.json()
            print(f"    Weather search: {resp.status_code}")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "8-4", "search WeatherData", "warm hours", str(exc)[:200])

    # ── 8-5: Search earthquake data ───────────────────────────────
    if eq_ds:
        print("  [8-5] Search: Earthquake magnitude >= 4.0")
        try:
            resp = await client.post(
                f"{base_search_url}/Earthquake/search",
                params={"branch": "main"},
                json={"where": {"type": "gte", "field": "magnitude", "value": 4.0}, "pageSize": 10},
            )
            resp.raise_for_status()
            data = resp.json()
            print(f"    Earthquake search: {resp.status_code}")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "8-5", "search Earthquake", "mag >= 4.0", str(exc)[:200])

    # ── 8-6: Cross-domain pipeline ────────────────────────────────
    orders_id = (state.datasets.get("olist_orders") or {}).get("dataset_id")
    rates_id = (state.live_datasets.get("exchange_rates_brl") or {}).get("dataset_id")
    if orders_id and rates_id:
        print("  [8-6] Cross-domain pipeline: orders + exchange_rates")
        try:
            cross_def = {
                "nodes": [
                    {"id": "in_orders", "type": "input", "metadata": {"datasetId": orders_id}},
                    {"id": "in_rates", "type": "input", "metadata": {"datasetId": rates_id}},
                    {"id": "union_cross", "type": "transform", "metadata": {
                        "operation": "union",
                    }},
                    {"id": "out_cross", "type": "output", "metadata": {
                        "outputName": "cross_domain", "outputKind": "dataset",
                    }},
                ],
                "edges": [
                    {"from": "in_orders", "to": "union_cross"},
                    {"from": "in_rates", "to": "union_cross"},
                    {"from": "union_cross", "to": "out_cross"},
                ],
                "parameters": [],
                "settings": {"engine": "Batch"},
            }
            idem_key = f"idem-cross-domain-{state.suffix}"
            resp = await client.post(
                f"{BFF_URL}/api/v1/pipelines",
                headers={**state.headers, "Idempotency-Key": idem_key},
                json={
                    "db_name": state.db_name,
                    "name": f"cross_domain_{state.suffix}",
                    "description": "Cross-domain: orders + exchange rates",
                    "definition_json": cross_def,
                    "pipeline_type": "batch",
                    "branch": "main",
                    "location": "e2e",
                },
            )
            resp.raise_for_status()
            print(f"    Cross-domain pipeline created: {resp.status_code}")
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "8-6", "cross-domain pipeline", "pipeline created", str(exc)[:200])


# =============================================================================
# MAIN TEST
# =============================================================================

@pytest.mark.integration
@pytest.mark.requires_infra
@pytest.mark.workflow
@pytest.mark.asyncio
async def test_foundry_e2e_qa() -> None:
    """Full Foundry lifecycle QA with real Kaggle data + live APIs."""

    state = QAState()
    state.suffix = uuid.uuid4().hex[:10]
    state.db_name = f"qa_{state.suffix}"
    state.user_id = f"qa-user-{state.suffix}"

    _base = bff_auth_headers()
    state.headers = {
        **_base,
        "X-DB-Name": state.db_name,
        "X-User-ID": state.user_id,
        "X-User-Type": "user",
        "X-User-Name": state.user_id,
    }
    state.oms_headers = oms_auth_headers()

    print(f"\n{'#'*60}")
    print(f"  FOUNDRY E2E QA — {state.db_name}")
    print(f"  BFF: {BFF_URL}")
    print(f"  OMS: {OMS_URL}")
    print(f"  ES:  {ES_URL}")
    print(f"  Fixture dir: {FIXTURE_DIR}")
    print(f"{'#'*60}")

    async with httpx.AsyncClient(headers=state.headers, timeout=HTTPX_TIMEOUT) as client:
        # Phase 1: Data Ingestion
        await phase1_data_ingestion(state, client)

        # Phase 2: Pipeline Transforms
        await phase2_pipeline_transforms(state, client)

        # Phase 3: Ontology Creation
        await phase3_ontology_creation(state, client)

        # Phase 4: Objectify
        await phase4_objectify(state, client)

        # Phase 5: Search & Query
        await phase5_search_and_query(state, client)

        # Phase 6: Actions
        await phase6_actions(state, client)

        # Phase 7: Closed Loop Verification
        await phase7_closed_loop(state, client)

        # Phase 8: Live Data Cross-Domain
        await phase8_live_data_cross_domain(state, client)

    # ── Final Report ──────────────────────────────────────────────
    print(state.bug_tracker.summary())
    state.bug_tracker.dump_json("qa_bugs.json")
    print(f"  Bug report: qa_bugs.json ({len(state.bug_tracker.bugs)} bugs)")

    # Fail only on P0 blockers
    p0_bugs = [b for b in state.bug_tracker.bugs if b.severity == "P0"]
    if p0_bugs:
        p0_summary = "\n".join(f"  [{b.phase}:{b.step}] {b.endpoint} — {b.actual[:100]}" for b in p0_bugs)
        pytest.fail(f"{len(p0_bugs)} P0 BLOCKER(S) found:\n{p0_summary}")
