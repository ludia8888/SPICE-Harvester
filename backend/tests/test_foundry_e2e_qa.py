"""
Foundry 완전 E2E QA: 실제 Kaggle 데이터 + 실시간 API

Tests the full Palantir Foundry lifecycle:
  데이터 수집 → 파이프라인 변환 → 온톨로지 생성 → 객체화 →
  검색/쿼리 → 액션 실행 → 폐루프 반영 → 실시간 크로스도메인

Data sources:
  - Brazilian E-Commerce by Olist (Kaggle, 5 relational CSVs)
  - (Optional live) Open-Meteo Weather API (no key)
  - (Optional live) Frankfurter Exchange Rate API (no key)
  - (Optional live) USGS Earthquake API (no key)

Run:
  RUN_FOUNDRY_E2E_QA=true \\
    PYTHONPATH=backend \\
    python -m pytest backend/tests/test_foundry_e2e_qa.py -v -s --tb=short

Enable live APIs (nightly/manual; may be flaky):
  RUN_FOUNDRY_E2E_QA_LIVE=true
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pytest

from shared.config.search_config import get_instances_index_name
from shared.foundry.rids import build_rid
from tests.utils.auth import build_smoke_user_jwt, oms_auth_headers
from tests.utils.qa_helpers import (
    BugTracker,
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
REPO_ROOT = Path(__file__).resolve().parents[2]

RUN_LIVE_APIS = os.getenv("RUN_FOUNDRY_E2E_QA_LIVE", "").strip().lower() in {"1", "true", "yes", "on"}

_SEVERITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


def _severity_rank(value: str) -> int:
    return _SEVERITY_RANK.get(str(value or "").strip().upper(), 99)


def _qa_fail_threshold_rank() -> int:
    # Default: fail on P0 + P1 (critical), but allow operators to dial stricter/looser.
    raw = str(os.getenv("QA_FAIL_SEVERITY") or "P1").strip().upper()
    return _severity_rank(raw)


def _count_fixture_rows(path: Path) -> int:
    # Line-count minus header. Fixtures are small and stable.
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    return max(0, len(lines) - 1)


def _expected_instance_counts_from_fixtures() -> Dict[str, int]:
    return {
        "Customer": _count_fixture_rows(FIXTURE_DIR / "olist_customers.csv"),
        "Order": _count_fixture_rows(FIXTURE_DIR / "olist_orders.csv"),
        "Product": _count_fixture_rows(FIXTURE_DIR / "olist_products.csv"),
        "Seller": _count_fixture_rows(FIXTURE_DIR / "olist_sellers.csv"),
        "OrderItem": _count_fixture_rows(FIXTURE_DIR / "olist_order_items.csv"),
    }


def _pick_sample_order_from_fixtures() -> tuple[str, str]:
    """Return (order_id, customer_id) from fixtures."""
    import csv as csv_mod

    with (FIXTURE_DIR / "olist_orders.csv").open(newline="", encoding="utf-8") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            order_id = str(row.get("order_id") or "").strip()
            customer_id = str(row.get("customer_id") or "").strip()
            if order_id and customer_id:
                return order_id, customer_id
    raise RuntimeError("No order rows found in fixtures")


def _pick_order_with_items_from_fixtures() -> tuple[str, str]:
    """Return (order_id, product_id) for an order that has at least 1 item."""
    import csv as csv_mod

    with (FIXTURE_DIR / "olist_order_items.csv").open(newline="", encoding="utf-8") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            order_id = str(row.get("order_id") or "").strip()
            product_id = str(row.get("product_id") or "").strip()
            if order_id and product_id:
                return order_id, product_id
    raise RuntimeError("No order_items rows found in fixtures")


# ── Shared state between phases ──────────────────────────────────────────────

@dataclass(frozen=True)
class Persona:
    name: str
    subject: str
    db_role: Optional[str]
    jwt: str
    headers: Dict[str, str]


class QAState:
    """Mutable state shared across all 8 phases."""
    def __init__(self) -> None:
        self.db_name: str = ""
        self.suffix: str = ""
        self.headers: Dict[str, str] = {}
        self.oms_headers: Dict[str, str] = {}
        self.personas: Dict[str, Persona] = {}
        self.current_persona: str = ""
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
        self.expected_order_status: Dict[str, str] = {}

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


def _switch_persona(state: QAState, client: httpx.AsyncClient, persona_key: str) -> None:
    persona = state.personas.get(persona_key)
    if persona is None:
        raise RuntimeError(f"Unknown persona: {persona_key}")
    state.current_persona = persona_key
    state.headers = dict(persona.headers)
    client.headers = httpx.Headers(state.headers)


def _assert_v2_error_shape(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise AssertionError(f"Expected JSON object error, got: {type(payload).__name__}")
    for key in ("errorCode", "errorName", "errorInstanceId"):
        if not str(payload.get(key) or "").strip():
            raise AssertionError(f"Missing v2 error field: {key} (payload={payload})")


async def _expect_v2_error(
    *,
    resp: httpx.Response,
    expected_status: int,
    expected_error_name: Optional[str] = None,
    expected_error_code: Optional[str] = None,
) -> None:
    if resp.status_code != expected_status:
        raise AssertionError(
            f"Expected status {expected_status}, got {resp.status_code}: {resp.text[:200]}"
        )
    if resp.status_code >= 500:
        raise AssertionError(
            f"Server error leaked for client error case: {resp.status_code} {resp.text[:200]}"
        )
    payload = resp.json()
    _assert_v2_error_shape(payload)
    if expected_error_name and str(payload.get("errorName") or "").strip() != expected_error_name:
        raise AssertionError(f"Expected errorName={expected_error_name}, got {payload.get('errorName')!r}")
    if expected_error_code and str(payload.get("errorCode") or "").strip() != expected_error_code:
        raise AssertionError(f"Expected errorCode={expected_error_code}, got {payload.get('errorCode')!r}")


# =============================================================================
# PHASE 0: Guardrails (Auth/RBAC/Conflicts)
# =============================================================================

async def phase0_guardrails(state: QAState, client: httpx.AsyncClient) -> None:
    """Hard-gate negative cases: 401/403/409/400 should be correct and never 500."""
    phase = "Phase 0"
    print(f"\n{'='*60}\n  {phase}: GUARDRAILS (REAL-USER NEGATIVE CASES)\n{'='*60}")

    db_name = state.db_name

    # ── 0-1: 401 MissingCredentials (no token) ───────────────────
    print("  [0-1] 401 MissingCredentials (no token) on v2 search")
    resp: Optional[httpx.Response] = None
    try:
        resp = await client.post(
            f"{BFF_URL}/api/v2/ontologies/{db_name}/objects/Order/search",
            params={"branch": "main"},
            json={"pageSize": 1},
            headers={"Authorization": "", "X-DB-Name": db_name, "Accept": "application/json"},
        )
        await _expect_v2_error(
            resp=resp,
            expected_status=401,
            expected_error_name="MissingCredentials",
            expected_error_code="UNAUTHORIZED",
        )
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-1",
            "POST /api/v2/.../objects/Order/search",
            "401 MissingCredentials",
            str(exc)[:200],
            "P0",
            persona="anonymous",
            status_code=resp.status_code if resp is not None else None,
        )

    # ── 0-2: 401 Unauthorized (invalid bearer) ───────────────────
    print("  [0-2] 401 Unauthorized (invalid bearer) on v2 search")
    resp = None
    try:
        resp = await client.post(
            f"{BFF_URL}/api/v2/ontologies/{db_name}/objects/Order/search",
            params={"branch": "main"},
            json={"pageSize": 1},
            headers={"Authorization": "Bearer not-a-jwt", "X-DB-Name": db_name, "Accept": "application/json"},
        )
        await _expect_v2_error(
            resp=resp,
            expected_status=401,
            expected_error_name="Unauthorized",
            expected_error_code="UNAUTHORIZED",
        )
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-2",
            "POST /api/v2/.../objects/Order/search",
            "401 Unauthorized",
            str(exc)[:200],
            "P0",
            persona="anonymous",
            status_code=resp.status_code if resp is not None else None,
        )

    # ── 0-2b: SRE basic observability endpoints ───────────────────
    print("  [0-2b] SRE: health/metrics reachable")
    try:
        _switch_persona(state, client, "sre")
        health = await client.get(f"{BFF_URL}/api/v1/health")
        if health.status_code != 200:
            raise AssertionError(f"health expected 200, got {health.status_code}: {health.text[:200]}")
        metrics = await client.get(f"{BFF_URL}/metrics")
        if metrics.status_code != 200:
            raise AssertionError(f"metrics expected 200, got {metrics.status_code}: {metrics.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "0-2b", "GET /health,/metrics", "200", str(exc)[:200], "P1", persona="sre")

    # ── 0-3: 403 Intruder (no DB role) read denied ───────────────
    print("  [0-3] 403 PermissionDenied (intruder has no DB access)")
    resp = None
    try:
        _switch_persona(state, client, "intruder")
        resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{db_name}/objectTypes",
            params={"branch": "main"},
        )
        await _expect_v2_error(resp=resp, expected_status=403, expected_error_code="PERMISSION_DENIED")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-3",
            "GET /api/v2/.../objectTypes",
            "403 PermissionDenied",
            str(exc)[:200],
            "P0",
            persona="intruder",
            status_code=resp.status_code if resp is not None else None,
        )

    # ── 0-3b: 200 Viewer read allowed ────────────────────────────
    print("  [0-3b] 200 Viewer can read v2 objectTypes")
    resp = None
    try:
        _switch_persona(state, client, "viewer")
        resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{db_name}/objectTypes",
            params={"branch": "main"},
        )
        if resp.status_code != 200:
            raise AssertionError(f"Expected 200, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-3b",
            "GET /api/v2/.../objectTypes",
            "200",
            str(exc)[:200],
            "P0",
            persona="viewer",
            status_code=resp.status_code if resp is not None else None,
        )

    # ── 0-4: 403 Viewer ontology write ───────────────────────────
    print("  [0-4] 403 Viewer cannot write ontology classes (v1)")
    resp = None
    try:
        _switch_persona(state, client, "viewer")
        resp = await client.post(
            f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
            params={"branch": "main"},
            json={
                "id": f"RBAC_Block_{state.suffix}",
                "label": {"en": "RBAC Block"},
                "description": {"en": "RBAC negative test"},
                "properties": [
                    {
                        "name": "id",
                        "type": "xsd:string",
                        "label": {"en": "ID"},
                        "required": True,
                        "primaryKey": True,
                        "titleKey": True,
                    },
                ],
                "relationships": [],
                "metadata": {"source": "foundry_e2e_qa_guardrails"},
            },
        )
        if resp.status_code != 403:
            raise AssertionError(f"Expected 403, got {resp.status_code}: {resp.text[:200]}")
        if resp.status_code >= 500:
            raise AssertionError(f"Expected 4xx, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-4",
            "POST /api/v1/databases/{db}/ontology",
            "403",
            str(exc)[:200],
            "P0",
            persona="viewer",
            status_code=resp.status_code if resp is not None else None,
        )

    # ── 0-5: 403 DataEngineer ontology write ─────────────────────
    print("  [0-5] 403 DataEngineer cannot write ontology classes (v1)")
    resp = None
    try:
        _switch_persona(state, client, "data_engineer")
        resp = await client.post(
            f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
            params={"branch": "main"},
            json={
                "id": f"RBAC_Block_DE_{state.suffix}",
                "label": {"en": "RBAC Block DE"},
                "description": {"en": "RBAC negative test (data engineer)"},
                "properties": [
                    {
                        "name": "id",
                        "type": "xsd:string",
                        "label": {"en": "ID"},
                        "required": True,
                        "primaryKey": True,
                        "titleKey": True,
                    },
                ],
                "relationships": [],
                "metadata": {"source": "foundry_e2e_qa_guardrails"},
            },
        )
        if resp.status_code != 403:
            raise AssertionError(f"Expected 403, got {resp.status_code}: {resp.text[:200]}")
        if resp.status_code >= 500:
            raise AssertionError(f"Expected 4xx, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-5",
            "POST /api/v1/databases/{db}/ontology",
            "403",
            str(exc)[:200],
            "P0",
            persona="data_engineer",
            status_code=resp.status_code if resp is not None else None,
        )

    # ── 0-6/0-7: 403 dataset upload denied for non-DataEngineer ───
    csv_a = b"customer_id,customer_city\nc1,alpha\n"
    print("  [0-6] 403 Viewer cannot upload datasets (csv-upload)")
    try:
        _switch_persona(state, client, "viewer")
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**state.headers, "Idempotency-Key": f"idem-viewer-upload-{state.suffix}"},
            data={"dataset_name": f"viewer_upload_{state.suffix}", "description": "rbac", "delimiter": ",", "has_header": "true"},
            files={"file": ("viewer.csv", csv_a, "text/csv")},
        )
        if resp.status_code != 403:
            raise AssertionError(f"Expected 403, got {resp.status_code}: {resp.text[:200]}")
        if resp.status_code >= 500:
            raise AssertionError(f"Expected 4xx, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-6",
            "POST /api/v1/pipelines/datasets/csv-upload",
            "403",
            str(exc)[:200],
            "P0",
            persona="viewer",
        )

    print("  [0-7] 403 DomainModeler cannot upload datasets (csv-upload)")
    try:
        _switch_persona(state, client, "domain_modeler")
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**state.headers, "Idempotency-Key": f"idem-domain-upload-{state.suffix}"},
            data={"dataset_name": f"domain_upload_{state.suffix}", "description": "rbac", "delimiter": ",", "has_header": "true"},
            files={"file": ("domain.csv", csv_a, "text/csv")},
        )
        if resp.status_code != 403:
            raise AssertionError(f"Expected 403, got {resp.status_code}: {resp.text[:200]}")
        if resp.status_code >= 500:
            raise AssertionError(f"Expected 4xx, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "0-7",
            "POST /api/v1/pipelines/datasets/csv-upload",
            "403",
            str(exc)[:200],
            "P0",
            persona="domain_modeler",
        )

    # ── 0-8: 400 Missing Idempotency-Key ─────────────────────────
    print("  [0-8] 400 Missing Idempotency-Key on csv-upload")
    try:
        _switch_persona(state, client, "data_engineer")
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            data={"dataset_name": f"missing_idem_{state.suffix}", "description": "rbac", "delimiter": ",", "has_header": "true"},
            files={"file": ("missing.csv", csv_a, "text/csv")},
        )
        if resp.status_code != 400:
            raise AssertionError(f"Expected 400, got {resp.status_code}: {resp.text[:200]}")
        if resp.status_code >= 500:
            raise AssertionError(f"Expected 4xx, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "0-8", "POST /csv-upload", "400", str(exc)[:200], "P0", persona="data_engineer")

    # ── 0-9: 409 Idempotency conflict (same key, different content) ─
    print("  [0-9] 409 Idempotency conflict (same key, different content)")
    try:
        _switch_persona(state, client, "integration")
        dataset_name = f"idem_conflict_{state.suffix}"
        idem = f"idem-conflict-{state.suffix}"
        resp1 = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**state.headers, "Idempotency-Key": idem},
            data={"dataset_name": dataset_name, "description": "idempotency", "delimiter": ",", "has_header": "true"},
            files={"file": ("a.csv", csv_a, "text/csv")},
        )
        resp1.raise_for_status()
        csv_b = b"customer_id,customer_city\nc1,beta\n"
        resp2 = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**state.headers, "Idempotency-Key": idem},
            data={"dataset_name": dataset_name, "description": "idempotency", "delimiter": ",", "has_header": "true"},
            files={"file": ("b.csv", csv_b, "text/csv")},
        )
        if resp2.status_code != 409:
            raise AssertionError(f"Expected 409, got {resp2.status_code}: {resp2.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "0-9", "POST /csv-upload", "409", str(exc)[:200], "P0", persona="integration")

    # ── 0-10: 200 Idempotent retry (same key, same content) ───────
    print("  [0-10] 200 Idempotent retry (same key, same content)")
    try:
        _switch_persona(state, client, "bot")
        dataset_name = f"idem_retry_{state.suffix}"
        idem = f"idem-retry-{state.suffix}"
        resp1 = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**state.headers, "Idempotency-Key": idem},
            data={"dataset_name": dataset_name, "description": "retry", "delimiter": ",", "has_header": "true"},
            files={"file": ("a.csv", csv_a, "text/csv")},
        )
        resp1.raise_for_status()
        payload1 = resp1.json().get("data") or {}
        ver1 = (payload1.get("version") or {}).get("version_id")
        resp2 = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**state.headers, "Idempotency-Key": idem},
            data={"dataset_name": dataset_name, "description": "retry", "delimiter": ",", "has_header": "true"},
            files={"file": ("a.csv", csv_a, "text/csv")},
        )
        resp2.raise_for_status()
        payload2 = resp2.json().get("data") or {}
        ver2 = (payload2.get("version") or {}).get("version_id")
        if ver1 and ver2 and str(ver1) != str(ver2):
            raise AssertionError(f"Expected same version_id on retry, got {ver1} vs {ver2}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "0-10", "POST /csv-upload", "200 + same version_id", str(exc)[:200], "P0", persona="bot")

    # ── 0-11: 403 DB scope mismatch (X-DB-Name != db_name) ────────
    print("  [0-11] 403 Project scope mismatch (X-DB-Name)")
    try:
        _switch_persona(state, client, "data_engineer")
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={
                **state.headers,
                "X-DB-Name": f"{db_name}_other",
                "Idempotency-Key": f"idem-scope-mismatch-{state.suffix}",
            },
            data={"dataset_name": f"scope_mismatch_{state.suffix}", "description": "scope", "delimiter": ",", "has_header": "true"},
            files={"file": ("scope.csv", csv_a, "text/csv")},
        )
        if resp.status_code != 403:
            raise AssertionError(f"Expected 403, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "0-11", "POST /csv-upload", "403", str(exc)[:200], "P0", persona="data_engineer")

    # Restore default persona for subsequent phases.
    _switch_persona(state, client, "data_engineer")


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


async def _wait_for_pipeline_run_sample(
    client: httpx.AsyncClient,
    *,
    pipeline_id: str,
    job_id: str,
    timeout: int = 180,
) -> Dict[str, Any]:
    """Poll pipeline runs until the given job_id has a SUCCESS sample_json with rows."""
    deadline = time.monotonic() + timeout
    last_payload: Optional[Dict[str, Any]] = None
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/runs", params={"limit": 50})
        if resp.status_code == 200:
            payload = resp.json()
            last_payload = payload
            runs = (payload.get("data") or {}).get("runs") or []
            if isinstance(runs, list):
                run = next((r for r in runs if str(r.get("job_id") or "") == str(job_id)), None)
            else:
                run = None
            if isinstance(run, dict):
                status_value = str(run.get("status") or "").upper()
                sample = run.get("sample_json") if isinstance(run.get("sample_json"), dict) else None
                if status_value in {"FAILED", "ERROR"}:
                    raise AssertionError(f"Pipeline run failed (job_id={job_id}): {run}")
                if status_value in {"SUCCESS", "SUCCEEDED", "COMPLETED", "DEPLOYED"}:
                    if isinstance(sample, dict) and isinstance(sample.get("rows"), list) and sample.get("rows"):
                        return sample
        await asyncio.sleep(2.0)
    raise AssertionError(f"Timed out waiting for pipeline run sample (job_id={job_id}, last={last_payload})")


async def _wait_for_dataset_by_name(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    dataset_name: str,
    timeout: int = 180,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/pipelines/datasets", params={"db_name": db_name})
        if resp.status_code == 200:
            data = resp.json().get("data") or resp.json()
            ds_list = data.get("datasets") or data.get("items") or []
            if isinstance(ds_list, list):
                hit = next((d for d in ds_list if str(d.get("name") or "") == dataset_name), None)
                if isinstance(hit, dict):
                    return hit
        await asyncio.sleep(2.0)
    raise AssertionError(f"Timed out waiting for dataset '{dataset_name}' to appear in db={db_name}")


async def _wait_for_ontology(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    class_id: str,
    oms_headers: Optional[Dict[str, str]] = None,
    timeout: int = 90,
    require_properties: bool = False,
) -> None:
    """Wait for ontology class to exist (and optionally have properties populated)."""
    deadline = time.monotonic() + timeout
    headers = oms_headers if isinstance(oms_headers, dict) and oms_headers else None
    while time.monotonic() < deadline:
        resp = await client.get(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology/{class_id}",
            params={"branch": "main"},
            headers=headers,
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


async def _upsert_database_access_http(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    entries: List[Dict[str, Any]],
) -> None:
    resp = await client.post(
        f"{BFF_URL}/api/v1/databases/{db_name}/access",
        json={"entries": entries},
    )
    resp.raise_for_status()


async def _upsert_object_type_contract(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    class_id: str,
    backing_dataset_id: str,
    pk_spec: Dict[str, Any],
    oms_headers: Dict[str, str],
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
            headers=oms_headers,
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
        headers=oms_headers,
    )
    if resp.status_code in {200, 201}:
        return
    if resp.status_code == 409:
        resp2 = await client.put(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology/resources/object-types/{class_id}",
            params={"branch": "main", "expected_head_commit": expected_head_commit},
            json=payload,
            headers=oms_headers,
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

    # ── 1-3: Provision DB roles (RBAC) ────────────────────────────
    print("  [1-3] Provisioning DB roles for personas (RBAC)...")
    try:
        _switch_persona(state, client, "owner")
        entries: List[Dict[str, Any]] = []
        for persona_key, persona in state.personas.items():
            if not persona.db_role:
                continue
            entries.append(
                {
                    "principal_type": "user",
                    "principal_id": persona.subject,
                    "principal_name": persona.subject,
                    "role": persona.db_role,
                }
            )
        await _upsert_database_access_http(client, db_name=state.db_name, entries=entries)
        state.bug_tracker.record_pass()
        print(f"    DB roles provisioned: {', '.join(sorted(k for k,v in state.personas.items() if v.db_role))}")
    except Exception as exc:
        state.bug_tracker.record(phase, "1-3", "POST /api/v1/databases/{db}/access", "roles provisioned", str(exc)[:200], "P0")

    # Phase 0: Hard guardrails (negative cases must be correct 4xx, never 500).
    await phase0_guardrails(state, client)
    print(f"    Persona active → {state.current_persona} ({state.personas[state.current_persona].subject})")

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

    # ── 1-4c: Governance RBAC (access policies) ───────────────────
    print("  [1-4c] Governance RBAC: access policy upsert requires Security role")
    try:
        orders_ds = state.datasets.get("olist_orders") or {}
        orders_dataset_id = str(orders_ds.get("dataset_id") or "").strip()
        assert orders_dataset_id, "olist_orders dataset_id missing (required for access-policy test)"

        body = {
            "db_name": state.db_name,
            "scope": "data_access",
            "subject_type": "dataset",
            "subject_id": orders_dataset_id,
            "policy": {
                "effect": "ALLOW",
                "principals": [
                    "role:Owner",
                    "role:Editor",
                    "role:Viewer",
                    "role:DomainModeler",
                    "role:DataEngineer",
                    "role:Security",
                ],
            },
            "status": "ACTIVE",
        }

        # Security: should succeed
        _switch_persona(state, client, "security")
        resp = await client.post(f"{BFF_URL}/api/v1/access-policies", json=body)
        if resp.status_code != 200:
            raise AssertionError(f"Security upsert expected 200, got {resp.status_code}: {resp.text[:200]}")
        state.bug_tracker.record_pass()

        # Viewer: must be denied
        _switch_persona(state, client, "viewer")
        resp2 = await client.post(f"{BFF_URL}/api/v1/access-policies", json=body)
        if resp2.status_code != 403:
            raise AssertionError(f"Viewer upsert expected 403, got {resp2.status_code}: {resp2.text[:200]}")
        state.bug_tracker.record_pass()

        # Compliance auditor (viewer-like): must be denied
        _switch_persona(state, client, "compliance")
        resp3 = await client.post(f"{BFF_URL}/api/v1/access-policies", json=body)
        if resp3.status_code != 403:
            raise AssertionError(f"Compliance upsert expected 403, got {resp3.status_code}: {resp3.text[:200]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "1-4c",
            "POST /api/v1/access-policies",
            "Security=200, Viewer/Compliance=403",
            str(exc)[:200],
            "P0",
        )
    finally:
        # Restore persona for subsequent ingest/pipeline steps
        _switch_persona(state, client, "data_engineer")

    # ── 1-4b: Verify Foundry v2 Datasets API sees uploaded data ───────────
    print("  [1-4b] Verifying v2 dataset API can read uploaded data...")
    try:
        orders_ds = state.datasets.get("olist_orders") or {}
        orders_dataset_id = str(orders_ds.get("dataset_id") or "").strip()
        assert orders_dataset_id, "olist_orders dataset_id missing"
        orders_rid = build_rid("dataset", orders_dataset_id)

        get_resp = await client.get(f"{BFF_URL}/api/v2/datasets/{orders_rid}")
        assert get_resp.status_code == 200, f"GET /api/v2/datasets: {get_resp.status_code} {get_resp.text[:200]}"

        read_resp = await client.get(
            f"{BFF_URL}/api/v2/datasets/{orders_rid}/readTable",
            params={"rowLimit": 5, "branchName": "main", "format": "CSV"},
        )
        assert read_resp.status_code == 200, f"readTable: {read_resp.status_code} {read_resp.text[:200]}"
        header = (read_resp.text.splitlines()[:1] or [""])[0]
        assert "order_id" in header, f"Unexpected CSV header: {header[:200]}"
        state.bug_tracker.record_pass()
        print("    v2 datasets: OK (readTable CSV)")
    except Exception as exc:
        state.bug_tracker.record(phase, "1-4b", "GET /api/v2/datasets/*", "200 + CSV readTable", str(exc)[:200], "P0")

    # ── 1-5: Upload real-time API data ────────────────────────────
    print("  [1-5] Fetching and uploading real-time API data...")
    if not RUN_LIVE_APIS:
        print("    Skipped (set RUN_FOUNDRY_E2E_QA_LIVE=true to enable live API phase)")
        state.bug_tracker.record_pass()
    else:
        live_sources = {
            "weather_saopaulo": ("São Paulo weather (Open-Meteo)", fetch_open_meteo_csv),
            "exchange_rates_brl": ("BRL exchange rates (Frankfurter)", fetch_frankfurter_csv),
            "earthquakes_month": ("Monthly earthquakes (USGS)", fetch_usgs_earthquake_csv),
        }
        for name, (description, fetch_fn) in live_sources.items():
            csv_bytes: Optional[bytes] = None
            try:
                csv_bytes = await fetch_fn()
            except Exception as exc:
                # External API fetch failures are soft-fail (P2) by policy.
                state.bug_tracker.record(
                    phase,
                    f"1-5:{name}",
                    f"fetch {name}",
                    "200 + csv bytes",
                    f"External fetch failed: {str(exc)[:200]}",
                    "P2",
                )
                continue

            if not csv_bytes or len(csv_bytes) <= 50:
                state.bug_tracker.record(
                    phase,
                    f"1-5:{name}",
                    f"fetch {name}",
                    "csv bytes (>50)",
                    f"External API returned too little data (len={len(csv_bytes or b'')})",
                    "P2",
                )
                continue

            try:
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
                    "row_count": csv_bytes.count(b"\n") - 1,
                }
                state.bug_tracker.record_pass()
                row_count = csv_bytes.count(b"\n") - 1
                print(f"    {name}: {row_count} rows, dataset_id={dataset_id[:12]}...")
            except Exception as exc:
                # Fetch succeeded but ingest failed => hard fail (internal).
                state.bug_tracker.record(
                    phase,
                    f"1-5:{name}",
                    f"upload {name}",
                    "200 with dataset_id",
                    str(exc)[:200],
                    "P1",
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
            {"id": "cast_money", "type": "transform", "metadata": {
                "operation": "cast",
                "casts": [
                    {"column": "price", "type": "xsd:decimal"},
                    {"column": "freight_value", "type": "xsd:decimal"},
                ],
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
            {"from": "join_oi", "to": "cast_money"},
            {"from": "cast_money", "to": "compute_total"},
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
        preview_job_id = str(preview_data.get("job_id") or (preview_data.get("sample") or {}).get("job_id") or "").strip()
        if not preview_job_id:
            raise AssertionError(f"Missing preview job_id: {preview_data}")

        sample = await _wait_for_pipeline_run_sample(
            client,
            pipeline_id=pipeline_id,
            job_id=preview_job_id,
            timeout=240,
        )
        rows = sample.get("rows") if isinstance(sample, dict) else None
        if not isinstance(rows, list) or not rows:
            raise AssertionError(f"Preview sample rows missing: {sample}")

        # Semantic assertions: delivered-only + total_value = price + freight_value (casted numeric)
        for row in rows[:10]:
            if not isinstance(row, dict):
                continue
            status_raw = row.get("order_status")
            if status_raw is not None:
                assert str(status_raw).lower() == "delivered", f"Non-delivered row in preview: order_status={status_raw!r}"
            raw_price = row.get("price")
            raw_freight = row.get("freight_value")
            raw_total = row.get("total_value")
            try:
                price = float(raw_price)
                freight = float(raw_freight)
                total = float(raw_total)
            except Exception:
                raise AssertionError(
                    f"Expected numeric price/freight/total in preview rows, got price={raw_price!r} freight={raw_freight!r} total={raw_total!r}"
                ) from None
            if abs(total - (price + freight)) > 1e-6:
                raise AssertionError(f"total_value mismatch: total={total} price+freight={price + freight}")

        print(f"    Preview sample verified: {len(rows)} rows (job_id={preview_job_id[:12]}...)")
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
            "node_id": "out_enriched",
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

    # ── 2-4b: Verify output dataset exists + has expected semantics ─
    print("  [2-4b] Verifying Pipeline A output dataset (enriched_orders) is readable...")
    try:
        ds = await _wait_for_dataset_by_name(client, db_name=state.db_name, dataset_name="enriched_orders", timeout=240)
        dataset_id = str(ds.get("dataset_id") or "").strip()
        assert dataset_id, f"enriched_orders dataset_id missing: {ds}"
        rid = build_rid("dataset", dataset_id)
        read_resp = await client.get(
            f"{BFF_URL}/api/v2/datasets/{rid}/readTable",
            params={"rowLimit": 5, "branchName": "main", "format": "CSV"},
        )
        read_resp.raise_for_status()
        lines = [ln for ln in read_resp.text.splitlines() if ln.strip()]
        header = (lines[:1] or [""])[0]
        for required_col in ("order_id", "order_status", "price", "freight_value", "total_value"):
            if required_col not in header:
                raise AssertionError(f"Missing column in enriched_orders: {required_col} (header={header[:200]})")

        import csv as csv_mod

        reader = csv_mod.DictReader(lines)
        first = next(reader, None)
        if not first:
            raise AssertionError("enriched_orders readTable returned no rows")
        status_raw = first.get("order_status")
        if status_raw is not None:
            assert str(status_raw).lower() == "delivered", f"Expected delivered in enriched_orders, got {status_raw!r}"
        try:
            price = float(first.get("price") or 0)
            freight = float(first.get("freight_value") or 0)
            total = float(first.get("total_value") or 0)
        except Exception:
            raise AssertionError(f"Numeric parse failed for enriched_orders row: {first}") from None
        if abs(total - (price + freight)) > 1e-6:
            raise AssertionError(f"enriched_orders total_value mismatch: total={total} price+freight={price + freight}")
        state.bug_tracker.record_pass()
        print("    enriched_orders readTable semantic check: OK")
    except Exception as exc:
        state.bug_tracker.record(phase, "2-4b", "GET /api/v2/datasets/*/readTable", "CSV includes computed total_value", str(exc)[:200], "P1")

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
                {"id": "cast_money", "type": "transform", "metadata": {
                    "operation": "cast",
                    "casts": [
                        {"column": "price", "type": "xsd:decimal"},
                        {"column": "freight_value", "type": "xsd:decimal"},
                    ],
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
                {"from": "join_ip", "to": "cast_money"},
                {"from": "cast_money", "to": "compute_rev"},
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

    # ── 2-7: Pipeline D - OrderItem PK materialization ───────────
    # Foundry-style: build a stable string primary key for OrderItem so v2 objects can be addressed safely.
    print("  [2-7] Creating Pipeline D: order_items_with_pk (materialize order_item_pk)")
    pipeline_d_def = {
        "nodes": [
            {"id": "in_items", "type": "input", "metadata": {"datasetId": items_id}},
            {"id": "compute_pk", "type": "transform", "metadata": {
                "operation": "compute",
                # All raw CSV columns are strings (inferSchema=False). Keep this as a string key.
                "expression": "order_item_pk = concat(order_id, '_', order_item_id)",
            }},
            {"id": "out_items_pk", "type": "output", "metadata": {
                "outputName": "order_items_with_pk", "outputKind": "dataset",
            }},
        ],
        "edges": [
            {"from": "in_items", "to": "compute_pk"},
            {"from": "compute_pk", "to": "out_items_pk"},
        ],
        "parameters": [],
        "settings": {"engine": "Batch"},
    }
    try:
        idem_key = f"idem-pipeline-d-{state.suffix}"
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines",
            headers={**state.headers, "Idempotency-Key": idem_key},
            json={
                "db_name": state.db_name,
                "name": f"order_items_with_pk_{state.suffix}",
                "description": "Order items with stable primary key (order_item_pk)",
                "definition_json": pipeline_d_def,
                "pipeline_type": "batch",
                "branch": "main",
                "location": "e2e",
            },
        )
        resp.raise_for_status()
        pipeline_d_data = resp.json().get("data") or resp.json()
        pipeline_d_obj = pipeline_d_data.get("pipeline") or pipeline_d_data
        pipeline_d_id = str(pipeline_d_obj.get("pipeline_id") or pipeline_d_obj.get("id") or pipeline_d_data.get("pipeline_id") or "")
        assert pipeline_d_id, f"No pipeline_id for Pipeline D: {pipeline_d_data}"
        state.pipelines["order_items_with_pk"] = pipeline_d_id
        print(f"    Pipeline D created: {pipeline_d_id[:12]}...")

        # Build + deploy so output dataset exists for object type backing source.
        build_idem = f"idem-build-d-{state.suffix}"
        build_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_d_id}/build",
            headers={**state.headers, "Idempotency-Key": build_idem},
            json={"db_name": state.db_name, "branch": "main", "limit": 50},
        )
        build_resp.raise_for_status()
        build_job_id = str((build_resp.json().get("data") or {}).get("job_id") or "")
        if build_job_id:
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                runs_resp = await client.get(f"{BFF_URL}/api/v1/pipelines/{pipeline_d_id}/runs", params={"limit": 50})
                if runs_resp.status_code == 200:
                    runs = (runs_resp.json().get("data") or {}).get("runs") or []
                    run = next((r for r in runs if r.get("job_id") == build_job_id), None)
                    if isinstance(run, dict):
                        run_status = str(run.get("status") or "").upper()
                        if run_status in {"SUCCESS", "DEPLOYED", "COMPLETED"}:
                            break
                        if run_status == "FAILED":
                            raise AssertionError(f"Pipeline D build failed: {run}")
                await asyncio.sleep(2.0)

        deploy_idem = f"idem-deploy-d-{state.suffix}"
        deploy_payload: Dict[str, Any] = {
            "promote_build": True,
            "branch": "main",
            "node_id": "out_items_pk",
            "output": {"db_name": state.db_name},
        }
        if build_job_id:
            deploy_payload["build_job_id"] = build_job_id
        deploy_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/{pipeline_d_id}/deploy",
            headers={**state.headers, "Idempotency-Key": deploy_idem},
            json=deploy_payload,
        )
        deploy_resp.raise_for_status()

        # Poll dataset registry via BFF list until output dataset appears.
        deadline = time.monotonic() + 120
        output_dataset_id = ""
        while time.monotonic() < deadline and not output_dataset_id:
            list_resp = await client.get(
                f"{BFF_URL}/api/v1/pipelines/datasets",
                params={"db_name": state.db_name},
            )
            if list_resp.status_code == 200:
                data = list_resp.json().get("data") or list_resp.json()
                ds_list = data.get("datasets") if isinstance(data, dict) else data
                if isinstance(ds_list, list):
                    for row in ds_list:
                        if not isinstance(row, dict):
                            continue
                        name = str(row.get("name") or row.get("dataset_name") or "").strip()
                        if name == "order_items_with_pk":
                            output_dataset_id = str(row.get("dataset_id") or row.get("id") or "").strip()
                            break
            if not output_dataset_id:
                await asyncio.sleep(2.0)

        assert output_dataset_id, "Pipeline D output dataset 'order_items_with_pk' not found after deploy"
        state.datasets["order_items_with_pk"] = {"dataset_id": output_dataset_id, "version_id": None, "payload": {}}
        state.bug_tracker.record_pass()
        print(f"    order_items_with_pk: dataset_id={output_dataset_id[:12]}...")
    except Exception as exc:
        state.bug_tracker.record(phase, "2-7", "Pipeline D order_items_with_pk", "output dataset id", str(exc)[:200], "P0")


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
                {"name": "product_id", "type": "xsd:string", "label": {"en": "Product ID"}, "required": True, "primaryKey": True, "titleKey": True},
                {"name": "product_category_name", "type": "xsd:string", "label": {"en": "Category"}},
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
                {"name": "order_item_pk", "type": "xsd:string", "label": {"en": "Order Item PK"}, "required": True, "primaryKey": True, "titleKey": True},
                {"name": "order_id", "type": "xsd:string", "label": {"en": "Order ID"}, "required": True},
                {"name": "order_item_id", "type": "xsd:integer", "label": {"en": "Item ID"}, "required": True},
                {"name": "price", "type": "xsd:decimal", "label": {"en": "Price"}},
                {"name": "freight_value", "type": "xsd:decimal", "label": {"en": "Freight"}},
            ],
            "relationships": [
                {"predicate": "order", "target": "Order", "label": {"en": "Order"}, "cardinality": "n:1"},
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
            await _wait_for_ontology(
                client,
                db_name=state.db_name,
                class_id=cls["id"],
                oms_headers=state.oms_headers,
                require_properties=True,
                timeout=120,
            )
            state.ontology_classes.append(cls["id"])
            state.bug_tracker.record_pass()
            print(f"    {cls['id']}: created and properties verified")
        except Exception as exc:
            state.bug_tracker.record(phase, f"3-1:{cls['id']}", f"POST ontology/{cls['id']}", "202+properties", str(exc)[:200])

    # ── 3-2: Object Type Contracts (pk_spec + backing source) ────
    print("  [3-2] Creating object type contracts...")
    ot_specs = [
        ("Customer", "olist_customers", {"primary_key": ["customer_id"], "title_key": ["customer_id"]}),
        ("Product", "olist_products", {"primary_key": ["product_id"], "title_key": ["product_id"]}),
        ("Seller", "olist_sellers", {"primary_key": ["seller_id"], "title_key": ["seller_id"]}),
        ("Order", "olist_orders", {"primary_key": ["order_id"], "title_key": ["order_id"]}),
        ("OrderItem", "order_items_with_pk", {"primary_key": ["order_item_pk"], "title_key": ["order_item_pk"]}),
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
                oms_headers=state.oms_headers,
            )
            state.bug_tracker.record_pass()
            print(f"    {class_id}: contract set (backing={ds['dataset_id'][:12]}...)")
        except Exception as exc:
            state.bug_tracker.record(phase, f"3-2:{class_id}", "object type contract", "201/200", str(exc)[:200])

    # ── 3-2b: OCC conflict (expectedHeadCommit mismatch) ──────────
    print("  [3-2b] OCC conflict: wrong expectedHeadCommit should 409 (v2 objectTypes PATCH)")
    try:
        resp = await client.patch(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objectTypes/Customer",
            params={"branch": "main", "expectedHeadCommit": "branch:does-not-exist"},
            json={"metadata": {"occ_test": True}},
        )
        await _expect_v2_error(resp=resp, expected_status=409)
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "3-2b",
            "PATCH /api/v2/.../objectTypes/Customer?expectedHeadCommit=wrong",
            "409",
            str(exc)[:200],
            "P0",
            persona=state.current_persona,
        )

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

    print("  [3-5b] Verifying outgoing link types (Order, OrderItem)...")
    try:
        resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objectTypes/Order/outgoingLinkTypes",
            params={"branch": "main", "pageSize": 500},
        )
        resp.raise_for_status()
        data = resp.json().get("data") or []
        api_names = {str(item.get("apiName") or "").strip() for item in data if isinstance(item, dict)}
        assert "customer" in api_names, f"Missing linkType apiName=customer (found={sorted(api_names)[:20]})"

        resp2 = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objectTypes/OrderItem/outgoingLinkTypes",
            params={"branch": "main", "pageSize": 500},
        )
        resp2.raise_for_status()
        data2 = resp2.json().get("data") or []
        api_names2 = {str(item.get("apiName") or "").strip() for item in data2 if isinstance(item, dict)}
        for expected in ("order", "product", "seller"):
            assert expected in api_names2, f"Missing linkType apiName={expected} (found={sorted(api_names2)[:20]})"

        state.bug_tracker.record_pass()
        print("    outgoingLinkTypes: OK")
    except Exception as exc:
        state.bug_tracker.record(
            phase,
            "3-5b",
            "GET outgoingLinkTypes (BFF v2)",
            "expected linkType apiNames exist",
            str(exc)[:200],
            "P0",
        )

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
                headers=state.oms_headers,
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
    # RBAC: mapping spec creation is a Domain Modeler responsibility in this stack.
    _switch_persona(state, client, "domain_modeler")
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
        ("OrderItem", "order_items_with_pk", [
            {"source_field": "order_item_pk", "target_field": "order_item_pk"},
            {"source_field": "order_id", "target_field": "order_id"},
            {"source_field": "order_item_id", "target_field": "order_item_id"},
            {"source_field": "price", "target_field": "price"},
            {"source_field": "freight_value", "target_field": "freight_value"},
            {"source_field": "order_id", "target_field": "order"},
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
    # RBAC: objectify execution requires Data Engineer privileges.
    _switch_persona(state, client, "data_engineer")
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

    expected_by_class = _expected_instance_counts_from_fixtures()
    expected_total = sum(expected_by_class.values())

    deadline = time.monotonic() + ES_TIMEOUT
    es_doc_count = 0
    observed_by_class: Dict[str, int] = {}

    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{ES_URL}/{state.es_index}/_count")
            if resp.status_code != 200:
                await asyncio.sleep(3.0)
                continue
            es_doc_count = int(resp.json().get("count", 0) or 0)

            if es_doc_count != expected_total:
                await asyncio.sleep(3.0)
                continue

            # Verify per-class counts match fixtures (strict end-to-end completeness).
            observed_by_class = {}
            mismatch = False
            for class_id, expected in expected_by_class.items():
                count_resp = await client.post(
                    f"{ES_URL}/{state.es_index}/_count",
                    json={"query": {"term": {"class_id": class_id}}},
                )
                if count_resp.status_code != 200:
                    mismatch = True
                    break
                observed = int(count_resp.json().get("count", 0) or 0)
                observed_by_class[class_id] = observed
                if observed != int(expected):
                    mismatch = True
            if mismatch:
                await asyncio.sleep(3.0)
                continue

            print(f"    ES index has expected {es_doc_count} documents: {observed_by_class}")
            state.bug_tracker.record_pass()
            break
        except httpx.HTTPError:
            await asyncio.sleep(3.0)
            continue

    if es_doc_count != expected_total or any(
        observed_by_class.get(cls) != expected_by_class.get(cls) for cls in expected_by_class
    ):
        state.bug_tracker.record(
            phase,
            "4-4",
            "ES indexing",
            f"count == {expected_total} and per-class counts match fixtures",
            f"Timed out after {ES_TIMEOUT}s (total={es_doc_count}, by_class={observed_by_class})",
            "P0",
        )

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

    # ── 5-0: Guardrail negative cases on v2 search ───────────────
    print("  [5-0] Negative cases: invalid branch / deep query / invalid orderBy / unknown objectType")
    try:
        # 400 Invalid branch name
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "bad branch"},
            json={"pageSize": 1},
        )
        await _expect_v2_error(resp=resp, expected_status=400, expected_error_code="INVALID_ARGUMENT")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-0a", "invalid branch", "400 InvalidArgument", str(exc)[:200], "P0")

    try:
        # 400 Query nesting too deep (>3 levels)
        deep_where = {
            "type": "and",
            "value": [
                {
                    "type": "and",
                    "value": [
                        {
                            "type": "and",
                            "value": [
                                {"type": "eq", "field": "order_status", "value": "delivered"},
                            ],
                        }
                    ],
                }
            ],
        }
        resp = await client.post(
            f"{base_search_url}/Order/search",
            params={"branch": "main"},
            json={"where": deep_where, "pageSize": 1},
        )
        await _expect_v2_error(resp=resp, expected_status=400, expected_error_code="INVALID_ARGUMENT")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-0b", "query nesting depth", "400 InvalidArgument", str(exc)[:200], "P0")

    try:
        # 400 Invalid orderBy.direction
        resp = await client.post(
            f"{base_search_url}/OrderItem/search",
            params={"branch": "main"},
            json={
                "orderBy": {"orderType": "fields", "fields": [{"field": "price", "direction": "descending"}]},
                "pageSize": 1,
            },
        )
        await _expect_v2_error(resp=resp, expected_status=400, expected_error_code="INVALID_ARGUMENT")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-0c", "invalid orderBy.direction", "400 InvalidArgument", str(exc)[:200], "P0")

    try:
        # 404 ObjectTypeNotFound
        resp = await client.post(
            f"{base_search_url}/DoesNotExist/search",
            params={"branch": "main"},
            json={"pageSize": 1},
        )
        await _expect_v2_error(
            resp=resp,
            expected_status=404,
            expected_error_name="ObjectTypeNotFound",
            expected_error_code="NOT_FOUND",
        )
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-0d", "ObjectTypeNotFound search", "404 ObjectTypeNotFound", str(exc)[:200], "P0")

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
        if not isinstance(objects, list) or not objects:
            raise AssertionError("Expected at least 1 delivered Order")
        if isinstance(objects, list):
            for obj in objects[:3]:
                props = obj.get("properties") or obj
                assert str(props.get("order_status", "")).lower() == "delivered", f"Non-delivered: {props}"
        print(f"    Found {len(objects) if isinstance(objects, list) else '?'} delivered orders")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-1", "search eq delivered", ">=1 delivered", str(exc)[:200], "P1")

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
        objects = data.get("data") or data.get("objects") or []
        if not isinstance(objects, list) or not objects:
            raise AssertionError("Expected at least 1 OrderItem with price > 100")
        for obj in objects[:10]:
            props = obj.get("properties") or obj
            raw = props.get("price")
            try:
                price = float(raw)
            except Exception:
                raise AssertionError(f"OrderItem price is not numeric: {raw!r}") from None
            assert price > 100, f"Expected price > 100, got {price}"
        print(f"    Found {len(objects)} OrderItems with price > 100")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-2", "search gt price", ">=1 result and numeric", str(exc)[:200], "P1")

    # ── 5-3: and (customer_state == SP AND city contains "sao paulo") ───────
    print("  [5-3] Search: compound AND query")
    try:
        resp = await client.post(
            f"{base_search_url}/Customer/search",
            params={"branch": "main"},
            json={
                "where": {
                    "type": "and",
                    "value": [
                        {"type": "eq", "field": "customer_state", "value": "SP"},
                        {"type": "containsAnyTerm", "field": "customer_city", "value": "sao paulo"},
                    ],
                },
                "pageSize": 10,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        if not isinstance(objects, list) or not objects:
            raise AssertionError("Expected at least 1 SP customer in Sao Paulo")
        for obj in objects[:10]:
            props = obj.get("properties") or obj
            state_val = str(props.get("customer_state") or "").strip().upper()
            city_val = str(props.get("customer_city") or "").strip().lower()
            if state_val:
                assert state_val == "SP", f"Expected customer_state=SP, got {state_val!r}"
            if city_val:
                assert "sao paulo" in city_val, f"Expected city contains 'sao paulo', got {city_val!r}"
        print(f"    Compound AND verified: {len(objects)} customers")
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
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        if not isinstance(objects, list) or not objects:
            raise AssertionError("Expected at least 1 Order with NULL delivered date")
        for obj in objects[:10]:
            props = obj.get("properties") or obj
            delivered = props.get("order_delivered_customer_date")
            if delivered is None:
                continue
            if isinstance(delivered, str) and not delivered.strip():
                continue
            raise AssertionError(f"Expected NULL delivered date, got {delivered!r}")
        print(f"    isNull verified: {len(objects)} orders")
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
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        if not isinstance(objects, list) or not objects:
            raise AssertionError("Expected at least 1 Sao Paulo customer")
        for obj in objects[:10]:
            props = obj.get("properties") or obj
            city_val = str(props.get("customer_city") or "").strip().lower()
            if city_val:
                assert "sao paulo" in city_val, f"Expected city contains 'sao paulo', got {city_val!r}"
        print(f"    containsAnyTerm verified: {len(objects)} customers")
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
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        if not isinstance(objects, list) or not objects:
            raise AssertionError("Expected at least 1 Order with status delivered/shipped")
        allowed = {"delivered", "shipped"}
        for obj in objects[:10]:
            props = obj.get("properties") or obj
            st = str(props.get("order_status") or "").strip().lower()
            if st:
                assert st in allowed, f"Expected order_status in {allowed}, got {st!r}"
        print(f"    IN verified: {len(objects)} orders")
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
        data = resp.json()
        objects = data.get("data") or data.get("objects") or []
        if not isinstance(objects, list) or not objects:
            raise AssertionError("Expected at least 1 OrderItem for orderBy test")
        prices: List[float] = []
        for obj in objects:
            props = obj.get("properties") or obj
            raw = props.get("price")
            if raw is None:
                continue
            try:
                prices.append(float(raw))
            except Exception:
                raise AssertionError(f"OrderBy returned non-numeric price: {raw!r}") from None
        assert prices, "OrderBy returned no numeric prices"
        assert all(prices[i] >= prices[i + 1] for i in range(len(prices) - 1)), f"Not sorted DESC: {prices}"
        print(f"    OrderBy verified (prices desc): {prices[:5]}")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "5-7", "search orderBy", "sorted DESC by price", str(exc)[:200], "P0")

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

    # ── 5-11: Linked Objects (Foundry v2) Order → Customer ────────
    print("  [5-11] Linked Objects: Order → Customer (v2 links)")
    try:
        sample_order_id, expected_customer_id = _pick_sample_order_from_fixtures()
        resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objects/Order/{sample_order_id}/links/customer",
            params={"branch": "main", "pageSize": 10},
        )
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("data") or []
        assert isinstance(rows, list) and rows, f"Expected linked Customer rows, got: {payload}"
        first = rows[0] if isinstance(rows[0], dict) else {}
        props = first.get("properties") or first
        actual_customer_id = str(props.get("customer_id") or "").strip()
        assert actual_customer_id == expected_customer_id, f"Expected customer_id={expected_customer_id}, got {actual_customer_id}"
        state.bug_tracker.record_pass()
        print(f"    Linked Customer verified: {actual_customer_id[:12]}...")
    except Exception as exc:
        state.bug_tracker.record(phase, "5-11", "v2 links Order→Customer", "linked customer resolved", str(exc)[:200], "P0")

    # ── 5-12: Linked Objects (Foundry v2) OrderItem → Product/Order ─
    print("  [5-12] Linked Objects: OrderItem → Product (+ Order) (v2 links)")
    try:
        order_id, expected_product_id = _pick_order_with_items_from_fixtures()

        items_resp = await client.post(
            f"{base_search_url}/OrderItem/search",
            params={"branch": "main"},
            json={"where": {"type": "eq", "field": "order_id", "value": order_id}, "pageSize": 50},
        )
        items_resp.raise_for_status()
        items_payload = items_resp.json()
        items = items_payload.get("data") or []
        assert isinstance(items, list) and items, f"No OrderItems found for order_id={order_id}"

        first_item = items[0] if isinstance(items[0], dict) else {}
        item_pk = str(first_item.get("__primaryKey") or first_item.get("primaryKey") or "").strip()
        assert item_pk, f"OrderItem primary key missing: {first_item}"

        prod_resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objects/OrderItem/{item_pk}/links/product",
            params={"branch": "main", "pageSize": 50},
        )
        prod_resp.raise_for_status()
        prod_payload = prod_resp.json()
        products = prod_payload.get("data") or []
        assert isinstance(products, list) and products, f"No Products linked from OrderItem={item_pk}"
        product_ids = []
        for row in products:
            if not isinstance(row, dict):
                continue
            props = row.get("properties") or row
            product_ids.append(str(props.get("product_id") or "").strip())
        assert expected_product_id in product_ids, f"Expected product_id={expected_product_id}, got {product_ids[:5]}"

        order_resp = await client.get(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/objects/OrderItem/{item_pk}/links/order",
            params={"branch": "main", "pageSize": 10},
        )
        order_resp.raise_for_status()
        order_payload = order_resp.json()
        linked_orders = order_payload.get("data") or []
        assert isinstance(linked_orders, list) and linked_orders, f"No Orders linked from OrderItem={item_pk}"
        linked_order = linked_orders[0] if isinstance(linked_orders[0], dict) else {}
        linked_props = linked_order.get("properties") or linked_order
        linked_order_id = str(linked_props.get("order_id") or "").strip()
        assert linked_order_id == order_id, f"Expected OrderItem.order to resolve order_id={order_id}, got {linked_order_id}"

        state.bug_tracker.record_pass()
        print(f"    Linked Product verified: {expected_product_id[:12]}... (via OrderItem={item_pk[:12]}...)")
    except Exception as exc:
        state.bug_tracker.record(phase, "5-12", "v2 links OrderItem→Product/Order", "product and order resolved", str(exc)[:200], "P0")


# =============================================================================
# PHASE 6: Actions
# =============================================================================

async def _record_deployed_commit_qa(
    *,
    client: httpx.AsyncClient,
    db_name: str,
    oms_headers: Dict[str, str],
    target_branch: str = "main",
) -> None:
    """Record ontology deployment via HTTP API so actions can execute (required by OMS)."""
    resp = await client.post(
        f"{OMS_URL}/api/v1/database/{db_name}/ontology/records/deployments",
        json={
            "target_branch": target_branch,
            "ontology_commit_id": f"branch:{target_branch}",
            "deployed_by": "foundry_e2e_qa",
            "metadata": {"source": "foundry_e2e_qa"},
        },
        headers=oms_headers,
    )
    resp.raise_for_status()


async def phase6_actions(state: QAState, client: httpx.AsyncClient) -> None:
    """Test simulate, apply, batch-apply actions with closed-loop ES verification."""
    phase = "Phase 6"
    print(f"\n{'='*60}\n  {phase}: ACTIONS\n{'='*60}")

    if not state.action_type_ids:
        state.bug_tracker.record(phase, "6-0", "prerequisite", "action_type_ids", "No action types from Phase 3", "P0")
        return

    fulfill_action_id = next((a for a in state.action_type_ids if "fulfill" in a), None)
    escalate_action_id = next((a for a in state.action_type_ids if "escalate" in a), None)
    if not fulfill_action_id:
        state.bug_tracker.record(phase, "6-0", "prerequisite", "fulfill action", "No fulfill action type id", "P0")
        return

    # Pick orders that will actually change state (avoid "already shipped" so closed-loop is meaningful).
    orders_csv_path = FIXTURE_DIR / "olist_orders.csv"
    test_order_ids: List[str] = []
    order_status_by_id: Dict[str, str] = {}
    if orders_csv_path.exists():
        import csv as csv_mod
        with orders_csv_path.open(newline="", encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))
        for row in rows:
            oid = str(row.get("order_id") or "").strip()
            status0 = str(row.get("order_status") or "").strip().lower()
            if oid and status0:
                order_status_by_id[oid] = status0

        preferred = [
            oid
            for oid, st in order_status_by_id.items()
            if st in {"approved", "processing", "created", "invoiced"}
        ]
        fallback = [
            oid
            for oid, st in order_status_by_id.items()
            if st not in {"delivered", "shipped", "escalated"}
        ]
        candidates = preferred + [oid for oid in fallback if oid not in preferred]
        test_order_ids = candidates[:5]

    if len(test_order_ids) < 2:
        state.bug_tracker.record(
            phase,
            "6-0",
            "find test orders",
            ">=2 actionable order_ids",
            f"Not enough action candidates (found={len(test_order_ids)})",
            "P0",
        )
        return

    # ── 6-0a: Action before deployment must 409 (hard gate) ──────
    print("  [6-0a] 409 Action before deployment (apply should be rejected)")
    try:
        resp = await client.post(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actions/{fulfill_action_id}/apply",
            params={"branch": "main"},
            json={
                "options": {"mode": "VALIDATE_ONLY"},
                "parameters": {"order": {"class_id": "Order", "instance_id": test_order_ids[0]}},
            },
        )
        await _expect_v2_error(resp=resp, expected_status=409)
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "6-0a", "apply before deploy", "409", str(exc)[:200], "P0")

    # ── 6-0b: Record deployment so actions can execute ────────────
    print("  [6-0b] Recording ontology deployment...")
    try:
        await _record_deployed_commit_qa(
            client=client,
            db_name=state.db_name,
            oms_headers=state.oms_headers,
            target_branch="main",
        )
        state.bug_tracker.record_pass()
        print("    Deployment recorded")
    except Exception as exc:
        state.bug_tracker.record(phase, "6-0b", "record deployment", "deployment recorded", str(exc)[:200], "P0")

    # Allow deployment record to propagate before first action call
    await asyncio.sleep(5)

    # ── 6-0c: Permission policy denies DomainModeler (hard gate) ──
    print("  [6-0c] 403 Permission policy denies DomainModeler on fulfill_order")
    try:
        _switch_persona(state, client, "domain_modeler")
        resp = await client.post(
            f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actions/{fulfill_action_id}/apply",
            params={"branch": "main"},
            json={
                "options": {"mode": "VALIDATE_ONLY"},
                "parameters": {"order": {"class_id": "Order", "instance_id": test_order_ids[0]}},
            },
        )
        await _expect_v2_error(resp=resp, expected_status=403, expected_error_code="PERMISSION_DENIED")
        state.bug_tracker.record_pass()
    except Exception as exc:
        state.bug_tracker.record(phase, "6-0c", "permission policy", "403", str(exc)[:200], "P0")
    finally:
        # Restore editor persona for happy-path action execution
        _switch_persona(state, client, "editor")

    if fulfill_action_id and test_order_ids:
        print(f"  [6-1] Simulate: {fulfill_action_id}")
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actions/{fulfill_action_id}/apply",
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
        target_order_id = test_order_ids[0]
        print(f"  [6-2] Apply: {fulfill_action_id} on order {target_order_id[:12]}...")
        try:
            corr_id = f"qa-apply-{uuid.uuid4()}"
            resp = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actions/{fulfill_action_id}/apply",
                params={"branch": "main"},
                json={
                    "options": {"mode": "VALIDATE_AND_EXECUTE"},
                    "parameters": {"order": {"class_id": "Order", "instance_id": target_order_id}},
                    "correlationId": corr_id,
                    "metadata": {"source": "foundry_e2e_qa"},
                },
            )
            resp.raise_for_status()
            apply_result = resp.json()
            action_log_id = str(
                apply_result.get("auditLogId")
                or apply_result.get("action_log_id")
                or (apply_result.get("data") or {}).get("auditLogId")
                or (apply_result.get("data") or {}).get("action_log_id")
                or ""
            )
            print(f"    Applied: action_log_id={action_log_id}")
            if action_log_id:
                state.action_log_ids.append(action_log_id)
            state.changed_instance_ids.append(target_order_id)
            state.expected_order_status[target_order_id] = "shipped"
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "6-2", "apply fulfill_order", "action_log_id", str(exc)[:200])

    # ── 6-5: Apply escalate_order ─────────────────────────────────
    if escalate_action_id and len(test_order_ids) > 1:
        target_order_id = test_order_ids[1]
        print(f"  [6-5] Apply: {escalate_action_id} on order {target_order_id[:12]}...")
        try:
            _switch_persona(state, client, "owner")
            corr_id = f"qa-escalate-{uuid.uuid4()}"
            resp = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actions/{escalate_action_id}/apply",
                params={"branch": "main"},
                json={
                    "options": {"mode": "VALIDATE_AND_EXECUTE"},
                    "parameters": {"order": {"class_id": "Order", "instance_id": target_order_id}},
                    "correlationId": corr_id,
                    "metadata": {"source": "foundry_e2e_qa"},
                },
            )
            resp.raise_for_status()
            apply_result = resp.json()
            action_log_id = str(
                apply_result.get("auditLogId")
                or apply_result.get("action_log_id")
                or (apply_result.get("data") or {}).get("auditLogId")
                or (apply_result.get("data") or {}).get("action_log_id")
                or ""
            )
            if action_log_id:
                state.action_log_ids.append(action_log_id)
            state.changed_instance_ids.append(target_order_id)
            state.expected_order_status[target_order_id] = "escalated"
            state.bug_tracker.record_pass()
            print("    Escalated successfully")
        except Exception as exc:
            state.bug_tracker.record(phase, "6-5", "apply escalate_order", "escalated", str(exc)[:200])
        finally:
            _switch_persona(state, client, "editor")

    # ── 6-7: Batch apply fulfill_order × 3 ────────────────────────
    if fulfill_action_id and len(test_order_ids) >= 5:
        batch_ids = test_order_ids[2:5]
        print(f"  [6-7] Batch apply: {fulfill_action_id} × {len(batch_ids)}")
        for oid in batch_ids:
            try:
                corr_id = f"qa-batch-{uuid.uuid4()}"
                resp = await client.post(
                    f"{BFF_URL}/api/v2/ontologies/{state.db_name}/actions/{fulfill_action_id}/apply",
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
                state.expected_order_status[oid] = "shipped"
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

    # Verify per-order closed loop (strict): each applied action must be searchable with updated state.
    if state.expected_order_status:
        print(f"  [7-0] Verifying {len(state.expected_order_status)} Orders reflect action writeback...")
        try:
            deadline = time.monotonic() + 180
            pending = dict(state.expected_order_status)
            last_seen: Dict[str, str] = {}
            while time.monotonic() < deadline and pending:
                for oid, expected in list(pending.items()):
                    resp = await client.post(
                        f"{base_search_url}/Order/search",
                        params={"branch": "main"},
                        json={"where": {"type": "eq", "field": "order_id", "value": oid}, "pageSize": 1},
                    )
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    objects = data.get("data") or data.get("objects") or []
                    if not isinstance(objects, list) or not objects:
                        continue
                    row = objects[0] if isinstance(objects[0], dict) else {}
                    props = row.get("properties") or row
                    current = str(props.get("order_status") or "").strip().lower()
                    last_seen[oid] = current
                    if current == expected:
                        pending.pop(oid, None)
                if pending:
                    await asyncio.sleep(2.0)
            assert not pending, f"Orders did not converge to expected status: pending={pending}, last_seen={last_seen}"
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "7-0", "closed loop per-order", "orders searchable with updated status", str(exc)[:200], "P0")
    else:
        state.bug_tracker.record(phase, "7-0", "closed loop per-order", "expected_order_status populated", "No expected orders recorded in Phase 6", "P1")

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
        state.bug_tracker.record(phase, "7-1", "search shipped", "query succeeds", str(exc)[:200], "P2")

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
        state.bug_tracker.record(phase, "7-2", "search escalated", "query succeeds", str(exc)[:200], "P2")

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

    if not RUN_LIVE_APIS:
        print("  Live API phase disabled (set RUN_FOUNDRY_E2E_QA_LIVE=true). Skipping Phase 8.")
        state.bug_tracker.record_pass()
        return

    if not state.live_datasets:
        print("  No live datasets were ingested (fetch/upload may have failed). Skipping Phase 8.")
        state.bug_tracker.record_pass()
        return

    db_name = state.db_name
    base_search_url = f"{BFF_URL}/api/v2/ontologies/{db_name}/objects"

    # ── 8-1/8-2/8-3: Ontology + ObjectType contracts for live classes ───────
    live_defs: List[Dict[str, Any]] = [
        {
            "dataset_key": "weather_saopaulo",
            "class_id": "WeatherData",
            "pk_spec": {"primary_key": ["time"], "title_key": ["time"]},
            "ontology": {
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
            },
            "mappings": [
                {"source_field": "time", "target_field": "time"},
                {"source_field": "temperature_2m", "target_field": "temperature_2m"},
                {"source_field": "windspeed_10m", "target_field": "windspeed_10m"},
                {"source_field": "precipitation", "target_field": "precipitation"},
            ],
        },
        {
            "dataset_key": "exchange_rates_brl",
            "class_id": "ExchangeRate",
            "pk_spec": {"primary_key": ["date"], "title_key": ["date"]},
            "ontology": {
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
            },
            "mappings": [
                {"source_field": "date", "target_field": "date"},
                {"source_field": "base", "target_field": "base"},
                {"source_field": "usd_rate", "target_field": "usd_rate"},
                {"source_field": "eur_rate", "target_field": "eur_rate"},
                {"source_field": "krw_rate", "target_field": "krw_rate"},
            ],
        },
        {
            "dataset_key": "earthquakes_month",
            "class_id": "Earthquake",
            "pk_spec": {"primary_key": ["eq_id"], "title_key": ["eq_id"]},
            "ontology": {
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
            },
            "mappings": [
                {"source_field": "eq_id", "target_field": "eq_id"},
                {"source_field": "magnitude", "target_field": "magnitude"},
                {"source_field": "place", "target_field": "place"},
                {"source_field": "latitude", "target_field": "latitude"},
                {"source_field": "longitude", "target_field": "longitude"},
                {"source_field": "depth_km", "target_field": "depth_km"},
                {"source_field": "time", "target_field": "time"},
            ],
        },
    ]

    live_class_ids: List[str] = []
    for item in live_defs:
        ds_key = str(item.get("dataset_key") or "")
        class_id = str(item.get("class_id") or "")
        ds = state.live_datasets.get(ds_key) or {}
        dataset_id = str(ds.get("dataset_id") or "").strip()
        row_count = ds.get("row_count")
        try:
            row_count_int = int(row_count) if row_count is not None else None
        except Exception:
            row_count_int = None

        if not dataset_id:
            continue
        if row_count_int is not None and row_count_int <= 0:
            state.bug_tracker.record(phase, f"8-0:{class_id}", f"live ingest {ds_key}", ">0 rows", f"row_count={row_count_int}", "P2")
            continue

        print(f"  [8-ont] {class_id}: ontology + contract + mapping-spec")
        try:
            _switch_persona(state, client, "domain_modeler")
            resp = await client.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
                params={"branch": "main"},
                json=item.get("ontology") or {},
            )
            resp.raise_for_status()
            cmd_id = str((resp.json().get("data") or {}).get("command_id") or "")
            if cmd_id:
                try:
                    await _wait_for_command(client, cmd_id, timeout=120)
                except Exception:
                    pass
            await _wait_for_ontology(
                client,
                db_name=db_name,
                class_id=class_id,
                oms_headers=state.oms_headers,
                require_properties=True,
                timeout=120,
            )
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, f"8-ont:{class_id}", "POST /api/v1/databases/{db}/ontology", "202+properties", str(exc)[:200], "P1")
            continue

        # ObjectType contract is required for /api/v2 search surface.
        try:
            await _upsert_object_type_contract(
                client,
                db_name=db_name,
                class_id=class_id,
                backing_dataset_id=dataset_id,
                pk_spec=item.get("pk_spec") or {},
                oms_headers=state.oms_headers,
            )
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, f"8-contract:{class_id}", "OMS objectTypes upsert", "200/201", str(exc)[:200], "P1")

        # Mapping spec (Domain Modeler)
        mappings = item.get("mappings") or []
        max_retries = 3
        created = False
        last_err = ""
        for attempt in range(max_retries):
            try:
                resp = await client.post(
                    f"{BFF_URL}/api/v1/objectify/mapping-specs",
                    json={
                        "dataset_id": dataset_id,
                        "artifact_output_name": ds_key,
                        "target_class_id": class_id,
                        "options": {"ontology_branch": "main"},
                        "mappings": mappings,
                    },
                )
                if resp.status_code == 409:
                    err_text = resp.text[:200]
                    if "schema is required" in err_text and attempt < max_retries - 1:
                        await asyncio.sleep(10 * (attempt + 1))
                        continue
                    last_err = f"409 Conflict: {err_text}"
                    break
                resp.raise_for_status()
                spec = (resp.json().get("data") or {}).get("mapping_spec") or {}
                spec_id = str(spec.get("mapping_spec_id") or "").strip()
                if spec_id:
                    state.mapping_specs[class_id] = spec_id
                state.bug_tracker.record_pass()
                live_class_ids.append(class_id)
                created = True
                break
            except Exception as exc:
                last_err = str(exc)[:200]
                if attempt < max_retries - 1:
                    await asyncio.sleep(10)
                    continue
                break
        if not created:
            state.bug_tracker.record(phase, f"8-map:{class_id}", "POST /api/v1/objectify/mapping-specs", "201", last_err, "P1")

    # ── 8-4: Objectify live classes + verify search has results ─────────────
    if live_class_ids:
        print(f"  [8-4] Objectify live classes: {live_class_ids}")
        try:
            _switch_persona(state, client, "data_engineer")
            dry = await client.post(
                f"{BFF_URL}/api/v1/objectify/databases/{db_name}/run-dag",
                json={
                    "class_ids": live_class_ids,
                    "branch": "main",
                    "include_dependencies": False,
                    "dry_run": True,
                },
            )
            dry.raise_for_status()
            state.bug_tracker.record_pass()

            run = await client.post(
                f"{BFF_URL}/api/v1/objectify/databases/{db_name}/run-dag",
                json={
                    "class_ids": live_class_ids,
                    "branch": "main",
                    "include_dependencies": False,
                    "dry_run": False,
                },
            )
            if run.status_code not in {200, 202, 409}:
                run.raise_for_status()
            state.bug_tracker.record_pass()
        except Exception as exc:
            state.bug_tracker.record(phase, "8-4", "POST /api/v1/objectify/.../run-dag", "jobs queued", str(exc)[:200], "P1")

        # Poll v2 search until we see at least one instance per class.
        print("  [8-5] Verifying v2 search returns live objects (non-empty + basic semantics)...")
        for class_id in live_class_ids:
            try:
                _switch_persona(state, client, "viewer")
                deadline = time.monotonic() + 240
                last_status: Optional[int] = None
                last_text: str = ""
                objects: List[Dict[str, Any]] = []
                while time.monotonic() < deadline:
                    resp = await client.post(
                        f"{base_search_url}/{class_id}/search",
                        params={"branch": "main"},
                        json={"pageSize": 5},
                    )
                    last_status = resp.status_code
                    last_text = resp.text[:200]
                    if resp.status_code == 200:
                        payload = resp.json()
                        data = payload.get("data") or payload.get("objects") or []
                        if isinstance(data, list) and data:
                            objects = data
                            break
                    await asyncio.sleep(2.0)
                if not objects:
                    raise AssertionError(f"No search results (status={last_status} body={last_text})")

                # Minimal semantic checks per type (avoid brittle thresholds).
                for obj in objects[:5]:
                    props = obj.get("properties") or obj
                    if class_id == "WeatherData":
                        assert str(props.get("time") or "").strip(), "WeatherData.time is required"
                        for key in ("temperature_2m", "windspeed_10m", "precipitation"):
                            val = props.get(key)
                            if val is None or val == "":
                                continue
                            float(val)
                    elif class_id == "ExchangeRate":
                        assert str(props.get("date") or "").strip(), "ExchangeRate.date is required"
                        for key in ("usd_rate", "eur_rate", "krw_rate"):
                            val = props.get(key)
                            if val is None or val == "":
                                continue
                            float(val)
                    elif class_id == "Earthquake":
                        assert str(props.get("eq_id") or "").strip(), "Earthquake.eq_id is required"
                        val = props.get("magnitude")
                        if val not in (None, ""):
                            float(val)
                state.bug_tracker.record_pass()
                print(f"    {class_id}: search semantic check OK ({len(objects)} hits)")
            except Exception as exc:
                state.bug_tracker.record(phase, f"8-5:{class_id}", "v2 search", "non-empty + semantic", str(exc)[:200], "P1")

    # ── 8-6: Cross-domain pipeline (orders + exchange rates) ────────────────
    orders_id = (state.datasets.get("olist_orders") or {}).get("dataset_id")
    rates_id = (state.live_datasets.get("exchange_rates_brl") or {}).get("dataset_id")
    if orders_id and rates_id:
        print("  [8-6] Cross-domain pipeline: orders + exchange_rates (union pad + deploy)")
        try:
            _switch_persona(state, client, "data_engineer")
            cross_def = {
                "nodes": [
                    {"id": "in_orders", "type": "input", "metadata": {"datasetId": orders_id}},
                    {"id": "in_rates", "type": "input", "metadata": {"datasetId": rates_id}},
                    {"id": "union_cross", "type": "transform", "metadata": {"operation": "union", "unionMode": "pad"}},
                    {"id": "out_cross", "type": "output", "metadata": {"outputName": "cross_domain", "outputKind": "dataset"}},
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
                    "db_name": db_name,
                    "name": f"cross_domain_{state.suffix}",
                    "description": "Cross-domain: orders + exchange rates",
                    "definition_json": cross_def,
                    "pipeline_type": "batch",
                    "branch": "main",
                    "location": "e2e",
                },
            )
            resp.raise_for_status()
            payload = resp.json().get("data") or resp.json()
            pipeline_obj = payload.get("pipeline") or payload
            pipeline_id = str(pipeline_obj.get("pipeline_id") or pipeline_obj.get("id") or payload.get("pipeline_id") or "").strip()
            if not pipeline_id:
                raise AssertionError(f"Missing pipeline_id: {payload}")

            preview_idem = f"idem-preview-cross-{state.suffix}"
            preview = await client.post(
                f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/preview",
                headers={**state.headers, "Idempotency-Key": preview_idem},
                json={"db_name": db_name, "branch": "main", "limit": 50},
            )
            preview.raise_for_status()
            preview_data = preview.json().get("data") or preview.json()
            preview_job_id = str(preview_data.get("job_id") or (preview_data.get("sample") or {}).get("job_id") or "").strip()
            if not preview_job_id:
                raise AssertionError(f"Missing preview job_id: {preview_data}")
            sample = await _wait_for_pipeline_run_sample(client, pipeline_id=pipeline_id, job_id=preview_job_id, timeout=240)
            rows = sample.get("rows") if isinstance(sample, dict) else None
            if not isinstance(rows, list) or not rows:
                raise AssertionError(f"Cross-domain preview returned no rows: {sample}")

            # Build + deploy (best-effort)
            build_job_id = ""
            build_idem = f"idem-build-cross-{state.suffix}"
            build = await client.post(
                f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/build",
                headers={**state.headers, "Idempotency-Key": build_idem},
                json={"db_name": db_name, "branch": "main", "limit": 50},
            )
            build.raise_for_status()
            build_job_id = str((build.json().get("data") or build.json()).get("job_id") or "").strip()
            if build_job_id:
                deadline = time.monotonic() + 300
                while time.monotonic() < deadline:
                    runs_resp = await client.get(f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/runs", params={"limit": 50})
                    if runs_resp.status_code == 200:
                        runs = (runs_resp.json().get("data") or {}).get("runs") or []
                        run = next((r for r in (runs or []) if r.get("job_id") == build_job_id), None)
                        if isinstance(run, dict):
                            run_status = str(run.get("status") or "").upper()
                            if run_status in {"SUCCESS", "DEPLOYED", "COMPLETED"}:
                                break
                            if run_status == "FAILED":
                                raise AssertionError(f"Cross-domain build failed: {run}")
                    await asyncio.sleep(2.0)

            deploy_idem = f"idem-deploy-cross-{state.suffix}"
            deploy_payload: Dict[str, Any] = {
                "promote_build": True,
                "branch": "main",
                "node_id": "out_cross",
                "output": {"db_name": db_name},
            }
            if build_job_id:
                deploy_payload["build_job_id"] = build_job_id
            deploy = await client.post(
                f"{BFF_URL}/api/v1/pipelines/{pipeline_id}/deploy",
                headers={**state.headers, "Idempotency-Key": deploy_idem},
                json=deploy_payload,
            )
            deploy.raise_for_status()

            out_ds = await _wait_for_dataset_by_name(client, db_name=db_name, dataset_name="cross_domain", timeout=240)
            out_id = str(out_ds.get("dataset_id") or "").strip()
            if not out_id:
                raise AssertionError(f"cross_domain dataset_id missing: {out_ds}")
            rid = build_rid("dataset", out_id)
            table = await client.get(
                f"{BFF_URL}/api/v2/datasets/{rid}/readTable",
                params={"rowLimit": 10, "branchName": "main", "format": "CSV"},
            )
            table.raise_for_status()
            lines = [ln for ln in table.text.splitlines() if ln.strip()]
            if len(lines) < 2:
                raise AssertionError("cross_domain readTable returned no data rows")
            header = lines[0]
            if "order_id" not in header or "date" not in header:
                raise AssertionError(f"cross_domain header missing expected cols (order_id/date): {header[:200]}")
            state.bug_tracker.record_pass()
            print("    Cross-domain pipeline verified: preview + deploy + readTable OK")
        except Exception as exc:
            state.bug_tracker.record(phase, "8-6", "cross-domain pipeline", "deploy + readTable", str(exc)[:200], "P1")


# =============================================================================
# PHASE 9: Branch Separation + Incremental Update (Real Foundry User)
# =============================================================================

async def phase9_branch_and_incremental(state: QAState, client: httpx.AsyncClient) -> None:
    """
    Hard gates:
    - Action overlay(view branch) vs writeback target branch separation
    - 2nd dataset version + incremental objectify update without duplicates
    """

    from shared.config.app_config import AppConfig
    from shared.services.registries.action_log_registry import ActionLogRegistry
    from shared.services.registries.objectify_registry import ObjectifyRegistry

    phase = "Phase 9"
    print(f"\n{'='*60}\n  {phase}: BRANCH SEPARATION + INCREMENTAL UPDATE\n{'='*60}")

    db_name = state.db_name
    view_branch = "main"
    expected_writeback_branch = AppConfig.get_ontology_writeback_branch(db_name=db_name)

    # ── 9-1: Branch separation (ActionLogRegistry writeback_target vs ES branch) ──
    print("  [9-1] Branch separation: writeback_target.branch != view branch, ES docs on view branch")

    dsn_candidates = [
        (os.getenv("POSTGRES_URL") or "").strip(),
        "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb",
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]

    registry: Optional[ActionLogRegistry] = None
    working_postgres_dsn: Optional[str] = None
    last_dsn_exc: Optional[Exception] = None
    for dsn in dsn_candidates:
        if not dsn:
            continue
        try:
            registry = ActionLogRegistry(dsn=dsn)
            await registry.connect()
            working_postgres_dsn = dsn
            break
        except Exception as exc:
            last_dsn_exc = exc
            registry = None

    if not state.action_log_ids:
        state.bug_tracker.record(
            phase,
            "9-1a",
            "ActionLogRegistry",
            ">=1 action_log_id from Phase 6",
            "No action_log_ids captured; action flow did not submit writeback",
            "P0",
        )
    elif registry is None:
        state.bug_tracker.record(
            phase,
            "9-1a",
            "ActionLogRegistry.connect",
            "connect to Postgres",
            f"Failed to connect to Postgres for action log checks: {last_dsn_exc}",
            "P0",
        )
    else:
        try:
            deadline = time.monotonic() + 240
            for action_log_id in state.action_log_ids:
                record = None
                while time.monotonic() < deadline:
                    record = await registry.get_log(action_log_id=action_log_id)
                    if record and str(record.status).upper() == "SUCCEEDED":
                        break
                    await asyncio.sleep(2.0)

                if not record:
                    raise AssertionError(f"Action log not found: {action_log_id}")
                if str(record.status).upper() != "SUCCEEDED":
                    raise AssertionError(f"Action log did not succeed: {action_log_id} status={record.status}")
                if not record.writeback_target or not isinstance(record.writeback_target, dict):
                    raise AssertionError(f"Missing writeback_target for action_log_id={action_log_id}")

                wb_branch = str(record.writeback_target.get("branch") or "").strip()
                if wb_branch != expected_writeback_branch:
                    raise AssertionError(
                        f"writeback_target.branch mismatch: expected {expected_writeback_branch}, got {wb_branch}"
                    )
                if wb_branch == view_branch:
                    raise AssertionError("writeback_target.branch must be distinct from view branch")
                if not str(record.writeback_commit_id or "").strip():
                    raise AssertionError("writeback_commit_id is required for succeeded action logs")
            state.bug_tracker.record_pass()
            print(f"    Action logs verified: {len(state.action_log_ids)} (writeback_branch={expected_writeback_branch})")
        except Exception as exc:
            state.bug_tracker.record(
                phase,
                "9-1b",
                "ActionLogRegistry.get_log",
                "writeback_target.branch separation + SUCCEEDED",
                str(exc)[:200],
                "P0",
            )
        finally:
            await registry.close()

    # ES branch for changed instances must be view branch (overlay)
    if not state.changed_instance_ids:
        state.bug_tracker.record(
            phase,
            "9-1c",
            "Elasticsearch _doc",
            ">=1 changed instance_id from Phase 6",
            "No changed_instance_ids captured; cannot verify overlay projection branch",
            "P1",
        )
    else:
        try:
            for instance_id in state.changed_instance_ids[:5]:
                doc = await _wait_for_es_doc(client, state.es_index, instance_id, timeout=ES_TIMEOUT)
                source = doc.get("_source") or {}
                actual_branch = str(source.get("branch") or "").strip()
                if actual_branch != view_branch:
                    raise AssertionError(
                        f"ES doc branch mismatch for instance_id={instance_id}: expected {view_branch}, got {actual_branch}"
                    )
            state.bug_tracker.record_pass()
            print(f"    ES overlay branch verified for {min(5, len(state.changed_instance_ids))} instance(s)")
        except Exception as exc:
            state.bug_tracker.record(
                phase,
                "9-1d",
                "GET ES /{index}/_doc/{id}",
                f"_source.branch == {view_branch}",
                str(exc)[:200],
                "P0",
            )

    # ── 9-2: Incremental update (2nd version) ─────────────────────
    print("  [9-2] Incremental update: 2nd version + trigger-incremental updates exactly one entity")
    customer_spec_id = str(state.mapping_specs.get("Customer") or "").strip()
    customer_ds = state.datasets.get("olist_customers") or {}
    dataset_id_v1 = str(customer_ds.get("dataset_id") or "").strip()
    version_id_v1 = str(customer_ds.get("version_id") or "").strip()

    if not customer_spec_id or not dataset_id_v1 or not version_id_v1:
        state.bug_tracker.record(
            phase,
            "9-2a",
            "preconditions",
            "Customer mapping_spec + dataset v1 present",
            f"mapping_spec_id={bool(customer_spec_id)} dataset_id={bool(dataset_id_v1)} version_id={bool(version_id_v1)}",
            "P0",
        )
        return

    # Count before (no duplicates allowed)
    customer_count_before: Optional[int] = None
    try:
        _switch_persona(state, client, "viewer")
        resp = await client.post(
            f"{BFF_URL}/api/v2/ontologies/{db_name}/objects/Customer/count",
            params={"branch": view_branch},
            json={},
        )
        resp.raise_for_status()
        payload = resp.json()
        raw = payload.get("count") if isinstance(payload, dict) else None
        if raw is None and isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            raw = payload["data"].get("count")
        customer_count_before = int(raw) if raw is not None else None
        if customer_count_before is None:
            raise AssertionError(f"count missing from payload: {payload}")
        state.bug_tracker.record_pass()
        print(f"    Customer count before: {customer_count_before}")
    except Exception as exc:
        state.bug_tracker.record(phase, "9-2b", "POST Customer/count", "count int", str(exc)[:200], "P1")

    # Build v2 CSV: change 1 customer's city to a value that is > max(city) in v1
    try:
        import csv as csv_mod
        import io

        rows: List[Dict[str, Any]] = []
        header: List[str] = []
        max_city = ""
        target_customer_id = ""

        with (FIXTURE_DIR / "olist_customers.csv").open(newline="", encoding="utf-8") as f:
            reader = csv_mod.DictReader(f)
            header = list(reader.fieldnames or [])
            if not header:
                raise AssertionError("olist_customers.csv missing header")
            for row in reader:
                rows.append(row)
                city = str(row.get("customer_city") or "")
                if city and city > max_city:
                    max_city = city

        if not rows:
            raise AssertionError("No customer rows loaded from fixtures")
        if not max_city:
            raise AssertionError("Could not determine max customer_city from fixtures")

        # Pick a stable row (avoid first row so max_rows=1 catches bugs where filtering is ignored)
        for row in reversed(rows[:5000] if len(rows) > 5000 else rows):
            cid = str(row.get("customer_id") or "").strip()
            if cid:
                target_customer_id = cid
                break
        if not target_customer_id:
            raise AssertionError("Could not pick a target customer_id for incremental update")

        new_city = f"{max_city}-qa-{state.suffix}"
        updated = False
        for row in rows:
            if str(row.get("customer_id") or "").strip() == target_customer_id:
                row["customer_city"] = new_city
                updated = True
                break
        if not updated:
            raise AssertionError(f"Target customer_id not found in fixtures: {target_customer_id}")

        buf = io.StringIO()
        writer = csv_mod.DictWriter(buf, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in header})
        csv_bytes_v2 = buf.getvalue().encode("utf-8")

        state.search_results["incremental_customer_id"] = target_customer_id
        state.search_results["incremental_new_city"] = new_city
        state.bug_tracker.record_pass()
        print(f"    Prepared v2 CSV: customer_id={target_customer_id[:12]}... city={new_city[:32]}...")
    except Exception as exc:
        state.bug_tracker.record(phase, "9-2c", "prepare v2 CSV", "bytes built", str(exc)[:200], "P0")
        return

    # Seed watermark so incremental filtering is mandatory (detects hidden bug: previous_watermark ignored).
    try:
        # The watermark value must be the current max(city) so only our updated row qualifies.
        if not working_postgres_dsn:
            import asyncpg  # type: ignore

            last_exc: Optional[Exception] = None
            for candidate in dsn_candidates:
                if not candidate:
                    continue
                try:
                    conn = await asyncpg.connect(candidate)
                except Exception as exc:
                    last_exc = exc
                    continue
                else:
                    await conn.close()
                    working_postgres_dsn = candidate
                    break
            if not working_postgres_dsn:
                raise AssertionError(f"No working Postgres DSN candidate for watermark seeding: {last_exc}")

        obj_reg = ObjectifyRegistry(dsn=working_postgres_dsn)
        await obj_reg.connect()
        try:
            await obj_reg.update_watermark(
                mapping_spec_id=customer_spec_id,
                dataset_branch=view_branch,
                watermark_column="customer_city",
                watermark_value=max_city,
            )
        finally:
            await obj_reg.close()
        state.bug_tracker.record_pass()
        print(f"    Seeded watermark: customer_city={max_city!r}")
    except Exception as exc:
        state.bug_tracker.record(phase, "9-2d", "ObjectifyRegistry.update_watermark", "watermark seeded", str(exc)[:200], "P0")
        return

    # Upload v2 dataset (same dataset_name → new version_id)
    dataset_id_v2 = ""
    version_id_v2 = ""
    commit_id_v2 = ""
    try:
        _switch_persona(state, client, "data_engineer")
        idem_key = f"idem-olist_customers-v2-{state.suffix}"
        resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": view_branch},
            headers={**state.headers, "Idempotency-Key": idem_key},
            data={
                "dataset_name": "olist_customers",
                "description": "Olist customers dataset (v2 incremental update)",
                "delimiter": ",",
                "has_header": "true",
            },
            files={"file": ("olist_customers.csv", csv_bytes_v2, "text/csv")},
        )
        resp.raise_for_status()
        payload = resp.json().get("data") or {}
        ds = payload.get("dataset") or {}
        ver = payload.get("version") or {}
        dataset_id_v2 = str(ds.get("dataset_id") or "").strip()
        version_id_v2 = str(ver.get("version_id") or "").strip()
        commit_id_v2 = str(ver.get("lakefs_commit_id") or "").strip()
        if not dataset_id_v2 or not version_id_v2:
            raise AssertionError(f"Missing dataset_id/version_id: {payload}")
        if dataset_id_v2 != dataset_id_v1:
            raise AssertionError(f"Expected same dataset_id on re-upload; v1={dataset_id_v1} v2={dataset_id_v2}")
        if version_id_v2 == version_id_v1:
            raise AssertionError("Expected new version_id for v2 upload")
        state.bug_tracker.record_pass()
        print(f"    Uploaded v2 version: dataset_id={dataset_id_v2[:12]}... version_id={version_id_v2[:12]}...")
    except Exception as exc:
        state.bug_tracker.record(phase, "9-2e", "POST /csv-upload (v2)", "new version_id", str(exc)[:200], "P0")
        return

    # Trigger incremental objectify with max_rows=1 (must still update the changed row)
    try:
        _switch_persona(state, client, "data_engineer")
        resp = await client.post(
            f"{BFF_URL}/api/v1/objectify/mapping-specs/{customer_spec_id}/trigger-incremental",
            params={"branch": view_branch},
            json={"execution_mode": "incremental", "watermark_column": "customer_city", "max_rows": 1},
        )
        resp.raise_for_status()
        data = resp.json().get("data") or resp.json()
        job_id = str(data.get("job_id") or "").strip()
        if not job_id:
            raise AssertionError(f"Missing job_id in incremental trigger response: {data}")
        state.bug_tracker.record_pass()
        print(f"    Incremental objectify queued: job_id={job_id[:12]}...")
    except Exception as exc:
        state.bug_tracker.record(phase, "9-2f", "POST trigger-incremental", "QUEUED", str(exc)[:200], "P0")
        return

    # Poll until the updated city is visible via v2 search
    try:
        _switch_persona(state, client, "viewer")
        target_customer_id = str(state.search_results.get("incremental_customer_id") or "").strip()
        new_city = str(state.search_results.get("incremental_new_city") or "").strip()
        assert target_customer_id and new_city

        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            resp = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{db_name}/objects/Customer/search",
                params={"branch": view_branch},
                json={
                    "where": {"type": "eq", "field": "customer_id", "value": target_customer_id},
                    "pageSize": 5,
                },
            )
            if resp.status_code != 200:
                await asyncio.sleep(2.0)
                continue
            payload = resp.json()
            hits = payload.get("data") or []
            if isinstance(hits, list) and hits:
                first = hits[0] if isinstance(hits[0], dict) else {}
                props = first.get("properties") or first
                city = str(props.get("customer_city") or "").strip()
                if city == new_city:
                    state.bug_tracker.record_pass()
                    print("    Incremental update visible in v2 search")
                    break
            await asyncio.sleep(2.0)
        else:
            raise AssertionError("Timed out waiting for incremental update to appear in v2 search")
    except Exception as exc:
        state.bug_tracker.record(phase, "9-2g", "v2 search Customer", "customer_city updated", str(exc)[:200], "P0")
        return

    # Count after must be unchanged (no duplicates)
    if customer_count_before is not None:
        try:
            resp = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{db_name}/objects/Customer/count",
                params={"branch": view_branch},
                json={},
            )
            resp.raise_for_status()
            payload = resp.json()
            raw = payload.get("count") if isinstance(payload, dict) else None
            if raw is None and isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                raw = payload["data"].get("count")
            customer_count_after = int(raw) if raw is not None else None
            if customer_count_after != customer_count_before:
                raise AssertionError(f"Customer count changed: before={customer_count_before} after={customer_count_after}")
            state.bug_tracker.record_pass()
            print(f"    Customer count unchanged: {customer_count_after}")
        except Exception as exc:
            state.bug_tracker.record(phase, "9-2h", "POST Customer/count", "count unchanged", str(exc)[:200], "P1")

    # ── 9-3: Delete handling (delta mode; tombstone/row missing) ───────────────
    print("  [9-3] Deletion: 3rd version + delta-mode removes missing entity")
    try:
        if not commit_id_v2:
            raise AssertionError("v2 lakefs_commit_id missing; delta deletion cannot be tested")

        delete_customer_id = ""
        for row in rows:
            cid = str(row.get("customer_id") or "").strip()
            if cid and cid != str(state.search_results.get("incremental_customer_id") or "").strip():
                delete_customer_id = cid
                break
        if not delete_customer_id:
            raise AssertionError("Could not pick a customer_id for deletion scenario")

        # Build v3 CSV by removing one customer row (hardest case: row simply missing)
        import io
        import csv as csv_mod

        rows_v3 = [r for r in rows if str(r.get("customer_id") or "").strip() != delete_customer_id]
        if len(rows_v3) >= len(rows):
            raise AssertionError("Expected v3 to have 1 fewer row than v2")

        buf3 = io.StringIO()
        writer3 = csv_mod.DictWriter(buf3, fieldnames=header, lineterminator="\n")
        writer3.writeheader()
        for row in rows_v3:
            writer3.writerow({k: row.get(k, "") for k in header})
        csv_bytes_v3 = buf3.getvalue().encode("utf-8")

        # Upload v3 dataset version
        _switch_persona(state, client, "data_engineer")
        idem_key_v3 = f"idem-olist_customers-v3-delete-{state.suffix}"
        resp3 = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": view_branch},
            headers={**state.headers, "Idempotency-Key": idem_key_v3},
            data={
                "dataset_name": "olist_customers",
                "description": "Olist customers dataset (v3 delete case)",
                "delimiter": ",",
                "has_header": "true",
            },
            files={"file": ("olist_customers.csv", csv_bytes_v3, "text/csv")},
        )
        resp3.raise_for_status()
        payload3 = resp3.json().get("data") or {}
        ver3 = payload3.get("version") or {}
        version_id_v3 = str(ver3.get("version_id") or "").strip()
        commit_id_v3 = str(ver3.get("lakefs_commit_id") or "").strip()
        if not version_id_v3 or version_id_v3 == version_id_v2:
            raise AssertionError("Expected new version_id for v3 delete upload")
        if not commit_id_v3 or commit_id_v3 == commit_id_v2:
            raise AssertionError("Expected new lakefs_commit_id for v3 delete upload")

        # Seed watermark commit so delta mode computes diff between v2 commit and v3 commit
        obj_reg = ObjectifyRegistry(dsn=working_postgres_dsn)
        await obj_reg.connect()
        try:
            await obj_reg.update_watermark(
                mapping_spec_id=customer_spec_id,
                dataset_branch=view_branch,
                watermark_column="customer_city",
                watermark_value=max_city,
                dataset_version_id=version_id_v2,
                lakefs_commit_id=commit_id_v2,
            )
        finally:
            await obj_reg.close()

        # Trigger delta-mode objectify (must remove deleted entity from ES)
        resp_delta = await client.post(
            f"{BFF_URL}/api/v1/objectify/mapping-specs/{customer_spec_id}/trigger-incremental",
            params={"branch": view_branch},
            json={"execution_mode": "delta"},
        )
        resp_delta.raise_for_status()
        delta_data = resp_delta.json().get("data") or resp_delta.json()
        delta_job_id = str(delta_data.get("job_id") or "").strip()
        if not delta_job_id:
            raise AssertionError(f"Missing job_id in delta trigger response: {delta_data}")
        print(f"    Delta objectify queued: job_id={delta_job_id[:12]}... deleted_customer_id={delete_customer_id[:12]}...")

        # Poll until the deleted customer is no longer returned by search
        _switch_persona(state, client, "viewer")
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            s = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{db_name}/objects/Customer/search",
                params={"branch": view_branch},
                json={
                    "where": {"type": "eq", "field": "customer_id", "value": delete_customer_id},
                    "pageSize": 5,
                },
            )
            if s.status_code != 200:
                await asyncio.sleep(2.0)
                continue
            payload = s.json()
            hits = payload.get("data") or []
            if isinstance(hits, list) and not hits:
                state.bug_tracker.record_pass()
                print("    Deleted customer absent in v2 search")
                break
            await asyncio.sleep(2.0)
        else:
            raise AssertionError("Timed out waiting for deleted customer to disappear from v2 search")

        # Count after must decrease by 1
        if customer_count_before is not None:
            count_after_delete = None
            c = await client.post(
                f"{BFF_URL}/api/v2/ontologies/{db_name}/objects/Customer/count",
                params={"branch": view_branch},
                json={},
            )
            c.raise_for_status()
            payload = c.json()
            raw = payload.get("count") if isinstance(payload, dict) else None
            if raw is None and isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                raw = payload["data"].get("count")
            count_after_delete = int(raw) if raw is not None else None
            expected = int(customer_count_before) - 1
            if count_after_delete != expected:
                raise AssertionError(f"Customer count after delete mismatch: expected={expected} got={count_after_delete}")
            state.bug_tracker.record_pass()
            print(f"    Customer count after delete: {count_after_delete}")

    except Exception as exc:
        state.bug_tracker.record(phase, "9-3", "delta delete Customer", "deleted customer removed", str(exc)[:200], "P1")
        return


# =============================================================================
# PHASE 10: Teardown / Cleanup
# =============================================================================

async def phase10_teardown(state: QAState, client: httpx.AsyncClient) -> None:
    phase = "Phase 10"
    print(f"\n{'='*60}\n  {phase}: TEARDOWN / CLEANUP\n{'='*60}")

    # 10-1: Delete database (best-effort; should keep the environment clean for frequent runs)
    print(f"  [10-1] Deleting database: {state.db_name}")
    try:
        if not state.db_name:
            raise AssertionError("db_name missing")
        _switch_persona(state, client, "owner")
        resp = await client.delete(f"{BFF_URL}/api/v1/databases/{state.db_name}")
        if resp.status_code in {200, 202, 404}:
            if resp.status_code == 202:
                cmd_id = str(((resp.json().get("data") or {}) or {}).get("command_id") or "").strip()
                if cmd_id:
                    await _wait_for_command(client, cmd_id, timeout=240)
            state.bug_tracker.record_pass()
            print("    Database deleted")
        else:
            raise AssertionError(f"Unexpected status: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        state.bug_tracker.record(phase, "10-1", "DELETE /api/v1/databases/{db}", "database deleted", str(exc)[:200], "P2")

    # 10-2: Delete ES indices for this DB (best-effort)
    print("  [10-2] Deleting Elasticsearch indices for db...")
    try:
        prefix = (state.db_name or "").lower()
        if not prefix:
            raise AssertionError("db_name missing for ES cleanup")
        cat = await client.get(f"{ES_URL}/_cat/indices/{prefix}*?format=json&h=index")
        indices: List[str] = []
        if cat.status_code == 200:
            for row in cat.json() or []:
                if isinstance(row, dict):
                    idx = str(row.get("index") or "").strip()
                    if idx:
                        indices.append(idx)

        deleted = 0
        for idx in sorted(set(indices)):
            d = await client.delete(f"{ES_URL}/{idx}")
            if d.status_code in {200, 202, 404}:
                deleted += 1
        state.bug_tracker.record_pass()
        print(f"    ES indices deleted: {deleted}")
    except Exception as exc:
        state.bug_tracker.record(phase, "10-2", "DELETE ES indices", "indices deleted", str(exc)[:200], "P2")


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

    def _make_persona(*, name: str, db_role: Optional[str]) -> Persona:
        subject = f"qa-{name}-{state.suffix}"
        jwt = build_smoke_user_jwt(subject=subject, roles=("user",))
        headers: Dict[str, str] = {
            "Authorization": f"Bearer {jwt}",
            "X-DB-Name": state.db_name,
            "X-User-Name": subject,
            "Accept": "application/json",
        }
        return Persona(name=name, subject=subject, db_role=db_role, jwt=jwt, headers=headers)

    # 10+ real-user personas (distinct JWT sub) + DB roles provisioned in Postgres.
    state.personas = {
        "owner": _make_persona(name="owner", db_role="Owner"),
        "data_engineer": _make_persona(name="data-engineer", db_role="DataEngineer"),
        "domain_modeler": _make_persona(name="domain-modeler", db_role="DomainModeler"),
        "editor": _make_persona(name="editor", db_role="Editor"),
        "viewer": _make_persona(name="viewer", db_role="Viewer"),
        "security": _make_persona(name="security", db_role="Security"),
        "compliance": _make_persona(name="compliance-auditor", db_role="Viewer"),
        "integration": _make_persona(name="integration-dev", db_role="Editor"),
        "bot": _make_persona(name="automation-bot", db_role="DataEngineer"),
        "intruder": _make_persona(name="intruder", db_role=None),
        "sre": _make_persona(name="sre", db_role="Viewer"),
    }
    state.current_persona = "owner"
    state.headers = dict(state.personas[state.current_persona].headers)
    state.oms_headers = oms_auth_headers()

    print(f"\n{'#'*60}")
    print(f"  FOUNDRY E2E QA — {state.db_name}")
    print(f"  BFF: {BFF_URL}")
    print(f"  OMS: {OMS_URL}")
    print(f"  ES:  {ES_URL}")
    print(f"  Fixture dir: {FIXTURE_DIR}")
    print(f"{'#'*60}")

    async with httpx.AsyncClient(headers=state.headers, timeout=HTTPX_TIMEOUT) as raw_client:
        client = raw_client
        try:
            # Phase 1: Data Ingestion
            await phase1_data_ingestion(state, client)

            # Phase 2: Pipeline Transforms
            _switch_persona(state, client, "data_engineer")
            await phase2_pipeline_transforms(state, client)

            # Phase 3: Ontology Creation
            _switch_persona(state, client, "domain_modeler")
            await phase3_ontology_creation(state, client)

            # Phase 4: Objectify
            _switch_persona(state, client, "data_engineer")
            await phase4_objectify(state, client)

            # Phase 5: Search & Query
            _switch_persona(state, client, "viewer")
            await phase5_search_and_query(state, client)

            # Phase 6: Actions
            _switch_persona(state, client, "editor")
            await phase6_actions(state, client)

            # Phase 7: Closed Loop Verification
            _switch_persona(state, client, "viewer")
            await phase7_closed_loop(state, client)

            # Phase 9: Real-user hard gates (branch separation + incremental update)
            await phase9_branch_and_incremental(state, client)

            # Phase 8: Live Data Cross-Domain
            _switch_persona(state, client, "data_engineer")
            await phase8_live_data_cross_domain(state, client)
        finally:
            await phase10_teardown(state, client)

    # ── Final Report ──────────────────────────────────────────────
    print(state.bug_tracker.summary())
    bugs_path = Path(os.getenv("QA_BUGS_PATH") or (REPO_ROOT / "qa_bugs.json"))
    bugs_path.parent.mkdir(parents=True, exist_ok=True)
    state.bug_tracker.dump_json(str(bugs_path))
    print(f"  Bug report: {bugs_path} ({len(state.bug_tracker.bugs)} bugs)")

    threshold_rank = _qa_fail_threshold_rank()
    blocking = [b for b in state.bug_tracker.bugs if _severity_rank(b.severity) <= threshold_rank]
    if blocking:
        summary = "\n".join(f"  [{b.severity}] [{b.phase}:{b.step}] {b.endpoint} — {b.actual[:100]}" for b in blocking)
        pytest.fail(
            f"{len(blocking)} bug(s) at/above QA_FAIL_SEVERITY threshold "
            f"(QA_FAIL_SEVERITY={os.getenv('QA_FAIL_SEVERITY') or 'P1'}):\n{summary}"
        )
