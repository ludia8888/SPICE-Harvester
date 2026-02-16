from __future__ import annotations

import asyncio
import io
import os
import time
import uuid
from typing import Any, Optional

import httpx
import pytest

from shared.config.search_config import get_instances_index_name
from shared.security.database_access import ensure_database_access_table
from tests.utils.auth import bff_auth_headers


BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")
_ES_PORT = os.getenv("ELASTICSEARCH_PORT") or os.getenv("ELASTICSEARCH_PORT_HOST") or "9200"
ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{_ES_PORT}",
).rstrip("/")

HTTPX_TIMEOUT = float(os.getenv("WORKFLOW_HTTP_TIMEOUT", "180") or 180)
ES_TIMEOUT_SECONDS = int(os.getenv("WORKFLOW_ES_TIMEOUT_SECONDS", "240") or 240)


def _postgres_url_candidates() -> list[str]:
    env_url = (os.getenv("POSTGRES_URL") or "").strip()
    if env_url:
        return [env_url]
    return [
        "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb",
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]


async def _grant_db_role(
    *,
    db_name: str,
    principal_id: str,
    role: str = "Owner",
    principal_type: str = "user",
) -> None:
    import asyncpg

    last_error: Optional[Exception] = None
    for dsn in _postgres_url_candidates():
        try:
            conn = await asyncpg.connect(dsn)
        except Exception as exc:
            last_error = exc
            continue
        try:
            await ensure_database_access_table(conn)
            await conn.execute(
                """
                INSERT INTO database_access (
                    db_name, principal_type, principal_id, principal_name, role, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (db_name, principal_type, principal_id)
                DO UPDATE SET
                    principal_name = EXCLUDED.principal_name,
                    role = EXCLUDED.role,
                    updated_at = NOW()
                """,
                db_name,
                principal_type,
                principal_id,
                principal_id,
                role,
            )
            return
        except Exception as exc:
            last_error = exc
        finally:
            await conn.close()
    if last_error:
        raise RuntimeError("Failed to grant database role for test user") from last_error


async def _wait_for_command(
    client: httpx.AsyncClient,
    command_id: str,
    *,
    timeout_seconds: int = 120,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_payload: Optional[dict[str, Any]] = None
    while time.monotonic() < deadline:
        resp = await client.get(f"{BFF_URL}/api/v1/commands/{command_id}/status")
        if resp.status_code == 200:
            last_payload = resp.json()
            status_value = str(
                last_payload.get("status") or (last_payload.get("data") or {}).get("status") or ""
            ).upper()
            if status_value in {"COMPLETED", "SUCCESS", "SUCCEEDED", "DONE"}:
                return
            if status_value in {"FAILED", "ERROR"}:
                raise AssertionError(f"Command failed: {last_payload}")
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for command (command_id={command_id}, last={last_payload})")


async def _create_db_with_retry(client: httpx.AsyncClient, *, db_name: str) -> None:
    resp = await client.post(
        f"{BFF_URL}/api/v1/databases",
        json={"name": db_name, "description": "financial investigation workflow e2e"},
    )
    if resp.status_code == 409:
        return
    resp.raise_for_status()
    command_id = str(((resp.json().get("data") or {}) or {}).get("command_id") or "")
    if command_id:
        await _wait_for_command(client, command_id, timeout_seconds=180)


async def _wait_for_ontology(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    class_id: str,
    branch: str = "main",
    timeout_seconds: int = 90,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status: Optional[int] = None
    while time.monotonic() < deadline:
        resp = await client.get(
            f"{OMS_URL}/api/v1/database/{db_name}/ontology/{class_id}",
            params={"branch": branch},
        )
        last_status = resp.status_code
        if resp.status_code == 200:
            return
        await asyncio.sleep(1.0)
    raise AssertionError(f"Timed out waiting for ontology class (class_id={class_id}, status={last_status})")


async def _wait_for_es_doc(
    client: httpx.AsyncClient,
    *,
    index_name: str,
    doc_id: str,
    timeout_seconds: int = ES_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_status: Optional[int] = None
    while time.monotonic() < deadline:
        try:
            resp = await client.get(f"{ELASTICSEARCH_URL}/{index_name}/_doc/{doc_id}")
        except httpx.HTTPError:
            await asyncio.sleep(1.0)
            continue
        last_status = resp.status_code
        if resp.status_code == 200:
            payload = resp.json()
            if payload.get("found") is True or payload.get("_source"):
                return payload
        await asyncio.sleep(1.0)
    raise AssertionError(f"Timed out waiting for ES doc (index={index_name}, doc_id={doc_id}, status={last_status})")


async def _upsert_object_type_contract(
    client: httpx.AsyncClient,
    *,
    db_name: str,
    class_id: str,
    branch: str,
    contract_payload: dict[str, Any],
) -> None:
    expected_head_commit = f"branch:{branch}"
    create_resp = await client.post(
        f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types",
        params={"branch": branch, "expected_head_commit": expected_head_commit},
        json={"class_id": class_id, **contract_payload},
    )
    if create_resp.status_code in {200, 201}:
        return
    if create_resp.status_code == 409:
        update_url = f"{BFF_URL}/api/v1/databases/{db_name}/ontology/object-types/{class_id}"
        update_resp = await client.put(
            update_url,
            params={"branch": branch, "expected_head_commit": expected_head_commit},
            json=dict(contract_payload),
        )
        if update_resp.status_code == 200:
            return
        first_error = update_resp.text

        bootstrap_payload = dict(contract_payload)
        bootstrap_payload["status"] = "INACTIVE"
        bootstrap_payload["migration"] = {
            "approved": True,
            "reset_edits": True,
            "note": "bootstrap_object_type_contract",
        }
        bootstrap_resp = await client.put(
            update_url,
            params={"branch": branch, "expected_head_commit": expected_head_commit},
            json=bootstrap_payload,
        )
        if bootstrap_resp.status_code != 200:
            raise AssertionError(
                f"object-type bootstrap update failed: {bootstrap_resp.status_code} "
                f"{bootstrap_resp.text} (first_update_error={first_error})"
            )

        activate_resp = await client.put(
            update_url,
            params={"branch": branch, "expected_head_commit": expected_head_commit},
            json={"status": str(contract_payload.get('status') or 'ACTIVE').upper()},
        )
        if activate_resp.status_code == 200:
            return
        raise AssertionError(
            f"object-type activate update failed: {activate_resp.status_code} {activate_resp.text}"
        )
    raise AssertionError(f"object-type create failed: {create_resp.status_code} {create_resp.text}")


def _extract_funnel_types(payload: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in (payload.get("columns") or []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("column_name") or item.get("name") or "").strip()
        if not name:
            continue
        inferred = item.get("inferred_type")
        if isinstance(inferred, dict):
            t = inferred.get("type")
        else:
            t = item.get("type")
        if t is None and isinstance(item.get("data_type"), str):
            t = item.get("data_type")
        out[name] = str(t or "").strip()
    return out


def _build_transactions_xlsx_bytes() -> bytes:
    try:
        from openpyxl import Workbook  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("openpyxl is required for this workflow test") from exc

    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(["transaction_id", "date", "from_account", "to_account", "amount", "memo"])
    ws.append(["tx_001", "2026-01-05", "110-123-4567", "220-111-2222", 5000000, "seed"])
    ws.append(["tx_002", "2026-01-10", "110-123-4567", "330-333-4444", 3000000, "repay"])
    ws.append(["tx_003", "2026-01-15", "220-111-2222", "440-555-6666", 2000000, "invest"])
    ws.append(["tx_004", "2026-01-20", "330-333-4444", "550-777-8888", 1500000, ""])
    ws.append(["tx_005", "2026-01-25", "440-555-6666", "110-123-4567", 1000000, "return"])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


@pytest.mark.integration
@pytest.mark.workflow
@pytest.mark.requires_infra
@pytest.mark.asyncio
async def test_financial_investigation_workflow_e2e() -> None:
    suffix = uuid.uuid4().hex[:10]
    db_name = f"e2e_fin_{suffix}"
    user_id = f"fin-e2e-{suffix}"

    headers = bff_auth_headers()
    headers.update(
        {
            "X-DB-Name": db_name,
            "X-User-ID": user_id,
            "X-User-Type": "user",
        }
    )

    async with httpx.AsyncClient(headers=headers, timeout=HTTPX_TIMEOUT) as client:
        # Phase 0: DB + permissions
        await _create_db_with_retry(client, db_name=db_name)
        await _grant_db_role(db_name=db_name, principal_id=user_id, principal_type="user", role="Owner")

        # Phase 1: Ingest datasets (Excel + CSVs)
        xlsx_bytes = _build_transactions_xlsx_bytes()
        tx_excel_idem = f"idem-tx-excel-{suffix}"
        tx_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/excel-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**headers, "Idempotency-Key": tx_excel_idem},
            data={"dataset_name": "kb_bank_transactions_2026q1", "description": "bank tx"},
            files={
                "file": (
                    "KB_bank_transactions_2026Q1.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        tx_resp.raise_for_status()
        tx_payload = tx_resp.json().get("data") or {}
        tx_excel_dataset_id = str((tx_payload.get("dataset") or {}).get("dataset_id") or "")
        tx_excel_version_id = str((tx_payload.get("version") or {}).get("version_id") or "")
        assert tx_excel_dataset_id and tx_excel_version_id

        tx_types = _extract_funnel_types((tx_payload.get("funnel_analysis") or {}))
        assert tx_types.get("amount") in {"xsd:integer", "xsd:decimal"}, f"unexpected amount type: {tx_types}"
        assert tx_types.get("date") in {"xsd:date", "xsd:dateTime"}, f"unexpected date type: {tx_types}"

        # NOTE: ObjectifyWorker currently consumes CSV/JSON-part artifacts, not raw Excel artifacts.
        # We still verify Excel upload + Funnel analysis above, but objectify runs on an equivalent CSV dataset.
        transactions_csv = "\n".join(
            [
                "transaction_id,date,from_account,to_account,amount,memo",
                "tx_001,2026-01-05,110-123-4567,220-111-2222,5000000,seed",
                "tx_002,2026-01-10,110-123-4567,330-333-4444,3000000,repay",
                "tx_003,2026-01-15,220-111-2222,440-555-6666,2000000,invest",
                "tx_004,2026-01-20,330-333-4444,550-777-8888,1500000,",
                "tx_005,2026-01-25,440-555-6666,110-123-4567,1000000,return",
            ]
        ).encode("utf-8")

        tx_csv_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**headers, "Idempotency-Key": f"idem-tx-csv-{suffix}"},
            data={
                "dataset_name": "kb_bank_transactions_2026q1_csv",
                "description": "bank tx (csv canonical)",
                "delimiter": ",",
                "has_header": "true",
            },
            files={"file": ("kb_bank_transactions_2026q1.csv", transactions_csv, "text/csv")},
        )
        tx_csv_resp.raise_for_status()
        tx_csv_payload = tx_csv_resp.json().get("data") or {}
        tx_dataset_id = str((tx_csv_payload.get("dataset") or {}).get("dataset_id") or "")
        tx_version_id = str((tx_csv_payload.get("version") or {}).get("version_id") or "")
        assert tx_dataset_id and tx_version_id

        owners_csv = "\n".join(
            [
                "account_number,owner_name,ssn_prefix,phone,opened_at",
                "110-123-4567,김철수,850315,010-1234-5678,2020-03-15",
                "220-111-2222,박영희,780823,010-2222-3333,2019-11-10",
                "330-333-4444,이민수,920401,010-4444-5555,2018-06-01",
                "440-555-6666,최지영,881205,010-6666-7777,2022-01-05",
                "550-777-8888,정수진,UNKNOWN,010-8888-9999,2023-05-15",
            ]
        ).encode("utf-8")

        owners_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**headers, "Idempotency-Key": f"idem-owners-{suffix}"},
            data={"dataset_name": "account_owners", "description": "owners", "delimiter": ",", "has_header": "true"},
            files={"file": ("account_owners.csv", owners_csv, "text/csv")},
        )
        owners_resp.raise_for_status()
        owners_payload = owners_resp.json().get("data") or {}
        owners_dataset_id = str((owners_payload.get("dataset") or {}).get("dataset_id") or "")
        owners_version_id = str((owners_payload.get("version") or {}).get("version_id") or "")
        assert owners_dataset_id and owners_version_id

        calls_csv = "\n".join(
            [
                "call_id,caller_phone,receiver_phone,timestamp,duration_seconds",
                "call_001,010-1234-5678,010-2222-3333,2026-01-05 09:30:00,180",
                "call_002,010-2222-3333,010-4444-5555,2026-01-05 10:15:00,240",
                "call_003,010-4444-5555,010-6666-7777,2026-01-10 14:20:00,120",
                "call_004,010-6666-7777,010-8888-9999,2026-01-15 16:45:00,300",
                "call_005,010-8888-9999,010-1234-5678,2026-01-20 11:30:00,150",
            ]
        ).encode("utf-8")

        calls_resp = await client.post(
            f"{BFF_URL}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": db_name, "branch": "main"},
            headers={**headers, "Idempotency-Key": f"idem-calls-{suffix}"},
            data={"dataset_name": "telecom_calls", "description": "calls", "delimiter": ",", "has_header": "true"},
            files={"file": ("telecom_records_202601.csv", calls_csv, "text/csv")},
        )
        calls_resp.raise_for_status()
        calls_payload = calls_resp.json().get("data") or {}
        calls_dataset_id = str((calls_payload.get("dataset") or {}).get("dataset_id") or "")
        calls_version_id = str((calls_payload.get("version") or {}).get("version_id") or "")
        assert calls_dataset_id and calls_version_id

        # Phase 2: Ontology (classes + relationships)
        classes: list[dict[str, Any]] = [
            {
                "id": "Person",
                "label": "사람",
                "description": "Financial investigation person",
                "properties": [
                    {"name": "phone", "type": "xsd:string", "label": "전화번호", "required": True, "primaryKey": True},
                    {"name": "name", "type": "xsd:string", "label": "이름", "required": True, "titleKey": True},
                    {"name": "ssn_prefix", "type": "xsd:string", "label": "주민번호앞자리"},
                ],
                "relationships": [],
            },
            {
                "id": "BankAccount",
                "label": "계좌",
                "description": "Bank account",
                "properties": [
                    {"name": "account_number", "type": "xsd:string", "label": "계좌번호", "required": True, "primaryKey": True, "titleKey": True},
                    {"name": "opened_at", "type": "xsd:date", "label": "개설일"},
                ],
                "relationships": [
                    {"predicate": "owner", "target": "Person", "label": "소유자", "cardinality": "n:1"},
                ],
            },
            {
                "id": "Transaction",
                "label": "거래",
                "description": "Bank transaction",
                "properties": [
                    {"name": "transaction_id", "type": "xsd:string", "label": "거래ID", "required": True, "primaryKey": True, "titleKey": True},
                    {"name": "date", "type": "xsd:date", "label": "날짜"},
                    {"name": "amount", "type": "xsd:integer", "label": "금액"},
                    {"name": "memo", "type": "xsd:string", "label": "비고"},
                ],
                "relationships": [
                    {"predicate": "from_account", "target": "BankAccount", "label": "송금계좌", "cardinality": "n:1"},
                    {"predicate": "to_account", "target": "BankAccount", "label": "수취계좌", "cardinality": "n:1"},
                ],
            },
            {
                "id": "PhoneCall",
                "label": "통화",
                "description": "Telecom call record",
                "properties": [
                    {"name": "call_id", "type": "xsd:string", "label": "통화ID", "required": True, "primaryKey": True, "titleKey": True},
                    {"name": "timestamp", "type": "xsd:dateTime", "label": "통화시각"},
                    {"name": "duration_seconds", "type": "xsd:integer", "label": "통화시간(초)"},
                ],
                "relationships": [
                    {"predicate": "caller", "target": "Person", "label": "발신자", "cardinality": "n:1"},
                    {"predicate": "receiver", "target": "Person", "label": "수신자", "cardinality": "n:1"},
                ],
            },
        ]

        for cls in classes:
            resp = await client.post(
                f"{BFF_URL}/api/v1/databases/{db_name}/ontology",
                params={"branch": "main"},
                json={**cls, "metadata": {"source": "financial_investigation_workflow_e2e"}},
            )
            resp.raise_for_status()
            await _wait_for_ontology(client, db_name=db_name, class_id=str(cls["id"]), branch="main")

        # Phase 3: Object types (pk specs)
        object_types = [
            ("Person", owners_dataset_id, {"primary_key": ["phone"], "title_key": ["name"]}),
            ("BankAccount", owners_dataset_id, {"primary_key": ["account_number"], "title_key": ["account_number"]}),
            ("Transaction", tx_dataset_id, {"primary_key": ["transaction_id"], "title_key": ["transaction_id"]}),
            ("PhoneCall", calls_dataset_id, {"primary_key": ["call_id"], "title_key": ["call_id"]}),
        ]
        for class_id, backing_dataset_id, pk_spec in object_types:
            await _upsert_object_type_contract(
                client,
                db_name=db_name,
                class_id=class_id,
                branch="main",
                contract_payload={
                    "backing_dataset_id": backing_dataset_id,
                    "pk_spec": pk_spec,
                    "metadata": {"source": "financial_investigation_workflow_e2e"},
                },
            )

        # Phase 4: Mapping specs + objectify
        mapping_specs: dict[str, str] = {}

        async def _create_mapping(
            target_class_id: str,
            dataset_id: str,
            artifact_output_name: str,
            mappings: list[dict[str, str]],
        ) -> str:
            resp = await client.post(
                f"{BFF_URL}/api/v1/objectify/mapping-specs",
                json={
                    "dataset_id": dataset_id,
                    "artifact_output_name": artifact_output_name,
                    "target_class_id": target_class_id,
                    "options": {"ontology_branch": "main"},
                    "mappings": mappings,
                },
            )
            resp.raise_for_status()
            mapping_spec = (resp.json().get("data") or {}).get("mapping_spec") or {}
            mapping_spec_id = str(mapping_spec.get("mapping_spec_id") or "")
            assert mapping_spec_id
            return mapping_spec_id

        mapping_specs["Person"] = await _create_mapping(
            "Person",
            owners_dataset_id,
            "account_owners",
            [
                {"source_field": "owner_name", "target_field": "name"},
                {"source_field": "phone", "target_field": "phone"},
                {"source_field": "ssn_prefix", "target_field": "ssn_prefix"},
            ],
        )
        mapping_specs["BankAccount"] = await _create_mapping(
            "BankAccount",
            owners_dataset_id,
            "account_owners",
            [
                {"source_field": "account_number", "target_field": "account_number"},
                {"source_field": "opened_at", "target_field": "opened_at"},
                {"source_field": "phone", "target_field": "owner"},
            ],
        )
        mapping_specs["Transaction"] = await _create_mapping(
            "Transaction",
            tx_dataset_id,
            "kb_bank_transactions_2026q1_csv",
            [
                {"source_field": "transaction_id", "target_field": "transaction_id"},
                {"source_field": "date", "target_field": "date"},
                {"source_field": "amount", "target_field": "amount"},
                {"source_field": "memo", "target_field": "memo"},
                {"source_field": "from_account", "target_field": "from_account"},
                {"source_field": "to_account", "target_field": "to_account"},
            ],
        )
        mapping_specs["PhoneCall"] = await _create_mapping(
            "PhoneCall",
            calls_dataset_id,
            "telecom_calls",
            [
                {"source_field": "call_id", "target_field": "call_id"},
                {"source_field": "timestamp", "target_field": "timestamp"},
                {"source_field": "duration_seconds", "target_field": "duration_seconds"},
                {"source_field": "caller_phone", "target_field": "caller"},
                {"source_field": "receiver_phone", "target_field": "receiver"},
            ],
        )

        # Use enterprise DAG orchestrator: infer safe order from mapping-spec relationship deps.
        dag_plan_resp = await client.post(
            f"{BFF_URL}/api/v1/objectify/databases/{db_name}/run-dag",
            json={
                "class_ids": ["Transaction", "PhoneCall"],
                "branch": "main",
                "include_dependencies": True,
                "dry_run": True,
            },
        )
        dag_plan_resp.raise_for_status()
        dag_plan = dag_plan_resp.json().get("data") or {}
        ordered_classes = list(dag_plan.get("ordered_classes") or [])
        assert {"Person", "BankAccount", "Transaction", "PhoneCall"}.issubset(set(ordered_classes)), ordered_classes
        assert ordered_classes.index("Person") < ordered_classes.index("BankAccount") < ordered_classes.index("Transaction")
        assert ordered_classes.index("Person") < ordered_classes.index("PhoneCall")

        dag_run_resp = await client.post(
            f"{BFF_URL}/api/v1/objectify/databases/{db_name}/run-dag",
            json={
                "class_ids": ["Transaction", "PhoneCall"],
                "branch": "main",
                "include_dependencies": True,
                "dry_run": False,
            },
        )
        dag_run_resp.raise_for_status()
        dag_run = dag_run_resp.json().get("data") or {}
        assert dag_run.get("jobs"), dag_run

        index_name = get_instances_index_name(db_name, branch="main")
        await _wait_for_es_doc(client, index_name=index_name, doc_id="010-1234-5678")
        await _wait_for_es_doc(client, index_name=index_name, doc_id="010-2222-3333")
        await _wait_for_es_doc(client, index_name=index_name, doc_id="110-123-4567")
        await _wait_for_es_doc(client, index_name=index_name, doc_id="tx_001")
        await _wait_for_es_doc(client, index_name=index_name, doc_id="call_001")

        # Phase 5: Graph queries (reverse traversal + multi-hop)
        query_accounts = await client.post(
            f"{BFF_URL}/api/v1/graph-query/{db_name}",
            params={"base_branch": "main"},
            json={
                "start_class": "Person",
                "filters": {"phone": "010-1234-5678"},
                "hops": [{"predicate": "owner", "target_class": "BankAccount", "reverse": True}],
                "include_paths": True,
                "max_paths": 10,
                "include_documents": True,
                "limit": 50,
            },
        )
        query_accounts.raise_for_status()
        qa = query_accounts.json()
        nodes = [n for n in (qa.get("nodes") or []) if isinstance(n, dict)]
        by_id = {str(n.get("id") or ""): str(n.get("type") or "") for n in nodes}
        assert any(n.get("type") == "Person" and n.get("es_doc_id") == "010-1234-5678" for n in nodes), qa
        assert any(n.get("type") == "BankAccount" and n.get("es_doc_id") == "110-123-4567" for n in nodes), qa

        # Reverse hop should still emit the edge in the ontology direction: BankAccount -> Person (predicate=owner).
        edges = qa.get("edges") or []
        assert any(
            isinstance(e, dict)
            and e.get("predicate") == "owner"
            and by_id.get(str(e.get("from_node") or "")) == "BankAccount"
            and by_id.get(str(e.get("to_node") or "")) == "Person"
            for e in edges
        ), qa

        query_flow = await client.post(
            f"{BFF_URL}/api/v1/graph-query/{db_name}",
            params={"base_branch": "main"},
            json={
                "start_class": "Person",
                "filters": {"phone": "010-1234-5678"},
                "hops": [
                    {"predicate": "owner", "target_class": "BankAccount", "reverse": True},
                    {"predicate": "from_account", "target_class": "Transaction", "reverse": True},
                    {"predicate": "to_account", "target_class": "BankAccount"},
                    {"predicate": "owner", "target_class": "Person"},
                ],
                "include_paths": True,
                "max_paths": 25,
                "include_documents": False,
                "limit": 200,
            },
        )
        query_flow.raise_for_status()
        qf = query_flow.json()
        person_es_ids = {
            str(n.get("es_doc_id") or "")
            for n in (qf.get("nodes") or [])
            if isinstance(n, dict) and n.get("type") == "Person"
        }
        assert "010-2222-3333" in person_es_ids or "010-4444-5555" in person_es_ids, qf

        query_calls = await client.post(
            f"{BFF_URL}/api/v1/graph-query/{db_name}",
            params={"base_branch": "main"},
            json={
                "start_class": "Person",
                "filters": {"phone": "010-1234-5678"},
                "hops": [
                    {"predicate": "caller", "target_class": "PhoneCall", "reverse": True},
                    {"predicate": "receiver", "target_class": "Person"},
                ],
                "include_paths": True,
                "max_paths": 25,
                "include_documents": False,
                "limit": 200,
            },
        )
        query_calls.raise_for_status()
        qc = query_calls.json()
        called_person_es_ids = {
            str(n.get("es_doc_id") or "")
            for n in (qc.get("nodes") or [])
            if isinstance(n, dict) and n.get("type") == "Person"
        }
        assert "010-2222-3333" in called_person_es_ids, qc
