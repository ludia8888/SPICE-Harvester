"""
E2E test for branch-based data virtualization (copy-on-write semantics).

Goal:
- Create data in main (graph in TerminusDB + payload in Elasticsearch)
- Create a branch from main (no data copy)
- Apply an update in the branch (patch update)
- Verify:
  - main stays unchanged
  - branch sees the updated payload (overlay index)
  - untouched nodes in the branch fall back to main payload (base index)
  - branch overlay keeps base fields (no accidental "partial replace")

This test requires the full stack (OMS + workers + Kafka + MinIO + ES + TerminusDB) running.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Dict, Optional

import aiohttp
import pytest

from shared.config.search_config import get_instances_index_name
from tests.utils.auth import bff_auth_headers, oms_auth_headers


if os.getenv("RUN_LIVE_BRANCH_VIRTUALIZATION", "").strip().lower() not in {
    "1",
    "true",
    "yes",
    "on",
}:
    raise RuntimeError(
        "RUN_LIVE_BRANCH_VIRTUALIZATION must be enabled for this test run. "
        "Set RUN_LIVE_BRANCH_VIRTUALIZATION=true."
    )


OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")
BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
OMS_HEADERS = oms_auth_headers()
BFF_HEADERS = bff_auth_headers()
if OMS_HEADERS.get("X-Admin-Token") != BFF_HEADERS.get("X-Admin-Token"):
    raise AssertionError("BFF/OMS auth tokens differ; tests require a single admin token.")
AUTH_HEADERS = BFF_HEADERS


async def _get_write_side_last_sequence(*, aggregate_type: str, aggregate_id: str) -> Optional[int]:
    """Best-effort: fetch current write-side sequence for OCC cleanup (returns None if Postgres unavailable)."""
    import asyncpg
    import re
    from urllib.parse import urlparse

    schema = os.getenv("EVENT_STORE_SEQUENCE_SCHEMA", "spice_event_registry")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        return None

    prefix = (os.getenv("EVENT_STORE_SEQUENCE_HANDLER_PREFIX", "write_side") or "write_side").strip()
    handler = f"{prefix}:{aggregate_type}"

    candidates = []
    if (os.getenv("POSTGRES_URL") or "").strip():
        candidates.append(os.getenv("POSTGRES_URL"))
    candidates.extend(
        [
            "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb",
            "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
        ]
    )

    for dsn in candidates:
        try:
            parsed = urlparse(dsn)
            conn = await asyncpg.connect(
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                user=parsed.username or "spiceadmin",
                password=parsed.password or "spicepass123",
                database=(parsed.path or "/spicedb").lstrip("/") or "spicedb",
            )
        except Exception:
            continue

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
        except Exception:
            return None
        finally:
            await conn.close()

    return None


async def _wait_for_db_exists(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    expected: bool,
    timeout_seconds: int = 180,
    poll_interval_seconds: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        async with session.get(f"{OMS_URL}/api/v1/database/exists/{db_name}") as resp:
            assert resp.status == 200
            last = await resp.json()
            if (last.get("data") or {}).get("exists") is expected:
                return
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for db exists={expected} (last={last})")


async def _wait_for_ontology_present(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    ontology_id: str,
    timeout_seconds: int = 180,
    poll_interval_seconds: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        async with session.get(f"{OMS_URL}/api/v1/database/{db_name}/ontology") as resp:
            assert resp.status == 200
            last = await resp.json()
            ontologies = (last.get("data") or {}).get("ontologies") or []
            if any(o.get("id") == ontology_id for o in ontologies):
                return
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(f"Timed out waiting for ontology '{ontology_id}' (last={last})")


async def _wait_for_graph_node(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    branch: str,
    class_id: str,
    primary_key_value: str,
    expected_name: Optional[str] = None,
    require_overlay_index: Optional[bool] = None,
    timeout_seconds: int = 180,
    poll_interval_seconds: float = 2.0,
) -> Dict[str, Any]:
    """
    Poll BFF graph query until a single node is returned and ES enrichment is ready.

    `require_overlay_index`:
    - None: don't care
    - True: expect node.es_ref.index == branch index
    - False: expect node.es_ref.index == main index
    """
    pk_field = f"{class_id.lower()}_id"
    base_index = get_instances_index_name(db_name, branch="main")
    branch_index = get_instances_index_name(db_name, branch=branch)

    payload: Dict[str, Any] = {
        "start_class": class_id,
        "hops": [],
        "filters": {pk_field: primary_key_value},
        "limit": 10,
        "offset": 0,
        "include_documents": True,
        "include_audit": False,
    }

    deadline = time.monotonic() + timeout_seconds
    last: Optional[Dict[str, Any]] = None
    while time.monotonic() < deadline:
        async with session.post(
            f"{BFF_URL}/api/v1/graph-query/{db_name}",
            params={"branch": branch},
            json=payload,
        ) as resp:
            if resp.status != 200:
                last = {"status": resp.status, "body": await resp.text()}
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()

        nodes = list(last.get("nodes") or [])
        if not nodes:
            await asyncio.sleep(poll_interval_seconds)
            continue

        node = nodes[0]
        if node.get("data_status") != "FULL":
            await asyncio.sleep(poll_interval_seconds)
            continue

        if expected_name is not None:
            actual_name = (((node.get("data") or {}).get("data") or {}).get("name"))
            if actual_name != expected_name:
                await asyncio.sleep(poll_interval_seconds)
                continue

        if require_overlay_index is not None:
            actual_index = ((node.get("es_ref") or {}).get("index"))
            expected_index = branch_index if require_overlay_index else base_index
            if actual_index != expected_index:
                await asyncio.sleep(poll_interval_seconds)
                continue

        return node

    raise AssertionError(f"Timed out waiting for graph node (last={last})")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_branch_virtualization_overlay_copy_on_write():
    async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
        db_name = f"test_branch_viz_{uuid.uuid4().hex[:10]}"
        class_id = "Product"

        prod_a = f"PROD_A_{uuid.uuid4().hex[:6]}"
        prod_b = f"PROD_B_{uuid.uuid4().hex[:6]}"
        branch_name = f"feature/whatif_{uuid.uuid4().hex[:8]}"

        main_a_name = "Main A"
        main_b_name = "Main B"
        branch_a_name = "WhatIf A"

        try:
            # 1) Create DB (event-sourced)
            async with session.post(
                f"{OMS_URL}/api/v1/database/create",
                json={"name": db_name, "description": "branch virtualization e2e"},
            ) as resp:
                assert resp.status == 202

            await _wait_for_db_exists(session, db_name=db_name, expected=True)

            # 2) Create ontology in main
            ontology = {
                "id": class_id,
                "label": "Product",
                "description": "Product for branch virtualization test",
                "properties": [
                    {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                    {"name": "name", "type": "string", "label": "Name", "required": True},
                    {"name": "category", "type": "string", "label": "Category", "required": False},
                ],
                "relationships": [],
            }
            async with session.post(f"{OMS_URL}/api/v1/database/{db_name}/ontology", json=ontology) as resp:
                assert resp.status == 202

            await _wait_for_ontology_present(session, db_name=db_name, ontology_id=class_id)

            # 3) Create two instances in main (async command -> worker -> domain event -> ES projection)
            async with session.post(
                f"{OMS_URL}/api/v1/instances/{db_name}/async/{class_id}/create",
                params={"branch": "main"},
                json={"data": {"product_id": prod_a, "name": main_a_name, "category": "alpha"}},
            ) as resp:
                assert resp.status == 202

            async with session.post(
                f"{OMS_URL}/api/v1/instances/{db_name}/async/{class_id}/create",
                params={"branch": "main"},
                json={"data": {"product_id": prod_b, "name": main_b_name, "category": "beta"}},
            ) as resp:
                assert resp.status == 202

            # Wait until ES enrichment is ready in main (GraphFederation reads Terminus + ES)
            node_a_main = await _wait_for_graph_node(
                session,
                db_name=db_name,
                branch="main",
                class_id=class_id,
                primary_key_value=prod_a,
                expected_name=main_a_name,
                require_overlay_index=False,
            )
            node_b_main = await _wait_for_graph_node(
                session,
                db_name=db_name,
                branch="main",
                class_id=class_id,
                primary_key_value=prod_b,
                expected_name=main_b_name,
                require_overlay_index=False,
            )

            expected_seq_a = (node_a_main.get("index_status") or {}).get("event_sequence")
            assert expected_seq_a is not None

            # 4) Create branch (copy-on-write, no data copy)
            async with session.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={"branch_name": branch_name, "from_branch": "main"},
            ) as resp:
                assert resp.status in {200, 201}

            # 5) Patch-update only Product.name in the branch (expected_seq from main read)
            async with session.put(
                f"{OMS_URL}/api/v1/instances/{db_name}/async/{class_id}/{prod_a}/update",
                params={"branch": branch_name, "expected_seq": int(expected_seq_a)},
                json={"data": {"name": branch_a_name}},
            ) as resp:
                assert resp.status == 202

            # 6) Branch overlay: updated doc must come from branch index and preserve base fields
            node_a_branch = await _wait_for_graph_node(
                session,
                db_name=db_name,
                branch=branch_name,
                class_id=class_id,
                primary_key_value=prod_a,
                expected_name=branch_a_name,
                require_overlay_index=True,
            )
            assert (((node_a_branch.get("data") or {}).get("data") or {}).get("category")) == "alpha"

            # 7) Main remains unchanged
            node_a_main_after = await _wait_for_graph_node(
                session,
                db_name=db_name,
                branch="main",
                class_id=class_id,
                primary_key_value=prod_a,
                expected_name=main_a_name,
                require_overlay_index=False,
            )
            assert node_a_main_after.get("terminus_id") == node_a_main.get("terminus_id")

            # 8) Untouched instances in the branch fall back to main ES payload
            node_b_branch = await _wait_for_graph_node(
                session,
                db_name=db_name,
                branch=branch_name,
                class_id=class_id,
                primary_key_value=prod_b,
                expected_name=main_b_name,
                require_overlay_index=False,
            )
            assert (((node_b_branch.get("data") or {}).get("data") or {}).get("category")) == "beta"
        finally:
            # Best-effort cleanup (database delete is async + OCC-protected)
            try:
                expected_seq = await _get_write_side_last_sequence(
                    aggregate_type="Database", aggregate_id=db_name
                )
                if expected_seq is not None:
                    async with session.delete(
                        f"{OMS_URL}/api/v1/database/{db_name}",
                        params={"expected_seq": expected_seq},
                    ) as resp:
                        assert resp.status in {200, 202, 404}
                    if resp.status != 404:
                        await _wait_for_db_exists(
                            session, db_name=db_name, expected=False, timeout_seconds=90
                        )
            except Exception:
                # Avoid masking test failures; cleanup can be retried manually.
                pass
