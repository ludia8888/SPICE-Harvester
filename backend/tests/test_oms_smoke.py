"""
OMS module smoke tests (no direct infra credentials required).

These tests verify OMS' major functional areas through its public HTTP API:
- Database lifecycle (event-sourced)
- Ontology creation (event-sourced)
- Async instance creation (event-sourced) + read-side visibility
- Branch and version-control endpoints

Rationale:
Direct MinIO/Postgres credential checks are environment-specific (local vs SSH tunnel vs Docker).
Instead, we validate the same dependencies indirectly by exercising real OMS flows that require them.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Optional

import aiohttp
import pytest

from oms.services.event_store import EventStore
from tests.utils.auth import oms_auth_headers


if os.getenv("RUN_LIVE_OMS_SMOKE", "").strip().lower() not in {"1", "true", "yes", "on"}:
    raise RuntimeError(
        "RUN_LIVE_OMS_SMOKE must be enabled for this test run. "
        "Set RUN_LIVE_OMS_SMOKE=true."
    )


OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")
OMS_HEADERS = oms_auth_headers()


async def _assert_command_event_has_ontology_stamp(*, event_id: str) -> None:
    store = EventStore()
    key = await store.get_event_object_key(event_id=str(event_id))
    assert key, f"Missing by-event-id index for event_id={event_id}"

    envelope = await store.read_event_by_key(key=key)
    assert isinstance(envelope.data, dict)
    cmd_meta = envelope.data.get("metadata")
    assert isinstance(cmd_meta, dict), f"Missing command metadata in event_id={event_id}"

    ontology = cmd_meta.get("ontology")
    assert isinstance(ontology, dict), f"Missing ontology stamp in event_id={event_id}"
    assert (ontology.get("ref") or "").strip(), f"Missing ontology.ref in event_id={event_id}"
    assert (ontology.get("commit") or "").strip(), f"Missing ontology.commit in event_id={event_id}"


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
    timeout_seconds: int = 60,
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
    timeout_seconds: int = 60,
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


async def _wait_for_instance_count(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    class_id: str,
    expected_count: int,
    timeout_seconds: int = 90,
    poll_interval_seconds: float = 1.5,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        async with session.get(f"{OMS_URL}/api/v1/instance/{db_name}/class/{class_id}/count") as resp:
            assert resp.status == 200
            last = await resp.json()
            if int(last.get("count") or 0) >= expected_count:
                return
        await asyncio.sleep(poll_interval_seconds)
    raise AssertionError(
        f"Timed out waiting for instance count >= {expected_count} for {class_id} (last={last})"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_oms_end_to_end_smoke():
    async with aiohttp.ClientSession(headers=OMS_HEADERS) as session:
        db_name = f"test_oms_smoke_{uuid.uuid4().hex[:10]}"
        branch_name = "feature/oms_smoke"
        class_id = "Product"
        instance_id = f"prod_{uuid.uuid4().hex[:8]}"

        try:
            # 1) Database lifecycle (event sourcing)
            async with session.post(
                f"{OMS_URL}/api/v1/database/create",
                json={"name": db_name, "description": "OMS smoke test DB"},
            ) as resp:
                assert resp.status == 202

            await _wait_for_db_exists(session, db_name=db_name, expected=True)

            # 2) Branch endpoints
            async with session.get(f"{OMS_URL}/api/v1/branch/{db_name}/list") as resp:
                assert resp.status == 200
                payload = await resp.json()
                assert payload.get("status") == "success"

            async with session.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={"branch_name": branch_name, "from_branch": "main"},
            ) as resp:
                assert resp.status in {200, 201}

            async with session.get(
                f"{OMS_URL}/api/v1/branch/{db_name}/branch/{branch_name}/info"
            ) as resp:
                assert resp.status == 200

            # 3) Version control endpoints (smoke)
            async with session.get(f"{OMS_URL}/api/v1/version/{db_name}/history") as resp:
                assert resp.status == 200

            async with session.get(
                f"{OMS_URL}/api/v1/version/{db_name}/diff",
                params={"from_ref": "main", "to_ref": "main"},
            ) as resp:
                assert resp.status in {200, 404}

            # 4) Ontology creation (event sourcing)
            ontology = {
                "id": class_id,
                "label": "Product",
                "description": "Product for OMS smoke test",
                "properties": [
                    {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                    {"name": "name", "type": "string", "label": "Name", "required": True},
                ],
                "relationships": [],
            }
            async with session.post(
                f"{OMS_URL}/api/v1/database/{db_name}/ontology",
                json=ontology,
            ) as resp:
                assert resp.status == 202
                payload = await resp.json()
                ontology_command_id = (payload.get("data") or {}).get("command_id")
                assert ontology_command_id
                await _assert_command_event_has_ontology_stamp(event_id=str(ontology_command_id))

            await _wait_for_ontology_present(session, db_name=db_name, ontology_id=class_id)

            # 5) Async instance create (event sourcing) + read-side count
            async with session.post(
                f"{OMS_URL}/api/v1/instances/{db_name}/async/{class_id}/create",
                json={"data": {"product_id": instance_id, "name": "OMS Smoke Product"}},
            ) as resp:
                assert resp.status == 202
                payload = await resp.json()
                instance_command_id = payload.get("command_id")
                assert instance_command_id
                await _assert_command_event_has_ontology_stamp(event_id=str(instance_command_id))

            await _wait_for_instance_count(
                session,
                db_name=db_name,
                class_id=class_id,
                expected_count=1,
            )
        finally:
            # Best-effort cleanup (database delete is async)
            try:
                expected_seq = await _get_write_side_last_sequence(
                    aggregate_type="Database", aggregate_id=db_name
                )
                params = {"expected_seq": expected_seq} if expected_seq is not None else None

                async with session.delete(f"{OMS_URL}/api/v1/database/{db_name}", params=params) as resp:
                    assert resp.status in {200, 202, 404}
                if resp.status != 404:
                    await _wait_for_db_exists(session, db_name=db_name, expected=False, timeout_seconds=60)
            except Exception:
                # Avoid masking test failures; cleanup can be retried manually.
                pass
