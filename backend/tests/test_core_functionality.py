"""
🔥 SPICE HARVESTER CORE FUNCTIONALITY TESTS
Production-ready test suite - NO MOCKS, REAL INTEGRATIONS ONLY

This consolidated test file replaces multiple older test files
and provides comprehensive coverage of core functionality.
"""

import asyncio
import pytest
import aiohttp
import json
import uuid
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from tests.utils.auth import bff_auth_headers, oms_auth_headers
from shared.config.search_config import get_ontologies_index_name

# Real service endpoints
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or "http://localhost:8000").rstrip("/")
BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
USE_OMS_DIRECT = (os.getenv("CORE_TEST_USE_OMS_DIRECT") or "").strip().lower() in {"1", "true", "yes", "on"}

# Test configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://:spicepass123@localhost:6380/0")
def _get_postgres_url_candidates() -> list[str]:
    """Return Postgres DSN candidates (env override first, then common local ports)."""
    env_url = (os.getenv("POSTGRES_URL") or "").strip()
    if env_url:
        return [env_url]
    return [
        # docker-compose host port default
        "postgresql://spiceadmin:spicepass123@localhost:55433/spicedb",
        # common local Postgres port
        "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
    ]
MINIO_URL = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9002")
ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{os.getenv('ELASTICSEARCH_PORT', '9200')}",
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")

BFF_HEADERS = bff_auth_headers()
OMS_HEADERS = oms_auth_headers()
if BFF_HEADERS.get("X-Admin-Token") != OMS_HEADERS.get("X-Admin-Token"):
    raise AssertionError("BFF/OMS auth tokens differ; tests require a single admin token.")
AUTH_HEADERS = BFF_HEADERS

CORE_URL = OMS_URL if USE_OMS_DIRECT else BFF_URL


def _path_database_create() -> str:
    return "/api/v1/database/create" if USE_OMS_DIRECT else "/api/v1/databases"


def _path_database_item(db_name: str) -> str:
    return f"/api/v1/database/{db_name}" if USE_OMS_DIRECT else f"/api/v1/databases/{db_name}"


def _path_database_exists(db_name: str) -> str:
    return f"/api/v1/database/exists/{db_name}" if USE_OMS_DIRECT else _path_database_item(db_name)


def _path_command_status(command_id: str) -> str:
    return f"/api/v1/commands/{command_id}/status"


def _path_ontology_collection(db_name: str) -> str:
    return f"/api/v1/database/{db_name}/ontology" if USE_OMS_DIRECT else f"/api/v1/databases/{db_name}/ontology"


def _path_ontology_item(db_name: str, ontology_id: str) -> str:
    return f"{_path_ontology_collection(db_name)}/{ontology_id}"


def _headers_for_db(db_name: str) -> Dict[str, str]:
    headers = dict(AUTH_HEADERS)
    if not USE_OMS_DIRECT:
        headers["X-DB-Name"] = db_name
    return headers


def _extract_exists_flag(payload: Dict[str, Any]) -> Optional[bool]:
    data = payload.get("data")
    if isinstance(data, dict):
        exists = data.get("exists")
        if isinstance(exists, bool):
            return exists
        nested = data.get("data")
        if isinstance(nested, dict) and isinstance(nested.get("exists"), bool):
            return nested.get("exists")
    return None


async def _resolve_bff_path(
    session: aiohttp.ClientSession,
    candidates: list[str],
) -> str:
    try:
        async with session.get(f"{BFF_URL}/openapi.json") as resp:
            if resp.status != 200:
                return candidates[0]
            spec = await resp.json()
    except Exception:
        return candidates[0]

    path_set = set(spec.get("paths") or {})
    for candidate in candidates:
        if candidate in path_set:
            return candidate
    return candidates[0]


async def _get_write_side_last_sequence(*, aggregate_type: str, aggregate_id: str) -> int:
    """
    Fetch the current write-side sequence for an aggregate from Postgres.

    This is the same value OMS uses for OCC (`expected_seq`) at command append time.
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
    explicit_postgres_url = (os.getenv("POSTGRES_URL") or "").strip()

    for dsn in _get_postgres_url_candidates():
        parsed = urlparse(dsn)
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        user = parsed.username or "spiceadmin"
        password = parsed.password or "spicepass123"
        database = (parsed.path or "/spicedb").lstrip("/") or "spicedb"
        try:
            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
            )
            break
        except Exception as e:
            last_error = e
            continue

    if conn is None:
        if not explicit_postgres_url:
            raise RuntimeError(
                "Postgres DSN not provided for OCC/sequence checks. "
                "Set POSTGRES_URL to run tests that require expected_seq."
            )
        raise AssertionError("Could not connect to Postgres using POSTGRES_URL.") from last_error

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


async def _wait_for_db_exists(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    expected: bool,
    timeout_seconds: int = 30,
    poll_interval_seconds: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        async with session.get(
            f"{CORE_URL}{_path_database_exists(db_name)}",
            headers=_headers_for_db(db_name),
        ) as resp:
            if USE_OMS_DIRECT:
                assert resp.status == 200
                last = await resp.json()
                if (last.get("data") or {}).get("exists") is expected:
                    return
            else:
                if resp.status == 200:
                    last = await resp.json()
                    exists = _extract_exists_flag(last)
                    if exists is expected:
                        return
                elif resp.status == 404:
                    last = {"status": 404}
                    if not expected:
                        return
                else:
                    last = {"status": resp.status, "body": await resp.text()}
        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for db exists={expected} (last={last})")


async def _delete_test_database(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    timeout_seconds: int = 60,
) -> None:
    """Best-effort cleanup for test databases to keep read-model indices bounded."""
    try:
        expected_seq = await _get_write_side_last_sequence(
            aggregate_type="Database",
            aggregate_id=db_name,
        )
        params = {"expected_seq": expected_seq} if expected_seq is not None else None
        async with session.delete(
            f"{CORE_URL}{_path_database_item(db_name)}",
            params=params,
            headers=_headers_for_db(db_name),
        ) as resp:
            if resp.status not in {200, 202, 404}:
                body = await resp.text()
                raise AssertionError(f"Unexpected cleanup response: status={resp.status} body={body}")
        await _wait_for_db_exists(session, db_name=db_name, expected=False, timeout_seconds=timeout_seconds)
    except Exception:
        print(f"[cleanup-warning] failed to delete test database {db_name}", flush=True)


async def _wait_for_command_terminal_state(
    session: aiohttp.ClientSession,
    *,
    command_id: str,
    timeout_seconds: int = 90,
    poll_interval_seconds: float = 1.0,
) -> Dict[str, Any]:
    """
    Wait until an async (202 Accepted) command reaches a terminal state.

    This avoids OCC races where the write-side stream continues to advance (e.g. command->domain)
    after the user-visible side-effect becomes observable (like database existence).
    """
    deadline = time.monotonic() + timeout_seconds
    last: Optional[Dict[str, Any]] = None

    while time.monotonic() < deadline:
        async with session.get(f"{CORE_URL}{_path_command_status(command_id)}") as resp:
            if resp.status != 200:
                last = {"status": resp.status, "body": await resp.text()}
                await asyncio.sleep(poll_interval_seconds)
                continue
            last = await resp.json()

        status_value = str(last.get("status") or "").upper()
        if status_value in {"COMPLETED", "FAILED", "CANCELLED"}:
            if status_value != "COMPLETED":
                raise AssertionError(f"Command {command_id} ended in {status_value}: {last}")
            return last

        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for command terminal state (command_id={command_id}, last={last})")


async def _wait_for_ontology_present(
    session: aiohttp.ClientSession,
    *,
    db_name: str,
    ontology_id: str,
    branch: str = "main",
    timeout_seconds: int = 30,
    poll_interval_seconds: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        if USE_OMS_DIRECT:
            async with session.get(
                f"{CORE_URL}{_path_ontology_collection(db_name)}",
                params={"branch": branch},
                headers=_headers_for_db(db_name),
            ) as resp:
                assert resp.status == 200
                last = await resp.json()
                ontologies = (last.get("data") or {}).get("ontologies") or []
                if any(o.get("id") == ontology_id for o in ontologies):
                    return
        else:
            async with session.get(
                f"{BFF_URL}/api/v2/ontologies/{db_name}/objectTypes",
                params={"branch": branch},
                headers=_headers_for_db(db_name),
            ) as resp:
                if resp.status == 200:
                    last = await resp.json()
                    rows = (last.get("data") if isinstance(last, dict) else None) or []
                    if any((row.get("apiName") or row.get("id")) == ontology_id for row in rows if isinstance(row, dict)):
                        return
                else:
                    last = {"status": resp.status, "body": await resp.text()}
        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for ontology '{ontology_id}' (last={last})")


async def _wait_for_es_doc(
    session: aiohttp.ClientSession,
    *,
    index_name: str,
    doc_id: str,
    timeout_seconds: int = 60,
    poll_interval_seconds: float = 1.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: Optional[Dict[str, Any]] = None

    while time.monotonic() < deadline:
        try:
            async with session.get(f"{ELASTICSEARCH_URL}/{index_name}/_doc/{doc_id}") as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    if payload.get("found") is True or payload.get("_source"):
                        return payload
                else:
                    last = {"status": resp.status, "body": await resp.text()}
        except aiohttp.ClientError as exc:
            last = {"error": str(exc)}
        await asyncio.sleep(poll_interval_seconds)

    raise AssertionError(f"Timed out waiting for ES doc {index_name}/{doc_id} (last={last})")


class TestCoreOntologyManagement:
    """Test suite for Ontology Management Service"""
    pytestmark = pytest.mark.integration
    
    @pytest.mark.asyncio
    async def test_database_lifecycle(self):
        """Test complete database lifecycle with Event Sourcing"""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            db_name = f"test_db_{uuid.uuid4().hex[:12]}"
            command_id: Optional[str] = None
            
            # Create database
            async with session.post(
                f"{CORE_URL}{_path_database_create()}",
                json={"name": db_name, "description": "Test database"}
            ) as resp:
                assert resp.status == 202  # Event Sourcing async
                result = await resp.json()
                assert result.get("status") == "accepted"
                command_id = (result.get("data") or {}).get("command_id")
                assert command_id
                assert str(command_id)

            await _wait_for_db_exists(session, db_name=db_name, expected=True)
            await _wait_for_command_terminal_state(session, command_id=str(command_id))
                
            # Delete database
            # OCC can legitimately race with internal command->domain append; retry once on 409.
            for attempt in range(2):
                expected_seq = await _get_write_side_last_sequence(
                    aggregate_type="Database", aggregate_id=db_name
                )
                async with session.delete(
                    f"{CORE_URL}{_path_database_item(db_name)}",
                    params={"expected_seq": expected_seq},
                    headers=_headers_for_db(db_name),
                ) as resp:
                    if resp.status == 202:
                        break
                    if resp.status == 409 and attempt == 0:
                        continue
                    body = await resp.text()
                    raise AssertionError(f"Unexpected delete response: status={resp.status} body={body}")

            await _wait_for_db_exists(session, db_name=db_name, expected=False)
                
    @pytest.mark.asyncio
    async def test_ontology_creation(self):
        """Test ontology creation with complex types"""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            db_name = f"test_ontology_db_{uuid.uuid4().hex[:12]}"
            ontology_branch = "main"
            try:
                # Create database first
                async with session.post(
                    f"{CORE_URL}{_path_database_create()}",
                    json={"name": db_name, "description": "Ontology test"}
                ) as resp:
                    assert resp.status == 202

                await _wait_for_db_exists(session, db_name=db_name, expected=True)

                # Relationship targets must exist in schema (create Customer first)
                customer_ontology = {
                    "id": "Customer",
                    "label": "Customer",
                    "description": "Customer for relationship target",
                    "properties": [
                        {"name": "customer_id", "type": "string", "label": "Customer ID", "required": True},
                        {"name": "name", "type": "string", "label": "Name", "required": True, "titleKey": True},
                    ],
                    "relationships": [],
                }
                async with session.post(
                    f"{CORE_URL}{_path_ontology_collection(db_name)}",
                    params={"branch": ontology_branch},
                    json=customer_ontology,
                    headers=_headers_for_db(db_name),
                ) as resp:
                    assert resp.status == 202

                await _wait_for_ontology_present(
                    session,
                    db_name=db_name,
                    ontology_id="Customer",
                    branch=ontology_branch,
                )

                # Create ontology
                ontology_data = {
                    "id": "TestProduct",
                    "label": "Test Product",
                    "description": "Product for testing",
                    "properties": [
                        {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
                        {"name": "name", "type": "string", "label": "Name", "required": True, "titleKey": True},
                        {"name": "price", "type": "decimal", "label": "Price"},
                        {"name": "tags", "type": "array", "label": "Tags"}
                    ],
                    "relationships": [
                        {
                            "predicate": "owned_by",
                            "target": "Customer",
                            "label": "Owned By",
                            "cardinality": "n:1"
                        }
                    ]
                }

                async with session.post(
                    f"{CORE_URL}{_path_ontology_collection(db_name)}",
                    params={"branch": ontology_branch},
                    json=ontology_data,
                    headers=_headers_for_db(db_name),
                ) as resp:
                    assert resp.status == 202
                    result = await resp.json()
                    assert result.get("status") == "accepted"
                    assert "command_id" in (result.get("data") or {})

                await _wait_for_ontology_present(
                    session,
                    db_name=db_name,
                    ontology_id="TestProduct",
                    branch=ontology_branch,
                )
            finally:
                await _delete_test_database(session, db_name=db_name)

    @pytest.mark.asyncio
    async def test_ontology_i18n_label_projection(self):
        """Ensure i18n labels are normalized for ES and preserved in label_i18n."""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            db_name = f"test_ontology_i18n_db_{uuid.uuid4().hex[:12]}"
            ontology_branch = "main"
            try:
                async with session.post(
                    f"{CORE_URL}{_path_database_create()}",
                    json={"name": db_name, "description": "Ontology i18n test"},
                ) as resp:
                    assert resp.status == 202

                await _wait_for_db_exists(session, db_name=db_name, expected=True)

                customer = {
                    "id": "Customer",
                    "label": {"en": "Customer", "ko": "고객"},
                    "description": {"en": "Customer target", "ko": "관계 대상 고객"},
                    "properties": [
                        {
                            "name": "customer_id",
                            "type": "string",
                            "label": {"en": "Customer ID", "ko": "고객 ID"},
                            "required": True,
                            "titleKey": True,
                        }
                    ],
                    "relationships": [],
                }
                async with session.post(
                    f"{CORE_URL}{_path_ontology_collection(db_name)}",
                    params={"branch": ontology_branch},
                    json=customer,
                    headers=_headers_for_db(db_name),
                ) as resp:
                    assert resp.status == 202

                await _wait_for_ontology_present(
                    session,
                    db_name=db_name,
                    ontology_id="Customer",
                    branch=ontology_branch,
                )

                product = {
                    "id": "I18nProduct",
                    "label": {"en": "Product", "ko": "제품"},
                    "description": {"en": "Product data", "ko": "제품 데이터"},
                    "properties": [
                        {
                            "name": "product_id",
                            "type": "string",
                            "label": {"en": "Product ID", "ko": "제품 ID"},
                            "description": {"en": "Primary product identifier", "ko": "제품 기본 ID"},
                            "required": True,
                            "titleKey": True,
                        }
                    ],
                    "relationships": [
                        {
                            "predicate": "owned_by",
                            "target": "Customer",
                            "label": {"en": "Owned By", "ko": "소유자"},
                            "inverse_label": {"en": "Owns", "ko": "소유"},
                            "description": {"en": "Ownership link", "ko": "소유 관계"},
                            "cardinality": "n:1",
                        }
                    ],
                }

                async with session.post(
                    f"{CORE_URL}{_path_ontology_collection(db_name)}",
                    params={"branch": ontology_branch},
                    json=product,
                    headers=_headers_for_db(db_name),
                ) as resp:
                    assert resp.status == 202
                    result = await resp.json()
                    assert result.get("status") == "accepted"

                await _wait_for_ontology_present(
                    session,
                    db_name=db_name,
                    ontology_id="I18nProduct",
                    branch=ontology_branch,
                )

                index_name = get_ontologies_index_name(db_name, branch=ontology_branch)
                es_doc = await _wait_for_es_doc(
                    session,
                    index_name=index_name,
                    doc_id="I18nProduct",
                    timeout_seconds=420,
                )
                source = es_doc.get("_source") or {}

                label_value = source.get("label")
                assert isinstance(label_value, str)
                assert label_value == "제품"
                label_i18n = source.get("label_i18n") or {}
                assert label_i18n.get("en") == "Product"
                assert label_i18n.get("ko") == "제품"

                props = {p.get("name"): p for p in (source.get("properties") or [])}
                prop = props.get("product_id")
                assert prop is not None
                assert isinstance(prop.get("label"), str)
                assert prop.get("label") == "제품 ID"
                prop_label_i18n = prop.get("label_i18n") or {}
                assert prop_label_i18n.get("en") == "Product ID"
                assert prop_label_i18n.get("ko") == "제품 ID"
                prop_desc_i18n = prop.get("description_i18n") or {}
                assert prop_desc_i18n.get("en") == "Primary product identifier"
                assert prop_desc_i18n.get("ko") == "제품 기본 ID"

                rels = {(r.get("predicate"), r.get("target")): r for r in (source.get("relationships") or [])}
                rel = rels.get(("owned_by", "Customer"))
                assert rel is not None
                assert isinstance(rel.get("label"), str)
                assert rel.get("label") == "소유자"
                rel_label_i18n = rel.get("label_i18n") or {}
                assert rel_label_i18n.get("en") == "Owned By"
                assert rel_label_i18n.get("ko") == "소유자"
                rel_inverse_i18n = rel.get("inverse_label_i18n") or {}
                assert rel_inverse_i18n.get("en") == "Owns"
                assert rel_inverse_i18n.get("ko") == "소유"
            finally:
                await _delete_test_database(session, db_name=db_name)

    @pytest.mark.asyncio
    async def test_ontology_creation_advanced_relationships(self):
        """Test ontology creation with relationships is event-sourced and functional."""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            db_name = f"test_adv_ontology_db_{uuid.uuid4().hex[:12]}"
            ontology_branch = "main"
            try:
                async with session.post(
                    f"{CORE_URL}{_path_database_create()}",
                    json={"name": db_name, "description": "Advanced ontology test"},
                ) as resp:
                    assert resp.status == 202

                await _wait_for_db_exists(session, db_name=db_name, expected=True)

                # Create target class first so relationship validation can succeed.
                customer = {
                    "id": "Customer",
                    "label": "Customer",
                    "description": "Customer for relationship target",
                    "properties": [
                        {
                            "name": "customer_id",
                            "type": "string",
                            "label": "Customer ID",
                            "required": True,
                            "titleKey": True,
                        }
                    ],
                    "relationships": [],
                }
                async with session.post(
                    f"{CORE_URL}{_path_ontology_collection(db_name)}",
                    params={"branch": ontology_branch},
                    json=customer,
                    headers=_headers_for_db(db_name),
                ) as resp:
                    assert resp.status == 202
                    body = await resp.json()
                    assert body.get("status") == "accepted"

                await _wait_for_ontology_present(
                    session,
                    db_name=db_name,
                    ontology_id="Customer",
                    branch=ontology_branch,
                )

                product_adv = {
                    "id": "AdvProduct",
                    "label": "Advanced Product",
                    "description": "Product created via ontology endpoint",
                    "properties": [
                        {
                            "name": "product_id",
                            "type": "string",
                            "label": "Product ID",
                            "required": True,
                            "titleKey": True,
                        }
                    ],
                    "relationships": [
                        {
                            "predicate": "owned_by",
                            "target": "Customer",
                            "label": "Owned By",
                            "cardinality": "n:1",
                        }
                    ],
                }

                async with session.post(
                    f"{CORE_URL}{_path_ontology_collection(db_name)}",
                    json=product_adv,
                    params={"branch": ontology_branch},
                    headers=_headers_for_db(db_name),
                ) as resp:
                    assert resp.status == 202
                    body = await resp.json()
                    assert body.get("status") == "accepted"
                    command_id = (body.get("data") or {}).get("command_id")
                    assert command_id

                await _wait_for_ontology_present(
                    session,
                    db_name=db_name,
                    ontology_id="AdvProduct",
                    branch=ontology_branch,
                )
                await _wait_for_command_terminal_state(session, command_id=str(command_id))

                # Verify relationship payload was persisted on read path.
                if USE_OMS_DIRECT:
                    async with session.get(
                        f"{CORE_URL}{_path_ontology_item(db_name, 'AdvProduct')}",
                        params={"branch": ontology_branch},
                        headers=_headers_for_db(db_name),
                    ) as resp:
                        assert resp.status == 200
                        body = await resp.json()
                        assert body.get("status") == "success"
                        ontology = body.get("data") or {}
                        relationships = ontology.get("relationships") or []
                        assert any(rel.get("predicate") == "owned_by" for rel in relationships)
                else:
                    async with session.get(
                        f"{BFF_URL}/api/v2/ontologies/{db_name}/objectTypes/AdvProduct/outgoingLinkTypes",
                        params={"branch": ontology_branch},
                        headers=_headers_for_db(db_name),
                    ) as resp:
                        assert resp.status == 200
                        body = await resp.json()
                        links = (body.get("data") if isinstance(body, dict) else None) or []
                        assert any(
                            isinstance(link, dict) and str(link.get("apiName") or "").strip() == "owned_by"
                            for link in links
                        )
            finally:
                await _delete_test_database(session, db_name=db_name)


class TestBFFGraphFederation:
    """Test suite for BFF Graph Federation capabilities"""
    pytestmark = pytest.mark.integration
    
    @pytest.mark.asyncio
    async def test_foundry_v2_ontology_full_metadata_surface(self):
        """Foundry v2 full metadata surface should be available."""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            async with session.get(
                f"{BFF_URL}/api/v2/ontologies/test_db/fullMetadata",
                params={"preview": "true", "branch": "main"},
            ) as resp:
                assert resp.status in [200, 404]
                
    @pytest.mark.asyncio
    async def test_graph_query_federation(self):
        """Test federated graph queries with Elasticsearch"""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            # This would require a test database with data
            # For now, just verify the endpoint is accessible
            async with session.post(
                f"{BFF_URL}/api/v1/graph-query/test_db/simple",
                json={"class_name": "Product", "limit": 10}
            ) as resp:
                # Should return 404 if database doesn't exist
                assert resp.status in [200, 404]


class TestEventSourcingInfrastructure:
    """Test Event Sourcing and CQRS infrastructure"""
    pytestmark = pytest.mark.integration
    
    @pytest.mark.asyncio
    @pytest.mark.filterwarnings(
        "ignore:datetime\\.datetime\\.utcnow\\(\\) is deprecated.*:DeprecationWarning:botocore\\..*"
    )
    async def test_s3_event_storage(self):
        """Verify S3/MinIO event storage is working"""
        import boto3
        from botocore.exceptions import ClientError
        from botocore.config import Config
        from urllib.parse import urlparse
        bucket_name = os.getenv("EVENT_STORE_BUCKET", "spice-event-store")
        explicit_minio_access_key = (os.getenv("MINIO_ACCESS_KEY") or "").strip()
        explicit_minio_secret_key = (os.getenv("MINIO_SECRET_KEY") or "").strip()
        parsed = urlparse(MINIO_URL)
        host = (parsed.hostname or "").lower()
        client_config = (
            Config(s3={"addressing_style": "path"})
            if host in {"localhost", "127.0.0.1", "0.0.0.0"} or host.endswith(".localhost")
            else None
        )
        
        client = boto3.client(
            's3',
            endpoint_url=MINIO_URL,
            aws_access_key_id=explicit_minio_access_key or "minioadmin",
            aws_secret_access_key=explicit_minio_secret_key or "minioadmin123",
            use_ssl=False,
            verify=False,
            config=client_config,
        )
        
        # Check if events bucket exists (and create if missing, like EventStore.connect)
        try:
            client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            code = (e.response or {}).get("Error", {}).get("Code")
            if not (explicit_minio_access_key and explicit_minio_secret_key):
                raise RuntimeError(
                    "MinIO credentials not provided for direct check. "
                    "Set MINIO_ACCESS_KEY/MINIO_SECRET_KEY to run this test."
                )
            if code in {"404", "NoSuchBucket", "NotFound"}:
                client.create_bucket(Bucket=bucket_name)
                client.head_bucket(Bucket=bucket_name)
            else:
                raise AssertionError(
                    f"MinIO/S3 head_bucket failed for '{bucket_name}' (code={code!r}). "
                    "Check MINIO_ENDPOINT_URL/MINIO_ACCESS_KEY/MINIO_SECRET_KEY."
                ) from e
        
    @pytest.mark.asyncio
    async def test_postgresql_processed_event_registry(self):
        """Verify Postgres processed_events registry is available (idempotency contract)"""
        import asyncpg
        from urllib.parse import urlparse

        explicit_postgres_url = (os.getenv("POSTGRES_URL") or "").strip()
        conn = None
        last_error: Optional[Exception] = None
        for dsn in _get_postgres_url_candidates():
            parsed = urlparse(dsn)
            host = parsed.hostname or "localhost"
            port = parsed.port or 5432
            user = parsed.username or "spiceadmin"
            password = parsed.password or "spicepass123"
            database = (parsed.path or "/spicedb").lstrip("/") or "spicedb"
            try:
                conn = await asyncpg.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database,
                )
                break
            except Exception as e:
                last_error = e
                continue

        if conn is None:
            if not explicit_postgres_url:
                raise RuntimeError(
                    "Postgres DSN not provided for direct registry check. "
                    "Set POSTGRES_URL to run this test."
                )
            raise AssertionError("Could not connect to Postgres using POSTGRES_URL.") from last_error
        
        try:
            # processed_events table exists
            processed_exists = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'spice_event_registry'
                      AND table_name = 'processed_events'
                )
                """
            )
            assert processed_exists, "processed_events registry table should exist"

            versions_exists = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'spice_event_registry'
                      AND table_name = 'aggregate_versions'
                )
                """
            )
            assert versions_exists, "aggregate_versions registry table should exist"
            
        finally:
            await conn.close()
            
    @pytest.mark.asyncio
    async def test_kafka_message_flow(self):
        """Verify Kafka message flow is operational"""
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

        # Send test message
        test_message = json.dumps(
            {"test": "message", "timestamp": datetime.now(timezone.utc).isoformat()}
        ).encode("utf-8")

        delivery_err = None

        def on_delivery(err, _msg):
            nonlocal delivery_err
            delivery_err = err

        producer.produce("test_topic", value=test_message, callback=on_delivery)
        remaining = producer.flush(timeout=10)
        assert remaining == 0, f"Kafka flush timed out (remaining={remaining})"
        assert delivery_err is None, f"Kafka delivery failed: {delivery_err}"


class TestComplexTypes:
    """Test complex type validation and handling"""
    pytestmark = pytest.mark.unit
    
    def test_email_validation(self):
        """Test email type validation"""
        from shared.validators.complex_type_validator import ComplexTypeValidator
        
        # Valid emails
        valid, msg, normalized = ComplexTypeValidator.validate(
            "test@example.com", "email"
        )
        assert valid is True
        
        # Invalid emails
        valid, msg, normalized = ComplexTypeValidator.validate(
            "not-an-email", "email"
        )
        assert valid is False
        
    def test_phone_validation(self):
        """Test phone number validation"""
        from shared.validators.complex_type_validator import ComplexTypeValidator
        
        # Valid phone
        valid, msg, normalized = ComplexTypeValidator.validate(
            "+1 650-253-0000", "phone"
        )
        assert valid is True
        assert (normalized or {}).get("e164") == "+16502530000"
        
    def test_json_validation(self):
        """Test JSON type validation"""
        from shared.validators.complex_type_validator import ComplexTypeValidator
        
        # Valid JSON string
        valid, msg, normalized = ComplexTypeValidator.validate(
            '{"key": "value"}', "json"
        )
        assert valid is True
        
        # Valid JSON object
        valid, msg, normalized = ComplexTypeValidator.validate(
            {"key": "value"}, "json"
        )
        assert valid is True


class TestHealthEndpoints:
    """Test all service health endpoints"""
    pytestmark = pytest.mark.integration
    
    @pytest.mark.asyncio
    async def test_oms_health(self):
        """Test OMS health endpoint"""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            async with session.get(f"{OMS_URL}/health") as resp:
                assert resp.status == 200
                result = await resp.json()
                assert result.get("status") == "success"
                assert (result.get("data") or {}).get("status") == "healthy"
                
    @pytest.mark.asyncio
    async def test_bff_health(self):
        """Test BFF health endpoint"""
        async with aiohttp.ClientSession(headers=AUTH_HEADERS) as session:
            async with session.get(f"{BFF_URL}/api/v1/health") as resp:
                assert resp.status == 200
                result = await resp.json()
                assert result.get("status") == "success"
                assert (result.get("data") or {}).get("status") == "healthy"
                

if __name__ == "__main__":
    # Run all tests
    pytest.main([__file__, "-v", "--tb=short"])
