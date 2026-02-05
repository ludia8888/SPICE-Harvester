"""
E2E Smoke Test for Strong Consistency Fixes.

Tests the critical consistency fixes:
1. SafeKafkaConsumer with isolation.level=read_committed
2. Rebalance callback handling
3. Outbox atomicity with delivery tracking
4. Partition key ordering strategy

Prerequisites:
- All infrastructure running (Postgres, Kafka, Redis, MinIO, LakeFS)
- BFF running on localhost:8002

Run with:
    pytest tests/test_consistency_e2e_smoke.py -v --tb=short
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import os
import socket
import time
import uuid
from typing import Dict
from urllib.parse import urlparse

import aiohttp
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic

# Infrastructure URLs (use conftest.py defaults via environment)
from shared.config.settings import get_settings

_settings = get_settings()

BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or _settings.services.bff_base_url).rstrip("/")
OMS_URL = (os.getenv("OMS_BASE_URL") or os.getenv("OMS_URL") or _settings.services.oms_base_url).rstrip("/")
FUNNEL_URL = (os.getenv("FUNNEL_BASE_URL") or os.getenv("FUNNEL_URL") or _settings.services.funnel_base_url).rstrip("/")
AGENT_URL = (os.getenv("AGENT_BASE_URL") or os.getenv("AGENT_URL") or _settings.services.agent_base_url).rstrip("/")
INGEST_RECONCILER_PORT = (
    os.getenv("INGEST_RECONCILER_PORT_HOST")
    or os.getenv("INGEST_RECONCILER_PORT")
    or "8012"
).strip()
INGEST_RECONCILER_URL = (
    os.getenv("INGEST_RECONCILER_BASE_URL") or f"http://127.0.0.1:{INGEST_RECONCILER_PORT}"
).rstrip("/")

MINIO_ENDPOINT_URL = (os.getenv("MINIO_ENDPOINT_URL") or _settings.storage.minio_endpoint_url).rstrip("/")
LAKEFS_API_URL = (os.getenv("LAKEFS_API_URL") or _settings.storage.lakefs_api_url or "").rstrip("/")
ELASTICSEARCH_URL = (os.getenv("ELASTICSEARCH_URL") or _settings.database.elasticsearch_url).rstrip("/")
TERMINUS_URL = (os.getenv("TERMINUS_SERVER_URL") or _settings.database.terminus_url).rstrip("/")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_SERVERS") or "localhost:39092"
POSTGRES_DSN = os.getenv("POSTGRES_URL") or os.getenv("POSTGRES_DSN") or "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb"
REDIS_URL = os.getenv("REDIS_URL") or "redis://:spicepass123@localhost:6379"

pytestmark = [pytest.mark.smoke]

def _kafka_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_SERVERS})


def _auth_headers() -> Dict[str, str]:
    """Get auth headers for BFF requests."""
    from tests.utils.auth import bff_auth_headers
    return bff_auth_headers()

def _extract_constant_dict(node: ast.AST) -> dict[str, object]:
    """Extract a best-effort {str: constant} mapping from an ast.Dict node."""
    if not isinstance(node, ast.Dict):
        return {}
    out: dict[str, object] = {}
    for key, value in zip(node.keys, node.values):
        if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
            continue
        if isinstance(value, ast.Constant):
            out[key.value] = value.value
    return out


def _extract_constant_kwargs(node: ast.AST) -> dict[str, object]:
    """Extract a best-effort {str: constant} mapping from an ast.Call keyword list."""
    if not isinstance(node, ast.Call):
        return {}
    out: dict[str, object] = {}
    for kw in node.keywords:
        if not kw.arg:
            continue
        if isinstance(kw.value, ast.Constant):
            out[kw.arg] = kw.value.value
    return out


# =============================================================================
# Test: SafeKafkaConsumer Configuration
# =============================================================================


class TestSafeKafkaConsumerConfiguration:
    """Verify SafeKafkaConsumer enforces critical settings."""

    def test_enforced_isolation_level(self) -> None:
        """SafeKafkaConsumer should always use read_committed."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        assert "isolation.level" in SafeKafkaConsumer.ENFORCED_SETTINGS
        assert SafeKafkaConsumer.ENFORCED_SETTINGS["isolation.level"] == "read_committed"

    def test_enforced_auto_commit_disabled(self) -> None:
        """SafeKafkaConsumer should disable auto-commit."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        assert "enable.auto.commit" in SafeKafkaConsumer.ENFORCED_SETTINGS
        assert SafeKafkaConsumer.ENFORCED_SETTINGS["enable.auto.commit"] is False

    def test_workers_use_safe_consumer(self) -> None:
        """Verify all Kafka-consuming workers use SafeKafkaConsumer (no raw Consumer)."""
        import importlib.util

        worker_modules = [
            "objectify_worker.main",
            "pipeline_worker.main",
            "action_worker.main",
            "search_projection_worker.main",
            "connector_sync_worker.main",
            "ontology_worker.main",
            "instance_worker.main",
            "projection_worker.main",
        ]

        for module_name in worker_modules:
            spec = importlib.util.find_spec(module_name)
            if spec is None:
                pytest.fail(f"Module {module_name} not found (PYTHONPATH misconfigured?)")

            if not spec.origin:
                pytest.fail(f"Module {module_name} has no origin (namespace package?)")

            with open(spec.origin, "r", encoding="utf-8") as f:
                source = f.read()

            tree = ast.parse(source, filename=spec.origin)

            imports_raw_consumer = any(
                isinstance(node, ast.ImportFrom)
                and node.module == "confluent_kafka"
                and any(alias.name == "Consumer" for alias in node.names)
                for node in ast.walk(tree)
            )
            calls_raw_consumer = any(
                isinstance(node, ast.Call)
                and (
                    (isinstance(node.func, ast.Name) and node.func.id == "Consumer")
                    or (isinstance(node.func, ast.Attribute) and node.func.attr == "Consumer")
                )
                for node in ast.walk(tree)
            )
            uses_safe_consumer = any(
                (isinstance(node, ast.Name) and node.id == "SafeKafkaConsumer")
                or (isinstance(node, ast.Attribute) and node.attr == "SafeKafkaConsumer")
                for node in ast.walk(tree)
            )

            assert not imports_raw_consumer, f"{module_name} must not import confluent_kafka.Consumer"
            assert not calls_raw_consumer, f"{module_name} must not instantiate raw Consumer()"
            assert uses_safe_consumer, f"{module_name} must use SafeKafkaConsumer (directly or via a factory)"

    def test_no_raw_consumer_in_worker_tree(self) -> None:
        """Hard-fail if any *_worker module imports raw Consumer (regression guard)."""
        from pathlib import Path

        backend_root = Path(__file__).resolve().parents[1]
        worker_dirs = sorted([p for p in backend_root.iterdir() if p.is_dir() and p.name.endswith("_worker")])
        assert worker_dirs, "No *_worker directories found under backend/ (unexpected repo layout)"

        offenders: list[str] = []
        for worker_dir in worker_dirs:
            for path in worker_dir.rglob("*.py"):
                if path.name.startswith("test_"):
                    continue

                text = path.read_text(encoding="utf-8")
                try:
                    tree = ast.parse(text, filename=str(path))
                except SyntaxError as exc:
                    pytest.fail(f"SyntaxError while parsing {path}: {exc}")

                imported_ck_names: set[str] = set()
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            if alias.name == "confluent_kafka":
                                imported_ck_names.add(alias.asname or alias.name)

                imports_raw_consumer = any(
                    isinstance(node, ast.ImportFrom)
                    and node.module == "confluent_kafka"
                    and any(alias.name == "Consumer" for alias in node.names)
                    for node in ast.walk(tree)
                )
                attribute_raw_consumer = any(
                    isinstance(node, ast.Attribute)
                    and node.attr == "Consumer"
                    and isinstance(node.value, ast.Name)
                    and node.value.id in imported_ck_names
                    for node in ast.walk(tree)
                )
                calls_raw_consumer = any(
                    isinstance(node, ast.Call)
                    and (
                        (isinstance(node.func, ast.Name) and node.func.id == "Consumer")
                        or (
                            isinstance(node.func, ast.Attribute)
                            and node.func.attr == "Consumer"
                            and isinstance(node.func.value, ast.Name)
                            and node.func.value.id in imported_ck_names
                        )
                    )
                    for node in ast.walk(tree)
                )

                if imports_raw_consumer or attribute_raw_consumer or calls_raw_consumer:
                    offenders.append(str(path))

        assert not offenders, "Raw confluent_kafka.Consumer detected in worker code:\n" + "\n".join(offenders)


# =============================================================================
# Test: Kafka Producer Configuration
# =============================================================================


class TestKafkaProducerConfiguration:
    """Verify critical producer-side idempotence settings are present where required."""

    def test_pipeline_job_queue_producer_is_idempotent(self) -> None:
        import importlib.util

        spec = importlib.util.find_spec("shared.services.pipeline.pipeline_job_queue")
        assert spec and spec.origin, "Could not locate shared.services.pipeline.pipeline_job_queue"
        with open(spec.origin, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source, filename=spec.origin)

        legacy_config: dict[str, object] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not (isinstance(node.func, ast.Name) and node.func.id == "Producer"):
                continue
            if not node.args or not isinstance(node.args[0], ast.Dict):
                continue
            legacy_config = _extract_constant_dict(node.args[0])
            break

        if legacy_config:
            assert legacy_config.get("acks") == "all"
            assert legacy_config.get("enable.idempotence") is True
            assert legacy_config.get("max.in.flight.requests.per.connection") == 5
            return

        factory_kwargs: dict[str, object] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not (isinstance(node.func, ast.Name) and node.func.id in {"create_kafka_producer", "create_kafka_dlq_producer"}):
                continue
            factory_kwargs = _extract_constant_kwargs(node)
            break

        assert factory_kwargs, "Expected Producer({...}) or create_kafka_producer(...) in pipeline_job_queue"

        acks = factory_kwargs.get("acks")
        if acks is None:
            from shared.services.kafka.producer_factory import DEFAULT_ACKS
            assert DEFAULT_ACKS == "all"
        else:
            assert acks == "all"
        assert factory_kwargs.get("enable_idempotence") is True
        assert factory_kwargs.get("max_in_flight_requests_per_connection") == 5

    def test_connector_trigger_producer_is_idempotent(self) -> None:
        import importlib.util

        spec = importlib.util.find_spec("connector_trigger_service.main")
        assert spec and spec.origin, "Could not locate connector_trigger_service.main"
        with open(spec.origin, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source, filename=spec.origin)

        legacy_config: dict[str, object] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                continue
            if node.targets[0].id != "kafka_config":
                continue
            legacy_config = _extract_constant_dict(node.value)
            break

        if legacy_config:
            assert legacy_config.get("acks") == "all"
            assert legacy_config.get("enable.idempotence") is True
            assert legacy_config.get("max.in.flight.requests.per.connection") == 5
            return

        factory_kwargs: dict[str, object] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not (isinstance(node.func, ast.Name) and node.func.id in {"create_kafka_producer", "create_kafka_dlq_producer"}):
                continue
            factory_kwargs = _extract_constant_kwargs(node)
            break

        assert factory_kwargs, "Expected kafka_config={{...}} or create_kafka_producer(...) in connector_trigger_service"

        acks = factory_kwargs.get("acks")
        if acks is None:
            from shared.services.kafka.producer_factory import DEFAULT_ACKS
            assert DEFAULT_ACKS == "all"
        else:
            assert acks == "all"
        assert factory_kwargs.get("enable_idempotence") is True
        assert factory_kwargs.get("max_in_flight_requests_per_connection") == 5


# =============================================================================
# Test: Kafka Connectivity
# =============================================================================


class TestKafkaConnectivity:
    """Verify Kafka connectivity and topic access."""

    @pytest.mark.requires_infra
    def test_kafka_topics_exist(self) -> None:
        """Verify all declared Kafka topics exist (SSoT: AppConfig.get_all_topics)."""
        from shared.config.app_config import AppConfig

        admin = _kafka_admin()
        metadata = admin.list_topics(timeout=10)
        actual = set(metadata.topics.keys())

        expected = set(AppConfig.get_all_topics())
        missing = sorted([t for t in expected if t not in actual])
        assert not missing, f"Missing Kafka topics: {missing!r} (bootstrap={KAFKA_SERVERS})"

    @pytest.mark.requires_infra
    def test_kafka_consumer_groups_listable(self) -> None:
        """Verify consumer groups can be listed (connectivity + broker feature check)."""
        admin = _kafka_admin()
        future = admin.list_consumer_groups(request_timeout=10.0)
        result = future.result()
        assert result is not None
        errors = getattr(result, "errors", None)
        assert not errors, f"Kafka list_consumer_groups returned errors: {errors!r} (bootstrap={KAFKA_SERVERS})"

    @pytest.mark.requires_infra
    def test_expected_consumer_groups_present(self) -> None:
        """
        Verify critical worker consumer groups exist (proxy for "workers are actually running").

        This is intentionally strict: if a worker isn't joined to Kafka, the stack is not production-ready.
        """
        from shared.config.app_config import AppConfig

        settings = get_settings()

        expected: set[str] = {
            str(AppConfig.INSTANCE_WORKER_GROUP or "").strip() or "instance-worker-group",
            str(AppConfig.ONTOLOGY_WORKER_GROUP or "").strip() or "ontology-worker-group",
            str(AppConfig.ACTION_WORKER_GROUP or "").strip() or "action-worker-group",
            str(AppConfig.PROJECTION_WORKER_GROUP or "").strip() or "projection-worker-group",
            str(AppConfig.OBJECTIFY_JOBS_GROUP or "").strip() or "objectify-worker-group",
            str(settings.pipeline.jobs_group or "").strip() or "pipeline-worker-group",
            str(getattr(settings.workers.connector_sync, "group", "") or "").strip() or "connector-sync-worker-group",
        }

        # Optional components
        if bool(settings.workers.search_projection.enabled):
            expected.add(str(AppConfig.SEARCH_PROJECTION_GROUP or "").strip() or "search-projection-worker")

        admin = _kafka_admin()
        deadline = time.monotonic() + 30.0
        last_errors = None
        last_groups: set[str] = set()

        while True:
            future = admin.list_consumer_groups(request_timeout=10.0)
            result = future.result()
            errors = list(getattr(result, "errors", None) or [])
            if errors:
                last_errors = errors
            else:
                groups = getattr(result, "valid", None) or []
                gids = {str(getattr(g, "group_id", "") or "").strip() for g in groups}
                gids.discard("")
                last_groups = gids

                missing = sorted([g for g in expected if g not in gids])
                if not missing:
                    return

            if time.monotonic() >= deadline:
                pytest.fail(
                    "Kafka consumer groups missing (workers not running / not joined to Kafka). "
                    f"missing={sorted(expected - last_groups)!r} "
                    f"seen={sorted(last_groups)!r} "
                    f"errors={last_errors!r} "
                    f"(bootstrap={KAFKA_SERVERS})"
                )
            time.sleep(1.0)

    @pytest.mark.requires_infra
    def test_read_committed_filters_aborted_transactions(self) -> None:
        """
        End-to-end verification: read_committed consumers must NOT see aborted transactional messages.

        This is the core safety property behind "strong-consistency" reads from Kafka.
        """
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        topic = f"spice-smoke-tx-{uuid.uuid4().hex[:8]}"
        admin = _kafka_admin()
        try:
            futures = admin.create_topics(
                [NewTopic(topic, num_partitions=1, replication_factor=1)],
                request_timeout=10.0,
            )
            for _t, fut in futures.items():
                # Topic may already exist; treat as success.
                try:
                    fut.result()
                except Exception:
                    pass
        except Exception:
            # If topic auto-create is enabled, we can proceed even if explicit create fails.
            pass

        aborted_value = f"aborted-{uuid.uuid4().hex}".encode("utf-8")
        committed_value = f"committed-{uuid.uuid4().hex}".encode("utf-8")

        producer = Producer(
            {
                "bootstrap.servers": KAFKA_SERVERS,
                "client.id": "smoke-tx-producer",
                "enable.idempotence": True,
                "acks": "all",
                "transactional.id": f"smoke-tx-{uuid.uuid4().hex}",
            }
        )

        try:
            producer.init_transactions(10.0)

            producer.begin_transaction()
            producer.produce(topic=topic, key=b"k", value=aborted_value)
            producer.flush(10.0)
            producer.abort_transaction(10.0)

            producer.begin_transaction()
            producer.produce(topic=topic, key=b"k", value=committed_value)
            producer.flush(10.0)
            producer.commit_transaction(10.0)
        finally:
            try:
                producer.flush(5.0)
            except Exception:
                pass

        consumer = SafeKafkaConsumer(
            group_id=f"smoke-tx-consumer-{uuid.uuid4().hex[:8]}",
            topics=[topic],
            service_name="smoke-test",
            extra_config={"bootstrap.servers": KAFKA_SERVERS},
        )
        seen: list[bytes] = []
        try:
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline and committed_value not in seen:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                if msg.value() is not None:
                    seen.append(msg.value())
                    consumer.commit_sync(msg)
        finally:
            consumer.close()

        try:
            assert aborted_value not in seen, "read_committed consumer observed aborted transactional message"
            assert committed_value in seen, "read_committed consumer did not observe committed transactional message"
        finally:
            with contextlib.suppress(Exception):
                futures = admin.delete_topics([topic], operation_timeout=10)
                for fut in futures.values():
                    with contextlib.suppress(Exception):
                        fut.result()


# =============================================================================
# Test: BFF API Health
# =============================================================================


class TestBFFHealth:
    """Verify BFF is running and healthy."""

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_bff_health_endpoint(self) -> None:
        """BFF health endpoint should return success."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BFF_URL}/api/v1/health") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "success"
                assert data["data"]["status"] == "healthy"

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_bff_databases_connected(self) -> None:
        """BFF should have database connections."""
        async with aiohttp.ClientSession() as session:
            headers = _auth_headers()
            async with session.get(f"{BFF_URL}/api/v1/databases", headers=headers) as resp:
                # 200 or 404 (no databases) are both valid. Auth failures are not.
                assert resp.status in (200, 404), f"Unexpected status={resp.status} from /api/v1/databases"


# =============================================================================
# Test: MSA Service Health (Full Stack)
# =============================================================================


class TestMSAServiceHealth:
    """Verify all first-class services are reachable (full stack)."""

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_oms_health_endpoint(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{OMS_URL}/health") as resp:
                assert resp.status == 200

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_funnel_health_endpoint(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{FUNNEL_URL}/health") as resp:
                assert resp.status == 200

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_agent_health_endpoint(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{AGENT_URL}/health") as resp:
                assert resp.status == 200

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_ingest_reconciler_health_endpoint(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{INGEST_RECONCILER_URL}/health") as resp:
                assert resp.status == 200


# =============================================================================
# Test: Database Connectivity
# =============================================================================


class TestDatabaseConnectivity:
    """Verify database connectivity."""

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_postgres_connection(self) -> None:
        """PostgreSQL should be connectable."""
        try:
            import asyncpg
        except ImportError:
            pytest.fail("asyncpg not installed (required for requires_infra tests)")

        try:
            conn = await asyncpg.connect(POSTGRES_DSN)
            result = await conn.fetchval("SELECT 1")
            assert result == 1
            await conn.close()
        except Exception as e:
            pytest.fail(f"PostgreSQL connection failed: {e}")

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_redis_connection(self) -> None:
        """Redis should be connectable and require authentication (prod safety)."""
        try:
            import redis.asyncio as aioredis
            from redis.exceptions import AuthenticationError
        except ImportError:
            pytest.fail("redis not installed (required for requires_infra tests)")

        parsed = urlparse(REDIS_URL)
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
        db = parsed.path.lstrip("/") or "0"
        password = parsed.password or ""

        if not password:
            pytest.fail(
                "REDIS_URL must include a password for production safety "
                f"(got {REDIS_URL!r})."
            )

        # 1) Authenticated ping must succeed.
        authed = aioredis.from_url(REDIS_URL)
        try:
            assert await authed.ping() is True
        except AuthenticationError as e:
            msg = str(e)
            if "without any password configured" in msg.lower():
                pytest.fail(
                    "Redis is running WITHOUT requirepass, but REDIS_URL includes a password. "
                    "This is an unsafe configuration for production and usually means you're not "
                    "talking to the intended Redis instance (e.g. Homebrew Redis on 6379). "
                    f"Fix by starting the stack Redis with requirepass or adjusting REDIS_URL. "
                    f"(url={REDIS_URL!r}, error={e})"
                )
            pytest.fail(f"Redis AUTH failed (url={REDIS_URL!r}): {e}")
        except Exception as e:
            pytest.fail(f"Redis connection failed (url={REDIS_URL!r}): {e}")
        finally:
            await authed.aclose()

        # 2) Unauthenticated ping must FAIL (ensures requirepass is enforced).
        noauth_url = f"redis://{host}:{port}/{db}"
        noauth = aioredis.from_url(noauth_url)
        try:
            await noauth.ping()
            pytest.fail(
                "Redis accepted unauthenticated commands; requirepass/auth is not enforced. "
                f"(noauth_url={noauth_url!r})"
            )
        except AuthenticationError:
            pass
        finally:
            await noauth.aclose()


class TestInfraConnectivity:
    """Verify non-DB infra dependencies (S3/MinIO, lakeFS, Elasticsearch, TerminusDB)."""

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_minio_health(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{MINIO_ENDPOINT_URL}/minio/health/ready") as resp:
                assert 200 <= resp.status < 300

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_lakefs_health(self) -> None:
        if not LAKEFS_API_URL:
            pytest.fail("LAKEFS_API_URL is not configured")
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{LAKEFS_API_URL}/api/v1/healthcheck") as resp:
                assert 200 <= resp.status < 300

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_elasticsearch_health(self) -> None:
        if not ELASTICSEARCH_URL:
            pytest.fail("ELASTICSEARCH_URL is not configured")
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{ELASTICSEARCH_URL}/_cluster/health") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data.get("status") in {"yellow", "green"}, f"Elasticsearch unhealthy: {data!r}"

    @pytest.mark.requires_infra
    def test_terminusdb_port_open(self) -> None:
        parsed = urlparse(TERMINUS_URL)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 6363
        try:
            with socket.create_connection((host, port), timeout=3.0):
                pass
        except Exception as e:
            pytest.fail(f"TerminusDB not reachable on {host}:{port} ({TERMINUS_URL!r}): {e}")


class TestSystemWiring:
    """
    Verify producer/consumer wiring between key MSAs (no mocks).

    These tests validate that background services are actually doing work,
    not merely importable.
    """

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_message_relay_publishes_event_store_appends(self) -> None:
        """
        Message relay must publish newly appended Event Store events to Kafka.

        This validates: MinIO(S3) -> index -> message-relay -> Kafka.
        """
        from datetime import datetime, timezone

        from shared.models.event_envelope import EventEnvelope
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer
        from shared.services.storage.event_store import event_store

        topic = f"spice-smoke-relay-{uuid.uuid4().hex[:8]}"
        admin = _kafka_admin()
        try:
            try:
                futures = admin.create_topics(
                    [NewTopic(topic, num_partitions=1, replication_factor=1)],
                    request_timeout=10.0,
                )
                for _t, fut in futures.items():
                    with contextlib.suppress(Exception):
                        fut.result()
            except Exception:
                pass

            await event_store.connect()
            envelope = EventEnvelope(
                event_id=f"smoke-relay-{uuid.uuid4().hex}",
                event_type="SMOKE_MESSAGE_RELAY",
                aggregate_type="smoke",
                aggregate_id=f"smoke-{uuid.uuid4().hex[:8]}",
                occurred_at=datetime.now(timezone.utc),
                actor="smoke-test",
                data={"kind": "smoke", "component": "message-relay"},
                metadata={"kafka_topic": topic, "kind": "smoke", "service": "smoke-test"},
            )
            await event_store.append_event(envelope)

            consumer = SafeKafkaConsumer(
                group_id=f"smoke-relay-consumer-{uuid.uuid4().hex[:8]}",
                topics=[topic],
                service_name="smoke-test",
                extra_config={"bootstrap.servers": KAFKA_SERVERS},
            )
            try:
                deadline = time.monotonic() + 30.0
                while time.monotonic() < deadline:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None or msg.error() or not msg.value():
                        continue
                    try:
                        got = EventEnvelope.model_validate_json(msg.value())
                    except Exception:
                        continue
                    if str(got.event_id) == str(envelope.event_id):
                        consumer.commit_sync(msg)
                        return
            finally:
                consumer.close()

            pytest.fail(
                "Message relay did not publish appended event within timeout "
                f"(topic={topic!r}, event_id={envelope.event_id!r})."
            )
        finally:
            with contextlib.suppress(Exception):
                futures = admin.delete_topics([topic], operation_timeout=10)
                for fut in futures.values():
                    with contextlib.suppress(Exception):
                        fut.result()

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_connector_trigger_publishes_outbox(self) -> None:
        """
        Connector trigger service must publish pending outbox items to Kafka.

        This validates: Postgres(outbox) -> connector-trigger-service -> Kafka.
        """
        from shared.config.app_config import AppConfig
        from shared.models.event_envelope import EventEnvelope
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer
        from shared.services.registries.connector_registry import ConnectorRegistry

        registry = ConnectorRegistry()
        await registry.initialize()
        try:
            envelope = await registry.record_poll_result(
                source_type="smoke",
                source_id=f"smoke-{uuid.uuid4().hex[:8]}",
                current_cursor=uuid.uuid4().hex,
                kafka_topic=AppConfig.CONNECTOR_UPDATES_TOPIC,
            )
        finally:
            await registry.close()

        assert envelope is not None, "Expected record_poll_result to enqueue a connector update outbox item"

        consumer = SafeKafkaConsumer(
            group_id=f"smoke-connector-trigger-{uuid.uuid4().hex[:8]}",
            topics=[AppConfig.CONNECTOR_UPDATES_TOPIC],
            service_name="smoke-test",
            extra_config={"bootstrap.servers": KAFKA_SERVERS},
        )
        try:
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline:
                msg = consumer.poll(timeout=1.0)
                if msg is None or msg.error() or not msg.value():
                    continue
                try:
                    got = EventEnvelope.model_validate_json(msg.value())
                except Exception:
                    continue
                if str(got.event_id) == str(envelope.event_id):
                    consumer.commit_sync(msg)
                    return
        finally:
            consumer.close()

        pytest.fail(
            "Connector trigger service did not publish outbox item within timeout "
            f"(topic={AppConfig.CONNECTOR_UPDATES_TOPIC!r}, event_id={envelope.event_id!r})."
        )

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_pipeline_scheduler_enqueues_scheduled_pipeline(self) -> None:
        """
        Pipeline scheduler must enqueue due scheduled pipelines to Kafka.

        This validates: Postgres(pipeline registry) -> pipeline-scheduler -> Kafka(pipeline-jobs).
        """
        import json

        from shared.config.app_config import AppConfig
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer
        from shared.services.registries.pipeline_registry import PipelineRegistry

        settings = get_settings()

        consumer = SafeKafkaConsumer(
            group_id=f"smoke-pipeline-scheduler-{uuid.uuid4().hex[:8]}",
            topics=[AppConfig.PIPELINE_JOBS_TOPIC],
            service_name="smoke-test",
            extra_config={"bootstrap.servers": KAFKA_SERVERS, "auto.offset.reset": "latest"},
        )
        if not consumer.wait_for_assignment(timeout_seconds=10.0):
            pytest.fail(
                "Kafka consumer did not receive partition assignment in time "
                f"(topic={AppConfig.PIPELINE_JOBS_TOPIC!r}, bootstrap={KAFKA_SERVERS!r})."
            )

        registry = PipelineRegistry()
        await registry.initialize()

        pipeline_id: str = ""
        try:
            record = await registry.create_pipeline(
                db_name="smoke_db",
                name=f"smoke_pipeline_{uuid.uuid4().hex[:8]}",
                description="smoke pipeline scheduler",
                pipeline_type="batch",
                location="smoke",
                status="deployed",
                branch="main",
                schedule_interval_seconds=3600,  # due immediately once; avoids repeated triggers on retries
            )
            pipeline_id = str(record.pipeline_id)

            # The scheduler polls on a fixed cadence (default 30s).
            deadline = time.monotonic() + float(settings.pipeline.scheduler_poll_seconds) + 40.0
            while time.monotonic() < deadline:
                msg = consumer.poll(timeout=1.0)
                if msg is None or msg.error() or not msg.value():
                    continue
                try:
                    payload = json.loads(msg.value())
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                if str(payload.get("pipeline_id") or "") != pipeline_id:
                    continue
                job_id = str(payload.get("job_id") or "")
                if job_id.startswith(f"schedule-{pipeline_id}-"):
                    consumer.commit_sync(msg)
                    return

            pytest.fail(
                "Pipeline scheduler did not enqueue a scheduled pipeline within timeout "
                f"(pipeline_id={pipeline_id!r}, poll_seconds={settings.pipeline.scheduler_poll_seconds}, "
                f"topic={AppConfig.PIPELINE_JOBS_TOPIC!r})."
            )
        finally:
            consumer.close()
            with contextlib.suppress(Exception):
                if pipeline_id and registry._pool:
                    async with registry._pool.acquire() as conn:
                        await conn.execute(
                            f"DELETE FROM {registry._schema}.pipelines WHERE pipeline_id = $1::uuid",
                            pipeline_id,
                        )
            with contextlib.suppress(Exception):
                await registry.close()

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_action_outbox_emits_action_applied_event(self) -> None:
        """
        Action outbox worker must emit ActionApplied into the event store, and the relay must publish it to Kafka.

        This validates: Postgres(ActionLog) -> action-outbox-worker -> MinIO(event store) -> message-relay -> Kafka(action_events).
        """
        from datetime import datetime, timezone
        from uuid import NAMESPACE_URL, uuid4, uuid5

        from shared.config.app_config import AppConfig
        from shared.models.event_envelope import EventEnvelope
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer
        from shared.services.registries.action_log_registry import ActionLogRegistry, ActionLogStatus
        from shared.services.storage.lakefs_client import LakeFSClient
        from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
        from shared.utils.writeback_paths import ref_key, writeback_patchset_key

        settings = get_settings()

        consumer = SafeKafkaConsumer(
            group_id=f"smoke-action-outbox-{uuid.uuid4().hex[:8]}",
            topics=[AppConfig.ACTION_EVENTS_TOPIC],
            service_name="smoke-test",
            extra_config={"bootstrap.servers": KAFKA_SERVERS, "auto.offset.reset": "latest"},
        )
        if not consumer.wait_for_assignment(timeout_seconds=10.0):
            pytest.fail(
                "Kafka consumer did not receive partition assignment in time "
                f"(topic={AppConfig.ACTION_EVENTS_TOPIC!r}, bootstrap={KAFKA_SERVERS!r})."
            )

        repo = str(AppConfig.ONTOLOGY_WRITEBACK_REPO or "ontology-writeback").strip() or "ontology-writeback"
        patchset_branch = f"smoke-patchset-{uuid.uuid4().hex[:8]}"

        lakefs_client = LakeFSClient()
        lakefs_storage = create_lakefs_storage_service(settings)
        assert lakefs_storage is not None, "LakeFSStorageService unavailable (boto3 missing?)"

        action_logs = ActionLogRegistry()
        await action_logs.connect()

        action_log_uuid = uuid4()
        action_log_id = str(action_log_uuid)
        expected_event_id = str(uuid5(NAMESPACE_URL, f"action-applied:{action_log_id}"))

        commit_id = ""
        try:
            # 1) Create a minimal writeback patchset artifact at a fresh commit id.
            await lakefs_client.create_branch(repository=repo, name=patchset_branch, source="main")
            patchset_key = ref_key(patchset_branch, writeback_patchset_key(action_log_id))
            await lakefs_storage.save_json(
                bucket=repo,
                key=patchset_key,
                data={
                    "targets": [],
                    "metadata": {"submitted_at": datetime.now(timezone.utc).isoformat()},
                },
            )
            commit_id = await lakefs_client.commit(
                repository=repo,
                branch=patchset_branch,
                message=f"smoke patchset {action_log_id}",
                metadata={"kind": "smoke_patchset", "action_log_id": action_log_id},
            )

            # 2) Create an action log and move it to COMMIT_WRITTEN so the outbox worker picks it up.
            await action_logs.create_log(
                action_log_id=action_log_uuid,
                db_name="smoke_db",
                action_type_id="SMOKE_ACTION",
                input_payload={"kind": "smoke"},
                writeback_target={"repo": repo, "branch": "main"},
                metadata={"kind": "smoke", "service": "smoke-test"},
            )
            await action_logs.mark_commit_written(action_log_id=action_log_id, writeback_commit_id=commit_id)

            # 3) Observe the ActionApplied event on Kafka.
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline:
                msg = consumer.poll(timeout=1.0)
                if msg is None or msg.error() or not msg.value():
                    continue
                try:
                    env = EventEnvelope.model_validate_json(msg.value())
                except Exception:
                    continue
                if str(env.event_id) == expected_event_id:
                    consumer.commit_sync(msg)
                    return

            pytest.fail(
                "Action outbox worker did not emit ActionApplied event within timeout "
                f"(event_id={expected_event_id!r}, topic={AppConfig.ACTION_EVENTS_TOPIC!r})."
            )
        finally:
            consumer.close()
            with contextlib.suppress(Exception):
                await lakefs_client.delete_branch(repository=repo, name=patchset_branch)
            # Best-effort cleanup: wait briefly for the worker to finish and remove the log.
            with contextlib.suppress(Exception):
                deadline = time.monotonic() + 10.0
                while time.monotonic() < deadline:
                    log = await action_logs.get_log(action_log_id=action_log_id)
                    if not log:
                        break
                    if log.status == ActionLogStatus.SUCCEEDED.value:
                        break
                    await asyncio.sleep(0.5)
                if action_logs._pool:
                    async with action_logs._pool.acquire() as conn:
                        await conn.execute(
                            f"DELETE FROM {action_logs._schema}.ontology_action_logs WHERE action_log_id = $1::uuid",
                            action_log_id,
                        )
            with contextlib.suppress(Exception):
                await action_logs.close()


# =============================================================================
# Test: Outbox Pattern Verification
# =============================================================================


class TestOutboxPatternVerification:
    """Verify outbox pattern implementation."""

    def test_objectify_outbox_uses_aggregate_key(self) -> None:
        """Objectify outbox should scope ordering by run_id when present."""
        import inspect
        from shared.services.events.objectify_outbox import ObjectifyOutboxPublisher

        # Get the source of _publish_batch
        source = inspect.getsource(ObjectifyOutboxPublisher._publish_batch)

        # Verify it uses db_name+branch for key (dependency order across datasets/classes)
        assert "db_name" in source, "Outbox should use db_name for partition key"
        assert "ontology_branch" in source, "Outbox should prefer ontology_branch when provided"
        assert "dataset_branch" in source, "Outbox should use dataset_branch for partition key"
        assert "aggregate_id" in source, "Outbox should track aggregate_id"
        assert "run_id" in source, "Outbox should scope ordering to run_id when present"

    def test_objectify_outbox_tracks_delivery(self) -> None:
        """Objectify outbox should track individual delivery status."""
        import inspect
        from shared.services.events.objectify_outbox import ObjectifyOutboxPublisher

        source = inspect.getsource(ObjectifyOutboxPublisher._publish_batch)

        # Verify delivery tracking
        assert "delivery_results" in source or "delivery_errors" in source, (
            "Outbox should track delivery results"
        )
        assert "on_delivery" in source, "Outbox should use delivery callbacks"

    def test_message_relay_prefers_ordering_key(self) -> None:
        """Message relay should prefer ordering_key for Kafka partitioning when present."""
        import inspect
        from message_relay.main import EventPublisher

        source = inspect.getsource(EventPublisher)
        assert "ordering_key" in source, "Message relay should support ordering_key partitioning"
        assert 'idx_data.get("ordering_key")' in source, "Message relay should read ordering_key from index entries"


# =============================================================================
# Test: ProcessedEventRegistry Idempotency
# =============================================================================


class TestProcessedEventRegistryIdempotency:
    """Verify ProcessedEventRegistry provides idempotency."""

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_claim_idempotency(self) -> None:
        """ProcessedEventRegistry.claim should be idempotent."""
        from shared.services.registries.processed_event_registry import (
            ProcessedEventRegistry,
            ClaimDecision,
        )

        schema = f"spice_event_registry_smoke_{uuid.uuid4().hex[:8]}"
        registry = ProcessedEventRegistry(schema=schema)
        try:
            await registry.connect()

            handler = f"e2e-test-{uuid.uuid4().hex[:8]}"
            event_id = f"event-{uuid.uuid4().hex[:8]}"

            # First claim should succeed
            claim1 = await registry.claim(handler=handler, event_id=event_id)
            assert claim1.decision == ClaimDecision.CLAIMED

            # Mark as done
            await registry.mark_done(handler=handler, event_id=event_id)

            # Second claim should return DUPLICATE_DONE
            claim2 = await registry.claim(handler=handler, event_id=event_id)
            assert claim2.decision == ClaimDecision.DUPLICATE_DONE

        finally:
            if registry._pool:
                async with registry._pool.acquire() as conn:
                    await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            await registry.close()

    @pytest.mark.asyncio
    @pytest.mark.requires_infra
    async def test_sequence_ordering(self) -> None:
        """ProcessedEventRegistry should enforce sequence ordering."""
        from shared.services.registries.processed_event_registry import (
            ProcessedEventRegistry,
            ClaimDecision,
        )

        schema = f"spice_event_registry_smoke_{uuid.uuid4().hex[:8]}"
        registry = ProcessedEventRegistry(schema=schema)
        try:
            await registry.connect()

            handler = f"e2e-test-{uuid.uuid4().hex[:8]}"
            aggregate_id = f"agg-{uuid.uuid4().hex[:8]}"

            # Claim event with sequence 5
            event1 = f"event-{uuid.uuid4().hex[:8]}"
            claim1 = await registry.claim(
                handler=handler,
                event_id=event1,
                aggregate_id=aggregate_id,
                sequence_number=5,
            )
            assert claim1.decision == ClaimDecision.CLAIMED
            # Include aggregate_id and sequence_number to update aggregate_versions table
            await registry.mark_done(
                handler=handler,
                event_id=event1,
                aggregate_id=aggregate_id,
                sequence_number=5,
            )

            # Claim event with sequence 3 (older) should be STALE
            event2 = f"event-{uuid.uuid4().hex[:8]}"
            claim2 = await registry.claim(
                handler=handler,
                event_id=event2,
                aggregate_id=aggregate_id,
                sequence_number=3,
            )
            assert claim2.decision == ClaimDecision.STALE

        finally:
            if registry._pool:
                async with registry._pool.acquire() as conn:
                    await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            await registry.close()


# =============================================================================
# Test: Pipeline Job Queue
# =============================================================================


class TestPipelineJobQueue:
    """Verify PipelineJobQueue implementation."""

    def test_pipeline_job_has_dedupe_key(self) -> None:
        """PipelineJob should auto-generate dedupe_key."""
        from shared.models.pipeline_job import PipelineJob

        job = PipelineJob(
            job_id=f"job-{uuid.uuid4().hex[:8]}",
            pipeline_id=f"pipeline-{uuid.uuid4().hex[:8]}",
            db_name="test_db",
            output_dataset_name="output",
        )

        assert job.dedupe_key is not None
        assert len(job.dedupe_key) == 32

    def test_pipeline_job_queue_uses_pipeline_id_key(self) -> None:
        """PipelineJobQueue should use pipeline_id as partition key."""
        import inspect
        from shared.services.pipeline.pipeline_job_queue import PipelineJobQueue

        source = inspect.getsource(PipelineJobQueue.publish)
        assert "pipeline_id" in source, "Should use pipeline_id for partition key"


# =============================================================================
# Test: End-to-End Flow Simulation
# =============================================================================


class TestEndToEndFlowSimulation:
    """Simulate E2E flows without actually processing jobs."""

    @pytest.mark.asyncio
    async def test_objectify_job_creation_flow(self) -> None:
        """Test creating an objectify job payload."""
        from shared.models.objectify_job import ObjectifyJob
        from shared.observability.context_propagation import kafka_headers_with_dedup

        # Create a job
        job = ObjectifyJob(
            job_id=f"e2e-job-{uuid.uuid4().hex[:8]}",
            dataset_id=f"ds-{uuid.uuid4().hex[:8]}",
            dataset_version_id=f"dsv-{uuid.uuid4().hex[:8]}",
            mapping_spec_id=f"ms-{uuid.uuid4().hex[:8]}",
            mapping_spec_version=1,
            target_class_id=f"class-{uuid.uuid4().hex[:8]}",
            db_name="test_db",
        )

        # Verify dedup headers
        headers = kafka_headers_with_dedup(
            job.model_dump(mode="json"),
            event_id=job.job_id,
            aggregate_id=job.dataset_id,
        )

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert job.dataset_id.encode() in dedup_headers[0][1]

    @pytest.mark.asyncio
    async def test_pipeline_job_creation_flow(self) -> None:
        """Test creating a pipeline job payload."""
        from shared.models.pipeline_job import PipelineJob
        from shared.observability.context_propagation import kafka_headers_with_dedup

        # Create a job
        job = PipelineJob(
            job_id=f"e2e-job-{uuid.uuid4().hex[:8]}",
            pipeline_id=f"pipeline-{uuid.uuid4().hex[:8]}",
            db_name="test_db",
            output_dataset_name="output",
            mode="preview",
            idempotency_key=f"idem-{uuid.uuid4().hex[:8]}",
        )

        # Verify dedupe_key was generated
        assert job.dedupe_key is not None
        assert len(job.dedupe_key) == 32

        # Verify dedup headers
        headers = kafka_headers_with_dedup(
            job.model_dump(mode="json"),
            dedup_id=job.dedupe_key,
        )

        dedup_headers = [h for h in headers if h[0] == "dedup-id"]
        assert len(dedup_headers) == 1
        assert dedup_headers[0][1] == job.dedupe_key.encode()


# =============================================================================
# Test: Worker Module Import Verification
# =============================================================================


class TestWorkerModuleImports:
    """Verify workers can be imported without errors."""

    def test_objectify_worker_import(self) -> None:
        """Objectify worker should be importable."""
        from objectify_worker.main import ObjectifyWorker
        assert ObjectifyWorker is not None

    def test_pipeline_worker_import(self) -> None:
        """Pipeline worker should be importable."""
        from pipeline_worker.main import PipelineWorker
        assert PipelineWorker is not None

    def test_action_worker_import(self) -> None:
        """Action worker should be importable."""
        from action_worker.main import ActionWorker
        assert ActionWorker is not None

    def test_search_projection_worker_import(self) -> None:
        """Search projection worker should be importable."""
        from search_projection_worker.main import SearchProjectionWorker
        assert SearchProjectionWorker is not None

    def test_connector_sync_worker_import(self) -> None:
        """Connector sync worker should be importable."""
        from connector_sync_worker.main import ConnectorSyncWorker
        assert ConnectorSyncWorker is not None

    def test_ontology_worker_import(self) -> None:
        """Ontology worker should be importable."""
        from ontology_worker.main import OntologyWorker
        assert OntologyWorker is not None

    def test_instance_worker_import(self) -> None:
        """Instance worker should be importable."""
        from instance_worker.main import StrictInstanceWorker
        assert StrictInstanceWorker is not None

    def test_projection_worker_import(self) -> None:
        """Projection worker should be importable."""
        from projection_worker.main import ProjectionWorker
        assert ProjectionWorker is not None

    def test_action_outbox_worker_import(self) -> None:
        """Action outbox worker should be importable."""
        from action_outbox_worker.main import ActionOutboxWorker
        assert ActionOutboxWorker is not None

    def test_ingest_reconciler_worker_import(self) -> None:
        """Ingest reconciler worker should be importable."""
        from ingest_reconciler_worker.main import IngestReconcilerWorker
        assert IngestReconcilerWorker is not None

    def test_writeback_materializer_worker_import(self) -> None:
        """Writeback materializer worker should be importable."""
        from writeback_materializer_worker.main import WritebackMaterializerWorker
        assert WritebackMaterializerWorker is not None

    def test_connector_trigger_service_import(self) -> None:
        """Connector trigger service should be importable."""
        from connector_trigger_service.main import ConnectorTriggerService
        assert ConnectorTriggerService is not None

    def test_pipeline_scheduler_import(self) -> None:
        """Pipeline scheduler should be importable."""
        from pipeline_scheduler.main import main as pipeline_scheduler_main
        assert pipeline_scheduler_main is not None

    def test_message_relay_import(self) -> None:
        """Message relay should be importable."""
        from message_relay.main import EventPublisher
        assert EventPublisher is not None


# =============================================================================
# Test: Consistency Summary
# =============================================================================


class TestConsistencySummary:
    """Summary tests for all consistency features."""

    def test_all_critical_settings_enforced(self) -> None:
        """All critical Kafka settings should be enforced."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        enforced = SafeKafkaConsumer.ENFORCED_SETTINGS

        # Must have isolation.level=read_committed
        assert enforced.get("isolation.level") == "read_committed"

        # Must have auto.commit disabled
        assert enforced.get("enable.auto.commit") is False

    def test_idempotency_patterns_present(self) -> None:
        """Idempotency patterns should be present."""
        from shared.models.pipeline_job import PipelineJob
        from shared.observability.context_propagation import kafka_headers_with_dedup

        # PipelineJob should have dedupe_key
        job = PipelineJob(
            job_id="test",
            pipeline_id="test",
            db_name="test",
            output_dataset_name="test",
        )
        assert hasattr(job, "dedupe_key")
        assert job.dedupe_key is not None

        # kafka_headers_with_dedup should exist
        assert callable(kafka_headers_with_dedup)

    def test_occ_support_present(self) -> None:
        """OCC support should be present in PipelineRegistry."""
        from shared.services.registries.pipeline_registry import (
            PipelineOCCConflictError,
            PipelineRecord,
        )

        # PipelineOCCConflictError should exist
        assert PipelineOCCConflictError is not None

        # PipelineRecord should have occ_version
        assert hasattr(PipelineRecord, "__dataclass_fields__")
        assert "occ_version" in PipelineRecord.__dataclass_fields__
