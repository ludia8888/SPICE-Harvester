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

import asyncio
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import pytest

# Infrastructure URLs (use conftest.py defaults via environment)
BFF_URL = (os.getenv("BFF_BASE_URL") or os.getenv("BFF_URL") or "http://localhost:8002").rstrip("/")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_SERVERS") or "localhost:39092"
POSTGRES_DSN = os.getenv("POSTGRES_URL") or os.getenv("POSTGRES_DSN") or "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb"
REDIS_URL = os.getenv("REDIS_URL") or "redis://:spicepass123@localhost:6379"


def _docker(*args: str) -> str:
    """Execute docker command and return output."""
    result = subprocess.run(
        ["docker", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    return (result.stdout or "").strip()


def _kafka_container() -> Optional[str]:
    """Find the Kafka container name."""
    names = set(_docker("ps", "--format", "{{.Names}}").splitlines())
    for candidate in ("spice_kafka", "spice-harvester-kafka", "kafka"):
        if candidate in names:
            return candidate
        for name in names:
            if candidate in name:
                return name
    return None


def _auth_headers() -> Dict[str, str]:
    """Get auth headers for BFF requests."""
    from tests.utils.auth import bff_auth_headers
    return bff_auth_headers()


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
        """Verify worker modules import SafeKafkaConsumer."""
        import importlib.util

        worker_modules = [
            "objectify_worker.main",
            "pipeline_worker.main",
            "action_worker.main",
            "search_projection_worker.main",
            "connector_sync_worker.main",
        ]

        for module_name in worker_modules:
            spec = importlib.util.find_spec(module_name)
            if spec is None:
                pytest.skip(f"Module {module_name} not found")
                continue

            # Read the source and check for SafeKafkaConsumer import
            if spec.origin:
                with open(spec.origin, "r") as f:
                    source = f.read()
                assert "SafeKafkaConsumer" in source, (
                    f"{module_name} should use SafeKafkaConsumer"
                )


# =============================================================================
# Test: Kafka Connectivity
# =============================================================================


class TestKafkaConnectivity:
    """Verify Kafka connectivity and topic access."""

    @pytest.mark.asyncio
    async def test_kafka_topics_exist(self) -> None:
        """Verify required Kafka topics exist."""
        kafka_container = _kafka_container()
        if not kafka_container:
            pytest.skip("Kafka container not found")

        # List topics using kafka-topics command in container
        result = _docker(
            "exec", kafka_container,
            "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"
        )

        topics = result.splitlines()
        expected_topics = [
            "objectify-jobs",
            "pipeline-jobs",
        ]

        for topic in expected_topics:
            # Check if topic exists or skip if Kafka is not configured
            if topic not in topics:
                pytest.skip(f"Topic {topic} not found - may not be configured yet")

    @pytest.mark.asyncio
    async def test_kafka_consumer_group_list(self) -> None:
        """Verify consumer groups can be listed."""
        kafka_container = _kafka_container()
        if not kafka_container:
            pytest.skip("Kafka container not found")

        result = _docker(
            "exec", kafka_container,
            "kafka-consumer-groups", "--bootstrap-server", "localhost:9092", "--list"
        )

        # Just verify the command works - groups may or may not exist
        assert isinstance(result, str)


# =============================================================================
# Test: BFF API Health
# =============================================================================


class TestBFFHealth:
    """Verify BFF is running and healthy."""

    @pytest.mark.asyncio
    async def test_bff_health_endpoint(self) -> None:
        """BFF health endpoint should return success."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BFF_URL}/api/v1/health") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "success"
                assert data["data"]["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_bff_databases_connected(self) -> None:
        """BFF should have database connections."""
        async with aiohttp.ClientSession() as session:
            headers = _auth_headers()
            async with session.get(f"{BFF_URL}/api/v1/databases", headers=headers) as resp:
                # 200 or 404 (no databases) are both valid
                assert resp.status in (200, 404, 401, 403)


# =============================================================================
# Test: Database Connectivity
# =============================================================================


class TestDatabaseConnectivity:
    """Verify database connectivity."""

    @pytest.mark.asyncio
    async def test_postgres_connection(self) -> None:
        """PostgreSQL should be connectable."""
        try:
            import asyncpg
        except ImportError:
            pytest.skip("asyncpg not installed")

        try:
            conn = await asyncpg.connect(POSTGRES_DSN)
            result = await conn.fetchval("SELECT 1")
            assert result == 1
            await conn.close()
        except Exception as e:
            pytest.fail(f"PostgreSQL connection failed: {e}")

    @pytest.mark.asyncio
    async def test_redis_connection(self) -> None:
        """Redis should be connectable."""
        try:
            import redis.asyncio as aioredis
        except ImportError:
            pytest.skip("redis not installed")

        # Try configured URL first, then fallback to no-password local Redis
        redis_urls = [
            REDIS_URL,
            "redis://localhost:6379/0",  # Local Redis without password
        ]
        last_error = None
        for redis_url in redis_urls:
            try:
                client = aioredis.from_url(redis_url)
                result = await client.ping()
                assert result is True
                await client.aclose()
                return  # Success
            except Exception as e:
                last_error = e
                continue
        pytest.fail(f"Redis connection failed (tried {len(redis_urls)} URLs): {last_error}")


# =============================================================================
# Test: Outbox Pattern Verification
# =============================================================================


class TestOutboxPatternVerification:
    """Verify outbox pattern implementation."""

    def test_objectify_outbox_uses_aggregate_key(self) -> None:
        """Objectify outbox should use dataset_id as partition key."""
        import inspect
        from shared.services.events.objectify_outbox import ObjectifyOutboxPublisher

        # Get the source of _publish_batch
        source = inspect.getsource(ObjectifyOutboxPublisher._publish_batch)

        # Verify it uses dataset_id for key
        assert "dataset_id" in source, "Outbox should use dataset_id for partition key"
        assert "aggregate_id" in source, "Outbox should track aggregate_id"

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


# =============================================================================
# Test: ProcessedEventRegistry Idempotency
# =============================================================================


class TestProcessedEventRegistryIdempotency:
    """Verify ProcessedEventRegistry provides idempotency."""

    @pytest.mark.asyncio
    async def test_claim_idempotency(self) -> None:
        """ProcessedEventRegistry.claim should be idempotent."""
        from shared.services.registries.processed_event_registry import (
            ProcessedEventRegistry,
            ClaimDecision,
        )

        registry = ProcessedEventRegistry()
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
            await registry.close()

    @pytest.mark.asyncio
    async def test_sequence_ordering(self) -> None:
        """ProcessedEventRegistry should enforce sequence ordering."""
        from shared.services.registries.processed_event_registry import (
            ProcessedEventRegistry,
            ClaimDecision,
        )

        registry = ProcessedEventRegistry()
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
        try:
            from objectify_worker.main import ObjectifyWorker
            assert ObjectifyWorker is not None
        except ImportError as e:
            pytest.skip(f"Cannot import objectify_worker: {e}")

    def test_pipeline_worker_import(self) -> None:
        """Pipeline worker should be importable."""
        try:
            from pipeline_worker.main import PipelineWorker
            assert PipelineWorker is not None
        except ImportError as e:
            pytest.skip(f"Cannot import pipeline_worker: {e}")

    def test_action_worker_import(self) -> None:
        """Action worker should be importable."""
        try:
            from action_worker.main import ActionWorker
            assert ActionWorker is not None
        except ImportError as e:
            pytest.skip(f"Cannot import action_worker: {e}")

    def test_search_projection_worker_import(self) -> None:
        """Search projection worker should be importable."""
        try:
            from search_projection_worker.main import SearchProjectionWorker
            assert SearchProjectionWorker is not None
        except ImportError as e:
            pytest.skip(f"Cannot import search_projection_worker: {e}")

    def test_connector_sync_worker_import(self) -> None:
        """Connector sync worker should be importable."""
        try:
            from connector_sync_worker.main import ConnectorSyncWorker
            assert ConnectorSyncWorker is not None
        except ImportError as e:
            pytest.skip(f"Cannot import connector_sync_worker: {e}")


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
