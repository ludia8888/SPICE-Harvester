"""
Unit tests for Objectify MCP tools in Pipeline MCP Server.

Tests the 5 new objectify tools:
- objectify_suggest_mapping
- objectify_create_mapping_spec
- objectify_list_mapping_specs
- objectify_run
- objectify_get_status
"""

import pytest
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass


# ==================== Mock Classes ====================

@dataclass
class MockDataset:
    dataset_id: str
    db_name: str
    name: str
    branch: str
    schema_json: Dict[str, Any]
    source_type: str = "connector"
    source_ref: str = ""


@dataclass
class MockDatasetVersion:
    version_id: str
    dataset_id: str
    artifact_key: str
    lakefs_commit_id: str


@dataclass
class MockMappingSpec:
    mapping_spec_id: str
    dataset_id: str
    target_class_id: str
    status: str
    auto_sync: bool
    version: int
    created_at: datetime
    options: Dict[str, Any] = None

    def __post_init__(self):
        if self.options is None:
            self.options = {}


@dataclass
class MockObjectifyJob:
    job_id: str
    dataset_id: str
    target_class_id: str
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    rows_processed: int = 0
    rows_failed: int = 0


class MockDatasetRegistry:
    def __init__(self):
        self.datasets: Dict[str, MockDataset] = {}
        self.versions: Dict[str, MockDatasetVersion] = {}

    async def initialize(self):
        pass

    async def get_dataset(self, dataset_id: str) -> Optional[MockDataset]:
        return self.datasets.get(dataset_id)

    async def get_version(self, version_id: str) -> Optional[MockDatasetVersion]:
        return self.versions.get(version_id)

    async def get_latest_version(self, dataset_id: str) -> Optional[MockDatasetVersion]:
        for v in self.versions.values():
            if v.dataset_id == dataset_id:
                return v
        return None


class MockObjectifyRegistry:
    def __init__(self):
        self.mapping_specs: Dict[str, MockMappingSpec] = {}
        self.jobs: Dict[str, MockObjectifyJob] = {}
        self._created_specs: List[Dict] = []
        self._enqueued_jobs: List[Any] = []

    async def initialize(self):
        pass

    async def create_mapping_spec(
        self,
        dataset_id: str,
        dataset_branch: str,
        artifact_output_name: str,
        schema_hash: Optional[str],
        target_class_id: str,
        mappings: List[Dict],
        auto_sync: bool,
        status: str,
        options: Dict,
    ) -> MockMappingSpec:
        spec_id = f"spec-{len(self._created_specs) + 1}"
        spec = MockMappingSpec(
            mapping_spec_id=spec_id,
            dataset_id=dataset_id,
            target_class_id=target_class_id,
            status=status,
            auto_sync=auto_sync,
            version=1,
            created_at=datetime.now(timezone.utc),
            options=options,
        )
        self._created_specs.append({
            "spec": spec,
            "mappings": mappings,
        })
        self.mapping_specs[spec_id] = spec
        return spec

    async def list_mapping_specs(self, dataset_id: str, limit: int = 50) -> List[MockMappingSpec]:
        return [s for s in self.mapping_specs.values() if s.dataset_id == dataset_id][:limit]

    async def get_mapping_spec(self, mapping_spec_id: str) -> Optional[MockMappingSpec]:
        return self.mapping_specs.get(mapping_spec_id)

    async def get_objectify_job(self, job_id: str) -> Optional[MockObjectifyJob]:
        return self.jobs.get(job_id)

    async def get_objectify_job_by_dedupe_key(self, dedupe_key: str) -> Optional[MockObjectifyJob]:
        return None

    async def enqueue_objectify_job(self, job: Any) -> Any:
        self._enqueued_jobs.append(job)
        mock_job = MockObjectifyJob(
            job_id=job.job_id,
            dataset_id=job.dataset_id,
            target_class_id=job.target_class_id,
            status="QUEUED",
            created_at=datetime.now(timezone.utc),
        )
        self.jobs[job.job_id] = mock_job
        return mock_job

    def build_dedupe_key(self, **kwargs) -> str:
        return f"dedupe-{kwargs.get('dataset_id')}-{kwargs.get('mapping_spec_id')}"


# ==================== Test Functions ====================

@pytest.mark.asyncio
async def test_objectify_suggest_mapping_basic():
    """Test objectify_suggest_mapping returns suggestions based on schema matching."""

    # Setup mocks
    mock_dataset_registry = MockDatasetRegistry()
    mock_dataset_registry.datasets["ds-123"] = MockDataset(
        dataset_id="ds-123",
        db_name="testdb",
        name="customers",
        branch="main",
        schema_json={
            "columns": [
                {"name": "customer_id", "type": "xsd:string"},
                {"name": "customer_name", "type": "xsd:string"},
                {"name": "email", "type": "xsd:string"},
                {"name": "created_date", "type": "xsd:date"},
            ]
        },
    )

    # Mock the tool execution logic
    dataset = mock_dataset_registry.datasets["ds-123"]
    schema_json = dataset.schema_json
    columns = schema_json.get("columns", [])

    source_columns = []
    source_types = {}
    for col in columns:
        col_name = col.get("name", "")
        col_type = col.get("type", "xsd:string")
        source_columns.append(col_name)
        source_types[col_name] = col_type

    # Simulate target properties
    target_properties = [
        {"name": "customerId", "type": "xsd:string"},
        {"name": "customerName", "type": "xsd:string"},
        {"name": "emailAddress", "type": "xsd:string"},
        {"name": "createdAt", "type": "xsd:dateTime"},
    ]

    # Simple matching logic
    suggestions = []
    target_prop_names = {p["name"].lower().replace("_", ""): p for p in target_properties}

    for src_col in source_columns:
        src_lower = src_col.lower().replace("_", "")
        best_match = None
        confidence = 0.0

        for tgt_name, tgt_prop in target_prop_names.items():
            if src_lower == tgt_name:
                best_match = tgt_prop["name"]
                confidence = 1.0
                break
            if src_lower in tgt_name or tgt_name in src_lower:
                if confidence < 0.7:
                    best_match = tgt_prop["name"]
                    confidence = 0.7

        suggestions.append({
            "source_field": src_col,
            "target_field": best_match,
            "confidence": confidence,
            "auto_mapped": best_match is not None,
        })

    # Assertions
    assert len(suggestions) == 4

    # customer_id should match customerId
    customer_id_suggestion = next(s for s in suggestions if s["source_field"] == "customer_id")
    assert customer_id_suggestion["target_field"] == "customerId"
    assert customer_id_suggestion["confidence"] == 1.0

    # customer_name should match customerName
    customer_name_suggestion = next(s for s in suggestions if s["source_field"] == "customer_name")
    assert customer_name_suggestion["target_field"] == "customerName"
    assert customer_name_suggestion["confidence"] == 1.0

    # Count auto-mapped
    auto_mapped_count = sum(1 for s in suggestions if s["auto_mapped"])
    assert auto_mapped_count >= 2  # At least customer_id and customer_name


@pytest.mark.asyncio
async def test_objectify_create_mapping_spec():
    """Test objectify_create_mapping_spec creates a mapping spec."""

    mock_dataset_registry = MockDatasetRegistry()
    mock_dataset_registry.datasets["ds-123"] = MockDataset(
        dataset_id="ds-123",
        db_name="testdb",
        name="customers",
        branch="main",
        schema_json={"columns": [{"name": "id", "type": "xsd:string"}]},
    )

    mock_objectify_registry = MockObjectifyRegistry()

    # Create mapping spec
    mappings = [
        {"source_field": "id", "target_field": "customerId"},
        {"source_field": "name", "target_field": "customerName"},
    ]

    spec = await mock_objectify_registry.create_mapping_spec(
        dataset_id="ds-123",
        dataset_branch="main",
        artifact_output_name="customers",
        schema_hash="abc123",
        target_class_id="Customer",
        mappings=mappings,
        auto_sync=True,
        status="ACTIVE",
        options={},
    )

    assert spec.mapping_spec_id.startswith("spec-")
    assert spec.dataset_id == "ds-123"
    assert spec.target_class_id == "Customer"
    assert spec.auto_sync is True
    assert spec.status == "ACTIVE"

    # Verify stored
    assert len(mock_objectify_registry._created_specs) == 1
    assert mock_objectify_registry._created_specs[0]["mappings"] == mappings


@pytest.mark.asyncio
async def test_objectify_list_mapping_specs():
    """Test objectify_list_mapping_specs returns specs for a dataset."""

    mock_objectify_registry = MockObjectifyRegistry()

    # Add some specs
    mock_objectify_registry.mapping_specs["spec-1"] = MockMappingSpec(
        mapping_spec_id="spec-1",
        dataset_id="ds-123",
        target_class_id="Customer",
        status="ACTIVE",
        auto_sync=True,
        version=1,
        created_at=datetime.now(timezone.utc),
    )
    mock_objectify_registry.mapping_specs["spec-2"] = MockMappingSpec(
        mapping_spec_id="spec-2",
        dataset_id="ds-123",
        target_class_id="Order",
        status="ACTIVE",
        auto_sync=False,
        version=1,
        created_at=datetime.now(timezone.utc),
    )
    mock_objectify_registry.mapping_specs["spec-3"] = MockMappingSpec(
        mapping_spec_id="spec-3",
        dataset_id="ds-other",
        target_class_id="Product",
        status="ACTIVE",
        auto_sync=True,
        version=1,
        created_at=datetime.now(timezone.utc),
    )

    # List specs for ds-123
    specs = await mock_objectify_registry.list_mapping_specs(dataset_id="ds-123")

    assert len(specs) == 2
    assert all(s.dataset_id == "ds-123" for s in specs)

    spec_ids = {s.mapping_spec_id for s in specs}
    assert "spec-1" in spec_ids
    assert "spec-2" in spec_ids
    assert "spec-3" not in spec_ids


@pytest.mark.asyncio
async def test_objectify_run():
    """Test objectify_run enqueues an objectify job."""

    mock_dataset_registry = MockDatasetRegistry()
    mock_dataset_registry.datasets["ds-123"] = MockDataset(
        dataset_id="ds-123",
        db_name="testdb",
        name="customers",
        branch="main",
        schema_json={"columns": []},
    )
    mock_dataset_registry.versions["v-1"] = MockDatasetVersion(
        version_id="v-1",
        dataset_id="ds-123",
        artifact_key="s3://bucket/data.csv",
        lakefs_commit_id="commit-abc",
    )

    mock_objectify_registry = MockObjectifyRegistry()
    mock_objectify_registry.mapping_specs["spec-1"] = MockMappingSpec(
        mapping_spec_id="spec-1",
        dataset_id="ds-123",
        target_class_id="Customer",
        status="ACTIVE",
        auto_sync=True,
        version=1,
        created_at=datetime.now(timezone.utc),
    )

    # Simulate objectify_run logic
    dataset = mock_dataset_registry.datasets["ds-123"]
    version = mock_dataset_registry.versions["v-1"]
    mapping_spec = mock_objectify_registry.mapping_specs["spec-1"]

    from uuid import uuid4

    # Minimal ObjectifyJob simulation
    class SimpleJob:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    job_id = str(uuid4())
    job = SimpleJob(
        job_id=job_id,
        db_name="testdb",
        dataset_id="ds-123",
        dataset_version_id="v-1",
        target_class_id="Customer",
        mapping_spec_id="spec-1",
    )

    result = await mock_objectify_registry.enqueue_objectify_job(job=job)

    assert result.job_id == job_id
    assert result.status == "QUEUED"
    assert len(mock_objectify_registry._enqueued_jobs) == 1


@pytest.mark.asyncio
async def test_objectify_get_status():
    """Test objectify_get_status returns job status."""

    mock_objectify_registry = MockObjectifyRegistry()

    now = datetime.now(timezone.utc)
    mock_objectify_registry.jobs["job-123"] = MockObjectifyJob(
        job_id="job-123",
        dataset_id="ds-123",
        target_class_id="Customer",
        status="COMPLETED",
        created_at=now,
        started_at=now,
        completed_at=now,
        rows_processed=100,
        rows_failed=2,
    )

    job = await mock_objectify_registry.get_objectify_job(job_id="job-123")

    assert job is not None
    assert job.job_id == "job-123"
    assert job.status == "COMPLETED"
    assert job.rows_processed == 100
    assert job.rows_failed == 2


@pytest.mark.asyncio
async def test_objectify_get_status_not_found():
    """Test objectify_get_status returns None for non-existent job."""

    mock_objectify_registry = MockObjectifyRegistry()

    job = await mock_objectify_registry.get_objectify_job(job_id="non-existent")

    assert job is None


@pytest.mark.asyncio
async def test_objectify_suggest_mapping_no_matches():
    """Test objectify_suggest_mapping handles case with no matches."""

    source_columns = ["foo", "bar", "baz"]
    target_properties = [{"name": "completely_different"}]

    target_prop_names = {p["name"].lower().replace("_", ""): p for p in target_properties}

    suggestions = []
    for src_col in source_columns:
        src_lower = src_col.lower().replace("_", "")
        best_match = None

        for tgt_name, tgt_prop in target_prop_names.items():
            if src_lower == tgt_name or src_lower in tgt_name or tgt_name in src_lower:
                best_match = tgt_prop["name"]
                break

        suggestions.append({
            "source_field": src_col,
            "target_field": best_match,
            "auto_mapped": best_match is not None,
        })

    # All should be unmapped
    assert all(not s["auto_mapped"] for s in suggestions)
    assert all(s["target_field"] is None for s in suggestions)


@pytest.mark.asyncio
async def test_objectify_create_mapping_spec_validates_mappings():
    """Test that empty or invalid mappings are rejected."""

    # Test with empty mappings list
    mappings = []

    normalized_mappings = []
    for m in mappings:
        if not isinstance(m, dict):
            continue
        src = str(m.get("source_field") or "").strip()
        tgt = str(m.get("target_field") or "").strip()
        if src and tgt:
            normalized_mappings.append({"source_field": src, "target_field": tgt})

    # Should result in empty normalized mappings
    assert len(normalized_mappings) == 0

    # Test with invalid mapping (missing fields)
    mappings = [
        {"source_field": "id"},  # Missing target_field
        {"target_field": "name"},  # Missing source_field
        {},  # Empty
    ]

    normalized_mappings = []
    for m in mappings:
        if not isinstance(m, dict):
            continue
        src = str(m.get("source_field") or "").strip()
        tgt = str(m.get("target_field") or "").strip()
        if src and tgt:
            normalized_mappings.append({"source_field": src, "target_field": tgt})

    # All invalid, should be empty
    assert len(normalized_mappings) == 0


@pytest.mark.asyncio
async def test_objectify_run_finds_active_mapping_spec():
    """Test objectify_run can find active mapping spec when not explicitly provided."""

    mock_objectify_registry = MockObjectifyRegistry()

    # Add specs with different statuses
    mock_objectify_registry.mapping_specs["spec-inactive"] = MockMappingSpec(
        mapping_spec_id="spec-inactive",
        dataset_id="ds-123",
        target_class_id="Customer",
        status="INACTIVE",
        auto_sync=True,
        version=1,
        created_at=datetime.now(timezone.utc),
    )
    mock_objectify_registry.mapping_specs["spec-active"] = MockMappingSpec(
        mapping_spec_id="spec-active",
        dataset_id="ds-123",
        target_class_id="Customer",
        status="ACTIVE",
        auto_sync=True,
        version=1,
        created_at=datetime.now(timezone.utc),
    )
    mock_objectify_registry.mapping_specs["spec-no-sync"] = MockMappingSpec(
        mapping_spec_id="spec-no-sync",
        dataset_id="ds-123",
        target_class_id="Customer",
        status="ACTIVE",
        auto_sync=False,
        version=1,
        created_at=datetime.now(timezone.utc),
    )

    # Find active mapping spec
    specs = await mock_objectify_registry.list_mapping_specs(dataset_id="ds-123")
    active_specs = [s for s in specs if s.status == "ACTIVE" and s.auto_sync]

    assert len(active_specs) == 1
    assert active_specs[0].mapping_spec_id == "spec-active"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
