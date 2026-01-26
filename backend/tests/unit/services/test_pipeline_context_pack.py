from __future__ import annotations

from datetime import datetime, timezone

import pytest

from bff.services.pipeline_context_pack import build_pipeline_context_pack


class FakeDatasetRegistry:
    def __init__(self, datasets):
        self._datasets = list(datasets)

    async def list_datasets(self, *, db_name: str, branch: str | None = None):
        return list(self._datasets)


def _make_test_datasets():
    """Create test datasets for context pack tests."""
    dataset_a = {
        "dataset_id": "11111111-1111-1111-1111-111111111111",
        "db_name": "demo",
        "name": "customers",
        "description": None,
        "source_type": "csv_upload",
        "source_ref": "customers.csv",
        "branch": "main",
        "schema_json": {"columns": [{"name": "customer_id", "type": "xsd:string"}, {"name": "email", "type": "xsd:string"}]},
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "latest_commit_id": "c1",
        "artifact_key": None,
        "row_count": 2,
        "sample_json": {
            "columns": [{"name": "customer_id", "type": "xsd:string"}, {"name": "email", "type": "xsd:string"}],
            "rows": [
                {"customer_id": "1", "email": "alice@example.com"},
                {"customer_id": "2", "email": "bob@example.com"},
            ],
        },
        "version_created_at": datetime.now(timezone.utc),
    }
    dataset_b = {
        "dataset_id": "22222222-2222-2222-2222-222222222222",
        "db_name": "demo",
        "name": "orders",
        "description": None,
        "source_type": "csv_upload",
        "source_ref": "orders.csv",
        "branch": "main",
        "schema_json": {"columns": [{"name": "customerId", "type": "xsd:string"}, {"name": "order_total", "type": "xsd:decimal"}]},
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "latest_commit_id": "c2",
        "artifact_key": None,
        "row_count": 2,
        "sample_json": {
            "columns": [{"name": "customerId", "type": "xsd:string"}, {"name": "order_total", "type": "xsd:decimal"}],
            "rows": [
                {"customerId": "1", "order_total": "10.5"},
                {"customerId": "2", "order_total": "7.0"},
            ],
        },
        "version_created_at": datetime.now(timezone.utc),
    }
    return dataset_a, dataset_b


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_context_pack_suggests_join_keys_and_shows_real_data() -> None:
    """Test that context pack shows real data by default (PII masking disabled)."""
    dataset_a, dataset_b = _make_test_datasets()
    registry = FakeDatasetRegistry([dataset_a, dataset_b])

    pack = await build_pipeline_context_pack(
        db_name="demo",
        branch="main",
        dataset_ids=[dataset_a["dataset_id"], dataset_b["dataset_id"]],
        dataset_registry=registry,  # type: ignore[arg-type]
        max_sample_rows=5,
        max_join_candidates=5,
    )

    assert pack["db_name"] == "demo"
    assert len(pack["datasets_overview"]) == 2
    assert len(pack["selected_datasets"]) == 2

    # Default: PII masking disabled - Agent sees real data for accurate analysis
    sample_rows = pack["selected_datasets"][0]["sample_rows"]
    assert isinstance(sample_rows, list) and sample_rows
    assert sample_rows[0].get("email") == "alice@example.com"  # Real email visible

    # Join key suggestions
    candidates = pack["integration_suggestions"]["join_key_candidates"]
    assert any(
        c.get("left_column") == "customer_id" and c.get("right_column") == "customerId"
        for c in candidates
    )
    assert any(c.get("cardinality_hint") == "1:1" for c in candidates)

    # PK candidates with confidence metadata
    pk_candidates = pack["selected_datasets"][0].get("pk_candidates") or []
    assert any("customer_id" in (c.get("columns") or []) for c in pk_candidates)
    # Verify confidence metadata is present
    if pk_candidates:
        assert "confidence" in pk_candidates[0]
        assert "confidence_level" in pk_candidates[0]["confidence"]

    # Column profiles
    profiles = pack["selected_datasets"][0].get("column_profiles") or {}
    assert profiles.get("customer_id") is not None
    assert profiles.get("customer_id", {}).get("format") is not None

    # FK candidates with direction confidence
    fk_candidates = pack["integration_suggestions"].get("foreign_key_candidates") or []
    assert any(
        c.get("child_column") == "customerId" and c.get("parent_column") == "customer_id"
        for c in fk_candidates
    )
    # Verify direction confidence is present
    if fk_candidates:
        assert "direction_confidence" in fk_candidates[0]

    # Agent guidance should be present
    assert "agent_guidance" in pack
    assert "confidence_summary" in pack


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_context_pack_masks_pii_when_enabled() -> None:
    """Test that PII masking works when explicitly enabled."""
    dataset_a, dataset_b = _make_test_datasets()
    registry = FakeDatasetRegistry([dataset_a, dataset_b])

    pack = await build_pipeline_context_pack(
        db_name="demo",
        branch="main",
        dataset_ids=[dataset_a["dataset_id"], dataset_b["dataset_id"]],
        dataset_registry=registry,  # type: ignore[arg-type]
        max_sample_rows=5,
        max_join_candidates=5,
        mask_pii_enabled=True,  # Explicitly enable PII masking
    )

    # With PII masking enabled, email should be masked
    masked_rows = pack["selected_datasets"][0]["sample_rows"]
    assert isinstance(masked_rows, list) and masked_rows
    assert "<email:" in str(masked_rows[0].get("email") or "")
