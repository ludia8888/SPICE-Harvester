from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from bff.services.pipeline_execution_service import _ensure_dataset_key_spec_alignment


@pytest.mark.asyncio
async def test_ensure_dataset_key_spec_alignment_reuses_raced_identical_key_spec() -> None:
    existing = SimpleNamespace(
        spec={
            "primary_key": ["customer_id"],
            "source": "pipeline",
        }
    )

    class _Registry:
        def __init__(self) -> None:
            self.lookup_calls = 0
            self.get_or_create_calls = 0

        async def get_key_spec_for_dataset(self, *, dataset_id: str):
            self.lookup_calls += 1
            assert dataset_id == "dataset-1"
            return None

        async def get_or_create_key_spec(self, *, dataset_id: str, spec: dict, dataset_version_id=None):  # noqa: ANN001
            self.get_or_create_calls += 1
            assert dataset_id == "dataset-1"
            assert dataset_version_id is None
            assert spec["primary_key"] == ["customer_id"]
            return existing, False

    registry = _Registry()

    await _ensure_dataset_key_spec_alignment(
        dataset_registry=registry,  # type: ignore[arg-type]
        dataset_id="dataset-1",
        pk_columns=["customer_id"],
        node_id="node-1",
    )

    assert registry.lookup_calls == 1
    assert registry.get_or_create_calls == 1


@pytest.mark.asyncio
async def test_ensure_dataset_key_spec_alignment_raises_conflict_when_raced_key_spec_differs() -> None:
    existing = SimpleNamespace(
        spec={
            "primary_key": ["other_id"],
            "source": "pipeline",
        }
    )

    class _Registry:
        def __init__(self) -> None:
            self.lookup_calls = 0
            self.get_or_create_calls = 0

        async def get_key_spec_for_dataset(self, *, dataset_id: str):
            self.lookup_calls += 1
            assert dataset_id == "dataset-1"
            return None

        async def get_or_create_key_spec(self, *, dataset_id: str, spec: dict, dataset_version_id=None):  # noqa: ANN001
            self.get_or_create_calls += 1
            assert dataset_id == "dataset-1"
            assert dataset_version_id is None
            assert spec["primary_key"] == ["customer_id"]
            return existing, False

    registry = _Registry()

    with pytest.raises(HTTPException) as exc_info:
        await _ensure_dataset_key_spec_alignment(
            dataset_registry=registry,  # type: ignore[arg-type]
            dataset_id="dataset-1",
            pk_columns=["customer_id"],
            node_id="node-1",
        )

    assert exc_info.value.status_code == 409
    assert registry.lookup_calls == 1
    assert registry.get_or_create_calls == 1
