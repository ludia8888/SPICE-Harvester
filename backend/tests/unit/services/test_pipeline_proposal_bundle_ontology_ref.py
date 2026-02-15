from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from bff.services.pipeline_proposal_service import _build_proposal_bundle


@dataclass
class _Pipeline:
    pipeline_id: str
    db_name: str
    branch: str = "main"


class _PipelineRegistry:
    def __init__(self, output_json: dict[str, Any]) -> None:
        self._output_json = output_json

    async def get_run(self, *, pipeline_id: str, job_id: str):  # noqa: ANN201
        _ = pipeline_id, job_id
        return {
            "job_id": "build-1",
            "mode": "build",
            "status": "SUCCESS",
            "output_json": self._output_json,
        }

    async def get_artifact_by_job(self, *, pipeline_id: str, job_id: str, mode: str):  # noqa: ANN201
        _ = pipeline_id, job_id, mode
        return None


class _DatasetRegistry:
    async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str):  # noqa: ANN201
        _ = db_name, name, branch
        return None


class _ObjectifyRegistry:
    async def get_active_mapping_spec(self, **kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return None

    async def get_mapping_spec(self, *, mapping_spec_id: str):  # noqa: ANN201
        _ = mapping_spec_id
        return None


@pytest.mark.asyncio
async def test_build_proposal_bundle_adds_ontology_ref_fallback_when_commit_missing() -> None:
    pipeline = _Pipeline(pipeline_id="p-1", db_name="demo", branch="main")
    registry = _PipelineRegistry(
        output_json={
            "branch": "main",
            "ontology": {"branch": "main"},
        }
    )

    bundle = await _build_proposal_bundle(
        pipeline=pipeline,
        build_job_id="build-1",
        mapping_spec_ids=[],
        pipeline_registry=registry,
        dataset_registry=_DatasetRegistry(),
        objectify_registry=_ObjectifyRegistry(),
    )

    assert bundle["ontology"]["ref"] == "branch:main"
    assert bundle["ontology"]["commit"] == "branch:main"
    assert bundle["ontology"]["branch"] == "main"


@pytest.mark.asyncio
async def test_build_proposal_bundle_preserves_ontology_ref_when_present() -> None:
    pipeline = _Pipeline(pipeline_id="p-2", db_name="demo", branch="feature-a")
    registry = _PipelineRegistry(
        output_json={
            "branch": "feature-a",
            "ontology": {"branch": "feature-a", "ref": "branch:feature-a", "commit": "c-123"},
        }
    )

    bundle = await _build_proposal_bundle(
        pipeline=pipeline,
        build_job_id="build-2",
        mapping_spec_ids=[],
        pipeline_registry=registry,
        dataset_registry=_DatasetRegistry(),
        objectify_registry=_ObjectifyRegistry(),
    )

    assert bundle["ontology"]["ref"] == "branch:feature-a"
    assert bundle["ontology"]["commit"] == "c-123"
    assert bundle["ontology"]["branch"] == "feature-a"
