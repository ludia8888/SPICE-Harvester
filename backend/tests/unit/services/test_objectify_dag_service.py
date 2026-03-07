from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional

import pytest

from bff.schemas.objectify_requests import RunObjectifyDAGRequest
from bff.services.objectify_dag_service import _ObjectifyDagOrchestrator


@dataclass
class _FakeObjectifyRegistry:
    record: Optional[Any]
    mapping_branch: str = "main"
    branch_calls: list[str] | None = None

    async def get_objectify_job(self, job_id: str) -> Any:  # noqa: ARG002
        return self.record

    async def get_active_mapping_spec(self, **kwargs):  # noqa: ANN003
        branch = str(kwargs.get("dataset_branch") or "")
        if self.branch_calls is not None:
            self.branch_calls.append(branch)
        if branch != self.mapping_branch:
            return None
        return SimpleNamespace(
            mapping_spec_id="map-1",
            version=1,
            mappings=[],
            artifact_output_name="orders",
            options={},
        )


class _FakeDatasetRegistry:
    async def get_latest_version(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return SimpleNamespace(
            version_id="ver-1",
            dataset_id="ds-1",
            artifact_key="s3://bucket/orders.csv",
        )

    async def get_version(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return None


class _FakeOMSClient:
    async def get_ontology_resource(self, db_name, *, resource_type, resource_id, branch):  # noqa: ANN001, ANN003
        _ = db_name, resource_type, resource_id, branch
        return {
            "data": {
                "id": "Order",
                "spec": {
                    "status": "ACTIVE",
                    "backing_sources": [{"dataset_id": "ds-1", "branch": "main"}],
                },
            }
        }

    async def get_ontology(self, db_name, class_id, *, branch):  # noqa: ANN001, ANN003
        _ = db_name, class_id, branch
        return {"data": {"properties": [{"name": "order_id", "type": "xsd:string"}], "relationships": []}}


def _make_orchestrator(record: Any) -> _ObjectifyDagOrchestrator:
    return _ObjectifyDagOrchestrator(
        db_name="demo_db",
        body=RunObjectifyDAGRequest(class_ids=["Order"]),
        dataset_registry=SimpleNamespace(),
        objectify_registry=_FakeObjectifyRegistry(record=record),  # type: ignore[arg-type]
        job_queue=SimpleNamespace(),
        oms_client=SimpleNamespace(),
    )


@pytest.mark.asyncio
async def test_wait_for_objectify_submitted_allows_dataset_primary_completed_without_commands() -> None:
    record = SimpleNamespace(
        status="COMPLETED",
        report={"write_path_mode": "dataset_primary_index", "command_ids": []},
        command_id=None,
        error=None,
    )
    orchestrator = _make_orchestrator(record)

    command_ids = await orchestrator._wait_for_objectify_submitted("job-1", timeout_seconds=1)
    assert command_ids == []


@pytest.mark.asyncio
async def test_wait_for_objectify_submitted_defaults_to_dataset_primary_when_report_missing() -> None:
    record = SimpleNamespace(
        status="COMPLETED",
        report={"command_ids": []},
        command_id=None,
        error=None,
    )
    orchestrator = _make_orchestrator(record)

    command_ids = await orchestrator._wait_for_objectify_submitted("job-1", timeout_seconds=1)
    assert command_ids == []


@pytest.mark.asyncio
async def test_wait_for_objectify_submitted_requires_commands_for_submitted_status() -> None:
    record = SimpleNamespace(
        status="SUBMITTED",
        report={"command_ids": []},
        command_id=None,
        error=None,
    )
    orchestrator = _make_orchestrator(record)

    with pytest.raises(RuntimeError, match="without command_ids"):
        await orchestrator._wait_for_objectify_submitted("job-2", timeout_seconds=1)


@pytest.mark.asyncio
async def test_load_class_info_supports_backing_sources_contract_shape() -> None:
    orchestrator = _ObjectifyDagOrchestrator(
        db_name="demo_db",
        body=RunObjectifyDAGRequest(class_ids=["Order"]),
        dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
        objectify_registry=_FakeObjectifyRegistry(record=None),  # type: ignore[arg-type]
        job_queue=SimpleNamespace(),
        oms_client=_FakeOMSClient(),  # type: ignore[arg-type]
    )

    info = await orchestrator._load_class_info("Order")
    assert info["dataset_id"] == "ds-1"
    assert info["backing_source"]["dataset_id"] == "ds-1"


@pytest.mark.asyncio
async def test_load_class_info_falls_back_to_main_mapping_branch() -> None:
    class _FeatureBranchOMSClient(_FakeOMSClient):
        async def get_ontology_resource(self, db_name, *, resource_type, resource_id, branch):  # noqa: ANN001, ANN003
            _ = db_name, resource_type, resource_id, branch
            return {
                "data": {
                    "id": "Order",
                    "spec": {
                        "status": "ACTIVE",
                        "backing_sources": [{"dataset_id": "ds-1", "branch": "feature-objectify"}],
                    },
                }
            }

    branch_calls: list[str] = []
    orchestrator = _ObjectifyDagOrchestrator(
        db_name="demo_db",
        body=RunObjectifyDAGRequest(class_ids=["Order"]),
        dataset_registry=_FakeDatasetRegistry(),  # type: ignore[arg-type]
        objectify_registry=_FakeObjectifyRegistry(record=None, mapping_branch="main", branch_calls=branch_calls),  # type: ignore[arg-type]
        job_queue=SimpleNamespace(),
        oms_client=_FeatureBranchOMSClient(),  # type: ignore[arg-type]
    )

    info = await orchestrator._load_class_info("Order")

    assert info["dataset_branch"] == "main"
    assert branch_calls[0] == "feature-objectify"
    assert "main" in branch_calls[1:]
