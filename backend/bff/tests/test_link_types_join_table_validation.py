from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.routers import link_types as link_types_router


class _FakeDatasetRegistry:
    def __init__(self, *, dataset, version):
        self._dataset = dataset
        self._version = version

    async def get_dataset(self, *, dataset_id):  # noqa: ANN003
        if dataset_id == self._dataset.dataset_id:
            return self._dataset
        return None

    async def get_version(self, *, version_id):  # noqa: ANN003
        if version_id == self._version.version_id:
            return self._version
        return None

    async def get_latest_version(self, *, dataset_id):  # noqa: ANN003
        if dataset_id == self._dataset.dataset_id:
            return self._version
        return None


@pytest.mark.asyncio
async def test_join_table_missing_target_column_is_rejected():
    dataset = SimpleNamespace(
        dataset_id="join-1",
        db_name="test_db",
        name="join_ds",
        branch="main",
        source_type="dataset",
        source_ref=None,
        schema_json={"columns": [{"name": "source_id", "type": "xsd:string"}]},
    )
    version = SimpleNamespace(
        version_id="ver-1",
        dataset_id="join-1",
        artifact_key="s3://bucket/join",
        sample_json={"columns": [{"name": "source_id", "type": "xsd:string"}]},
    )
    dataset_registry = _FakeDatasetRegistry(dataset=dataset, version=version)

    source_props = {"source_id": {"type": "xsd:string"}}
    target_props = {"target_id": {"type": "xsd:string"}}
    source_contract = {"pk_spec": {"primary_key": ["source_id"]}}
    target_contract = {"pk_spec": {"primary_key": ["target_id"]}}
    spec_payload = {
        "type": "join_table",
        "join_dataset_id": "join-1",
        "join_dataset_version_id": "ver-1",
        "source_key_column": "source_id",
        "target_key_column": "target_id",
    }

    with pytest.raises(HTTPException) as exc_info:
        await link_types_router._build_mapping_request(
            db_name="test_db",
            request=Request({"type": "http", "headers": []}),
            oms_client=None,
            dataset_registry=dataset_registry,
            relationship_spec_id="rel-1",
            link_type_id="link-1",
            source_class="Source",
            target_class="Target",
            predicate="linked_to",
            cardinality="n:m",
            branch="main",
            source_props=source_props,
            target_props=target_props,
            source_contract=source_contract,
            target_contract=target_contract,
            spec_payload=spec_payload,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "target_key_column missing"


@pytest.mark.asyncio
async def test_join_table_source_type_mismatch_is_rejected():
    dataset = SimpleNamespace(
        dataset_id="join-1",
        db_name="test_db",
        name="join_ds",
        branch="main",
        source_type="dataset",
        source_ref=None,
        schema_json={
            "columns": [
                {"name": "source_id", "type": "xsd:string"},
                {"name": "target_id", "type": "xsd:string"},
            ]
        },
    )
    version = SimpleNamespace(
        version_id="ver-1",
        dataset_id="join-1",
        artifact_key="s3://bucket/join",
        sample_json={
            "columns": [
                {"name": "source_id", "type": "xsd:string"},
                {"name": "target_id", "type": "xsd:string"},
            ]
        },
    )
    dataset_registry = _FakeDatasetRegistry(dataset=dataset, version=version)

    source_props = {"source_id": {"type": "xsd:integer"}}
    target_props = {"target_id": {"type": "xsd:string"}}
    source_contract = {"pk_spec": {"primary_key": ["source_id"]}}
    target_contract = {"pk_spec": {"primary_key": ["target_id"]}}
    spec_payload = {
        "type": "join_table",
        "join_dataset_id": "join-1",
        "join_dataset_version_id": "ver-1",
        "source_key_column": "source_id",
        "target_key_column": "target_id",
    }

    with pytest.raises(HTTPException) as exc_info:
        await link_types_router._build_mapping_request(
            db_name="test_db",
            request=Request({"type": "http", "headers": []}),
            oms_client=None,
            dataset_registry=dataset_registry,
            relationship_spec_id="rel-1",
            link_type_id="link-1",
            source_class="Source",
            target_class="Target",
            predicate="linked_to",
            cardinality="n:m",
            branch="main",
            source_props=source_props,
            target_props=target_props,
            source_contract=source_contract,
            target_contract=target_contract,
            spec_payload=spec_payload,
        )

    detail = exc_info.value.detail
    assert exc_info.value.status_code == 409
    assert detail["code"] == "RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH"
