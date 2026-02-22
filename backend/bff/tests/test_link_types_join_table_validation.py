from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.services import link_types_mapping_service as link_types_service


class _FakeDatasetRegistry:
    def __init__(self, *, dataset, version):
        self._dataset = dataset
        self._version = version
        self._backing = SimpleNamespace(
            backing_id="backing-1",
            dataset_id=dataset.dataset_id,
            db_name=dataset.db_name,
            name=dataset.name,
            description=None,
            source_type=dataset.source_type,
            source_ref=dataset.source_ref,
            branch=dataset.branch,
            status="ACTIVE",
        )
        self._backing_version = SimpleNamespace(
            version_id="backing-ver-1",
            backing_id="backing-1",
            dataset_version_id=version.version_id,
            schema_hash="hash-1",
            artifact_key=version.artifact_key,
            metadata={},
            status="ACTIVE",
        )

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

    async def get_or_create_backing_datasource(self, *, dataset, source_type, source_ref):  # noqa: ANN003
        _ = dataset, source_type, source_ref
        return self._backing

    async def get_or_create_backing_datasource_version(  # noqa: ANN003
        self,
        *,
        backing_id,
        dataset_version_id,
        schema_hash,
        metadata,
    ):
        _ = backing_id, dataset_version_id, schema_hash, metadata
        return self._backing_version


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
        await link_types_service.build_mapping_request(
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
    detail = exc_info.value.detail
    assert detail["code"] == "OBJECTIFY_MAPPING_ERROR"
    assert detail["message"] == "target_key_column missing"


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

    mapping_request, dataset_id, dataset_version_id, spec_type = await link_types_service.build_mapping_request(
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

    assert spec_type == "join_table"
    assert dataset_id == "join-1"
    assert dataset_version_id == "ver-1"
    assert [(m.source_field, m.target_field) for m in mapping_request.mappings] == [
        ("source_id", "source_id"),
        ("target_id", "linked_to"),
    ]
