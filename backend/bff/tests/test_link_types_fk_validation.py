from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.routers import link_types as link_types_router


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

    async def get_backing_datasource(self, *, backing_id):  # noqa: ANN003
        if backing_id == self._backing.backing_id:
            return self._backing
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
async def test_fk_type_mismatch_is_rejected():
    dataset = SimpleNamespace(
        dataset_id="ds-1",
        db_name="test_db",
        name="source_ds",
        branch="main",
        source_type="dataset",
        source_ref=None,
        schema_json={"columns": [{"name": "source_id", "type": "xsd:string"}, {"name": "fk_id", "type": "xsd:string"}]},
    )
    version = SimpleNamespace(
        version_id="ver-1",
        dataset_id="ds-1",
        artifact_key="s3://bucket/source",
        sample_json={"columns": [{"name": "source_id", "type": "xsd:string"}, {"name": "fk_id", "type": "xsd:string"}]},
    )
    dataset_registry = _FakeDatasetRegistry(dataset=dataset, version=version)

    source_props = {"source_id": {"type": "xsd:string"}}
    target_props = {"target_id": {"type": "xsd:integer"}}
    source_contract = {"pk_spec": {"primary_key": ["source_id"]}}
    target_contract = {"pk_spec": {"primary_key": ["target_id"]}}
    spec_payload = {
        "type": "foreign_key",
        "source_dataset_id": "ds-1",
        "fk_column": "fk_id",
        "target_pk_field": "target_id",
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
            cardinality="1:1",
            branch="main",
            source_props=source_props,
            target_props=target_props,
            source_contract=source_contract,
            target_contract=target_contract,
            spec_payload=spec_payload,
        )

    detail = exc_info.value.detail
    assert exc_info.value.status_code == 409
    assert detail["code"] == "RELATIONSHIP_FK_TYPE_MISMATCH"
