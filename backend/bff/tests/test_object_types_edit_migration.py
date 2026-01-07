from types import SimpleNamespace

import pytest
from starlette.requests import Request

from bff.routers import object_types as object_types_router


class _FakeDatasetRegistry:
    def __init__(self, *, edit_count=0, edit_impact=None):
        self.edit_count = edit_count
        self.edit_impact = edit_impact or {"total": edit_count}
        self.moved = None
        self.dropped = None
        self.invalidated = None
        self.remapped = None
        self.cleared = None
        self.last_plan = None

    async def record_gate_result(self, **kwargs):  # noqa: ANN003
        return None

    async def create_schema_migration_plan(self, **kwargs):  # noqa: ANN003
        self.last_plan = kwargs

    async def count_instance_edits(self, **kwargs):  # noqa: ANN003
        return self.edit_count

    async def get_instance_edit_field_stats(self, **kwargs):  # noqa: ANN003
        return self.edit_impact

    async def apply_instance_edit_field_moves(self, **kwargs):  # noqa: ANN003
        self.moved = kwargs
        return {"updated": len(kwargs.get("field_moves") or {})}

    async def update_instance_edit_status_by_fields(self, **kwargs):  # noqa: ANN003
        note = kwargs.get("metadata_note")
        if note == "field_drop":
            self.dropped = kwargs
        elif note == "field_invalidate":
            self.invalidated = kwargs
        return {"updated": len(kwargs.get("fields") or [])}

    async def remap_instance_edits(self, **kwargs):  # noqa: ANN003
        self.remapped = kwargs
        return {"updated": len(kwargs.get("id_map") or {})}

    async def clear_instance_edits(self, **kwargs):  # noqa: ANN003
        self.cleared = kwargs
        return 0


class _FakeOMSClient:
    def __init__(self, *, existing_spec, properties):
        self._spec = existing_spec
        self._properties = properties

    async def get_ontology_resource(self, db_name, resource_type, resource_id, branch):  # noqa: ANN003
        _ = db_name, resource_type, resource_id, branch
        return {"data": {"id": resource_id, "label": resource_id, "spec": self._spec}}

    async def update_ontology_resource(  # noqa: ANN003
        self,
        db_name,
        resource_type,
        resource_id,
        payload,
        branch,
        expected_head_commit,
    ):
        _ = db_name, resource_type, resource_id, branch, expected_head_commit
        return {"data": payload}

    async def get_ontology(self, db_name, class_id, branch):  # noqa: ANN003
        _ = db_name, class_id, branch
        return {"data": {"properties": self._properties}}


class _FakeObjectifyRegistry:
    async def get_mapping_spec(self, *, mapping_spec_id):  # noqa: ANN003
        _ = mapping_spec_id
        return None


def _base_spec(status="ACTIVE"):
    return {
        "backing_source": {"ref": "backing-1", "schema_hash": "hash-1", "version_id": "backing-ver-1"},
        "pk_spec": {"primary_key": ["account_id"], "title_key": ["account_id"]},
        "mapping_spec": {"mapping_spec_id": "map-1", "mapping_spec_version": 1},
        "status": status,
    }


@pytest.mark.asyncio
async def test_edit_policy_moves_drops_invalidates_are_applied_and_recorded():
    dataset_registry = _FakeDatasetRegistry(edit_count=3, edit_impact={"total": 3, "fields": {"legacy": 1}})
    oms_client = _FakeOMSClient(
        existing_spec=_base_spec(),
        properties=[{"name": "account_id", "type": "xsd:string"}],
    )
    objectify_registry = _FakeObjectifyRegistry()
    request = Request({"type": "http", "headers": []})

    body = object_types_router.ObjectTypeContractUpdate(
        migration={
            "approved": True,
            "edit_field_moves": {"old_name": "name"},
            "edit_field_drops": ["legacy"],
            "edit_field_invalidates": ["status"],
        }
    )

    response = await object_types_router.update_object_type_contract(
        db_name="test_db",
        class_id="Account",
        body=body,
        request=request,
        branch="main",
        expected_head_commit="head",
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
    )

    assert response.data["object_type"]["spec"]["status"] == "ACTIVE"
    assert dataset_registry.moved["field_moves"] == {"old_name": "name"}
    assert dataset_registry.dropped["fields"] == ["legacy"]
    assert dataset_registry.invalidated["fields"] == ["status"]
    plan = dataset_registry.last_plan["plan"]
    assert plan["edit_policy"]["moves"] == {"old_name": "name"}
    assert plan["edit_policy"]["drops"] == ["legacy"]
    assert plan["edit_policy"]["invalidates"] == ["status"]
    assert plan["edit_impact"]["total"] == 3
    assert plan["edit_actions"]["moved_edits"]["updated"] == 1


@pytest.mark.asyncio
async def test_pk_change_with_id_remap_records_plan():
    dataset_registry = _FakeDatasetRegistry(edit_count=2)
    oms_client = _FakeOMSClient(
        existing_spec=_base_spec(status="INACTIVE"),
        properties=[
            {"name": "account_id", "type": "xsd:string"},
            {"name": "region", "type": "xsd:string"},
        ],
    )
    objectify_registry = _FakeObjectifyRegistry()
    request = Request({"type": "http", "headers": []})

    body = object_types_router.ObjectTypeContractUpdate(
        pk_spec={"primary_key": ["account_id", "region"], "title_key": ["account_id"]},
        migration={
            "approved": True,
            "id_remap": [{"from": "old_1", "to": "new_1"}],
        },
    )

    response = await object_types_router.update_object_type_contract(
        db_name="test_db",
        class_id="Account",
        body=body,
        request=request,
        branch="main",
        expected_head_commit="head",
        oms_client=oms_client,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
    )

    assert response.data["object_type"]["spec"]["pk_spec"]["primary_key"] == ["account_id", "region"]
    assert dataset_registry.remapped["id_map"] == {"old_1": "new_1"}
    plan = dataset_registry.last_plan["plan"]
    assert plan["id_remap"] == {"old_1": "new_1"}
    assert plan["remapped_edits"]["updated"] == 1
