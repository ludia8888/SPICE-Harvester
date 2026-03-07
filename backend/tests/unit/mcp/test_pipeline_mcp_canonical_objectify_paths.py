from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_objectify_create_mapping_spec_delegates_to_canonical_bff(monkeypatch: pytest.MonkeyPatch) -> None:
    from mcp_servers.pipeline_tools import objectify_tools as target

    captured: dict[str, object] = {}

    async def _fake_bff_json(method: str, path: str, **kwargs):  # noqa: ANN003
        captured["method"] = method
        captured["path"] = path
        captured["kwargs"] = kwargs
        return {
            "status": "success",
            "message": "Mapping spec created",
            "data": {
                "mapping_spec": {
                    "mapping_spec_id": "map-1",
                    "dataset_id": "ds-1",
                    "target_class_id": "Customer",
                    "schema_hash": "schema-1",
                    "auto_sync": True,
                    "version": 3,
                }
            },
        }

    monkeypatch.setattr(target, "bff_json", _fake_bff_json)

    result = await target._objectify_create_mapping_spec(
        None,
        {
            "db_name": "qa_db",
            "dataset_id": "ds-1",
            "target_class_id": "Customer",
            "dataset_branch": "feature-1",
            "auto_sync": True,
            "mappings": [{"source_field": "customer_id", "target_field": "customerId"}],
            "options": {"ontology_branch": "feature-1"},
        },
    )

    assert result["status"] == "success"
    assert result["mapping_spec_id"] == "map-1"
    assert result["mapping_spec_version"] == 3
    assert captured["method"] == "POST"
    assert captured["path"] == "/objectify/mapping-specs"
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    json_body = kwargs.get("json_body")
    assert isinstance(json_body, dict)
    assert json_body["dataset_id"] == "ds-1"
    assert json_body["target_class_id"] == "Customer"
    assert json_body["dataset_branch"] == "feature-1"
    assert json_body["mappings"] == [{"source_field": "customer_id", "target_field": "customerId"}]


@pytest.mark.asyncio
async def test_objectify_run_delegates_to_canonical_bff(monkeypatch: pytest.MonkeyPatch) -> None:
    from mcp_servers.pipeline_tools import objectify_tools as target

    captured: dict[str, object] = {}

    async def _fake_bff_json(method: str, path: str, **kwargs):  # noqa: ANN003
        captured["method"] = method
        captured["path"] = path
        captured["kwargs"] = kwargs
        return {
            "status": "success",
            "message": "Objectify job queued",
            "data": {
                "job_id": "job-1",
                "dataset_id": "ds-1",
                "dataset_version_id": "ver-1",
                "mapping_spec_id": "map-1",
                "status": "QUEUED",
                "oms_mode": False,
            },
        }

    monkeypatch.setattr(target, "bff_json", _fake_bff_json)

    result = await target._objectify_run(
        None,
        {
            "db_name": "qa_db",
            "dataset_id": "ds-1",
            "target_class_id": "Customer",
            "dataset_version_id": "ver-1",
            "batch_size": 200,
            "max_rows": 1000,
        },
    )

    assert result["status"] == "success"
    assert result["job_id"] == "job-1"
    assert result["job_status"] == "QUEUED"
    assert captured["method"] == "POST"
    assert captured["path"] == "/objectify/datasets/ds-1/run"
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    json_body = kwargs.get("json_body")
    assert isinstance(json_body, dict)
    assert json_body["target_class_id"] == "Customer"
    assert json_body["dataset_version_id"] == "ver-1"
    assert json_body["batch_size"] == 200
    assert json_body["max_rows"] == 1000


@pytest.mark.asyncio
async def test_ontology_register_object_type_uses_canonical_bff_contract_and_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    from mcp_servers.pipeline_tools import ontology_tools as target

    calls: list[tuple[str, str, dict]] = []

    async def _fake_bff_v2_json(method: str, path: str, **kwargs):  # noqa: ANN003
        calls.append((method, path, kwargs))
        if method == "POST":
            return {"apiName": "Customer", "primaryKey": "customerId"}
        if method == "PATCH":
            return {"apiName": "Customer", "mappingSpecId": "map-1"}
        raise AssertionError(f"unexpected bff_v2_json call: {method} {path}")

    async def _fake_bff_json(method: str, path: str, **kwargs):  # noqa: ANN003
        calls.append((method, path, kwargs))
        assert method == "POST"
        assert path == "/objectify/mapping-specs"
        return {
            "status": "success",
            "data": {
                "mapping_spec": {
                    "mapping_spec_id": "map-1",
                    "target_class_id": "Customer",
                    "version": 2,
                }
            },
        }

    monkeypatch.setattr(target, "bff_v2_json", _fake_bff_v2_json)
    monkeypatch.setattr(target, "bff_json", _fake_bff_json)

    result = await target._ontology_register_object_type(
        None,
        {
            "db_name": "qa_db",
            "class_id": "Customer",
            "dataset_id": "ds-1",
            "primary_key": ["customerId"],
            "title_key": ["customerName"],
            "branch": "feature-1",
            "property_mappings": [{"source_field": "customer_id", "target_field": "customerId"}],
            "target_field_types": {"customerId": "string"},
        },
    )

    assert result["status"] == "success"
    assert result["mapping_spec_id"] == "map-1"
    assert [entry[0:2] for entry in calls] == [
        ("POST", "/v2/ontologies/qa_db/objectTypes"),
        ("POST", "/objectify/mapping-specs"),
        ("PATCH", "/v2/ontologies/qa_db/objectTypes/Customer"),
    ]
    create_payload = calls[0][2]["json_body"]
    assert create_payload["apiName"] == "Customer"
    assert create_payload["backingSource"] == {"kind": "dataset", "ref": "ds-1"}
    assert create_payload["pkSpec"] == {"primary_key": ["customerId"], "title_key": ["customerName"]}
    mapping_payload = calls[1][2]["json_body"]
    assert mapping_payload["options"] == {"ontology_branch": "feature-1"}
    assert mapping_payload["target_field_types"] == {"customerId": "string"}
    link_payload = calls[2][2]["json_body"]
    assert link_payload == {"mappingSpecId": "map-1", "mappingSpecVersion": 2}


@pytest.mark.asyncio
async def test_ontology_register_object_type_updates_existing_contract_on_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    from mcp_servers.pipeline_tools import ontology_tools as target

    calls: list[tuple[str, str, dict]] = []

    async def _fake_bff_v2_json(method: str, path: str, **kwargs):  # noqa: ANN003
        calls.append((method, path, kwargs))
        if method == "POST":
            return {"error": "already exists", "status_code": 409, "response": {"detail": "already exists"}}
        if method == "PATCH":
            return {"apiName": "Customer", "primaryKey": "customerId"}
        raise AssertionError(f"unexpected bff_v2_json call: {method} {path}")

    async def _should_not_create_mapping(*args, **kwargs):  # noqa: ANN003
        raise AssertionError("property_mappings were not provided")

    monkeypatch.setattr(target, "bff_v2_json", _fake_bff_v2_json)
    monkeypatch.setattr(target, "bff_json", _should_not_create_mapping)

    result = await target._ontology_register_object_type(
        None,
        {
            "db_name": "qa_db",
            "class_id": "Customer",
            "dataset_id": "ds-1",
            "primary_key": ["customerId"],
            "title_key": ["customerName"],
        },
    )

    assert result["status"] == "success"
    assert [entry[0:2] for entry in calls] == [
        ("POST", "/v2/ontologies/qa_db/objectTypes"),
        ("PATCH", "/v2/ontologies/qa_db/objectTypes/Customer"),
    ]
