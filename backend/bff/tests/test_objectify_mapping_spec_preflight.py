from types import SimpleNamespace

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app
from bff.routers import objectify as objectify_router


class _FakeDatasetRegistry:
    def __init__(self, dataset, latest_version):
        self._dataset = dataset
        self._latest_version = latest_version

    async def get_dataset(self, *, dataset_id: str):
        return self._dataset

    async def get_latest_version(self, *, dataset_id: str):
        return self._latest_version


class _FakeObjectifyRegistry:
    async def create_mapping_spec(self, **kwargs):
        raise AssertionError("create_mapping_spec should not be called for preflight errors")


class _FakeOMSClient:
    def __init__(self, payload):
        self._payload = payload

    async def get_ontology(self, db_name, class_id, *, branch="main"):
        return self._payload


def _make_dataset(schema_columns):
    schema_json = {"columns": [{"name": name} for name in schema_columns]}
    return SimpleNamespace(
        dataset_id="ds-1",
        db_name="test_db",
        branch="main",
        name="Dataset",
        schema_json=schema_json,
    )


def _make_latest(schema_columns):
    return SimpleNamespace(sample_json={"columns": [{"name": name} for name in schema_columns]})


def _post_mapping_spec(body, *, schema_columns, ontology_payload):
    dataset = _make_dataset(schema_columns)
    latest_version = _make_latest(schema_columns)
    dataset_registry = _FakeDatasetRegistry(dataset, latest_version)
    objectify_registry = _FakeObjectifyRegistry()
    oms_client = _FakeOMSClient(ontology_payload)

    app.dependency_overrides[objectify_router.get_dataset_registry] = lambda: dataset_registry
    app.dependency_overrides[objectify_router.get_objectify_registry] = lambda: objectify_registry
    app.dependency_overrides[get_oms_client] = lambda: oms_client
    client = TestClient(app)
    try:
        return client.post(
            "/api/v1/objectify/mapping-specs",
            json=body,
            headers={"X-DB-Name": dataset.db_name},
        )
    finally:
        app.dependency_overrides.clear()


def _base_payload(mappings, *, target_field_types=None, options=None):
    payload = {
        "dataset_id": "ds-1",
        "target_class_id": "Product",
        "mappings": mappings,
        "schema_hash": "schema-hash",
    }
    if target_field_types is not None:
        payload["target_field_types"] = target_field_types
    if options is not None:
        payload["options"] = options
    return payload


def test_mapping_spec_source_missing_is_rejected():
    payload = _base_payload(
        [{"source_field": "missing", "target_field": "id"}],
    )
    ontology_payload = {"properties": [{"name": "id", "type": "xsd:string"}], "relationships": []}

    res = _post_mapping_spec(payload, schema_columns=["id"], ontology_payload=ontology_payload)

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_SOURCE_MISSING"
    assert detail["missing_sources"] == ["missing"]


def test_mapping_spec_target_unknown_is_rejected():
    payload = _base_payload(
        [{"source_field": "id", "target_field": "unknown"}],
    )
    ontology_payload = {"properties": [{"name": "id", "type": "xsd:string"}], "relationships": []}

    res = _post_mapping_spec(payload, schema_columns=["id"], ontology_payload=ontology_payload)

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_TARGET_UNKNOWN"
    assert detail["missing_targets"] == ["unknown"]


def test_mapping_spec_relationship_target_is_rejected():
    payload = _base_payload(
        [{"source_field": "owner_id", "target_field": "owner"}],
    )
    ontology_payload = {
        "properties": [{"name": "owner", "type": "xsd:string"}],
        "relationships": [{"predicate": "owner"}],
    }

    res = _post_mapping_spec(payload, schema_columns=["owner_id"], ontology_payload=ontology_payload)

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_RELATIONSHIP_TARGET"
    assert detail["targets"] == ["owner"]


def test_mapping_spec_required_missing_is_rejected():
    payload = _base_payload(
        [{"source_field": "id", "target_field": "id"}],
    )
    ontology_payload = {
        "properties": [
            {"name": "id", "type": "xsd:string", "primary_key": True},
            {"name": "email", "type": "xsd:string", "required": True},
        ],
        "relationships": [],
    }

    res = _post_mapping_spec(payload, schema_columns=["id", "email"], ontology_payload=ontology_payload)

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_REQUIRED_MISSING"
    assert detail["missing_targets"] == ["email"]


def test_mapping_spec_primary_key_missing_is_rejected():
    payload = _base_payload(
        [{"source_field": "name", "target_field": "name"}],
    )
    ontology_payload = {
        "properties": [
            {"name": "id", "type": "xsd:string", "primary_key": True},
            {"name": "name", "type": "xsd:string"},
        ],
        "relationships": [],
    }

    res = _post_mapping_spec(payload, schema_columns=["name"], ontology_payload=ontology_payload)

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_PRIMARY_KEY_MISSING"
    assert detail["missing_targets"] == ["id"]


def test_mapping_spec_unsupported_type_is_rejected():
    payload = _base_payload(
        [
            {"source_field": "tags", "target_field": "tags"},
            {"source_field": "product_id", "target_field": "product_id"},
        ],
    )
    ontology_payload = {
        "properties": [
            {"name": "tags", "type": "link"},
            {"name": "product_id", "type": "xsd:string", "primary_key": True},
        ],
        "relationships": [],
    }

    res = _post_mapping_spec(
        payload, schema_columns=["tags", "product_id"], ontology_payload=ontology_payload
    )

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_UNSUPPORTED_TYPES"
    assert detail["targets"] == ["tags"]


def test_mapping_spec_target_type_mismatch_is_rejected():
    payload = _base_payload(
        [{"source_field": "name", "target_field": "name"}],
        target_field_types={"name": "xsd:integer"},
        options={"primary_key_targets": ["name"]},
    )
    ontology_payload = {
        "properties": [{"name": "name", "type": "xsd:string"}],
        "relationships": [],
    }

    res = _post_mapping_spec(payload, schema_columns=["name"], ontology_payload=ontology_payload)

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_TARGET_TYPE_MISMATCH"
    assert detail["mismatches"][0]["target_field"] == "name"
