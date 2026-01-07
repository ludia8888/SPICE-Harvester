from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app
from bff.routers import objectify as objectify_router


class _FakeDatasetRegistry:
    def __init__(self, dataset, latest_version, *, key_spec=None):
        self._dataset = dataset
        self._latest_version = latest_version
        self._key_spec = key_spec
        self._backing = SimpleNamespace(
            backing_id="backing-1",
            dataset_id=dataset.dataset_id,
            db_name=dataset.db_name,
            name=dataset.name,
            description=None,
            source_type="dataset",
            source_ref=None,
            branch=dataset.branch,
            status="ACTIVE",
            created_at=None,
            updated_at=None,
        )
        self._backing_version = None

    async def get_dataset(self, *, dataset_id: str):
        return self._dataset

    async def get_latest_version(self, *, dataset_id: str):
        return self._latest_version

    async def get_backing_datasource(self, *, backing_id: str):
        if backing_id == self._backing.backing_id:
            return self._backing
        return None

    async def get_backing_datasource_version(self, *, version_id: str):
        if self._backing_version and version_id == self._backing_version.version_id:
            return self._backing_version
        return None

    async def get_or_create_backing_datasource(self, *, dataset, source_type: str, source_ref: str):
        return self._backing

    async def get_or_create_backing_datasource_version(
        self,
        *,
        backing_id: str,
        dataset_version_id: str,
        schema_hash: str,
        metadata: dict | None = None,
    ):
        if self._backing_version and self._backing_version.dataset_version_id == dataset_version_id:
            return self._backing_version
        self._backing_version = SimpleNamespace(
            version_id="backing-version-1",
            backing_id=backing_id,
            dataset_version_id=dataset_version_id,
            schema_hash=schema_hash,
            artifact_key=None,
            metadata=metadata or {},
            status="ACTIVE",
            created_at=None,
        )
        return self._backing_version

    async def get_key_spec_for_dataset(self, *, dataset_id: str, dataset_version_id: str | None = None):
        return self._key_spec

    async def record_gate_result(self, **kwargs):
        return None


class _FakeObjectifyRegistry:
    async def get_active_mapping_spec(self, **kwargs):
        return None

    async def create_mapping_spec(self, **kwargs):
        raise AssertionError("create_mapping_spec should not be called for preflight errors")


class _CapturingObjectifyRegistry:
    def __init__(self, active_spec):
        self._active_spec = active_spec
        self.created = None

    async def get_active_mapping_spec(self, **kwargs):
        return self._active_spec

    async def create_mapping_spec(self, **kwargs):
        self.created = kwargs
        now = datetime.now(timezone.utc)
        return SimpleNamespace(
            mapping_spec_id="spec-2",
            dataset_id=kwargs["dataset_id"],
            dataset_branch=kwargs["dataset_branch"],
            artifact_output_name=kwargs["artifact_output_name"],
            schema_hash=kwargs["schema_hash"],
            backing_datasource_id=kwargs.get("backing_datasource_id"),
            backing_datasource_version_id=kwargs.get("backing_datasource_version_id"),
            target_class_id=kwargs["target_class_id"],
            mappings=kwargs["mappings"],
            target_field_types=kwargs.get("target_field_types") or {},
            status=kwargs.get("status") or "ACTIVE",
            version=2,
            auto_sync=bool(kwargs.get("auto_sync", True)),
            options=kwargs.get("options") or {},
            created_at=now,
            updated_at=now,
        )


class _FakeOMSClient:
    def __init__(self, payload, object_type_resource):
        self._payload = payload
        self._object_type = object_type_resource

    async def get_ontology(self, db_name, class_id, *, branch="main"):
        return self._payload

    async def get_ontology_resource(self, db_name, *, resource_type, resource_id, branch="main"):
        if resource_type == "object_type":
            return self._object_type
        raise AssertionError(f"Unexpected resource_type: {resource_type}")


def _make_dataset(schema_columns, schema_types=None):
    schema_types = schema_types or {}
    schema_json = {
        "columns": [
            {"name": name, "type": schema_types.get(name, "string")} for name in schema_columns
        ]
    }
    return SimpleNamespace(
        dataset_id="ds-1",
        db_name="test_db",
        branch="main",
        name="Dataset",
        source_type="dataset",
        source_ref=None,
        schema_json=schema_json,
    )


def _make_latest(schema_columns, schema_types=None):
    schema_types = schema_types or {}
    return SimpleNamespace(
        version_id="ver-1",
        dataset_id="ds-1",
        sample_json={
            "columns": [{"name": name, "type": schema_types.get(name, "string")} for name in schema_columns]
        },
    )


def _build_object_type_resource(ontology_payload):
    props = []
    if isinstance(ontology_payload, dict):
        props = ontology_payload.get("properties") or []
    prop_names = [p.get("name") for p in props if isinstance(p, dict) and p.get("name")]
    primary_keys = [p.get("name") for p in props if isinstance(p, dict) and p.get("primary_key")]
    if not primary_keys and prop_names:
        primary_keys = [prop_names[0]]
    title_key = [prop_names[0]] if prop_names else []
    if not title_key and primary_keys:
        title_key = primary_keys[:1]
    return {
        "spec": {
            "pk_spec": {"primary_key": primary_keys, "title_key": title_key},
            "backing_source": {
                "kind": "backing_datasource",
                "ref": "backing-1",
                "schema_hash": "schema-hash",
                "version_id": "backing-version-1",
            },
            "status": "ACTIVE",
        }
    }


def _post_mapping_spec(
    body,
    *,
    schema_columns,
    ontology_payload,
    key_spec=None,
    object_type_resource=None,
    schema_types=None,
    objectify_registry=None,
):
    dataset = _make_dataset(schema_columns, schema_types=schema_types)
    latest_version = _make_latest(schema_columns, schema_types=schema_types)
    dataset_registry = _FakeDatasetRegistry(dataset, latest_version, key_spec=key_spec)
    objectify_registry = objectify_registry or _FakeObjectifyRegistry()
    object_type_resource = object_type_resource or _build_object_type_resource(ontology_payload)
    oms_client = _FakeOMSClient(ontology_payload, object_type_resource)

    original_enforce = objectify_router.enforce_database_role

    async def _noop_enforce_database_role(**kwargs):
        return None

    objectify_router.enforce_database_role = _noop_enforce_database_role
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
        objectify_router.enforce_database_role = original_enforce
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
        "relationships": [{"predicate": "owner", "cardinality": "1:n"}],
    }

    res = _post_mapping_spec(payload, schema_columns=["owner_id"], ontology_payload=ontology_payload)

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED"
    assert detail["targets"] == ["owner"]


def test_mapping_spec_dataset_pk_target_mismatch_is_rejected():
    payload = _base_payload(
        [
            {"source_field": "id", "target_field": "name"},
            {"source_field": "code", "target_field": "id"},
        ],
    )
    ontology_payload = {
        "properties": [
            {"name": "id", "type": "xsd:string", "primary_key": True},
            {"name": "name", "type": "xsd:string"},
        ],
        "relationships": [],
    }
    key_spec = SimpleNamespace(key_spec_id="ks-1", spec={"primary_key": ["id"]})

    res = _post_mapping_spec(
        payload,
        schema_columns=["id", "code", "name"],
        ontology_payload=ontology_payload,
        key_spec=key_spec,
    )

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH"


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
    assert detail["code"] == "MAPPING_SPEC_TITLE_KEY_MISSING"
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


def test_mapping_spec_source_type_incompatible_is_rejected():
    payload = _base_payload(
        [{"source_field": "amount", "target_field": "amount"}],
    )
    ontology_payload = {
        "properties": [{"name": "amount", "type": "xsd:decimal"}],
        "relationships": [],
    }

    res = _post_mapping_spec(
        payload,
        schema_columns=["amount"],
        schema_types={"amount": "string"},
        ontology_payload=ontology_payload,
    )

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_TYPE_INCOMPATIBLE"
    assert detail["mismatches"] == [
        {
            "source_field": "amount",
            "source_type": "xsd:string",
            "target_field": "amount",
            "expected_type": "xsd:decimal",
        }
    ]


def test_mapping_spec_source_type_unsupported_is_rejected():
    payload = _base_payload(
        [{"source_field": "payload", "target_field": "payload"}],
    )
    ontology_payload = {
        "properties": [{"name": "payload", "type": "xsd:string"}],
        "relationships": [],
    }

    res = _post_mapping_spec(
        payload,
        schema_columns=["payload"],
        schema_types={"payload": "struct"},
        ontology_payload=ontology_payload,
    )

    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["code"] == "MAPPING_SPEC_SOURCE_TYPE_UNSUPPORTED"
    assert detail["sources"] == [{"source_field": "payload", "source_type": "struct"}]


def test_mapping_spec_change_summary_is_recorded():
    ontology_payload = {
        "properties": [
            {"name": "id", "type": "xsd:string", "primary_key": True},
            {"name": "name", "type": "xsd:string"},
        ],
        "relationships": [],
    }
    schema_columns = ["id", "name", "full_name"]
    schema_types = {name: "xsd:string" for name in schema_columns}
    previous_spec = SimpleNamespace(
        mapping_spec_id="spec-1",
        dataset_id="ds-1",
        dataset_branch="main",
        artifact_output_name="Dataset",
        schema_hash="schema-hash",
        backing_datasource_id="backing-1",
        backing_datasource_version_id="backing-version-1",
        target_class_id="Product",
        mappings=[
            {"source_field": "id", "target_field": "id"},
            {"source_field": "name", "target_field": "name"},
        ],
        target_field_types={"id": "xsd:string", "name": "xsd:string"},
        status="ACTIVE",
        version=1,
        auto_sync=True,
        options={},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    objectify_registry = _CapturingObjectifyRegistry(previous_spec)
    payload = _base_payload(
        [
            {"source_field": "id", "target_field": "id"},
            {"source_field": "full_name", "target_field": "name"},
        ],
        options={"change_note": "rename source"},
    )
    payload["artifact_output_name"] = "Dataset"

    res = _post_mapping_spec(
        payload,
        schema_columns=schema_columns,
        schema_types=schema_types,
        ontology_payload=ontology_payload,
        objectify_registry=objectify_registry,
    )

    assert res.status_code == 201, res.text
    data = res.json()["data"]["mapping_spec"]
    options = data["options"]
    summary = options["change_summary"]
    assert summary["previous_mapping_spec_id"] == "spec-1"
    assert summary["counts"]["changed_targets"] == 1
    assert summary["changed_targets"] == [
        {"target_field": "name", "previous_sources": ["name"], "new_sources": ["full_name"]}
    ]
    assert options["impact_scope"]["schema_hash"] == "schema-hash"
