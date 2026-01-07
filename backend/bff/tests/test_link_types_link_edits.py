from types import SimpleNamespace

from fastapi.testclient import TestClient

from bff.main import app
from bff.routers import link_types as link_types_router


class _FakeDatasetRegistry:
    def __init__(self, *, spec):
        self._spec = spec
        self.recorded = None

    async def get_relationship_spec(self, *, link_type_id: str):
        return SimpleNamespace(
            relationship_spec_id="rs-1",
            link_type_id=link_type_id,
            db_name="test_db",
            source_object_type="Account",
            target_object_type="User",
            predicate="owned_by",
            spec=self._spec,
        )

    async def record_link_edit(self, **kwargs):
        self.recorded = dict(kwargs)
        return SimpleNamespace(**self.recorded)


def _post_link_edit(*, registry, payload):
    app.dependency_overrides[link_types_router.get_dataset_registry] = lambda: registry
    client = TestClient(app)
    try:
        return client.post(
            "/api/v1/databases/test_db/ontology/link-types/link-1/edits",
            json=payload,
        )
    finally:
        app.dependency_overrides.clear()


def test_link_edit_rejected_when_disabled():
    registry = _FakeDatasetRegistry(spec={})
    payload = {
        "source_instance_id": "acc-1",
        "target_instance_id": "user-1",
        "edit_type": "ADD",
    }

    res = _post_link_edit(registry=registry, payload=payload)

    assert res.status_code == 409
    detail = res.json()["detail"]
    assert detail["code"] == "LINK_EDITS_DISABLED"


def test_link_edit_records_when_enabled():
    registry = _FakeDatasetRegistry(spec={"edits_enabled": True})
    payload = {
        "source_instance_id": "acc-1",
        "target_instance_id": "user-1",
        "edit_type": "ADD",
        "metadata": {"reason": "manual"},
    }

    res = _post_link_edit(registry=registry, payload=payload)

    assert res.status_code == 200
    data = res.json()["data"]["link_edit"]
    assert data["link_type_id"] == "link-1"
    assert data["edit_type"] == "ADD"
