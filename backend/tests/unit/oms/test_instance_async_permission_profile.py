from __future__ import annotations

import pytest

from oms.routers import instance_async
from shared.models.commands import CommandStatus


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_instance_async_allows_missing_terminus_in_postgres_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ONTOLOGY_RESOURCE_STORAGE_BACKEND", "postgres")

    captured = {}

    async def _fake_publish_instance_command(**kwargs):  # noqa: ANN003
        captured["command"] = kwargs["command"]

    monkeypatch.setattr(instance_async, "_publish_instance_command", _fake_publish_instance_command)

    response = await instance_async.create_instance_async(
        db_name="demo",
        class_id="Product",
        branch="main",
        request=instance_async.InstanceCreateRequest(
            data={"product_id": "product-1", "name": "Demo"},
            metadata={"kind": "ingest"},
        ),
        command_status_service=None,
        event_store=object(),
        user_id="tester",
    )

    assert response.status == CommandStatus.PENDING
    assert response.result is not None
    assert response.result.get("instance_id") == "product-1"

    command = captured["command"]
    ontology_stamp = command.metadata.get("ontology")
    assert ontology_stamp.get("ref") == "branch:main"
    # Postgres profiles may not have an immutable commit id; parity requires a non-empty
    # commit field anyway (floating-head semantics).
    assert ontology_stamp.get("commit") == ontology_stamp.get("ref")
