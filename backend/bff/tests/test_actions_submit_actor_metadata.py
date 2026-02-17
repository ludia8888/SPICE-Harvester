from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app


def test_action_submit_batch_forwards_actor_identity_from_headers():
    fake_oms = AsyncMock()
    fake_oms.post.return_value = {"status": "success", "action_log_id": "00000000-0000-0000-0000-000000000000"}
    fake_oms.list_databases.return_value = {"data": {"databases": [{"name": "demo_db"}]}}

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    client = TestClient(app)
    try:
        with patch("bff.routers.foundry_ontology_v2.enforce_database_role", new=AsyncMock()):
            res = client.post(
                "/api/v2/ontologies/demo_db/actions/ApproveTicket/applyBatch?branch=main",
                headers={"X-User-ID": "alice", "X-User-Type": "service"},
                json={
                    "requests": [
                        {
                            "parameters": {"ticket_id": "T-1"},
                        }
                    ],
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 200

    called = fake_oms.post.await_args
    assert called.args[0] == "/api/v2/ontologies/demo_db/actions/ApproveTicket/applyBatch"
    metadata = called.kwargs["json"]["metadata"]
    assert metadata["user_id"] == "alice"
    assert metadata["user_type"] == "service"
