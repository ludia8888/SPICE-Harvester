from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app


def test_action_submit_forwards_actor_identity_from_headers():
    fake_oms = AsyncMock()
    fake_oms.post.return_value = {"status": "success", "action_log_id": "00000000-0000-0000-0000-000000000000"}

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    client = TestClient(app)
    try:
        with patch("bff.routers.actions.enforce_database_role", new=AsyncMock()):
            res = client.post(
                "/api/v1/databases/demo_db/actions/ApproveTicket/submit",
                params={"base_branch": "main", "user_id": "spoofed"},
                headers={"X-User-ID": "alice", "X-User-Type": "service"},
                json={"input": {"ticket_id": "T-1"}, "metadata": {"user_id": "also_spoofed"}},
            )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 202

    called = fake_oms.post.await_args
    assert called.args[0] == "/api/v1/actions/demo_db/async/ApproveTicket/submit"
    metadata = called.kwargs["json"]["metadata"]
    assert metadata["user_id"] == "alice"
    assert metadata["user_type"] == "service"

