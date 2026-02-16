from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from bff.dependencies import get_oms_client
from bff.main import app


def test_action_submit_batch_forwards_actor_identity():
    fake_oms = AsyncMock()
    fake_oms.post.return_value = {"batch_id": "b1", "items": []}

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    client = TestClient(app)
    try:
        with patch("bff.routers.actions.enforce_database_role", new=AsyncMock()):
            res = client.post(
                "/api/v1/databases/demo_db/actions/ApproveTicket/submit-batch",
                headers={"X-User-ID": "alice", "X-User-Type": "service"},
                json={
                    "items": [
                        {
                            "request_id": "r1",
                            "input": {"ticket_id": "T-1"},
                            "metadata": {"user_id": "spoofed"},
                        }
                    ],
                    "base_branch": "main",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 202, res.text
    called = fake_oms.post.await_args
    assert called.args[0] == "/api/v1/actions/demo_db/async/ApproveTicket/submit-batch"
    metadata = called.kwargs["json"]["items"][0]["metadata"]
    assert metadata["user_id"] == "alice"
    assert metadata["user_type"] == "service"


def test_action_undo_forwards_actor_identity():
    fake_oms = AsyncMock()
    fake_oms.post.return_value = {"status": "PENDING"}

    app.dependency_overrides[get_oms_client] = lambda: fake_oms
    client = TestClient(app)
    try:
        with patch("bff.routers.actions.enforce_database_role", new=AsyncMock()):
            res = client.post(
                "/api/v1/databases/demo_db/actions/logs/00000000-0000-0000-0000-000000000123/undo",
                headers={"X-User-ID": "alice", "X-User-Type": "service"},
                json={"reason": "mistake"},
            )
    finally:
        app.dependency_overrides.clear()

    assert res.status_code == 202, res.text
    called = fake_oms.post.await_args
    assert called.args[0] == "/api/v1/actions/demo_db/async/logs/00000000-0000-0000-0000-000000000123/undo"
    metadata = called.kwargs["json"]["metadata"]
    assert metadata["user_id"] == "alice"
    assert metadata["user_type"] == "service"
