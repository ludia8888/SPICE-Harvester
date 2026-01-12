from datetime import datetime, timezone

import pytest

from shared.utils.action_template_engine import (
    ActionImplementationError,
    compile_template_v1,
    compile_template_v1_change_shape,
)


def test_compile_template_v1_change_shape_merges_and_tracks_touched_fields():
    impl = {
        "type": "template_v1",
        "targets": [
            {
                "target": {"from": "input.ticket"},
                "changes": {
                    "set": {"status": "APPROVED"},
                    "unset": ["rejected_reason"],
                    "link_add": [{"field": "assignees", "value": "user:bob"}],
                    "delete": False,
                },
            },
            {
                "target": {"from": "input.ticket"},
                "changes": {
                    "set": {"status": "OVERRIDE"},
                    "unset": ["status"],
                    "link_add": [{"assignees": "user:carol"}],
                    "delete": False,
                },
            },
        ],
    }
    input_payload = {"ticket": {"class_id": "Ticket", "instance_id": "t1"}}
    compiled = compile_template_v1_change_shape(impl, input_payload=input_payload)
    assert len(compiled) == 1
    changes = compiled[0].changes
    assert changes["set"] == {"status": None}
    assert changes["unset"] == ["rejected_reason"]
    assert changes["delete"] is False
    assert {op.get("field") for op in changes["link_add"]} == {"assignees"}
    assert {op.get("value") for op in changes["link_add"]} == {"__touch__"}


def test_compile_template_v1_resolves_refs_and_now():
    now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    impl = {
        "type": "template_v1",
        "targets": [
            {
                "target": {"from": "input.ticket"},
                "changes": {
                    "set": {
                        "status": "APPROVED",
                        "approved_by": {"$ref": "user.id"},
                        "comment": {"$ref": "input.comment"},
                        "previous_status": {"$ref": "target.status"},
                        "approved_at": {"$now": True},
                    },
                    "unset": ["rejected_reason"],
                    "delete": False,
                },
            }
        ],
    }
    input_payload = {"ticket": {"class_id": "Ticket", "instance_id": "t1"}, "comment": "LGTM"}
    user = {"id": "alice", "role": "DomainModeler", "is_system": False}
    target_docs = {("Ticket", "t1"): {"status": "OPEN"}}
    compiled = compile_template_v1(impl, input_payload=input_payload, user=user, target_docs=target_docs, now=now)
    assert len(compiled) == 1
    changes = compiled[0].changes
    assert changes["set"]["approved_by"] == "alice"
    assert changes["set"]["comment"] == "LGTM"
    assert changes["set"]["previous_status"] == "OPEN"
    assert changes["set"]["approved_at"] == now.isoformat()
    assert changes["unset"] == ["rejected_reason"]


def test_compile_template_v1_rejects_delete_plus_edits_for_same_target():
    impl = {
        "type": "template_v1",
        "targets": [
            {"target": {"from": "input.ticket"}, "changes": {"delete": True}},
            {"target": {"from": "input.ticket"}, "changes": {"set": {"status": "X"}}},
        ],
    }
    input_payload = {"ticket": {"class_id": "Ticket", "instance_id": "t1"}}
    user = {"id": "alice", "role": "DomainModeler", "is_system": False}
    with pytest.raises(ActionImplementationError):
        compile_template_v1(impl, input_payload=input_payload, user=user, target_docs={}, now=datetime.now(timezone.utc))


def test_compile_template_v1_supports_bulk_targets_from_list():
    impl = {
        "type": "template_v1",
        "targets": [
            {
                "target": {"from": "input.tickets"},
                "changes": {"set": {"status": "APPROVED"}, "delete": False},
            }
        ],
    }
    input_payload = {
        "tickets": [
            {"class_id": "Ticket", "instance_id": "t1"},
            {"class_id": "Ticket", "instance_id": "t2"},
        ]
    }
    user = {"id": "alice", "role": "DomainModeler", "is_system": False}
    target_docs = {("Ticket", "t1"): {}, ("Ticket", "t2"): {}}
    compiled = compile_template_v1(impl, input_payload=input_payload, user=user, target_docs=target_docs)
    assert [(t.class_id, t.instance_id) for t in compiled] == [("Ticket", "t1"), ("Ticket", "t2")]
