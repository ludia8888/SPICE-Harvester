from __future__ import annotations

import pytest

from oms.services.action_simulation_service import (
    ActionPreflight,
    ActionSimulationRejected,
    TargetPreflight,
    build_patchset_for_scenario,
)


def _preflight(*, conflict_fields: list[str], conflict_policy: str | None = None) -> ActionPreflight:
    base_state = {"instance_id": "i1", "class_id": "Ticket", "status": "OPEN"}
    changes = {"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    observed_base = {"fields": {"status": "OPEN"}, "links": {}}
    base_token = {"db_name": "db", "instance_id": "i1", "lifecycle_id": "lc-0", "base_state_hash": "sha256:x"}

    target = TargetPreflight(
        resource_rid="object_type:Ticket@1",
        class_id="Ticket",
        instance_id="i1",
        lifecycle_id="lc-0",
        base_state=base_state,
        changes=changes,
        observed_base=observed_base,
        base_token=base_token,
        conflict_fields=conflict_fields,
        conflict_links=[],
        object_conflict_policy=None,
    )
    return ActionPreflight(
        db_name="db",
        action_type_id="ApproveTicket",
        action_type_rid="action_type:ApproveTicket@1",
        ontology_commit_id="commit",
        base_branch="main",
        overlay_branch="writeback-db",
        writeback_target={"repo": "ontology-writeback", "branch": "writeback-db"},
        submitted_by="u",
        submitted_by_type="user",
        actor_role="Owner",
        input_payload={"ticket": {"class_id": "Ticket", "instance_id": "i1"}},
        action_conflict_policy=conflict_policy,
        loaded_targets=[target],
    )


@pytest.mark.unit
def test_conflict_policy_fail_rejects() -> None:
    preflight = _preflight(conflict_fields=["status"], conflict_policy=None)
    with pytest.raises(ActionSimulationRejected) as exc:
        build_patchset_for_scenario(preflight=preflight, action_log_id="preview", conflict_policy_override="FAIL")
    assert exc.value.status_code == 409
    assert exc.value.payload.get("error") == "conflict_detected"


@pytest.mark.unit
def test_conflict_policy_base_wins_skips() -> None:
    preflight = _preflight(conflict_fields=["status"], conflict_policy=None)
    patchset, targets, conflicts, policies_used = build_patchset_for_scenario(
        preflight=preflight,
        action_log_id="preview",
        conflict_policy_override="BASE_WINS",
    )
    assert patchset["targets"][0]["conflict"]["resolution"] == "SKIPPED"
    applied = patchset["targets"][0]["applied_changes"]
    assert applied.get("set") == {}
    assert applied.get("unset") == []
    assert applied.get("delete") is False
    assert conflicts and conflicts[0]["policy"] == "BASE_WINS"
    assert policies_used == ["BASE_WINS"]


@pytest.mark.unit
def test_conflict_policy_writeback_wins_applies() -> None:
    preflight = _preflight(conflict_fields=["status"], conflict_policy=None)
    patchset, targets, conflicts, policies_used = build_patchset_for_scenario(
        preflight=preflight,
        action_log_id="preview",
        conflict_policy_override="WRITEBACK_WINS",
    )
    assert patchset["targets"][0]["conflict"]["resolution"] == "APPLIED"
    applied = patchset["targets"][0]["applied_changes"]
    assert applied.get("set") == {"status": "APPROVED"}
    assert conflicts and conflicts[0]["policy"] == "WRITEBACK_WINS"
    assert policies_used == ["WRITEBACK_WINS"]


@pytest.mark.unit
def test_no_conflict_does_not_reject_under_fail() -> None:
    preflight = _preflight(conflict_fields=[], conflict_policy=None)
    patchset, targets, conflicts, policies_used = build_patchset_for_scenario(
        preflight=preflight,
        action_log_id="preview",
        conflict_policy_override="FAIL",
    )
    assert not conflicts
    assert patchset["targets"][0]["applied_changes"]["set"] == {"status": "APPROVED"}
    assert policies_used == ["FAIL"]

