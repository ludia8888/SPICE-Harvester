from __future__ import annotations

import pytest

from oms.services.action_simulation_service import (
    ActionSimulationRejected,
    _apply_assumption_patch,
    _apply_observed_base_overrides,
)
from shared.utils.writeback_conflicts import compute_observed_base, detect_overlap_fields


@pytest.mark.unit
def test_apply_assumption_patch_applies_set_unset_and_links() -> None:
    base = {"instance_id": "i1", "class_id": "Ticket", "status": "OPEN", "tags": ["a"], "note": "x"}
    patch = {
        "set": {"status": "CLOSED"},
        "unset": ["note"],
        "link_add": [{"field": "tags", "value": "b"}],
        "link_remove": [],
        "delete": False,
    }
    assumed, report = _apply_assumption_patch(scope="base_overrides", base_state=base, patch=patch)
    assert assumed["status"] == "CLOSED"
    assert "note" not in assumed
    assert assumed["tags"] == ["a", "b"]
    assert report == {"fields": ["note", "status"], "links": ["tags"]}


@pytest.mark.unit
def test_apply_assumption_patch_rejects_forbidden_field() -> None:
    base = {"instance_id": "i1", "class_id": "Ticket", "status": "OPEN"}
    patch = {"set": {"class_id": "Other"}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    with pytest.raises(ActionSimulationRejected) as exc:
        _apply_assumption_patch(scope="base_overrides", base_state=base, patch=patch)
    assert exc.value.payload.get("error") == "simulation_assumption_forbidden_field"


@pytest.mark.unit
def test_apply_observed_base_overrides_rejects_unknown_field() -> None:
    observed = {"fields": {"status": "OPEN"}, "links": {}}
    overrides = {"fields": {"priority": "HIGH"}}
    with pytest.raises(ActionSimulationRejected) as exc:
        _apply_observed_base_overrides(observed_base=observed, overrides=overrides)
    assert exc.value.payload.get("error") == "simulation_assumption_invalid"


@pytest.mark.unit
def test_observed_base_override_can_create_conflict() -> None:
    base_state = {"status": "PAID"}
    changes = {"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    observed_base = compute_observed_base(base=base_state, changes=changes)

    _apply_observed_base_overrides(observed_base=observed_base, overrides={"fields": {"status": "PENDING"}})
    conflicts = detect_overlap_fields(observed_base=observed_base, current_base=base_state)
    assert conflicts == ["status"]

