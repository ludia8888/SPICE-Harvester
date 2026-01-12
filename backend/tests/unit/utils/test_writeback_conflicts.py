from shared.utils.writeback_conflicts import (
    detect_overlap_fields,
    detect_overlap_links,
    normalize_conflict_policy,
    parse_conflict_policy,
    resolve_applied_changes,
)


def test_normalize_conflict_policy_defaults_to_fail():
    assert normalize_conflict_policy(None) == "FAIL"
    assert normalize_conflict_policy("") == "FAIL"
    assert normalize_conflict_policy("unknown") == "FAIL"


def test_parse_conflict_policy_returns_none_for_missing_or_unknown():
    assert parse_conflict_policy(None) is None
    assert parse_conflict_policy("") is None
    assert parse_conflict_policy("unknown") is None


def test_parse_conflict_policy_accepts_known_values_case_insensitive():
    assert parse_conflict_policy("writeback_wins") == "WRITEBACK_WINS"
    assert parse_conflict_policy("BASE_WINS") == "BASE_WINS"
    assert parse_conflict_policy("manual_review") == "MANUAL_REVIEW"
    assert parse_conflict_policy("FAIL") == "FAIL"


def test_normalize_conflict_policy_accepts_known_values_case_insensitive():
    assert normalize_conflict_policy("writeback_wins") == "WRITEBACK_WINS"
    assert normalize_conflict_policy("BASE_WINS") == "BASE_WINS"
    assert normalize_conflict_policy("Manual_Review") == "MANUAL_REVIEW"


def test_detect_overlap_fields_compares_current_to_observed():
    observed = {"fields": {"status": "OPEN", "priority": 1}}
    current = {"status": "OPEN", "priority": 2}
    assert detect_overlap_fields(observed_base=observed, current_base=current) == ["priority"]


def test_detect_overlap_links_flags_base_removed_patch_adds():
    observed = {"links": {"assignees": ["user:bob"]}}
    current = {"assignees": []}
    changes = {"set": {}, "unset": [], "link_add": [{"field": "assignees", "value": "user:bob"}], "link_remove": [], "delete": False}
    conflicts = detect_overlap_links(observed_base=observed, current_base=current, changes=changes)
    assert conflicts == [{"field": "assignees", "value": "user:bob", "direction": "BASE_REMOVED_PATCH_ADDS"}]


def test_detect_overlap_links_flags_base_added_patch_removes():
    observed = {"links": {"assignees": []}}
    current = {"assignees": ["user:alice"]}
    changes = {"set": {}, "unset": [], "link_add": [], "link_remove": [{"field": "assignees", "value": "user:alice"}], "delete": False}
    conflicts = detect_overlap_links(observed_base=observed, current_base=current, changes=changes)
    assert conflicts == [{"field": "assignees", "value": "user:alice", "direction": "BASE_ADDED_PATCH_REMOVES"}]


def test_resolve_applied_changes_no_conflict_always_applies():
    changes = {"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    applied, resolution = resolve_applied_changes(conflict_policy="FAIL", changes=changes, conflict_fields=[])
    assert applied == changes
    assert resolution == "APPLIED"


def test_resolve_applied_changes_writeback_wins_applies_on_conflict():
    changes = {"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    applied, resolution = resolve_applied_changes(
        conflict_policy="WRITEBACK_WINS", changes=changes, conflict_fields=["status"]
    )
    assert applied == changes
    assert resolution == "APPLIED"


def test_resolve_applied_changes_base_wins_skips_on_conflict():
    changes = {"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    applied, resolution = resolve_applied_changes(conflict_policy="BASE_WINS", changes=changes, conflict_fields=["status"])
    assert applied == {"set": {}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    assert resolution == "SKIPPED"


def test_resolve_applied_changes_base_wins_skips_on_link_conflict():
    changes = {"set": {}, "unset": [], "link_add": [{"field": "assignees", "value": "user:bob"}], "link_remove": [], "delete": False}
    applied, resolution = resolve_applied_changes(
        conflict_policy="BASE_WINS",
        changes=changes,
        conflict_fields=[],
        conflict_links=[{"field": "assignees", "value": "user:bob", "direction": "BASE_REMOVED_PATCH_ADDS"}],
    )
    assert applied == {"set": {}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    assert resolution == "SKIPPED"


def test_resolve_applied_changes_fail_rejects_on_conflict():
    changes = {"set": {"status": "APPROVED"}, "unset": [], "link_add": [], "link_remove": [], "delete": False}
    applied, resolution = resolve_applied_changes(conflict_policy="FAIL", changes=changes, conflict_fields=["status"])
    assert applied == changes
    assert resolution == "REJECTED"


def test_resolve_applied_changes_fail_rejects_on_link_conflict():
    changes = {"set": {}, "unset": [], "link_add": [{"field": "assignees", "value": "user:bob"}], "link_remove": [], "delete": False}
    applied, resolution = resolve_applied_changes(
        conflict_policy="FAIL",
        changes=changes,
        conflict_fields=[],
        conflict_links=[{"field": "assignees", "value": "user:bob", "direction": "BASE_REMOVED_PATCH_ADDS"}],
    )
    assert applied == changes
    assert resolution == "REJECTED"
