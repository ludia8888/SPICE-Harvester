from shared.utils.action_audit_policy import audit_action_log_input, audit_action_log_result


def test_audit_action_log_input_redacts_keys_recursively():
    payload = {"password": "secret", "nested": {"token": "abc", "keep": 1}}
    policy = {"redact_keys": ["password", "token"], "redact_value": "X", "max_input_bytes": 10_000}
    out = audit_action_log_input(payload, audit_policy=policy)
    assert out["password"] == "X"
    assert out["nested"]["token"] == "X"
    assert out["nested"]["keep"] == 1


def test_audit_action_log_input_truncates_when_exceeds_max_bytes():
    payload = {"comment": "a" * 1000}
    policy = {"max_input_bytes": 50}
    out = audit_action_log_input(payload, audit_policy=policy)
    assert out.get("__truncated__") is True
    assert "sha256" in out
    assert out.get("bytes", 0) > 0


def test_audit_action_log_result_summarizes_large_change_arrays():
    payload = {"attempted_changes": [{"x": 1}] * 5}
    policy = {"max_changes": 2, "max_result_bytes": 10_000}
    out = audit_action_log_result(payload, audit_policy=policy)
    summary = out["attempted_changes"]
    assert summary["__truncated__"] is True
    assert summary["count"] == 5
    assert summary["sample_count"] == 2
    assert len(summary["sample"]) == 2

