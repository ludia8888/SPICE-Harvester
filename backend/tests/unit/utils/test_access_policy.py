from shared.utils.access_policy import apply_access_policy


def test_access_policy_allows_matching_rows():
    rows = [{"status": "active", "name": "alpha"}, {"status": "inactive", "name": "beta"}]
    policy = {"row_filters": [{"field": "status", "op": "eq", "value": "active"}]}

    filtered, stats = apply_access_policy(rows, policy=policy)

    assert len(filtered) == 1
    assert filtered[0]["name"] == "alpha"
    assert stats["filtered"] == 1


def test_access_policy_denies_matching_rows():
    rows = [{"status": "active"}, {"status": "inactive"}]
    policy = {
        "row_filters": [{"field": "status", "op": "eq", "value": "inactive"}],
        "filter_mode": "deny",
    }

    filtered, stats = apply_access_policy(rows, policy=policy)

    assert len(filtered) == 1
    assert filtered[0]["status"] == "active"
    assert stats["filtered"] == 1


def test_access_policy_masks_columns():
    rows = [{"email": "a@example.com", "name": "alpha"}]
    policy = {"mask_columns": ["email"], "mask_value": None}

    filtered, stats = apply_access_policy(rows, policy=policy)

    assert filtered[0]["email"] is None
    assert filtered[0]["name"] == "alpha"
    assert stats["masked_fields"] == 1
