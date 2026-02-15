from shared.utils.principal_policy import build_principal_tags, policy_allows


def test_build_principal_tags_defaults_to_user_prefix():
    tags = build_principal_tags(principal_id="alice", role="DomainModeler")
    assert tags == {"user:alice", "role:DomainModeler"}


def test_build_principal_tags_emits_typed_principal():
    tags = build_principal_tags(principal_type="service", principal_id="svc-1", role="Editor")
    assert tags == {"service:svc-1", "role:Editor"}


def test_policy_allows_matches_typed_principal():
    tags = build_principal_tags(principal_type="service", principal_id="svc-1", role=None)
    policy = {"effect": "ALLOW", "principals": ["service:svc-1"]}
    assert policy_allows(policy=policy, principal_tags=tags) is True


def test_policy_rejects_legacy_roles_alias():
    tags = build_principal_tags(principal_id="alice", role="DomainModeler")
    policy = {"effect": "ALLOW", "roles": ["DomainModeler"]}
    assert policy_allows(policy=policy, principal_tags=tags) is False


def test_policy_rejects_legacy_roles_alias_even_for_deny_effect():
    tags = build_principal_tags(principal_id="alice", role="DomainModeler")
    policy = {"effect": "DENY", "roles": ["DomainModeler"]}
    assert policy_allows(policy=policy, principal_tags=tags) is False


def test_policy_fails_closed_for_unsupported_legacy_policy_shapes():
    tags = build_principal_tags(principal_id="alice", role="DomainModeler")
    policy = {"effect": "ALLOW", "rules": [{"op": "always_true"}]}
    assert policy_allows(policy=policy, principal_tags=tags) is False


def test_policy_fails_closed_for_unsupported_legacy_fields_even_with_principals():
    tags = build_principal_tags(principal_id="alice", role="DomainModeler")
    policy = {
        "effect": "ALLOW",
        "principals": ["role:DomainModeler"],
        "rules": [{"op": "always_true"}],
    }
    assert policy_allows(policy=policy, principal_tags=tags) is False
