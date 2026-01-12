from shared.utils.principal_policy import build_principal_tags, policy_allows


def test_build_principal_tags_user_id_back_compat():
    tags = build_principal_tags(user_id="alice", role="DomainModeler")
    assert tags == {"user:alice", "role:DomainModeler"}


def test_build_principal_tags_emits_typed_principal():
    tags = build_principal_tags(principal_type="service", principal_id="svc-1", role="Editor")
    assert tags == {"service:svc-1", "role:Editor"}


def test_policy_allows_matches_typed_principal():
    tags = build_principal_tags(principal_type="service", principal_id="svc-1", role=None)
    policy = {"effect": "ALLOW", "principals": ["service:svc-1"]}
    assert policy_allows(policy=policy, principal_tags=tags) is True

