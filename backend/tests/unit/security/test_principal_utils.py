from shared.security.principal_utils import actor_label, resolve_principal_from_headers


def test_resolve_principal_defaults_when_missing_headers() -> None:
    assert resolve_principal_from_headers(None) == ("user", "system")


def test_resolve_principal_uses_header_values() -> None:
    headers = {"X-Principal-Id": "alice", "X-Principal-Type": "service"}
    assert resolve_principal_from_headers(headers) == ("service", "alice")


def test_resolve_principal_enforces_allowed_types() -> None:
    headers = {"X-Principal-Id": "alice", "X-Principal-Type": "admin"}
    assert resolve_principal_from_headers(headers, allowed_principal_types={"user", "service"}) == ("user", "alice")


def test_resolve_principal_supports_custom_defaults() -> None:
    assert (
        resolve_principal_from_headers(
            {},
            default_principal_type="service",
            default_principal_id="bff",
            allowed_principal_types={"user", "service"},
        )
        == ("service", "bff")
    )


def test_resolve_principal_uses_custom_header_keys() -> None:
    headers = {"X-Actor": "worker-1", "X-Actor-Type": "service"}
    assert (
        resolve_principal_from_headers(
            headers,
            principal_id_headers=("X-Actor",),
            principal_type_headers=("X-Actor-Type",),
        )
        == ("service", "worker-1")
    )


def test_actor_label_defaults() -> None:
    assert actor_label("", "") == "user:unknown"

