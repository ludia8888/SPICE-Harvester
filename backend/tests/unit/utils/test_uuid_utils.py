from shared.utils.uuid_utils import safe_uuid


def test_safe_uuid_accepts_valid_uuid() -> None:
    assert safe_uuid("550e8400-e29b-41d4-a716-446655440000") == "550e8400-e29b-41d4-a716-446655440000"


def test_safe_uuid_rejects_invalid_values() -> None:
    assert safe_uuid("") is None
    assert safe_uuid(None) is None
    assert safe_uuid("not-a-uuid") is None

