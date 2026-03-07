from shared.utils.connector_import_config import resolve_primary_key_column


def test_resolve_primary_key_column_prefers_snake_case() -> None:
    assert resolve_primary_key_column({"primary_key_column": "account_id", "primaryKeyColumn": "ignored"}) == "account_id"


def test_resolve_primary_key_column_supports_camel_case() -> None:
    assert resolve_primary_key_column({"primaryKeyColumn": "account_id"}) == "account_id"


def test_resolve_primary_key_column_returns_none_for_blank_or_non_dict() -> None:
    assert resolve_primary_key_column({"primary_key_column": "  "}) is None
    assert resolve_primary_key_column(None) is None
