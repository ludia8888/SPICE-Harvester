from __future__ import annotations

from shared.services.pipeline.pipeline_definition_utils import is_truthy
from shared.utils.bool_utils import coerce_optional_bool, parse_boolish


def test_parse_boolish_supports_base_and_short_tokens() -> None:
    assert parse_boolish("yes") is True
    assert parse_boolish("off") is False
    assert parse_boolish("y") is None
    assert parse_boolish("y", allow_short_tokens=True) is True
    assert parse_boolish("f", allow_short_tokens=True) is False


def test_parse_boolish_supports_numeric_when_enabled() -> None:
    assert parse_boolish(1) is None
    assert parse_boolish(1, allow_numeric=True) is True
    assert parse_boolish(0, allow_numeric=True) is False


def test_coerce_optional_bool_uses_default_when_value_is_not_parseable() -> None:
    assert coerce_optional_bool("maybe", default=True) is True
    assert coerce_optional_bool(None, default=False) is False
    assert coerce_optional_bool("1", default=False) is True


def test_pipeline_definition_is_truthy_uses_shared_bool_parser() -> None:
    assert is_truthy("yes") is True
    assert is_truthy("0") is False
    assert is_truthy(2) is True
    assert is_truthy("maybe") is False
