from datetime import datetime, timezone

from shared.services.pipeline.pipeline_value_predicates import (
    is_bool_like,
    is_datetime_like,
    is_decimal_like,
    is_int_like,
)


def test_is_bool_like() -> None:
    assert is_bool_like(True)
    assert is_bool_like("false")
    assert not is_bool_like("nope")


def test_is_int_like() -> None:
    assert is_int_like(42)
    assert is_int_like("42")
    assert not is_int_like(True)
    assert not is_int_like("42.3")


def test_is_decimal_like_include_int_policy() -> None:
    assert is_decimal_like(1.5)
    assert is_decimal_like("1.5")
    assert not is_decimal_like(3, include_int=False)
    assert is_decimal_like(3, include_int=True)


def test_is_datetime_like_iso_only_and_general_modes() -> None:
    assert is_datetime_like(datetime.now(timezone.utc))
    assert is_datetime_like("2026-01-01T10:00:00Z", iso_only=True)
    assert is_datetime_like("2026-01-01T10:00:00Z", iso_only=False, allow_ambiguous=False)
    assert not is_datetime_like("not-a-date", iso_only=True)
