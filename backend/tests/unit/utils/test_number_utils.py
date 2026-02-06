from shared.utils.number_utils import to_int_or_none


def test_to_int_or_none_with_none() -> None:
    assert to_int_or_none(None) is None


def test_to_int_or_none_with_valid_values() -> None:
    assert to_int_or_none("42") == 42
    assert to_int_or_none(7) == 7


def test_to_int_or_none_with_invalid_values() -> None:
    assert to_int_or_none("not-a-number") is None
    assert to_int_or_none(object()) is None
