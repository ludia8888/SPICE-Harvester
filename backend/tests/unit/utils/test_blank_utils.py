from shared.utils.blank_utils import is_blank_value, strip_to_none


def test_is_blank_value() -> None:
    assert is_blank_value(None)
    assert is_blank_value("")
    assert is_blank_value("   ")
    assert not is_blank_value("x")
    assert not is_blank_value(0)


def test_strip_to_none() -> None:
    assert strip_to_none(None) is None
    assert strip_to_none("  ") is None
    assert strip_to_none(" x ") == "x"
    assert strip_to_none(42) == "42"
