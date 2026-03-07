from __future__ import annotations

from shared.utils.string_list_utils import normalize_string_list


def test_normalize_string_list_splits_and_strips_strings() -> None:
    assert normalize_string_list(" a, b ,, c ") == ["a", "b", "c"]


def test_normalize_string_list_preserves_list_items_without_recursive_split() -> None:
    assert normalize_string_list(["a", " b ", "c,d", 1]) == ["a", "b", "c,d", "1"]


def test_normalize_string_list_can_dedupe_while_preserving_order() -> None:
    assert normalize_string_list(["a", "a", "b", "a"], dedupe=True) == ["a", "b"]


def test_normalize_string_list_supports_tuples() -> None:
    assert normalize_string_list(("a", " b ", 1)) == ["a", "b", "1"]


def test_normalize_string_list_can_keep_single_string_without_comma_split() -> None:
    assert normalize_string_list("left_id,right_id", split_commas=False) == ["left_id,right_id"]
