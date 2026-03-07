from __future__ import annotations

from shared.services.pipeline.pipeline_join_keys import normalize_join_key_list


def test_normalize_join_key_list_keeps_single_string_intact() -> None:
    assert normalize_join_key_list("left_id,right_id") == ["left_id,right_id"]


def test_normalize_join_key_list_supports_lists_and_tuples() -> None:
    assert normalize_join_key_list(["left_id", " right_id "]) == ["left_id", "right_id"]
    assert normalize_join_key_list(("left_id", " right_id ")) == ["left_id", "right_id"]
