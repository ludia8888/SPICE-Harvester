from shared.utils.token_count import approx_token_count, approx_token_count_json


def test_approx_token_count_handles_none_and_empty_string() -> None:
    assert approx_token_count(None) == 0
    assert approx_token_count("") == 0
    assert approx_token_count("   ") == 0


def test_approx_token_count_counts_payload() -> None:
    assert approx_token_count("abcd") == 1
    assert approx_token_count("abcdefgh") == 2


def test_approx_token_count_collection_policy_toggle() -> None:
    assert approx_token_count({}) >= 1
    assert approx_token_count([], empty_collections_as_zero=True) == 0
    assert approx_token_count({}, empty_collections_as_zero=True) == 0


def test_approx_token_count_json_matches_json_serialized_size() -> None:
    assert approx_token_count_json("abcd") == 2
    assert approx_token_count_json("", empty_collections_as_zero=True) == 0
