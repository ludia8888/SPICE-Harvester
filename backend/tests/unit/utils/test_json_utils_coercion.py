from shared.utils.json_utils import coerce_json_dict, coerce_json_list


def test_coerce_json_list_from_list() -> None:
    assert coerce_json_list([1, 2, 3]) == [1, 2, 3]


def test_coerce_json_list_from_string_list() -> None:
    assert coerce_json_list('["a","b"]') == ["a", "b"]


def test_coerce_json_list_from_wrapped_dict() -> None:
    assert coerce_json_list({"value": [1, 2]}, allow_wrapped_value=True) == [1, 2]
    assert coerce_json_list('{"value":[3,4]}', allow_wrapped_value=True) == [3, 4]


def test_coerce_json_list_wrap_dict_when_requested() -> None:
    assert coerce_json_list({"k": "v"}, wrap_dict=True) == [{"k": "v"}]
    assert coerce_json_list('{"k":"v"}', wrap_dict=True) == [{"k": "v"}]


def test_coerce_json_dict_from_dict_and_string() -> None:
    assert coerce_json_dict({"a": 1}) == {"a": 1}
    assert coerce_json_dict('{"a":1}') == {"a": 1}


def test_coerce_json_dict_wraps_non_dict_json_by_default() -> None:
    assert coerce_json_dict('"hello"') == {"value": "hello"}
    assert coerce_json_dict("42") == {"value": 42}


def test_coerce_json_dict_can_disable_parsed_fallback() -> None:
    assert coerce_json_dict("42", parsed_fallback_key=None) == {}

