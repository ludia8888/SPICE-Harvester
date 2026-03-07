from shared.utils.payload_utils import extract_payload_object, extract_payload_rows


def test_extract_payload_rows_filters_non_dict_entries_after_unwrap() -> None:
    payload = {
        "status": "success",
        "data": {
            "resources": [
                {"id": "a"},
                "ignored",
                {"id": "b"},
            ]
        },
    }

    assert extract_payload_rows(payload, key="resources") == [{"id": "a"}, {"id": "b"}]


def test_extract_payload_object_unwraps_data_dict() -> None:
    payload = {"status": "ok", "data": {"id": "resource-1"}}

    assert extract_payload_object(payload) == {"id": "resource-1"}


def test_extract_payload_object_returns_empty_dict_for_non_mapping() -> None:
    assert extract_payload_object(["not", "a", "dict"]) == {}
