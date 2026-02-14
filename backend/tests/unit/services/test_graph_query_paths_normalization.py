from __future__ import annotations

from bff.services.graph_query_service import _normalize_es_doc_id, _normalize_paths_for_response


def test_normalize_paths_converts_list_paths_to_object_shape() -> None:
    raw_paths = [
        ["Person/p1", "BankAccount/a1", "Transaction/t1"],
        ["Person/p1", "PhoneCall/c1"],
    ]

    normalized = _normalize_paths_for_response(raw_paths)

    assert normalized == [
        {
            "path_id": 0,
            "nodes": ["Person/p1", "BankAccount/a1", "Transaction/t1"],
            "hops": 2,
        },
        {
            "path_id": 1,
            "nodes": ["Person/p1", "PhoneCall/c1"],
            "hops": 1,
        },
    ]


def test_normalize_paths_preserves_existing_object_paths() -> None:
    raw_paths = [{"path_id": "a", "nodes": ["A/1", "B/1"], "hops": 1}]
    assert _normalize_paths_for_response(raw_paths) == raw_paths


def test_normalize_es_doc_id_strips_class_prefix() -> None:
    assert _normalize_es_doc_id("Person/010-1234-5678") == "010-1234-5678"
    assert _normalize_es_doc_id("010-1234-5678") == "010-1234-5678"
    assert _normalize_es_doc_id("") == ""
