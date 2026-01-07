from __future__ import annotations

import uuid

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from bff.routers import pipeline as pipeline_router


def test_pipeline_protected_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_PROTECTED_BRANCHES", "main,prod")
    monkeypatch.setenv("PIPELINE_REQUIRE_PROPOSALS", "true")
    branches = pipeline_router._resolve_pipeline_protected_branches()
    assert "main" in branches
    assert pipeline_router._pipeline_requires_proposal("prod") is True


def test_normalize_mapping_spec_ids() -> None:
    assert pipeline_router._normalize_mapping_spec_ids("abc") == ["abc"]
    ids = pipeline_router._normalize_mapping_spec_ids(["a", "a", 1])
    assert ids == ["a", "1"]


def test_schema_change_detection() -> None:
    previous = {"columns": [{"name": "a", "type": "string"}, {"name": "b", "type": "int"}]}
    next_cols = [{"name": "a", "type": "string"}]
    breaking = pipeline_router._detect_breaking_schema_changes(previous_schema=previous, next_columns=next_cols)
    assert {item["kind"] for item in breaking} == {"MISSING_COLUMN"}


def test_dependency_payload_normalization() -> None:
    pipeline_id = str(uuid.uuid4())
    normalized = pipeline_router._normalize_dependencies_payload(
        [{"pipeline_id": pipeline_id, "status": "DEPLOYED"}]
    )
    assert normalized == [{"pipeline_id": pipeline_id, "status": "DEPLOYED"}]

    with pytest.raises(HTTPException):
        pipeline_router._normalize_dependencies_payload([{"pipeline_id": "bad", "status": "DEPLOYED"}])


def test_format_dependencies_for_api() -> None:
    pipeline_id = str(uuid.uuid4())
    formatted = pipeline_router._format_dependencies_for_api([{"pipeline_id": pipeline_id, "status": "success"}])
    assert formatted == [{"pipelineId": pipeline_id, "status": "SUCCESS"}]


def test_resolve_principal_and_actor_label() -> None:
    scope = {
        "type": "http",
        "headers": [(b"x-principal-id", b"user-1"), (b"x-principal-type", b"user")],
    }
    request = Request(scope)
    principal_type, principal_id = pipeline_router._resolve_principal(request)
    assert principal_type == "user"
    assert pipeline_router._actor_label(principal_type, principal_id) == "user:user-1"


def test_location_and_dataset_name_helpers() -> None:
    assert pipeline_router._default_dataset_name("file.csv") == "file"
    with pytest.raises(HTTPException):
        pipeline_router._normalize_location("")


def test_definition_diff_and_bbox() -> None:
    previous = {"nodes": [{"id": "n1"}], "edges": [{"source": "n1", "target": "n2"}]}
    current = {"nodes": [{"id": "n2"}], "edges": []}
    diff = pipeline_router._definition_diff(previous, current)
    assert "n2" in diff["nodes_added"]
    assert diff["edge_count"] == 0

    bbox = pipeline_router._normalize_table_bbox(
        table_top=1, table_left=2, table_bottom=3, table_right=4
    )
    assert bbox == {"top": 1, "left": 2, "bottom": 3, "right": 4}


def test_csv_helpers() -> None:
    sample = "a;b;c\n1;2;3\n"
    delimiter = pipeline_router._detect_csv_delimiter(sample)
    assert delimiter == ";"

    columns, rows, total = pipeline_router._parse_csv_content(sample, delimiter=delimiter, has_header=True)
    assert columns == ["a", "b", "c"]
    assert total == 1


def test_idempotency_key_required() -> None:
    scope = {
        "type": "http",
        "headers": [(b"idempotency-key", b"abc")],
    }
    request = Request(scope)
    assert pipeline_router._require_idempotency_key(request) == "abc"

    with pytest.raises(HTTPException):
        pipeline_router._require_idempotency_key(None)
