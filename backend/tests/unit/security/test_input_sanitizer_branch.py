from __future__ import annotations

import pytest

from shared.security.input_sanitizer import (
    SecurityViolationError,
    input_sanitizer,
    sanitize_es_query,
    sanitize_input,
    validate_branch_name,
)


def test_validate_branch_name_accepts_foundry_branch_rid() -> None:
    branch_rid = "ri.ontology.main.branch.809f45f2-8f80-4f18-ba5e-34725fb85f65"
    assert validate_branch_name(branch_rid) == branch_rid


def test_validate_branch_name_rejects_reserved_head() -> None:
    with pytest.raises(SecurityViolationError):
        validate_branch_name("HEAD")


def test_validate_branch_name_rejects_legacy_master() -> None:
    with pytest.raises(SecurityViolationError):
        validate_branch_name("master")


def test_validate_branch_name_rejects_path_traversal_segments() -> None:
    with pytest.raises(SecurityViolationError):
        validate_branch_name("../main")

    with pytest.raises(SecurityViolationError):
        validate_branch_name("feature/../../prod")

    with pytest.raises(SecurityViolationError):
        validate_branch_name("feature//branch")


def test_sanitize_input_accepts_main_branch_value() -> None:
    payload = {
        "spec": {
            "backing_source": {
                "dataset_id": "dataset_123",
                "branch": "main",
            }
        }
    }
    sanitized = sanitize_input(payload)
    assert sanitized["spec"]["backing_source"]["branch"] == "main"


def test_sql_detector_still_matches_information_schema_keyword() -> None:
    assert input_sanitizer.detect_sql_injection("select * from INFORMATION_SCHEMA.tables")


def test_sanitize_es_query_escapes_reserved_chars_once() -> None:
    assert sanitize_es_query("name:alice") == r"name\:alice"
    assert sanitize_es_query("hello/world") == r"hello\/world"
    assert sanitize_es_query("C++") == r"C\+\+"
    assert sanitize_es_query(r"foo\bar") == r"foo\\bar"


def test_sanitize_es_query_drops_only_wildcard_queries() -> None:
    assert sanitize_es_query("*") == ""
    assert sanitize_es_query("??") == ""
    assert sanitize_es_query("*prefix") == "prefix"
