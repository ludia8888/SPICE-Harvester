from __future__ import annotations

import pytest

from data_connector.adapters.sql_query_guard import normalize_sql_query


@pytest.mark.unit
def test_normalize_sql_query_allows_single_statement() -> None:
    assert normalize_sql_query("SELECT 1", field_name="query") == "SELECT 1"
    assert normalize_sql_query(" SELECT 1; ", field_name="query") == "SELECT 1"


@pytest.mark.unit
def test_normalize_sql_query_rejects_empty() -> None:
    with pytest.raises(ValueError, match="query is required"):
        normalize_sql_query("   ", field_name="query")


@pytest.mark.unit
def test_normalize_sql_query_rejects_multi_statement() -> None:
    with pytest.raises(ValueError, match="must contain exactly one SQL statement"):
        normalize_sql_query("SELECT 1; SELECT 2", field_name="query")
