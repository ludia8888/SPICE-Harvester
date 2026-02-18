"""Tests for sync-mode helpers in bff.services.data_connector_pipelining_service.

Covers:
  - _row_hash         deterministic SHA-256 of row values joined by "|"
  - _parse_csv_bytes  CSV bytes -> (columns, rows)
  - _apply_append_mode   APPEND: add only new rows by hash
  - _apply_update_mode   UPDATE: merge/upsert by primary-key column
"""

import hashlib

import pytest

from bff.services.data_connector_pipelining_service import (
    _row_hash,
    _parse_csv_bytes,
    _apply_append_mode,
    _apply_update_mode,
)


# ---------------------------------------------------------------------------
# _row_hash
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_row_hash_deterministic():
    """Same row produces the same hash; different rows produce different hashes."""
    row_a = ["a", "1", "hello"]
    row_b = ["b", "2", "world"]

    hash_a1 = _row_hash(row_a)
    hash_a2 = _row_hash(row_a)

    # Determinism: identical input -> identical output
    assert hash_a1 == hash_a2

    # Verify it is indeed SHA-256 hex of "|"-joined values
    expected = hashlib.sha256("a|1|hello".encode("utf-8")).hexdigest()
    assert hash_a1 == expected

    # Different rows must produce different hashes
    hash_b = _row_hash(row_b)
    assert hash_a1 != hash_b


# ---------------------------------------------------------------------------
# _parse_csv_bytes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_csv_bytes():
    """Standard CSV bytes are parsed into (columns, rows)."""
    raw = b"col1,col2\nval1,val2\nval3,val4\n"
    columns, rows = _parse_csv_bytes(raw)

    assert columns == ["col1", "col2"]
    assert rows == [["val1", "val2"], ["val3", "val4"]]


@pytest.mark.unit
def test_parse_csv_bytes_empty():
    """Empty bytes produce empty columns and empty rows."""
    columns, rows = _parse_csv_bytes(b"")

    assert columns == []
    assert rows == []


# ---------------------------------------------------------------------------
# _apply_append_mode
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_append_mode_adds_only_new_rows():
    """Rows that already exist (by hash) are not duplicated."""
    columns = ["letter", "number"]
    existing_rows = [["a", "1"], ["b", "2"]]
    new_rows = [["a", "1"], ["c", "3"]]  # "a","1" is a duplicate

    result_cols, result_rows = _apply_append_mode(
        columns, existing_rows, columns, new_rows
    )

    assert result_cols == columns
    assert len(result_rows) == 3
    assert ["a", "1"] in result_rows
    assert ["b", "2"] in result_rows
    assert ["c", "3"] in result_rows

    # Order: existing rows first, then appended new rows
    assert result_rows[0] == ["a", "1"]
    assert result_rows[1] == ["b", "2"]
    assert result_rows[2] == ["c", "3"]


@pytest.mark.unit
def test_append_mode_empty_existing():
    """When there are no existing rows, all new rows are added."""
    new_columns = ["x", "y"]
    new_rows = [["1", "2"], ["3", "4"]]

    result_cols, result_rows = _apply_append_mode([], [], new_columns, new_rows)

    # With no existing columns, new_columns become canonical
    assert result_cols == new_columns
    assert result_rows == [["1", "2"], ["3", "4"]]


@pytest.mark.unit
def test_append_mode_no_new_rows():
    """When all new rows already exist, nothing changes."""
    columns = ["a", "b"]
    existing_rows = [["1", "2"], ["3", "4"]]
    new_rows = [["1", "2"], ["3", "4"]]  # identical set

    result_cols, result_rows = _apply_append_mode(
        columns, existing_rows, columns, new_rows
    )

    assert result_cols == columns
    assert result_rows == existing_rows


@pytest.mark.unit
def test_append_mode_preserves_column_names():
    """Existing column names take precedence over new column names."""
    existing_columns = ["Name", "Value"]
    new_columns = ["name", "value"]  # different casing
    existing_rows = [["a", "1"]]
    new_rows = [["b", "2"]]

    result_cols, _ = _apply_append_mode(
        existing_columns, existing_rows, new_columns, new_rows
    )

    # existing_columns are used when they are non-empty
    assert result_cols == existing_columns


@pytest.mark.unit
def test_append_mode_realigns_new_rows_to_existing_column_order():
    """When column order changes, APPEND aligns new rows before dedup/hash."""
    existing_columns = ["id", "name"]
    existing_rows = [["1", "Alice"]]
    new_columns = ["name", "id"]  # reversed
    new_rows = [["Alice", "1"], ["Bob", "2"]]

    result_cols, result_rows = _apply_append_mode(
        existing_columns, existing_rows, new_columns, new_rows
    )

    assert result_cols == existing_columns
    # Existing row should not be duplicated after realignment.
    assert result_rows == [["1", "Alice"], ["2", "Bob"]]


# ---------------------------------------------------------------------------
# _apply_update_mode
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_update_mode_updates_existing_rows_by_pk():
    """Rows with a matching PK are updated; new PKs are appended."""
    columns = ["id", "num", "status"]
    existing_rows = [["a", "1", "old"], ["b", "2", "old"]]
    new_rows = [["a", "1", "NEW"], ["c", "3", "new"]]

    result_cols, result_rows = _apply_update_mode(
        columns, existing_rows, columns, new_rows, primary_key_column="id"
    )

    assert result_cols == columns
    assert len(result_rows) == 3

    # Row "a" was updated in-place
    assert result_rows[0] == ["a", "1", "NEW"]
    # Row "b" was not touched
    assert result_rows[1] == ["b", "2", "old"]
    # Row "c" was inserted at the end
    assert result_rows[2] == ["c", "3", "new"]


@pytest.mark.unit
def test_update_mode_default_pk_first_column():
    """When no primary_key_column is specified, the first column is used."""
    columns = ["id", "num", "status"]
    existing_rows = [["a", "1", "old"], ["b", "2", "old"]]
    new_rows = [["a", "1", "NEW"], ["c", "3", "new"]]

    # No primary_key_column argument -> defaults to first column ("id")
    result_cols, result_rows = _apply_update_mode(
        columns, existing_rows, columns, new_rows
    )

    assert result_cols == columns
    assert len(result_rows) == 3

    # Identical behaviour to the explicit PK test
    assert result_rows[0] == ["a", "1", "NEW"]
    assert result_rows[1] == ["b", "2", "old"]
    assert result_rows[2] == ["c", "3", "new"]


@pytest.mark.unit
def test_update_mode_empty_existing():
    """When there is no existing data, the result is just the new data."""
    new_columns = ["id", "value"]
    new_rows = [["x", "10"], ["y", "20"]]

    result_cols, result_rows = _apply_update_mode(
        [], [], new_columns, new_rows, primary_key_column="id"
    )

    # new_columns become canonical when existing is empty
    assert result_cols == new_columns
    assert result_rows == [["x", "10"], ["y", "20"]]


@pytest.mark.unit
def test_update_mode_realigns_new_rows_when_column_order_changes():
    """UPDATE should map incoming columns to existing schema before PK upsert."""
    existing_columns = ["id", "status", "amount"]
    existing_rows = [["1", "OLD", "100"]]
    new_columns = ["amount", "id", "status"]  # reordered
    new_rows = [["110", "1", "NEW"], ["200", "2", "ACTIVE"]]

    result_cols, result_rows = _apply_update_mode(
        existing_columns,
        existing_rows,
        new_columns,
        new_rows,
        primary_key_column="id",
    )

    assert result_cols == existing_columns
    assert result_rows[0] == ["1", "NEW", "110"]
    assert result_rows[1] == ["2", "ACTIVE", "200"]
