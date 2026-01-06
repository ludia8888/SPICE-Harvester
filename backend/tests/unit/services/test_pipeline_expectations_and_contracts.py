import pytest

from shared.services.pipeline_executor import PipelineTable, _build_table_ops
from shared.services.pipeline_validation_utils import validate_expectations, validate_schema_contract


@pytest.mark.unit
def test_expectations_unique_detects_duplicate_primary_key() -> None:
    table = PipelineTable(columns=["id", "value"], rows=[{"id": 1, "value": "a"}, {"id": 1, "value": "b"}])
    errors = validate_expectations(_build_table_ops(table), [{"rule": "unique", "column": "id"}])

    assert errors == ["unique failed: id"]


@pytest.mark.unit
def test_expectations_unique_detects_duplicate_composite_key() -> None:
    table = PipelineTable(
        columns=["id", "sub_id", "value"],
        rows=[{"id": 1, "sub_id": 1, "value": "a"}, {"id": 1, "sub_id": 1, "value": "b"}],
    )
    errors = validate_expectations(_build_table_ops(table), [{"rule": "unique", "column": "id,sub_id"}])

    assert errors == ["unique failed: id,sub_id"]


@pytest.mark.unit
def test_expectations_row_count_bounds() -> None:
    table = PipelineTable(columns=["id"], rows=[{"id": 1}, {"id": 2}])

    errors = validate_expectations(_build_table_ops(table), [{"rule": "row_count_min", "value": 3}])
    assert errors == ["row_count_min failed: 3"]

    errors = validate_expectations(_build_table_ops(table), [{"rule": "row_count_max", "value": 1}])
    assert errors == ["row_count_max failed: 1"]


@pytest.mark.unit
def test_schema_contract_missing_required_column_is_reported() -> None:
    table = PipelineTable(columns=["id"], rows=[{"id": 1}])
    errors = validate_schema_contract(
        _build_table_ops(table),
        [{"column": "missing_col", "type": "xsd:string", "required": True}],
    )

    assert errors == ["schema contract missing column: missing_col"]


@pytest.mark.unit
def test_schema_contract_type_mismatch_is_reported() -> None:
    table = PipelineTable(columns=["amount"], rows=[{"amount": 1}, {"amount": 2}])
    errors = validate_schema_contract(_build_table_ops(table), [{"column": "amount", "type": "xsd:string"}])

    assert errors == ["schema contract type mismatch: amount xsd:integer != xsd:string"]
