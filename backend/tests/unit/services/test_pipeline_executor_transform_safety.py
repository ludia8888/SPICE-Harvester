import pytest

from shared.services.pipeline_executor import PipelineTable, _join_tables, _union_tables


@pytest.mark.unit
def test_join_requires_keys_by_default():
    left = PipelineTable(columns=["id"], rows=[{"id": 1}])
    right = PipelineTable(columns=["id"], rows=[{"id": 2}])

    with pytest.raises(ValueError, match="join requires leftKey/rightKey"):
        _join_tables(left, right, join_type="inner")


@pytest.mark.unit
def test_join_allow_cross_join_requires_cross_type():
    left = PipelineTable(columns=["id"], rows=[{"id": 1}])
    right = PipelineTable(columns=["id"], rows=[{"id": 2}])

    with pytest.raises(ValueError, match="allowCrossJoin requires joinType='cross'"):
        _join_tables(left, right, join_type="inner", allow_cross_join=True)


@pytest.mark.unit
def test_cross_join_explicit_opt_in_produces_cartesian_product_and_preserves_column_mapping():
    left = PipelineTable(
        columns=["id", "left_val"],
        rows=[
            {"id": "L1", "left_val": "a"},
            {"id": "L2", "left_val": "b"},
        ],
    )
    right = PipelineTable(
        columns=["id", "right_val"],
        rows=[
            {"right_val": "x", "id": "R1"},
            {"right_val": "y", "id": "R2"},
            {"right_val": "z", "id": "R3"},
        ],
    )

    out = _join_tables(left, right, join_type="cross", allow_cross_join=True)

    assert out.columns == ["id", "left_val", "right_id", "right_val"]
    assert len(out.rows) == 6
    assert out.rows[0] == {"id": "L1", "left_val": "a", "right_id": "R1", "right_val": "x"}
    assert out.rows[-1] == {"id": "L2", "left_val": "b", "right_id": "R3", "right_val": "z"}


@pytest.mark.unit
def test_union_strict_raises_on_schema_mismatch():
    left = PipelineTable(columns=["a"], rows=[{"a": 1}])
    right = PipelineTable(columns=["a", "b"], rows=[{"a": 2, "b": 3}])

    with pytest.raises(ValueError, match="union schema mismatch"):
        _union_tables(left, right, union_mode="strict")


@pytest.mark.unit
def test_union_common_only_keeps_only_shared_columns():
    left = PipelineTable(columns=["a", "b"], rows=[{"a": 1, "b": 2}])
    right = PipelineTable(columns=["b", "c"], rows=[{"b": 20, "c": 30}])

    out = _union_tables(left, right, union_mode="common_only")

    assert out.columns == ["b"]
    assert out.rows == [{"b": 2}, {"b": 20}]


@pytest.mark.unit
def test_union_pad_missing_nulls_includes_superset_columns():
    left = PipelineTable(columns=["a"], rows=[{"a": 1}])
    right = PipelineTable(columns=["a", "b"], rows=[{"a": 2, "b": 3}])

    out = _union_tables(left, right, union_mode="pad_missing_nulls")

    assert out.columns == ["a", "b"]
    assert out.rows == [{"a": 1, "b": None}, {"a": 2, "b": 3}]

