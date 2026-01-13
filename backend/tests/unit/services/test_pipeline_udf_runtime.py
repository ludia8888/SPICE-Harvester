import pytest

from shared.services.pipeline_udf_runtime import PipelineUdfError, compile_row_udf


@pytest.mark.unit
def test_compile_row_udf_accepts_simple_transform():
    code = """
def transform(row):
    value = int(row.get("id") or 0)
    return {**row, "id_plus": value + 1}
""".strip()

    fn = compile_row_udf(code)
    assert fn({"id": 1})["id_plus"] == 2


@pytest.mark.unit
def test_compile_row_udf_rejects_imports():
    code = """
import os

def transform(row):
    return row
""".strip()

    with pytest.raises(PipelineUdfError, match="cannot import modules"):
        compile_row_udf(code)


@pytest.mark.unit
def test_compile_row_udf_rejects_private_attribute_access():
    code = """
def transform(row):
    return row.__class__
""".strip()

    with pytest.raises(PipelineUdfError, match="cannot access private attributes"):
        compile_row_udf(code)


@pytest.mark.unit
def test_compile_row_udf_rejects_top_level_statements():
    code = """
VALUE = 1

def transform(row):
    return row
""".strip()

    with pytest.raises(PipelineUdfError, match="no top-level statements"):
        compile_row_udf(code)


@pytest.mark.unit
def test_compile_row_udf_rejects_loops():
    code = """
def transform(row):
    while True:
        pass
    return row
""".strip()

    with pytest.raises(PipelineUdfError, match="cannot use while loops"):
        compile_row_udf(code)

