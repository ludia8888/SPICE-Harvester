import pytest

from shared.services.pipeline.pipeline_udf_runtime import (
    PipelineUdfError,
    compile_row_udf,
    resolve_udf_reference,
)


class _FakeUdfVersion:
    def __init__(self, version: int, code: str) -> None:
        self.version = version
        self.code = code


class _FakeUdfRegistry:
    def __init__(self) -> None:
        self.latest_by_udf_id: dict[str, _FakeUdfVersion] = {}
        self.by_udf_and_version: dict[tuple[str, int], _FakeUdfVersion] = {}

    async def get_udf_latest_version(self, *, udf_id: str):
        return self.latest_by_udf_id.get(udf_id)

    async def get_udf_version(self, *, udf_id: str, version: int):
        return self.by_udf_and_version.get((udf_id, version))


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
def test_compile_row_udf_accepts_escaped_newline_source():
    code = "def transform(row):\\n    row['id_plus'] = int(row.get('id') or 0) + 1\\n    return row"

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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_resolve_udf_reference_requires_udf_id_when_policy_enabled() -> None:
    with pytest.raises(PipelineUdfError, match="udf requires udfId"):
        await resolve_udf_reference(
            metadata={"operation": "udf"},
            pipeline_registry=None,
            require_reference=True,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_resolve_udf_reference_uses_registry_and_cache_key() -> None:
    registry = _FakeUdfRegistry()
    registry.latest_by_udf_id["udf-1"] = _FakeUdfVersion(2, "def transform(row): return row")
    registry.by_udf_and_version[("udf-1", 2)] = _FakeUdfVersion(2, "def transform(row): return row")
    cache: dict[str, str] = {}

    resolved = await resolve_udf_reference(
        metadata={"operation": "udf", "udfId": "udf-1"},
        pipeline_registry=registry,
        require_reference=True,
        code_cache=cache,
    )

    assert resolved.udf_id == "udf-1"
    assert resolved.version == 2
    assert resolved.cache_key.startswith("udf-1:2:")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_resolve_udf_reference_rejects_inline_code_even_when_reference_flag_disabled() -> None:
    with pytest.raises(PipelineUdfError, match="udfCode is not allowed"):
        await resolve_udf_reference(
            metadata={"operation": "udf", "udfCode": "def transform(row): return row"},
            pipeline_registry=None,
            require_reference=False,
        )
