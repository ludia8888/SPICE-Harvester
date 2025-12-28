import pytest

from shared.services.pipeline_scheduler import (
    _dependencies_satisfied,
    _is_valid_cron_expression,
    _normalize_dependencies,
)


@pytest.mark.unit
def test_normalize_dependencies_accepts_pipeline_id_variants():
    deps, invalid = _normalize_dependencies(
        [
            {"pipelineId": "abc"},
            {"pipeline_id": "def", "status": "success"},
            {"pipeline_id": "ghi", "status": "DePloYeD"},
        ]
    )

    assert invalid == 0
    assert deps == [
        {"pipeline_id": "abc", "status": "DEPLOYED"},
        {"pipeline_id": "def", "status": "SUCCESS"},
        {"pipeline_id": "ghi", "status": "DEPLOYED"},
    ]


@pytest.mark.unit
def test_normalize_dependencies_reports_invalid_entries():
    deps, invalid = _normalize_dependencies(
        [
            {"pipelineId": "ok"},
            {"status": "DEPLOYED"},
            "not-a-dict",
            {"pipeline_id": ""},
        ]
    )

    assert deps == [{"pipeline_id": "ok", "status": "DEPLOYED"}]
    assert invalid == 3


@pytest.mark.unit
def test_cron_expression_validation_matches_supported_subset():
    assert _is_valid_cron_expression("* * * * *")
    assert _is_valid_cron_expression("*/5 * * * *")
    assert _is_valid_cron_expression("0,15,30,45 * * * *")
    assert _is_valid_cron_expression("0 0 1-10/2 * *")
    assert _is_valid_cron_expression("0 9 * * 1-5")

    assert not _is_valid_cron_expression("")
    assert not _is_valid_cron_expression("0 0 * *")  # too few fields
    assert not _is_valid_cron_expression("MON * * * *")  # unsupported names
    assert not _is_valid_cron_expression("*/0 * * * *")  # invalid step
    assert not _is_valid_cron_expression("5-1 * * * *")  # inverted range
    assert not _is_valid_cron_expression("1/2 * * * *")  # unsupported pattern


class _MissingPipelineRegistry:
    async def get_pipeline(self, *, pipeline_id: str):
        return None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dependencies_satisfied_raises_when_dependency_pipeline_missing():
    with pytest.raises(ValueError):
        await _dependencies_satisfied(
            _MissingPipelineRegistry(),
            [{"pipeline_id": "does-not-exist", "status": "DEPLOYED"}],
        )

