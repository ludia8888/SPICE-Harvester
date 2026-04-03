from __future__ import annotations

import pytest

from pipeline_worker import fk_validation


class _DummyOutputFrame:
    columns = ["account_id"]


class _FailingWorker:
    def _parse_fk_expectation(self, expectation, default_branch=None):  # noqa: ANN001, ANN201
        return {
            "columns": ["account_id"],
            "ref_columns": ["account_id"],
            "dataset_id": "ds-ref",
            "dataset_name": "accounts",
            "branch": default_branch or "main",
            "allow_nulls": False,
        }

    async def _load_input_dataframe(self, **kwargs):  # noqa: ANN003, ANN201
        raise RuntimeError("lakefs unavailable")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fk_validation_surfaces_reference_load_failures_as_load_errors() -> None:
    errors = await fk_validation.evaluate_fk_expectations(
        _FailingWorker(),
        expectations=[{"type": "fk_exists"}],
        output_df=_DummyOutputFrame(),
        db_name="test_db",
        branch="main",
        temp_dirs=[],
    )

    assert errors == ["fk_exists reference dataset load failed: lakefs unavailable"]
