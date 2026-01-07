import asyncio

import pytest

from funnel.services.type_inference_adapter import FunnelTypeInferenceAdapter
from shared.models.common import DataType


@pytest.mark.asyncio
async def test_infer_column_type_respects_metadata_override() -> None:
    adapter = FunnelTypeInferenceAdapter()
    data = ["2024-01-01", "2024-02-01", "2024-03-01"]

    result = await adapter.infer_column_type(
        column_data=data,
        column_name="event_date",
        include_complex_types=False,
        metadata={"include_complex_types": True},
    )

    assert result.column_name == "event_date"
    assert result.inferred_type.type in {DataType.DATE.value, DataType.DATETIME.value}
    assert result.total_count == len(data)


@pytest.mark.asyncio
async def test_analyze_dataset_uses_metadata_sample_size() -> None:
    adapter = FunnelTypeInferenceAdapter()
    data = [[1], [2], [3], [4]]

    results = await adapter.analyze_dataset(
        data=data,
        columns=["value"],
        sample_size=10,
        metadata={"sample_size": 2},
    )

    assert len(results) == 1
    assert results[0].total_count == 2
    assert results[0].inferred_type.type in {DataType.INTEGER.value, DataType.DECIMAL.value}


@pytest.mark.asyncio
async def test_infer_single_value_type_returns_type() -> None:
    adapter = FunnelTypeInferenceAdapter()

    inferred = await adapter.infer_single_value_type(
        value="user@example.com",
        context={"column_name": "email", "include_complex_types": False},
    )

    assert inferred.type in {DataType.EMAIL.value, DataType.STRING.value}
    assert 0.0 <= inferred.confidence <= 1.0
