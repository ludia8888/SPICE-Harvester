from __future__ import annotations

from funnel.services.risk_assessor import assess_dataset_risks
from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult


def test_assess_dataset_risks_name_collision() -> None:
    data = [
        ["1", "1"],
        ["2", "2"],
    ]
    columns = ["User ID", "user-id"]
    analysis_results = [
        ColumnAnalysisResult(
            column_name="User ID",
            inferred_type=TypeInferenceResult(type="xsd:string", confidence=0.9, reason="ok"),
            total_count=2,
            non_empty_count=2,
            sample_values=["1"],
            null_count=0,
            unique_count=2,
            null_ratio=0.0,
            unique_ratio=1.0,
        ),
        ColumnAnalysisResult(
            column_name="user-id",
            inferred_type=TypeInferenceResult(type="xsd:string", confidence=0.9, reason="ok"),
            total_count=2,
            non_empty_count=2,
            sample_values=["1"],
            null_count=0,
            unique_count=2,
            null_ratio=0.0,
            unique_ratio=1.0,
        ),
    ]

    risk_summary, _, _ = assess_dataset_risks(data, columns, analysis_results)
    codes = {item.code for item in risk_summary}

    assert "normalized_name_collision" in codes


def test_assess_dataset_risks_low_confidence_and_nulls() -> None:
    data = [[None], [None], ["1"], [None], ["2"], [None], [None], [None], ["3"], [None]]
    columns = ["user_id"]
    analysis_results = [
        ColumnAnalysisResult(
            column_name="user_id",
            inferred_type=TypeInferenceResult(type="xsd:integer", confidence=0.4, reason="low"),
            total_count=10,
            non_empty_count=3,
            sample_values=["1", "2", "3"],
            null_count=7,
            unique_count=3,
            null_ratio=0.7,
            unique_ratio=1.0,
        )
    ]

    risk_summary, _, _ = assess_dataset_risks(data, columns, analysis_results)
    codes = {item.code for item in risk_summary}

    assert "low_confidence_type" in codes
    assert "high_null_ratio" in codes
