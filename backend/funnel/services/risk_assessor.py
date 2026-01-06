"""
Funnel risk assessor (suggestion-only, sample-based).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shared.models.common import DataType
from shared.models.type_inference import ColumnAnalysisResult, ColumnProfile, FunnelRiskItem

from funnel.services.schema_utils import normalize_property_name


LOW_CONFIDENCE_THRESHOLD = 0.7
WARN_NULL_RATIO = 0.5
INFO_NULL_RATIO = 0.3
KEY_UNIQUE_RATIO_MIN = 0.9
KEY_NULL_RATIO_MAX = 0.05
MIN_SAMPLE_ROWS = 20
AMBIGUOUS_GAP_MAX = 0.1


def assess_dataset_risks(
    data: List[List[Any]],
    columns: List[str],
    analysis_results: List[ColumnAnalysisResult],
) -> Tuple[List[FunnelRiskItem], Dict[str, List[FunnelRiskItem]], Dict[str, ColumnProfile]]:
    sample_rows = len(data)
    risk_summary: List[FunnelRiskItem] = []
    column_risks: Dict[str, List[FunnelRiskItem]] = {}
    column_profiles: Dict[str, ColumnProfile] = {}

    if sample_rows == 0:
        risk_summary.append(
            FunnelRiskItem(
                code="no_sample_rows",
                severity="info",
                message="No sample rows available for inference.",
                evidence={"sample_rows": 0},
                suggested_actions=["Provide sample rows to improve inference stability."],
            )
        )
        return risk_summary, column_risks, column_profiles

    if sample_rows < MIN_SAMPLE_ROWS:
        risk_summary.append(
            FunnelRiskItem(
                code="sample_size_low",
                severity="info",
                message="Sample size is small; inference may be unstable.",
                evidence={"sample_rows": sample_rows, "min_recommended": MIN_SAMPLE_ROWS},
                suggested_actions=[
                    "Provide more sample rows to improve inference confidence.",
                    "Confirm inferred types manually before approval.",
                ],
            )
        )

    column_data_map = _build_column_data_map(data, columns)
    _append_name_collision_risks(columns, risk_summary)

    for result in analysis_results:
        column_name = result.column_name
        values = column_data_map.get(column_name, [])
        profile = _build_column_profile(values, result)
        if profile:
            column_profiles[column_name] = profile

        risks = _assess_column_risks(column_name, result)
        if risks:
            column_risks[column_name] = risks
            risk_summary.extend(risks)

    return risk_summary, column_risks, column_profiles


def _build_column_data_map(
    data: List[List[Any]],
    columns: List[str],
) -> Dict[str, List[Any]]:
    data_map: Dict[str, List[Any]] = {col: [] for col in columns}
    for row in data:
        for idx, col_name in enumerate(columns):
            data_map[col_name].append(row[idx] if idx < len(row) else None)
    return data_map


def _append_name_collision_risks(columns: List[str], risks: List[FunnelRiskItem]) -> None:
    normalized: Dict[str, List[str]] = {}
    for col in columns:
        key = normalize_property_name(col)
        normalized.setdefault(key, []).append(col)

    for norm_name, original_names in normalized.items():
        if len(original_names) < 2:
            continue
        risks.append(
            FunnelRiskItem(
                code="normalized_name_collision",
                severity="warning",
                message="Multiple columns normalize to the same schema name.",
                evidence={"normalized_name": norm_name, "columns": original_names},
                suggested_actions=[
                    "Rename columns to avoid collisions before schema approval.",
                    "Provide explicit column mappings when creating the schema.",
                ],
            )
        )


def _assess_column_risks(
    column_name: str, result: ColumnAnalysisResult
) -> List[FunnelRiskItem]:
    risks: List[FunnelRiskItem] = []
    inferred = result.inferred_type
    metadata = inferred.metadata or {}

    if result.total_count == 0 or result.non_empty_count == 0:
        risks.append(
            FunnelRiskItem(
                code="all_values_empty",
                severity="info",
                message="Column has no non-empty sample values.",
                column=column_name,
                evidence={"total_count": result.total_count, "non_empty_count": 0},
                suggested_actions=["Provide sample values or set the type manually."],
            )
        )
        return risks

    if inferred.confidence < LOW_CONFIDENCE_THRESHOLD:
        risks.append(
            FunnelRiskItem(
                code="low_confidence_type",
                severity="warning",
                message="Inferred type confidence is low.",
                column=column_name,
                evidence={"confidence": inferred.confidence, "type": inferred.type},
                suggested_actions=[
                    "Review the inferred type before approval.",
                    "Consider casting or overriding the type manually.",
                ],
            )
        )

    if result.null_ratio >= WARN_NULL_RATIO:
        risks.append(
            FunnelRiskItem(
                code="high_null_ratio",
                severity="warning",
                message="Null/empty ratio is high.",
                column=column_name,
                evidence={"null_ratio": result.null_ratio, "null_count": result.null_count},
                suggested_actions=[
                    "Verify whether missing values are expected.",
                    "Consider filtering or imputing before build.",
                ],
            )
        )
    elif result.null_ratio >= INFO_NULL_RATIO:
        risks.append(
            FunnelRiskItem(
                code="moderate_null_ratio",
                severity="info",
                message="Null/empty ratio is noticeable.",
                column=column_name,
                evidence={"null_ratio": result.null_ratio, "null_count": result.null_count},
                suggested_actions=["Review missing value handling in downstream steps."],
            )
        )

    if _is_key_like(column_name) and (
        result.unique_ratio < KEY_UNIQUE_RATIO_MIN or result.null_ratio > KEY_NULL_RATIO_MAX
    ):
        risks.append(
            FunnelRiskItem(
                code="key_candidate_not_unique",
                severity="warning",
                message="Key-like column is not unique or has missing values.",
                column=column_name,
                evidence={
                    "unique_ratio": result.unique_ratio,
                    "null_ratio": result.null_ratio,
                },
                suggested_actions=[
                    "Confirm primary key semantics before using this column as a join key.",
                    "Consider deduplication or choosing a different key.",
                ],
            )
        )

    candidate_gap = metadata.get("confidence_gap")
    if isinstance(candidate_gap, (int, float)) and candidate_gap <= AMBIGUOUS_GAP_MAX:
        risks.append(
            FunnelRiskItem(
                code="ambiguous_type_candidates",
                severity="info",
                message="Top inferred types are close; ambiguity is possible.",
                column=column_name,
                evidence={"confidence_gap": candidate_gap, "type": inferred.type},
                suggested_actions=["Review candidate types before approval."],
            )
        )

    if inferred.type in {DataType.INTEGER.value, DataType.DECIMAL.value}:
        matched = metadata.get("matched")
        total = metadata.get("total")
        if isinstance(matched, int) and isinstance(total, int) and total > 0:
            ratio = matched / total
            if ratio < 0.9:
                risks.append(
                    FunnelRiskItem(
                        code="mixed_numeric_values",
                        severity="warning",
                        message="Not all samples match the numeric type.",
                        column=column_name,
                        evidence={"matched_ratio": ratio, "matched": matched, "total": total},
                        suggested_actions=[
                            "Check for non-numeric tokens and clean or cast explicitly.",
                        ],
                    )
                )

    if inferred.type in {DataType.DATE.value, DataType.DATETIME.value}:
        ambiguous = metadata.get("ambiguous_count")
        if isinstance(ambiguous, int) and ambiguous > 0:
            risks.append(
                FunnelRiskItem(
                    code="ambiguous_date_format",
                    severity="warning",
                    message="Date format appears ambiguous in samples.",
                    column=column_name,
                    evidence={"ambiguous_count": ambiguous},
                    suggested_actions=[
                        "Standardize date format before approval.",
                        "Provide explicit parsing rules if needed.",
                    ],
                )
            )
        fuzzy = metadata.get("fuzzy_matches")
        if isinstance(fuzzy, int) and fuzzy > 0:
            risks.append(
                FunnelRiskItem(
                    code="fuzzy_datetime_matches",
                    severity="info",
                    message="Datetime patterns are inferred with fuzzy matches.",
                    column=column_name,
                    evidence={"fuzzy_matches": fuzzy},
                    suggested_actions=["Validate datetime format consistency."],
                )
            )

    return risks


def _build_column_profile(
    values: List[Any], result: ColumnAnalysisResult
) -> Optional[ColumnProfile]:
    non_empty = [v for v in values if v is not None and str(v).strip() != ""]
    length_stats = _compute_length_stats(non_empty)
    numeric_stats = _compute_numeric_stats(non_empty, result.inferred_type.metadata or {})
    format_stats = _extract_format_stats(result.inferred_type.metadata or {})

    if not (length_stats or numeric_stats or format_stats):
        return None

    return ColumnProfile(
        length_stats=length_stats,
        numeric_stats=numeric_stats,
        format_stats=format_stats,
    )


def _compute_length_stats(values: List[Any]) -> Optional[Dict[str, float]]:
    strings = [v for v in values if isinstance(v, str)]
    if not strings:
        return None
    lengths = [len(s) for s in strings]
    if not lengths:
        return None
    return {
        "min": float(min(lengths)),
        "max": float(max(lengths)),
        "mean": float(sum(lengths) / len(lengths)),
    }


def _compute_numeric_stats(
    values: List[Any], metadata: Dict[str, Any]
) -> Optional[Dict[str, float]]:
    if {"min", "max", "mean", "std"} <= metadata.keys():
        return {
            "min": float(metadata["min"]),
            "max": float(metadata["max"]),
            "mean": float(metadata["mean"]),
            "std": float(metadata["std"]),
        }

    numeric_values: List[float] = []
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            numeric_values.append(float(value))
            continue
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                continue
            try:
                numeric_values.append(float(raw))
            except ValueError:
                continue

    if not numeric_values:
        return None
    mean = sum(numeric_values) / len(numeric_values)
    if len(numeric_values) > 1:
        variance = sum((v - mean) ** 2 for v in numeric_values) / (len(numeric_values) - 1)
        std = variance**0.5
    else:
        std = 0.0

    return {
        "min": float(min(numeric_values)),
        "max": float(max(numeric_values)),
        "mean": float(mean),
        "std": float(std),
    }


def _extract_format_stats(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    format_keys = ["detected_format", "ambiguous_count", "timezone_count", "fuzzy_matches"]
    payload = {key: metadata[key] for key in format_keys if key in metadata}
    return payload or None


def _is_key_like(name: str) -> bool:
    lowered = name.strip().lower()
    if lowered in {"id", "key", "uuid", "guid"}:
        return True
    if lowered.endswith("_id") or lowered.endswith("id"):
        return True
    if " id " in f" {lowered} ":
        return True
    return any(token in lowered for token in ("_key", "key_", "code", "uuid"))
