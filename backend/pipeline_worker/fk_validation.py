from __future__ import annotations

import logging
import operator
from functools import reduce
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - import guard for unit tests without Spark
    from pyspark.sql import DataFrame  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
except ImportError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment,misc]
    F = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class FKReferenceDatasetLoadError(RuntimeError):
    """Raised when a FK reference dataset cannot be loaded due to infra/internal failure."""


def parse_fk_expectation(
    worker: Any,
    expectation: Dict[str, Any],
    *,
    default_branch: Optional[str],
) -> Optional[Dict[str, Any]]:
    return worker._parse_fk_expectation(expectation, default_branch=default_branch)


async def load_fk_reference_dataframe(
    worker: Any,
    *,
    db_name: str,
    dataset_id: Optional[str],
    dataset_name: Optional[str],
    branch: Optional[str],
    temp_dirs: list[str],
) -> Optional[DataFrame]:
    metadata: Dict[str, Any] = {}
    if dataset_id:
        metadata["datasetId"] = dataset_id
    if dataset_name:
        metadata["datasetName"] = dataset_name
    if branch:
        metadata["datasetBranch"] = branch
    try:
        return await worker._load_input_dataframe(
            db_name=db_name,
            metadata=metadata,
            temp_dirs=temp_dirs,
            branch=branch or "main",
            node_id=f"fk_ref_{dataset_name or dataset_id or 'dataset'}",
            input_snapshots=None,
            previous_commit_id=None,
            use_lakefs_diff=False,
        )
    except Exception as exc:
        logger.warning("Failed to load FK reference dataset: %s", exc)
        raise FKReferenceDatasetLoadError(str(exc)) from exc


async def evaluate_fk_expectations(
    worker: Any,
    *,
    expectations: List[Dict[str, Any]],
    output_df: DataFrame,
    db_name: str,
    branch: Optional[str],
    temp_dirs: list[str],
) -> List[str]:
    errors: List[str] = []
    for exp in expectations or []:
        if not isinstance(exp, dict):
            continue
        spec = parse_fk_expectation(worker, exp, default_branch=branch)
        if not spec:
            continue
        columns = spec["columns"]
        ref_columns = spec["ref_columns"]
        if not columns or not ref_columns:
            errors.append("fk_exists missing columns or reference columns")
            continue
        if len(columns) != len(ref_columns):
            errors.append(f"fk_exists column count mismatch: {columns} vs {ref_columns}")
            continue
        missing_cols = [col for col in columns if col not in output_df.columns]
        if missing_cols:
            errors.append(f"fk_exists missing column(s): {', '.join(missing_cols)}")
            continue

        try:
            ref_df = await load_fk_reference_dataframe(
                worker,
                db_name=db_name,
                dataset_id=spec["dataset_id"],
                dataset_name=spec["dataset_name"],
                branch=spec["branch"],
                temp_dirs=temp_dirs,
            )
        except FKReferenceDatasetLoadError as exc:
            errors.append(f"fk_exists reference dataset load failed: {exc}")
            continue
        missing_ref_cols = [col for col in ref_columns if col not in ref_df.columns]
        if missing_ref_cols:
            errors.append(f"fk_exists reference missing column(s): {', '.join(missing_ref_cols)}")
            continue
        left = output_df.select(*columns)
        if spec["allow_nulls"]:
            for col in columns:
                left = left.filter(F.col(col).isNotNull())
        right = ref_df.select(*ref_columns)
        for col in ref_columns:
            right = right.filter(F.col(col).isNotNull())
        right = right.dropDuplicates()
        if len(columns) == 1:
            join_cond = left[columns[0]] == right[ref_columns[0]]
        else:
            join_cond = reduce(
                operator.and_,
                [left[col] == right[ref_col] for col, ref_col in zip(columns, ref_columns)],
            )
        missing_count = await worker._run_spark(
            lambda: left.join(right, join_cond, how="left_anti").count(),
            label="fk_exists:missing_count",
        )
        if missing_count:
            ref_label = spec["dataset_name"] or spec["dataset_id"] or "reference"
            errors.append(
                f"fk_exists failed: {','.join(columns)} -> {ref_label}({','.join(ref_columns)}) missing={missing_count}"
            )
    return errors
