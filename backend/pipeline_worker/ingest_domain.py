from __future__ import annotations

import logging
from typing import Any, Optional

try:
    from pyspark.sql import functions as F  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    F = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class PipelineIngestDomain:
    def __init__(self, worker: Any) -> None:
        self._worker = worker

    def _spark_functions(self) -> Any:
        if F is None:
            raise RuntimeError("PySpark functions are unavailable")
        return F

    def apply_watermark_filter(
        self,
        df: Any,
        *,
        watermark_column: str,
        watermark_after: Optional[Any],
        watermark_keys: Optional[list[str]],
    ) -> Any:
        spark_functions = self._spark_functions()
        if watermark_after is None:
            return df
        if not watermark_keys:
            return df.filter(spark_functions.col(watermark_column) >= spark_functions.lit(watermark_after))
        key_col = "__watermark_key"
        df = df.withColumn(key_col, self._worker._row_hash_expr(df))
        df = df.filter(
            (spark_functions.col(watermark_column) > spark_functions.lit(watermark_after))
            | (
                (spark_functions.col(watermark_column) == spark_functions.lit(watermark_after))
                & (~spark_functions.col(key_col).isin(watermark_keys))
            )
        ).drop(key_col)
        return df

    def collect_watermark_keys(
        self,
        df: Any,
        *,
        watermark_column: str,
        watermark_value: Any,
    ) -> list[str]:
        spark_functions = self._spark_functions()
        if watermark_value is None:
            return []
        key_col = "__watermark_key"
        try:
            rows = (
                df.filter(spark_functions.col(watermark_column) == spark_functions.lit(watermark_value))
                .select(self._worker._row_hash_expr(df).alias(key_col))
                .distinct()
                .collect()
            )
        except Exception as exc:
            logger.warning("Failed to collect changed row keys: %s", exc, exc_info=True)
            return []
        keys: list[str] = []
        for row in rows:
            value = row[key_col]
            if value:
                keys.append(str(value))
        return keys

    async def apply_input_watermark_and_snapshot(
        self,
        *,
        df: Any,
        node_id: str,
        snapshot: Optional[dict[str, Any]],
        watermark_column: Optional[str],
        watermark_after: Optional[Any],
        watermark_keys: Optional[list[str]],
        label_scope: Optional[str],
        tolerate_max_errors: bool,
        always_compute_watermark_max: bool,
    ) -> Any:
        spark_functions = self._spark_functions()
        resolved_watermark_column = str(watermark_column or "").strip()
        if not resolved_watermark_column:
            return df
        if resolved_watermark_column not in df.columns:
            raise ValueError(
                f"Input node {node_id} is missing watermark column '{resolved_watermark_column}'"
            )
        if snapshot is not None:
            snapshot["watermark_column"] = resolved_watermark_column
            if watermark_after is not None:
                snapshot["watermark_after"] = watermark_after
        df = self.apply_watermark_filter(
            df,
            watermark_column=resolved_watermark_column,
            watermark_after=watermark_after,
            watermark_keys=watermark_keys,
        )

        if not always_compute_watermark_max and snapshot is None:
            return df

        label_suffix = f"{label_scope}:{node_id}" if label_scope else node_id
        if tolerate_max_errors:
            try:
                watermark_max = await self._worker._run_spark(
                    lambda: df.agg(spark_functions.max(spark_functions.col(resolved_watermark_column)).alias("watermark_max"))
                    .collect()[0]["watermark_max"],
                    label=f"watermark_max:{label_suffix}",
                )
            except Exception as exc:
                logger.warning(
                    "Failed to compute watermark max for node %s (column=%s): %s",
                    node_id,
                    resolved_watermark_column,
                    exc,
                    exc_info=True,
                )
                watermark_max = None
        else:
            watermark_max = await self._worker._run_spark(
                lambda: df.agg(spark_functions.max(spark_functions.col(resolved_watermark_column)).alias("watermark_max"))
                .collect()[0]["watermark_max"],
                label=f"watermark_max:{label_suffix}",
            )

        if snapshot is not None:
            snapshot["watermark_max"] = watermark_max
            if watermark_max is not None:
                snapshot["watermark_keys"] = await self._worker._run_spark(
                    lambda: self.collect_watermark_keys(
                        df,
                        watermark_column=resolved_watermark_column,
                        watermark_value=watermark_max,
                    ),
                    label=f"watermark_keys:{label_suffix}",
                )
        return df
