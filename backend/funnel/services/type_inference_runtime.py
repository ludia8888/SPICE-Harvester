"""
Funnel-local type inference runtime implementation.

Kept outside `shared` to avoid cross-layer reverse imports from shared -> funnel.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from shared.interfaces.type_inference import TypeInferenceInterface
from shared.models.type_inference import ColumnAnalysisResult

from funnel.services.type_inference import PatternBasedTypeDetector


class FunnelProductionTypeInferenceService(TypeInferenceInterface):
    """Production type inference backed by Funnel pattern detectors."""

    async def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ColumnAnalysisResult:
        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)

        return await asyncio.to_thread(
            PatternBasedTypeDetector.infer_column_type,
            column_data=column_data,
            column_name=column_name,
            include_complex_types=include_complex_types,
            context_columns=context_columns,
        )

    async def analyze_dataset(
        self,
        data: List[List[Any]],
        columns: List[str],
        sample_size: int = 1000,
        include_complex_types: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[ColumnAnalysisResult]:
        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)
            sample_size = metadata.get("sample_size", sample_size)

        return await asyncio.to_thread(
            PatternBasedTypeDetector.analyze_dataset,
            data=data,
            columns=columns,
            sample_size=sample_size,
            include_complex_types=include_complex_types,
        )


def get_funnel_type_inference_service() -> TypeInferenceInterface:
    """Factory for Funnel production type inference service."""
    return FunnelProductionTypeInferenceService()

