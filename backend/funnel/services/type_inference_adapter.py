"""
🔥 THINK ULTRA! Type Inference Service Adapter
Adapts FunnelTypeInferenceService to conform to TypeInferenceInterface
"""

from typing import Any, Dict, List, Optional

from shared.interfaces.type_inference import (
    TypeInferenceInterface,
    get_production_type_inference_service,
)
from shared.models.type_inference import ColumnAnalysisResult


class FunnelTypeInferenceAdapter(TypeInferenceInterface):
    """
    Adapter that wraps FunnelTypeInferenceService to implement TypeInferenceInterface.

    This adapter handles the conversion between the interface types and the
    FunnelTypeInferenceService implementation types.
    """

    def __init__(self):
        import logging

        self.logger = logging.getLogger(__name__)
        self._delegate = get_production_type_inference_service()
        self.logger.info("🔥 FunnelTypeInferenceAdapter initialized with shared production delegate")

    async def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ColumnAnalysisResult:
        """
        Analyze a column of data and infer its type.
        """
        return await self._delegate.infer_column_type(
            column_data=column_data,
            column_name=column_name,
            include_complex_types=include_complex_types,
            context_columns=context_columns,
            metadata=metadata,
        )

    async def analyze_dataset(
        self,
        data: List[List[Any]],
        columns: List[str],
        sample_size: int = 1000,
        include_complex_types: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[ColumnAnalysisResult]:
        """
        Analyze an entire dataset and infer types for all columns.
        """
        return await self._delegate.analyze_dataset(
            data=data,
            columns=columns,
            sample_size=sample_size,
            include_complex_types=include_complex_types,
            metadata=metadata,
        )
