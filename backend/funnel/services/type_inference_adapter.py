"""
🔥 THINK ULTRA! Type Inference Service Adapter
Adapts FunnelTypeInferenceService to conform to TypeInferenceInterface
"""

from typing import Any, Dict, List, Optional

from funnel.services.type_inference import FunnelTypeInferenceService
from shared.interfaces.type_inference import TypeInferenceInterface
from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult


class FunnelTypeInferenceAdapter(TypeInferenceInterface):
    """
    Adapter that wraps FunnelTypeInferenceService to implement TypeInferenceInterface.

    This adapter handles the conversion between the interface types and the
    FunnelTypeInferenceService implementation types.
    """

    def __init__(self):
        """🔥 REAL IMPLEMENTATION! Initialize adapter with logging and validation."""
        # We use class methods on FunnelTypeInferenceService, so no instance needed.
        import logging

        self.logger = logging.getLogger(__name__)
        self.logger.info("🔥 FunnelTypeInferenceAdapter initialized with REAL implementation")

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
        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)

        return FunnelTypeInferenceService.infer_column_type(
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
        """
        Analyze an entire dataset and infer types for all columns.
        """
        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)
            sample_size = metadata.get("sample_size", sample_size)

        return FunnelTypeInferenceService.analyze_dataset(
            data=data,
            columns=columns,
            sample_size=sample_size,
            include_complex_types=include_complex_types,
        )

    async def infer_single_value_type(
        self, value: Any, context: Optional[Dict[str, Any]] = None
    ) -> TypeInferenceResult:
        """
        Infer the type of a single value.
        
        Args:
            value: The value to analyze
            context: Optional context for inference
            
        Returns:
            TypeInferenceResult with inferred type and confidence
        """
        column_name = context.get("column_name") if context else None
        include_complex_types = context.get("include_complex_types", False) if context else False

        analysis = await self.infer_column_type(
            column_data=[value],
            column_name=column_name,
            include_complex_types=include_complex_types,
            metadata=context,
        )
        return analysis.inferred_type
