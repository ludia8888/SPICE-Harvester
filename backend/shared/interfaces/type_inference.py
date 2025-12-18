"""
🔥 THINK ULTRA! Type Inference Interface
Abstract interface for type inference services
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

# Import shared models instead of duplicating
from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult


class TypeInferenceInterface(ABC):
    """
    Abstract interface for type inference services.

    This interface defines the contract that all type inference implementations
    must follow, ensuring consistency across different services.
    """

    @abstractmethod
    async def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ColumnAnalysisResult:
        """
        Analyze a single column and infer its type.

        Args:
            column_data: Column values (sample or full column)
            column_name: Optional column name for context/hints
            include_complex_types: Whether to include complex type detection
            context_columns: Optional surrounding columns for contextual hints
            metadata: Optional extra hints/config for inference

        Returns:
            ColumnAnalysisResult containing inferred type and profiling stats
        """
        raise NotImplementedError

    @abstractmethod
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

        Args:
            data: Dataset rows (2D table)
            columns: Column names aligned with row positions
            sample_size: Max rows to analyze
            include_complex_types: Whether to include complex type detection
            metadata: Optional extra hints/config for analysis

        Returns:
            List of ColumnAnalysisResult for each column
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError


class RealTypeInferenceService(TypeInferenceInterface):
    """
    🔥 REAL IMPLEMENTATION! Production-ready type inference service.
    
    Uses pattern-based detection algorithms for accurate type identification.
    No more mock implementations!
    """

    def __init__(self):
        """Initialize with real pattern-based type detection service."""
        # Import here to avoid circular dependencies
        from funnel.services.type_inference import PatternBasedTypeDetector

        self.funnel_service = PatternBasedTypeDetector

    async def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ColumnAnalysisResult:
        """🔥 REAL implementation using Funnel service algorithms."""
        import asyncio

        # Allow metadata to override flags while keeping explicit args as defaults
        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)

        return await asyncio.to_thread(
            self.funnel_service.infer_column_type,
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
        """🔥 REAL implementation using Funnel service algorithms."""
        import asyncio

        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)
            sample_size = metadata.get("sample_size", sample_size)

        return await asyncio.to_thread(
            self.funnel_service.analyze_dataset,
            data=data,
            columns=columns,
            sample_size=sample_size,
            include_complex_types=include_complex_types,
        )

    async def infer_single_value_type(
        self, value: Any, context: Optional[Dict[str, Any]] = None
    ) -> TypeInferenceResult:
        """🔥 REAL implementation using Funnel service algorithms."""
        column_name = context.get("column_name") if context else None
        include_complex_types = context.get("include_complex_types", False) if context else False

        analysis = await self.infer_column_type(
            column_data=[value],
            column_name=column_name,
            include_complex_types=include_complex_types,
            metadata=context,
        )
        return analysis.inferred_type


def get_production_type_inference_service() -> TypeInferenceInterface:
    """
    🔥 Get REAL production type inference service!
    
    No more mocks - this returns the actual Funnel-powered service.

    Returns:
        RealTypeInferenceService instance with actual AI algorithms
    """
    return RealTypeInferenceService()


def get_mock_type_inference_service() -> TypeInferenceInterface:
    """
    Legacy helper kept for backward compatibility.

    This project no longer ships a mock implementation for type inference.
    The returned service is the real production implementation.
    """
    return get_production_type_inference_service()


if __name__ == "__main__":
    import asyncio

    # Quick smoke test for the real implementation
    service = get_production_type_inference_service()

    # Test single value inference
    result = asyncio.run(service.infer_single_value_type("test_value"))
    print(f"Single value result: {result.model_dump() if hasattr(result, 'model_dump') else result}")

    # Test dataset analysis
    test_rows = [
        ["John", "25", "New York"],
        ["Jane", "30", "San Francisco"],
    ]
    cols = ["name", "age", "city"]

    results = asyncio.run(service.analyze_dataset(test_rows, cols))
    print(f"Dataset analysis results: {len(results)} columns analyzed")
    for col in results:
        print(f"  {col.column_name}: {col.inferred_type.type} (confidence: {col.inferred_type.confidence})")
