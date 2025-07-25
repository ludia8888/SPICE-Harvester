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
    def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TypeInferenceResult:
        """
        Infer the data type of a single column.

        Args:
            column_data: List of values from the column
            column_name: Optional column name for context
            metadata: Optional metadata for inference hints

        Returns:
            TypeInferenceResult with inferred type, confidence, and reasoning
        """
        pass

    @abstractmethod
    def analyze_dataset(
        self,
        data: List[Dict[str, Any]],
        headers: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[ColumnAnalysisResult]:
        """
        Analyze an entire dataset and infer types for all columns.

        Args:
            data: List of dictionaries representing rows
            headers: Optional list of column headers
            metadata: Optional metadata for analysis hints

        Returns:
            List of ColumnAnalysisResult for each column
        """
        pass

    @abstractmethod
    def infer_single_value_type(
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
        pass


class RealTypeInferenceService(TypeInferenceInterface):
    """
    🔥 REAL IMPLEMENTATION! Production-ready type inference service.
    
    Uses actual Funnel service algorithms for accurate type detection.
    No more mock implementations!
    """

    def __init__(self):
        """Initialize with real Funnel type inference service."""
        # Import here to avoid circular dependencies
        from funnel.services.type_inference import FunnelTypeInferenceService
        self.funnel_service = FunnelTypeInferenceService

    def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TypeInferenceResult:
        """🔥 REAL implementation using Funnel service algorithms."""
        # Use actual Funnel service implementation
        include_complex_types = metadata.get("include_complex_types", False) if metadata else False
        
        analysis_result = self.funnel_service.infer_column_type(
            column_data=column_data,
            column_name=column_name,
            include_complex_types=include_complex_types
        )
        
        # Extract the TypeInferenceResult from ColumnAnalysisResult
        return analysis_result.inferred_type

    def analyze_dataset(
        self,
        data: List[Dict[str, Any]],
        headers: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[ColumnAnalysisResult]:
        """🔥 REAL implementation using Funnel service algorithms."""
        if not headers:
            # Extract headers from first row if not provided
            headers = list(data[0].keys()) if data else []
        
        # Convert dict data to list of lists format expected by Funnel
        table_data = []
        for row in data:
            table_row = [row.get(header) for header in headers]
            table_data.append(table_row)
        
        # Use actual Funnel service
        include_complex_types = metadata.get("include_complex_types", False) if metadata else False
        sample_size = metadata.get("sample_size", 1000) if metadata else 1000
        
        return self.funnel_service.analyze_dataset(
            data=table_data,
            columns=headers,
            sample_size=sample_size,
            include_complex_types=include_complex_types
        )

    def infer_single_value_type(
        self, value: Any, context: Optional[Dict[str, Any]] = None
    ) -> TypeInferenceResult:
        """🔥 REAL implementation using Funnel service algorithms."""
        # Create a single-item column for analysis
        analysis_result = self.infer_column_type(
            column_data=[value],
            column_name=context.get("column_name") if context else None,
            metadata=context
        )
        return analysis_result


def get_production_type_inference_service() -> TypeInferenceInterface:
    """
    🔥 Get REAL production type inference service!
    
    No more mocks - this returns the actual Funnel-powered service.

    Returns:
        RealTypeInferenceService instance with actual AI algorithms
    """
    return RealTypeInferenceService()


if __name__ == "__main__":
    # Test the mock implementation
    mock_service = get_mock_type_inference_service()

    # Test single value inference
    result = mock_service.infer_single_value_type("test_value")
    print(f"Single value result: {result}")

    # Test dataset analysis
    test_data = [
        {"name": "John", "age": "25", "city": "New York"},
        {"name": "Jane", "age": "30", "city": "San Francisco"},
    ]

    results = mock_service.analyze_dataset(test_data, headers=["name", "age", "city"])
    print(f"Dataset analysis results: {len(results)} columns analyzed")
    for result in results:
        print(
            f"  {result.column_name}: {result.inferred_type.type} (confidence: {result.inferred_type.confidence})"
        )
