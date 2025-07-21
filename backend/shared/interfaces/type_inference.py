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


class MockTypeInferenceService(TypeInferenceInterface):
    """
    Mock implementation for testing purposes.

    This service always returns "string" type with high confidence.
    Useful for testing and development when actual type inference is not needed.
    """

    def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TypeInferenceResult:
        """Mock implementation - always returns string type."""
        return TypeInferenceResult(
            type="string", confidence=0.95, reason="Mock implementation - always returns string"
        )

    def analyze_dataset(
        self,
        data: List[Dict[str, Any]],
        headers: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[ColumnAnalysisResult]:
        """Mock implementation - analyzes each column as string."""
        results = []

        if headers:
            for header in headers:
                # Extract column data
                column_data = [row.get(header) for row in data if header in row]

                mock_result = ColumnAnalysisResult(
                    column_name=header,
                    inferred_type=self.infer_column_type(column_data, header),
                    sample_values=column_data[:5],  # First 5 values as samples
                    null_count=sum(1 for val in column_data if val is None or val == ""),
                    unique_count=len(set(str(val) for val in column_data if val is not None)),
                )
                results.append(mock_result)

        return results

    def infer_single_value_type(
        self, value: Any, context: Optional[Dict[str, Any]] = None
    ) -> TypeInferenceResult:
        """Mock implementation - always returns string type."""
        return TypeInferenceResult(
            type="string", confidence=0.95, reason="Mock implementation - always returns string"
        )


def get_mock_type_inference_service() -> TypeInferenceInterface:
    """
    Get a mock type inference service for testing.

    Returns:
        MockTypeInferenceService instance
    """
    return MockTypeInferenceService()


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
