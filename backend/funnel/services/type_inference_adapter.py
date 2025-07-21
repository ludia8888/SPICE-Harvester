"""
🔥 THINK ULTRA! Type Inference Service Adapter
Adapts FunnelTypeInferenceService to conform to TypeInferenceInterface
"""

from typing import Any, Dict, List, Optional

from funnel.services.type_inference import FunnelTypeInferenceService
from shared.interfaces.type_inference import ColumnAnalysisResult as InterfaceColumnResult
from shared.interfaces.type_inference import TypeInferenceInterface
from shared.interfaces.type_inference import TypeInferenceResult as InterfaceTypeResult
from shared.models.type_inference import ColumnAnalysisResult


class FunnelTypeInferenceAdapter(TypeInferenceInterface):
    """
    Adapter that wraps FunnelTypeInferenceService to implement TypeInferenceInterface.

    This adapter handles the conversion between the interface types and the
    FunnelTypeInferenceService implementation types.
    """

    def __init__(self):
        # We use class methods on FunnelTypeInferenceService, so no instance needed
        pass

    def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
    ) -> InterfaceColumnResult:
        """
        Analyze a column of data and infer its type.
        """
        # Call the actual FunnelTypeInferenceService method
        result = FunnelTypeInferenceService.infer_column_type(
            column_data, column_name, include_complex_types
        )

        # Convert FunnelTypeInferenceService result to interface result
        return self._convert_column_result(result)

    def analyze_dataset(
        self,
        data: List[List[Any]],
        headers: Optional[List[str]] = None,
        include_complex_types: bool = False,
        sample_size: Optional[int] = None,
    ) -> Dict[str, InterfaceColumnResult]:
        """
        Analyze an entire dataset and infer types for all columns.
        """
        # Use headers or generate default column names
        if headers is None:
            headers = [f"column_{i}" for i in range(len(data[0]) if data else 0)]

        # Call the actual FunnelTypeInferenceService method
        results = FunnelTypeInferenceService.analyze_dataset(
            data,
            headers,  # FunnelTypeInferenceService uses 'columns' parameter name
            sample_size or 1000,
            include_complex_types,
        )

        # Convert results to interface format
        converted_results = {}
        for column_result in results:
            converted_results[column_result.column_name] = self._convert_column_result(
                column_result
            )

        return converted_results

    def infer_type_with_confidence(
        self, values: List[Any], check_complex: bool = False
    ) -> InterfaceTypeResult:
        """
        Infer type for a list of values with confidence score.
        """
        # Since FunnelTypeInferenceService doesn't have this method,
        # we'll use infer_column_type with the values
        column_result = FunnelTypeInferenceService.infer_column_type(
            values, column_name="temp", include_complex_types=check_complex
        )

        # Extract the type inference result
        return InterfaceTypeResult(
            type=column_result.inferred_type.type,
            confidence=column_result.inferred_type.confidence,
            reason=column_result.inferred_type.reason,
        )

    def infer_single_value_type(
        self, value: Any, context: Optional[Dict[str, Any]] = None
    ) -> InterfaceTypeResult:
        """
        Infer the type of a single value.
        
        Args:
            value: The value to analyze
            context: Optional context for inference
            
        Returns:
            TypeInferenceResult with inferred type and confidence
        """
        # Use infer_column_type with a single value list
        column_result = FunnelTypeInferenceService.infer_column_type(
            [value], 
            column_name="single_value",
            include_complex_types=True
        )
        
        # Extract and return the type inference result
        return InterfaceTypeResult(
            type=column_result.inferred_type.type,
            confidence=column_result.inferred_type.confidence,
            reason=column_result.inferred_type.reason,
        )

    def _convert_column_result(self, funnel_result: ColumnAnalysisResult) -> InterfaceColumnResult:
        """
        Convert FunnelTypeInferenceService ColumnAnalysisResult to interface ColumnAnalysisResult.
        """
        # Calculate non_empty_count from total minus null_count
        # This is an approximation since we don't have the total count in funnel result
        non_empty_count = (
            funnel_result.unique_count + funnel_result.null_count
        ) - funnel_result.null_count

        # Create interface type inference result
        type_result = InterfaceTypeResult(
            type=funnel_result.inferred_type.type,
            confidence=funnel_result.inferred_type.confidence,
            reason=funnel_result.inferred_type.reason,
        )

        # Create interface column result
        return InterfaceColumnResult(
            column_name=funnel_result.column_name,
            inferred_type=type_result,
            non_empty_count=non_empty_count,
            unique_count=funnel_result.unique_count,
            null_count=funnel_result.null_count,
            sample_values=funnel_result.sample_values,
            complex_type_info=getattr(funnel_result.inferred_type, "metadata", None),
        )
