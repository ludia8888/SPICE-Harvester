"""
Type inference interface contracts.

This module must remain infrastructure-agnostic and should not import service
implementations from other packages.
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
        context_dict = context if isinstance(context, dict) else {}
        analysis = await self.infer_column_type(
            column_data=[value],
            column_name=context_dict.get("column_name"),
            include_complex_types=bool(context_dict.get("include_complex_types", False)),
            metadata=context_dict or None,
        )
        return analysis.inferred_type


if __name__ == "__main__":
    print("TypeInferenceInterface module loaded successfully.")
