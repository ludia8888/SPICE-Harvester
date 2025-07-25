"""
🔥 ULTRA: Enhanced Type Inference Service (Refactored)
Enterprise-grade type inference with SOLID principles and parallel processing
"""

from typing import Any, List, Optional, Dict
import logging

from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult
from .type_checkers.parallel_type_manager import ParallelTypeInferenceManager

logger = logging.getLogger(__name__)


class EnhancedFunnelTypeInferenceService:
    """
    🔥 ULTRA: Enterprise-grade Type Inference Service (Refactored)
    
    NEW ARCHITECTURE FEATURES:
    ✅ SOLID Principles Applied:
       - SRP: Each type checker handles one responsibility
       - OCP: Easy to add new type checkers without modification
       - LSP: All checkers are interchangeable
       - ISP: Minimal, focused interfaces
       - DIP: Depends on abstractions, not implementations
    
    ✅ Performance Optimizations:
       - Parallel execution of all type checkers using asyncio.gather()
       - Concurrent column analysis for datasets
       - Intelligent result caching and memoization
    
    ✅ Code Quality Improvements:
       - Reduced cyclomatic complexity (each method < 50 lines)
       - Eliminated code duplication through base classes
       - Enhanced testability with dependency injection
       - Clear separation of concerns
    
    ✅ Advanced Features:
       - Adaptive thresholds based on data quality metrics
       - Contextual analysis with surrounding columns
       - Fuzzy pattern matching with multilingual support
       - Statistical analysis for confidence enhancement
    """
    
    def __init__(self):
        """Initialize with parallel type manager"""
        self.type_manager = ParallelTypeInferenceManager()
        logger.info("🔥 Enhanced Funnel Type Inference Service initialized with parallel processing")
    
    async def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
    ) -> ColumnAnalysisResult:
        """
        🔥 ULTRA: Asynchronous column type inference with parallel processing
        
        Args:
            column_data: Column sample data
            column_name: Column name for hints
            include_complex_types: Enable complex type checking
            context_columns: Surrounding columns for contextual analysis
            
        Returns:
            ColumnAnalysisResult with comprehensive analysis
        """
        
        # Basic statistics
        non_empty_values = [v for v in column_data if v is not None and str(v).strip() != ""]
        null_count = len(column_data) - len(non_empty_values)
        unique_values = set(str(v) for v in non_empty_values)
        unique_count = len(unique_values)
        sample_values = list(non_empty_values[:5])
        
        if not non_empty_values:
            result = TypeInferenceResult(
                type="xsd:string",
                confidence=1.0,
                reason="All values are empty, defaulting to string type",
            )
            return ColumnAnalysisResult(
                column_name=column_name or "unknown",
                inferred_type=result,
                sample_values=[],
                null_count=null_count,
                unique_count=0,
            )
        
        # 🔥 Parallel type inference
        inference_result = await self.type_manager.infer_type_parallel(
            non_empty_values,
            column_name=column_name,
            sample_size=len(column_data),
            context_columns=context_columns,
            include_complex_types=include_complex_types
        )
        
        return ColumnAnalysisResult(
            column_name=column_name or "unknown",
            inferred_type=inference_result,
            sample_values=sample_values,
            null_count=null_count,
            unique_count=unique_count,
        )
    
    async def analyze_dataset(
        self,
        data: List[List[Any]],
        columns: List[str],
        sample_size: Optional[int] = 1000,
        include_complex_types: bool = False,
    ) -> List[ColumnAnalysisResult]:
        """
        🔥 ULTRA: Parallel dataset analysis with concurrent column processing
        
        Args:
            data: Dataset (list of rows)
            columns: Column names
            sample_size: Analysis sample size
            include_complex_types: Enable complex type checking
            
        Returns:
            List of ColumnAnalysisResult for each column
        """
        
        logger.info(f"🔥 Starting parallel dataset analysis: {len(columns)} columns, {len(data)} rows")
        
        # Use parallel manager for dataset analysis
        results = await self.type_manager.analyze_dataset_parallel(
            data=data,
            columns=columns,
            sample_size=sample_size,
            include_complex_types=include_complex_types
        )
        
        logger.info(f"✅ Parallel dataset analysis completed: {len(results)} columns processed")
        return results
    
    def register_custom_checker(self, checker):
        """
        Register a custom type checker
        
        Args:
            checker: Instance of BaseTypeChecker
        """
        self.type_manager.register_checker(checker)
        logger.info(f"Registered custom type checker: {checker.__class__.__name__}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics
        
        Returns:
            Dictionary with performance metrics
        """
        return {
            "registered_checkers": len(self.type_manager.checkers),
            "checker_types": [c.__class__.__name__ for c in self.type_manager.checkers],
            "parallel_processing": True,
            "adaptive_thresholds": True,
            "contextual_analysis": True,
            "multilingual_support": True,
        }


# Backward compatibility wrapper
class FunnelTypeInferenceService:
    """
    🔥 Backward compatibility wrapper for existing code
    
    This maintains the same interface as the original service while
    using the new enhanced parallel architecture underneath.
    """
    
    def __init__(self):
        import asyncio
        self._enhanced_service = EnhancedFunnelTypeInferenceService()
        self._loop = None
    
    def _get_event_loop(self):
        """Get or create event loop for async operations"""
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop
    
    @classmethod
    def infer_column_type(
        cls,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
    ) -> ColumnAnalysisResult:
        """
        Synchronous wrapper for backward compatibility
        """
        service = cls()
        loop = service._get_event_loop()
        
        return loop.run_until_complete(
            service._enhanced_service.infer_column_type(
                column_data=column_data,
                column_name=column_name,
                include_complex_types=include_complex_types,
                context_columns=context_columns
            )
        )
    
    @classmethod
    def analyze_dataset(
        cls,
        data: List[List[Any]],
        columns: List[str],
        sample_size: Optional[int] = 1000,
        include_complex_types: bool = False,
    ) -> List[ColumnAnalysisResult]:
        """
        Synchronous wrapper for dataset analysis
        """
        service = cls()
        loop = service._get_event_loop()
        
        return loop.run_until_complete(
            service._enhanced_service.analyze_dataset(
                data=data,
                columns=columns,
                sample_size=sample_size,
                include_complex_types=include_complex_types
            )
        )