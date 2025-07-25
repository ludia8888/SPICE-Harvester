"""
🔥 ULTRA: Parallel Type Inference Manager
High-performance parallel type checking with intelligent result selection
"""

import asyncio
from typing import List, Optional, Dict, Any
from shared.models.type_inference import TypeInferenceResult

from .base import BaseTypeChecker, TypeCheckContext, TypeCheckResult
from .adaptive_threshold_calculator import AdaptiveThresholdCalculator
from .boolean_checker import BooleanTypeChecker
from .integer_checker import IntegerTypeChecker
from .date_checker import DateTypeChecker
from .phone_checker import PhoneTypeChecker


class ParallelTypeInferenceManager:
    """
    🔥 ULTRA: Parallel type inference manager
    
    Features:
    - Concurrent execution of all type checkers
    - Intelligent result selection based on confidence and priority
    - Adaptive threshold calculation
    - Extensible checker registration
    - Performance optimization through asyncio
    """
    
    def __init__(self):
        self.checkers: List[BaseTypeChecker] = []
        self._register_default_checkers()
    
    def _register_default_checkers(self):
        """Register all default type checkers"""
        self.checkers = [
            BooleanTypeChecker(),
            IntegerTypeChecker(), 
            DateTypeChecker(),
            PhoneTypeChecker(),
        ]
        
        # Sort by priority (lower number = higher priority)
        self.checkers.sort(key=lambda x: x.priority)
    
    def register_checker(self, checker: BaseTypeChecker):
        """Register a new type checker"""
        self.checkers.append(checker)
        self.checkers.sort(key=lambda x: x.priority)
    
    async def infer_type_parallel(
        self,
        values: List[Any],
        column_name: Optional[str] = None,
        sample_size: int = 0,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        include_complex_types: bool = False
    ) -> TypeInferenceResult:
        """
        🔥 ULTRA: Parallel type inference with all checkers
        
        Args:
            values: Values to analyze
            column_name: Column name for hints
            sample_size: Total sample size
            context_columns: Surrounding columns for context analysis
            include_complex_types: Whether to include complex type checking
            
        Returns:
            Best TypeInferenceResult based on confidence and priority
        """
        
        # Convert to strings and prepare context
        str_values = [str(v).strip() for v in values if v is not None]
        
        if not str_values:
            return self._create_default_result("All values are empty")
        
        # Calculate adaptive thresholds
        adaptive_thresholds = AdaptiveThresholdCalculator.calculate_adaptive_thresholds(
            str_values, sample_size
        )
        
        # Create context
        context = TypeCheckContext(
            values=str_values,
            column_name=column_name,
            sample_size=sample_size,
            context_columns=context_columns,
            adaptive_thresholds=adaptive_thresholds
        )
        
        # Run all checkers in parallel
        results = await self._run_checkers_parallel(context)
        
        # Select best result
        best_result = self._select_best_result(results, adaptive_thresholds)
        
        return best_result
    
    async def _run_checkers_parallel(
        self, 
        context: TypeCheckContext
    ) -> List[TypeCheckResult]:
        """Run all type checkers in parallel using asyncio.gather()"""
        
        # Create tasks for all checkers
        tasks = []
        for checker in self.checkers:
            task = self._run_single_checker(checker, context)
            tasks.append(task)
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Log exception but continue with other checkers
                print(f"Checker {self.checkers[i].__class__.__name__} failed: {result}")
                continue
            
            if isinstance(result, TypeCheckResult):
                result.priority = self.checkers[i].priority
                valid_results.append(result)
        
        return valid_results
    
    async def _run_single_checker(
        self, 
        checker: BaseTypeChecker, 
        context: TypeCheckContext
    ) -> TypeCheckResult:
        """Run a single type checker with error handling"""
        try:
            result = await checker.check_type(context)
            return TypeCheckResult(
                checker_name=checker.__class__.__name__,
                result=result
            )
        except Exception as e:
            # Return a low-confidence result for failed checkers
            from shared.models.common import DataType
            failed_result = TypeInferenceResult(
                type=DataType.STRING.value,
                confidence=0.0,
                reason=f"Checker {checker.__class__.__name__} failed: {str(e)}"
            )
            return TypeCheckResult(
                checker_name=checker.__class__.__name__,
                result=failed_result
            )
    
    def _select_best_result(
        self, 
        results: List[TypeCheckResult],
        adaptive_thresholds: Dict[str, float]
    ) -> TypeInferenceResult:
        """Select the best result based on confidence and priority"""
        
        if not results:
            return self._create_default_result("No checkers returned results")
        
        # Filter results that meet their adaptive threshold
        qualified_results = []
        for result in results:
            type_name = result.result.type.replace('xsd:', '').lower()
            threshold = adaptive_thresholds.get(type_name, 0.5)
            
            if result.result.confidence >= threshold:
                qualified_results.append(result)
        
        # If no results meet threshold, use the best confidence
        if not qualified_results:
            qualified_results = results
        
        # Sort by confidence (descending) then by priority (ascending)
        qualified_results.sort(key=lambda x: (-x.result.confidence, x.priority))
        
        best_result = qualified_results[0]
        
        # Enhance reason with parallel execution info
        enhanced_reason = f"{best_result.result.reason} [Parallel inference: {len(results)} checkers]"
        
        return TypeInferenceResult(
            type=best_result.result.type,
            confidence=best_result.result.confidence,
            reason=enhanced_reason,
            metadata=getattr(best_result.result, 'metadata', None)
        )
    
    def _create_default_result(self, reason: str) -> TypeInferenceResult:
        """Create default string result"""
        from shared.models.common import DataType
        return TypeInferenceResult(
            type=DataType.STRING.value,
            confidence=1.0,
            reason=f"Default to string: {reason}"
        )
    
    async def analyze_dataset_parallel(
        self,
        data: List[List[Any]],
        columns: List[str],
        sample_size: Optional[int] = 1000,
        include_complex_types: bool = False,
    ) -> List[Any]:  # Return type will be ColumnAnalysisResult
        """
        🔥 ULTRA: Analyze entire dataset with parallel processing
        
        Each column is analyzed independently and can be parallelized further
        """
        
        # Limit sample size
        if sample_size and len(data) > sample_size:
            data = data[:sample_size]
        
        if not data:
            return self._create_empty_results(columns)
        
        # Build context for advanced analysis
        context_columns = {}
        for i, col_name in enumerate(columns):
            context_columns[col_name] = [row[i] if i < len(row) else None for row in data]
        
        # Create tasks for each column
        column_tasks = []
        for i, column_name in enumerate(columns):
            column_data = [row[i] if i < len(row) else None for row in data]
            current_context = {k: v for k, v in context_columns.items() if k != column_name}
            
            task = self._analyze_single_column(
                column_data,
                column_name,
                include_complex_types,
                current_context,
                len(data)
            )
            column_tasks.append(task)
        
        # Execute all column analyses in parallel
        results = await asyncio.gather(*column_tasks)
        
        return results
    
    async def _analyze_single_column(
        self,
        column_data: List[Any],
        column_name: str,
        include_complex_types: bool,
        context_columns: Dict[str, List[Any]],
        sample_size: int
    ) -> Any:  # Will return ColumnAnalysisResult
        """Analyze a single column with full context"""
        
        # Get type inference result
        inference_result = await self.infer_type_parallel(
            column_data,
            column_name,
            sample_size,
            context_columns,
            include_complex_types
        )
        
        # Calculate statistics
        non_empty_values = [v for v in column_data if v is not None and str(v).strip() != ""]
        null_count = len(column_data) - len(non_empty_values)
        unique_count = len(set(str(v) for v in non_empty_values))
        sample_values = list(non_empty_values[:5])
        
        # Create ColumnAnalysisResult (importing here to avoid circular imports)
        from shared.models.type_inference import ColumnAnalysisResult
        
        return ColumnAnalysisResult(
            column_name=column_name,
            inferred_type=inference_result,
            sample_values=sample_values,
            null_count=null_count,
            unique_count=unique_count,
        )
    
    
    def _create_empty_results(self, columns: List[str]) -> List[Any]:
        """Create empty results for when no data is available"""
        from shared.models.common import DataType
        from shared.models.type_inference import ColumnAnalysisResult
        
        results = []
        for col in columns:
            result = TypeInferenceResult(
                type=DataType.STRING.value,
                confidence=1.0,
                reason="No data available, defaulting to string type",
            )
            results.append(
                ColumnAnalysisResult(
                    column_name=col,
                    inferred_type=result,
                    sample_values=[],
                    null_count=0,
                    unique_count=0,
                )
            )
        return results
