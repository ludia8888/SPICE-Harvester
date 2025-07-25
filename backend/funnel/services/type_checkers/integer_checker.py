"""
🔥 ULTRA: Integer Type Checker
Advanced integer detection with statistical analysis
"""

import statistics
from typing import List
from shared.models.common import DataType
from shared.models.type_inference import TypeInferenceResult
from .base import BaseTypeChecker, TypeCheckContext


class IntegerTypeChecker(BaseTypeChecker):
    """
    🔥 ULTRA: Integer type checker with statistical analysis
    
    Features:
    - Pattern recognition for different integer formats
    - Statistical analysis for confidence boosting
    - Range and digit analysis
    - Thousand separator handling
    """
    
    def __init__(self):
        super().__init__(priority=20)
    
    @property
    def type_name(self) -> str:
        return "integer"
    
    @property
    def default_threshold(self) -> float:
        return 0.9
    
    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:
        """Check if values are integers with statistical analysis"""
        
        matched_count, integer_values = self._analyze_integer_patterns(context.values)
        total = len(context.values)
        base_confidence = matched_count / total if total > 0 else 0
        
        # Statistical enhancement
        enhanced_confidence = self._enhance_with_statistics(
            base_confidence, integer_values
        )
        
        threshold = self._get_threshold(context)
        
        if enhanced_confidence >= threshold:
            stats_info = self._generate_stats_info(integer_values)
            return self._create_result(
                enhanced_confidence,
                f"Enhanced integer analysis: {matched_count}/{total} values ({enhanced_confidence*100:.1f}%){stats_info}"
            )
        else:
            return self._create_result(
                enhanced_confidence,
                f"Enhanced integer analysis: insufficient matches ({enhanced_confidence*100:.1f}%)"
            )
    
    def _analyze_integer_patterns(self, values: list) -> tuple:
        """Analyze integer patterns in values"""
        matched_count = 0
        integer_values = []
        
        for value in values:
            cleaned = self._clean_integer_value(str(value))
            integer_val = self._try_parse_integer(cleaned)
            
            if integer_val is not None:
                matched_count += 1
                integer_values.append(integer_val)
                
        return matched_count, integer_values
    
    def _clean_integer_value(self, value: str) -> str:
        """Clean integer value by removing separators"""
        return value.replace(",", "").replace(" ", "").strip()
    
    def _try_parse_integer(self, cleaned_value: str) -> int:
        """Try to parse as integer with validation"""
        try:
            int_val = int(cleaned_value)
            # Verify it's actually an integer (not a float disguised as int)
            if (cleaned_value == str(int_val) or 
                (cleaned_value.startswith("+") and cleaned_value[1:] == str(int_val))):
                return int_val
        except ValueError:
            pass
        return None
    
    def _enhance_with_statistics(self, base_confidence: float, 
                               integer_values: List[int]) -> float:
        """Enhance confidence using statistical analysis"""
        if not integer_values or len(integer_values) < 2:
            return base_confidence
        
        try:
            # Calculate statistical metrics
            value_range = max(integer_values) - min(integer_values)
            avg_digits = statistics.mean([len(str(abs(v))) for v in integer_values])
            
            # Boost confidence for typical integer patterns
            boost = 0.0
            if avg_digits <= 3 and value_range < 1000:  # IDs, counts, etc.
                boost += 0.1
            elif all(v >= 0 for v in integer_values):  # All positive
                boost += 0.05
                
            return min(1.0, base_confidence + boost)
            
        except statistics.StatisticsError:
            return base_confidence
    
    def _generate_stats_info(self, integer_values: List[int]) -> str:
        """Generate statistics information for reasoning"""
        if not integer_values or len(integer_values) < 2:
            return ""
        
        try:
            value_range = max(integer_values) - min(integer_values)
            avg_digits = statistics.mean([len(str(abs(v))) for v in integer_values])
            return f", range: {value_range}, avg_digits: {avg_digits:.1f}"
        except:
            return ""
    
    def _create_result(self, confidence: float, reason: str) -> TypeInferenceResult:
        """Create TypeInferenceResult for integer type"""
        return TypeInferenceResult(
            type=DataType.INTEGER.value,
            confidence=confidence,
            reason=reason
        )