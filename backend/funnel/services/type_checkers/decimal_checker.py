"""
🔥 ULTRA: Decimal Type Checker
Advanced decimal/float detection with international format support
"""

import re
from typing import Tuple
from shared.models.common import DataType
from shared.models.type_inference import TypeInferenceResult
from .base import BaseTypeChecker, TypeCheckContext


class DecimalTypeChecker(BaseTypeChecker):
    """
    🔥 ULTRA: Decimal type checker with international format support
    
    Features:
    - Multiple decimal notation support (. and ,)
    - Thousands separator handling
    - Scientific notation support
    - Statistical analysis for validation
    """
    
    def __init__(self):
        super().__init__(priority=25)
    
    @property
    def type_name(self) -> str:
        return "decimal"
    
    @property
    def default_threshold(self) -> float:
        return 0.9
    
    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:
        """Check if values are decimal numbers"""
        
        exact_matches, fuzzy_matches, has_consistent_decimals = self._analyze_decimal_patterns(
            context.values
        )
        total = len(context.values)
        
        # Calculate confidence
        exact_confidence = exact_matches / total if total > 0 else 0
        fuzzy_confidence = self._calculate_confidence(
            exact_matches, total, fuzzy_matches, fuzzy_weight=0.7
        )
        
        # Column hint boost
        column_boost = self._check_column_hints(
            context, 
            ['price', 'amount', 'cost', 'value', 'decimal', 'float', 'rate', 'ratio', 'percentage']
        )
        final_confidence = min(1.0, fuzzy_confidence + column_boost)
        
        threshold = self._get_threshold(context)
        
        if exact_confidence >= threshold:
            decimal_places_info = " (consistent decimal places)" if has_consistent_decimals else ""
            return self._create_result(
                exact_confidence,
                f"Enhanced decimal detection: {exact_matches}/{total} exact matches ({exact_confidence*100:.1f}%){decimal_places_info}"
            )
        elif final_confidence >= threshold * 0.8:
            return self._create_result(
                final_confidence,
                f"Enhanced decimal analysis: {exact_matches} exact + {fuzzy_matches} fuzzy matches ({final_confidence*100:.1f}%)"
            )
        else:
            return self._create_result(
                final_confidence,
                f"Enhanced decimal analysis: insufficient matches ({final_confidence*100:.1f}%)"
            )
    
    def _analyze_decimal_patterns(self, values: list) -> Tuple[int, int, bool]:
        """Analyze decimal patterns in values"""
        exact_matches = 0
        fuzzy_matches = 0
        decimal_places = []
        
        for value in values:
            cleaned = str(value).strip()
            
            # Try exact decimal matching
            match_result = self._check_exact_decimal(cleaned)
            if match_result is not None:
                exact_matches += 1
                decimal_places.append(match_result)
            # Try fuzzy matching
            elif self._check_fuzzy_decimal(cleaned):
                fuzzy_matches += 1
        
        # Check if decimal places are consistent
        has_consistent_decimals = len(set(decimal_places)) == 1 if decimal_places else False
        
        return exact_matches, fuzzy_matches, has_consistent_decimals
    
    def _check_exact_decimal(self, value: str) -> int:
        """Check exact decimal patterns and return decimal places if match"""
        # Remove thousands separators
        cleaned = value.replace(',', '').replace(' ', '')
        
        # Standard decimal pattern
        decimal_match = re.match(r'^[+-]?\d+\.\d+$', cleaned)
        if decimal_match:
            return len(cleaned.split('.')[1])
        
        # Scientific notation
        if re.match(r'^[+-]?\d+\.?\d*[eE][+-]?\d+$', cleaned):
            return -1  # Special marker for scientific notation
        
        # European format (comma as decimal)
        if ',' in value and '.' not in value:
            european_match = re.match(r'^[+-]?\d+,\d+$', value.replace(' ', ''))
            if european_match:
                return len(value.split(',')[1])
        
        return None
    
    def _check_fuzzy_decimal(self, value: str) -> bool:
        """Check fuzzy decimal patterns"""
        # Remove common separators
        cleaned = value.replace(',', '').replace(' ', '')
        
        # Check if it looks like a decimal with formatting issues
        if re.search(r'\d+[.,]\d+', cleaned):
            return True
        
        # Check for percentage values
        if re.match(r'^\d+\.?\d*%$', cleaned):
            return True
            
        return False
    
    def _create_result(self, confidence: float, reason: str) -> TypeInferenceResult:
        """Create TypeInferenceResult for decimal type"""
        return TypeInferenceResult(
            type=DataType.DECIMAL.value,
            confidence=confidence,
            reason=reason
        )