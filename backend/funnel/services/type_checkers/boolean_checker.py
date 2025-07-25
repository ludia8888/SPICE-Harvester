"""
🔥 ULTRA: Boolean Type Checker
High-performance boolean detection with fuzzy matching
"""

from typing import Dict, Set
from shared.models.common import DataType
from shared.models.type_inference import TypeInferenceResult
from .base import BaseTypeChecker, TypeCheckContext


class BooleanTypeChecker(BaseTypeChecker):
    """
    🔥 ULTRA: Boolean type checker with multilingual support
    
    Features:
    - Exact pattern matching
    - Fuzzy pattern matching
    - Multilingual support (Korean, Japanese, Chinese)
    - Adaptive confidence scoring
    """
    
    # Multilingual boolean values
    BOOLEAN_VALUES: Dict[str, bool] = {
        # English
        "true": True, "false": False, "yes": True, "no": False,
        "y": True, "n": False, "1": True, "0": False,
        "on": True, "off": False,
        # Korean
        "참": True, "거짓": False, "예": True, "아니오": False,
        # Japanese  
        "はい": True, "いいえ": False, "真": True, "偽": False,
        # Chinese
        "是": True, "否": False, "真": True, "假": False,
    }
    
    def __init__(self):
        super().__init__(priority=10)  # High priority - most specific
    
    @property
    def type_name(self) -> str:
        return "boolean"
    
    @property
    def default_threshold(self) -> float:
        return 0.9
    
    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:
        """Check if values are boolean with enhanced fuzzy matching"""
        
        exact_matches, fuzzy_matches = self._analyze_boolean_patterns(context.values)
        total = len(context.values)
        
        # Calculate confidence scores
        exact_confidence = exact_matches / total if total > 0 else 0
        fuzzy_confidence = self._calculate_confidence(
            exact_matches, total, fuzzy_matches, fuzzy_weight=0.7
        )
        
        # Get adaptive threshold
        threshold = self._get_threshold(context)
        
        # Determine result
        if exact_confidence >= threshold:
            return self._create_result(
                exact_confidence,
                f"Enhanced boolean detection: {exact_matches}/{total} exact matches ({exact_confidence*100:.1f}%)"
            )
        elif fuzzy_confidence >= threshold * 0.8:  # Lower threshold for fuzzy
            return self._create_result(
                fuzzy_confidence,
                f"Enhanced fuzzy boolean detection: {exact_matches} exact + {fuzzy_matches} fuzzy matches ({fuzzy_confidence*100:.1f}%)"
            )
        else:
            return self._create_result(
                fuzzy_confidence,
                f"Enhanced boolean analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)"
            )
    
    def _analyze_boolean_patterns(self, values: list) -> tuple:
        """Analyze boolean patterns in values"""
        exact_matches = 0
        fuzzy_matches = 0
        
        for value in values:
            value_lower = str(value).lower().strip()
            
            # Exact match
            if value_lower in self.BOOLEAN_VALUES:
                exact_matches += 1
            # Fuzzy matching for partial patterns
            elif self._is_fuzzy_boolean(value_lower):
                fuzzy_matches += 1
                
        return exact_matches, fuzzy_matches
    
    def _is_fuzzy_boolean(self, value: str) -> bool:
        """Check if value fuzzy matches boolean patterns"""
        # Check for boolean-like substrings
        bool_substrings = [k for k in self.BOOLEAN_VALUES.keys() if len(k) > 2]
        return any(bool_val in value for bool_val in bool_substrings)
    
    def _create_result(self, confidence: float, reason: str) -> TypeInferenceResult:
        """Create TypeInferenceResult for boolean type"""
        return TypeInferenceResult(
            type=DataType.BOOLEAN.value,
            confidence=confidence,
            reason=reason
        )