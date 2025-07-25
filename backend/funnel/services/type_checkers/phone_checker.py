"""
🔥 ULTRA: Phone Type Checker
Advanced phone number detection with global pattern support
"""

import re
from typing import List, Tuple
from shared.models.common import DataType
from shared.models.type_inference import TypeInferenceResult
from .base import BaseTypeChecker, TypeCheckContext


class PhoneTypeChecker(BaseTypeChecker):
    """
    🔥 ULTRA: Phone type checker with global pattern recognition
    
    Features:
    - Multi-format phone pattern recognition
    - International format support
    - Fuzzy digit-based matching
    - Column name hint integration
    """
    
    # Enhanced phone patterns for global coverage
    PHONE_PATTERNS = [
        r'^\+?1?[-\s]?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{4}$',  # US format
        r'^\+?\d{1,4}[-\s]?\(?\d{1,4}\)?[-\s]?\d{3,4}[-\s]?\d{4}$',  # International
        r'^\d{3}-\d{4}-\d{4}$',  # Korean format
        r'^\d{3}[-\s]\d{4}[-\s]\d{4}$',  # Korean alt
        r'^\d{10,15}$',  # Simple numeric
        r'^\+\d{10,15}$',  # International prefix
    ]
    
    # Multilingual phone keywords
    PHONE_KEYWORDS = [
        "phone", "tel", "mobile", "cell", 
        "전화", "휴대폰", "電話", "手机",
        "telefono", "telefone", "телефон", "تلفن"
    ]
    
    def __init__(self):
        super().__init__(priority=40)
    
    @property
    def type_name(self) -> str:
        return "phone"
    
    @property
    def default_threshold(self) -> float:
        return 0.7  # Lower threshold due to variety in phone formats
    
    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:
        """Check if values are phone numbers with pattern and fuzzy matching"""
        
        exact_matches, fuzzy_matches = self._analyze_phone_patterns(context.values)
        total = len(context.values)
        
        # Calculate base confidence
        exact_confidence = exact_matches / total if total > 0 else 0
        fuzzy_confidence = self._calculate_confidence(
            exact_matches, total, fuzzy_matches, fuzzy_weight=0.8
        )
        
        # Apply column hint boost
        column_boost = self._check_column_hints(context, self.PHONE_KEYWORDS)
        final_confidence = min(1.0, fuzzy_confidence + column_boost)
        
        threshold = self._get_threshold(context)
        
        if exact_confidence >= 0.8:
            return self._create_result(
                exact_confidence,
                f"Enhanced phone detection: {exact_matches}/{total} exact pattern matches ({exact_confidence*100:.1f}%)"
            )
        elif final_confidence >= threshold:
            boost_info = " + column hint boost" if column_boost > 0 else ""
            return self._create_result(
                final_confidence,
                f"Enhanced fuzzy phone detection: {exact_matches} exact + {fuzzy_matches} fuzzy matches ({final_confidence*100:.1f}%){boost_info}"
            )
        else:
            return self._create_result(
                final_confidence,
                f"Enhanced phone analysis: insufficient matches ({final_confidence*100:.1f}%)"
            )
    
    def _analyze_phone_patterns(self, values: list) -> Tuple[int, int]:
        """Analyze phone patterns in values"""
        exact_matches = 0
        fuzzy_matches = 0
        
        for value in values:
            cleaned = str(value).strip()
            
            # Try exact pattern matching
            if self._check_exact_phone_pattern(cleaned):
                exact_matches += 1
            # Try fuzzy matching
            elif self._check_fuzzy_phone_pattern(cleaned):
                fuzzy_matches += 1
                
        return exact_matches, fuzzy_matches
    
    def _check_exact_phone_pattern(self, value: str) -> bool:
        """Check exact phone patterns"""
        return any(re.match(pattern, value) for pattern in self.PHONE_PATTERNS)
    
    def _check_fuzzy_phone_pattern(self, value: str) -> bool:
        """Check fuzzy phone patterns based on digits and separators"""
        # Remove all non-digit characters and check length
        digits_only = re.sub(r'[^\d]', '', value)
        
        if not (7 <= len(digits_only) <= 15):  # Reasonable phone number length
            return False
        
        # Check if it has phone-like separators
        if any(sep in value for sep in ['-', ' ', '.', '(', ')']):
            return True
        
        # Check if it starts with country code patterns
        if value.startswith(('+', '00')) or len(digits_only) >= 10:
            return True
            
        return False
    
    def _create_result(self, confidence: float, reason: str) -> TypeInferenceResult:
        """Create TypeInferenceResult for phone type"""
        return TypeInferenceResult(
            type=DataType.PHONE.value,
            confidence=confidence,
            reason=reason
        )