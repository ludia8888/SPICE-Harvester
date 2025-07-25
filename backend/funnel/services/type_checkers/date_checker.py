"""
🔥 ULTRA: Date Type Checker
Advanced date detection with multilingual support and fuzzy matching
"""

import re
from datetime import datetime
from collections import Counter
from typing import Tuple, List
from shared.models.common import DataType
from shared.models.type_inference import TypeInferenceResult
from .base import BaseTypeChecker, TypeCheckContext


class DateTypeChecker(BaseTypeChecker):
    """
    🔥 ULTRA: Date type checker with multilingual and fuzzy matching
    
    Features:
    - Multi-format date pattern recognition
    - Multilingual date formats (Korean, Japanese, Chinese)
    - Fuzzy pattern matching
    - Format detection and reporting
    """
    
    # Enhanced date patterns with multilingual support
    DATE_PATTERNS = [
        # ISO formats
        (r"^\d{4}-\d{2}-\d{2}$", "%Y-%m-%d", "YYYY-MM-DD"),
        (r"^\d{4}/\d{2}/\d{2}$", "%Y/%m/%d", "YYYY/MM/DD"),
        # US formats
        (r"^\d{2}/\d{2}/\d{4}$", "%m/%d/%Y", "MM/DD/YYYY"),
        (r"^\d{2}-\d{2}-\d{4}$", "%m-%d-%Y", "MM-DD-YYYY"),
        # European formats
        (r"^\d{2}/\d{2}/\d{4}$", "%d/%m/%Y", "DD/MM/YYYY"),
        (r"^\d{2}\.\d{2}\.\d{4}$", "%d.%m.%Y", "DD.MM.YYYY"),
        # Korean format
        (r"^\d{4}년\s*\d{1,2}월\s*\d{1,2}일$", None, "YYYY년 MM월 DD일"),
        # Japanese formats
        (r"^\d{4}年\s*\d{1,2}月\s*\d{1,2}日$", None, "YYYY年MM月DD日"),
        (r"^令和\d+年\s*\d{1,2}月\s*\d{1,2}日$", None, "令和年月日"),
        # Chinese formats
        (r"^\d{4}年\s*\d{1,2}月\s*\d{1,2}日$", None, "YYYY年MM月DD日"),
    ]
    
    def __init__(self):
        super().__init__(priority=30)
    
    @property
    def type_name(self) -> str:
        return "date"
    
    @property
    def default_threshold(self) -> float:
        return 0.8
    
    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:
        """Check if values are dates with fuzzy matching"""
        
        exact_matches, fuzzy_matches, pattern_counts = self._analyze_date_patterns(
            context.values
        )
        total = len(context.values)
        
        # Calculate confidence scores
        exact_confidence = exact_matches / total if total > 0 else 0
        fuzzy_confidence = self._calculate_confidence(
            exact_matches, total, fuzzy_matches, fuzzy_weight=0.6
        )
        
        threshold = self._get_threshold(context)
        
        if exact_confidence >= threshold and pattern_counts:
            most_common = pattern_counts.most_common(1)[0][0]
            return self._create_result_with_metadata(
                exact_confidence,
                f"Enhanced date detection: {exact_matches}/{total} exact matches ({exact_confidence*100:.1f}%), primary pattern: {most_common}",
                {"detected_format": most_common, "fuzzy_matches": fuzzy_matches}
            )
        elif fuzzy_confidence >= threshold * 0.7:  # Lower threshold for fuzzy
            return self._create_result_with_metadata(
                fuzzy_confidence,
                f"Enhanced fuzzy date detection: {exact_matches} exact + {fuzzy_matches} fuzzy matches ({fuzzy_confidence*100:.1f}%)",
                {"fuzzy_matches": fuzzy_matches}
            )
        else:
            return self._create_result(
                fuzzy_confidence,
                f"Enhanced date analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)"
            )
    
    def _analyze_date_patterns(self, values: list) -> Tuple[int, int, Counter]:
        """Analyze date patterns in values"""
        exact_matches = 0
        fuzzy_matches = 0
        pattern_counts = Counter()
        
        for value in values:
            value_str = str(value).strip()
            
            # Try exact pattern matching
            if self._check_exact_date_pattern(value_str, pattern_counts):
                exact_matches += 1
            # Try fuzzy matching
            elif self._check_fuzzy_date_pattern(value_str):
                fuzzy_matches += 1
                
        return exact_matches, fuzzy_matches, pattern_counts
    
    def _check_exact_date_pattern(self, value: str, pattern_counts: Counter) -> bool:
        """Check exact date patterns"""
        for pattern_regex, format_str, pattern_name in self.DATE_PATTERNS:
            if re.match(pattern_regex, value):
                if format_str:
                    try:
                        datetime.strptime(value, format_str)
                        pattern_counts[pattern_name] += 1
                        return True
                    except ValueError:
                        continue
                else:
                    # Handle special formats (Korean, Japanese, Chinese)
                    pattern_counts[pattern_name] += 1
                    return True
        return False
    
    def _check_fuzzy_date_pattern(self, value: str) -> bool:
        """Check fuzzy date patterns"""
        # Year + month/day pattern
        if re.search(r'\d{4}', value) and re.search(r'\d{1,2}', value):
            return True
        
        # Month name patterns
        month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                      'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        if any(month in value.lower() for month in month_names):
            return True
            
        return False
    
    def _create_result(self, confidence: float, reason: str) -> TypeInferenceResult:
        """Create TypeInferenceResult for date type"""
        return TypeInferenceResult(
            type=DataType.DATE.value,
            confidence=confidence,
            reason=reason
        )
    
    def _create_result_with_metadata(self, confidence: float, reason: str, 
                                   metadata: dict) -> TypeInferenceResult:
        """Create TypeInferenceResult with metadata"""
        return TypeInferenceResult(
            type=DataType.DATE.value,
            confidence=confidence,
            reason=reason,
            metadata=metadata
        )