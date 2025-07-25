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
    ]\n    
    def __init__(self):\n        super().__init__(priority=30)\n    \n    @property\n    def type_name(self) -> str:\n        return "date"\n    \n    @property\n    def default_threshold(self) -> float:\n        return 0.8\n    \n    async def check_type(self, context: TypeCheckContext) -> TypeInferenceResult:\n        """Check if values are dates with fuzzy matching"""\n        \n        exact_matches, fuzzy_matches, pattern_counts = self._analyze_date_patterns(\n            context.values\n        )\n        total = len(context.values)\n        \n        # Calculate confidence scores\n        exact_confidence = exact_matches / total if total > 0 else 0\n        fuzzy_confidence = self._calculate_confidence(\n            exact_matches, total, fuzzy_matches, fuzzy_weight=0.6\n        )\n        \n        threshold = self._get_threshold(context)\n        \n        if exact_confidence >= threshold and pattern_counts:\n            most_common = pattern_counts.most_common(1)[0][0]\n            return self._create_result_with_metadata(\n                exact_confidence,\n                f"Enhanced date detection: {exact_matches}/{total} exact matches ({exact_confidence*100:.1f}%), primary pattern: {most_common}",\n                {"detected_format": most_common, "fuzzy_matches": fuzzy_matches}\n            )\n        elif fuzzy_confidence >= threshold * 0.7:  # Lower threshold for fuzzy\n            return self._create_result_with_metadata(\n                fuzzy_confidence,\n                f"Enhanced fuzzy date detection: {exact_matches} exact + {fuzzy_matches} fuzzy matches ({fuzzy_confidence*100:.1f}%)",\n                {"fuzzy_matches": fuzzy_matches}\n            )\n        else:\n            return self._create_result(\n                fuzzy_confidence,\n                f"Enhanced date analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)"\n            )\n    \n    def _analyze_date_patterns(self, values: list) -> Tuple[int, int, Counter]:\n        """Analyze date patterns in values"""\n        exact_matches = 0\n        fuzzy_matches = 0\n        pattern_counts = Counter()\n        \n        for value in values:\n            value_str = str(value).strip()\n            \n            # Try exact pattern matching\n            if self._check_exact_date_pattern(value_str, pattern_counts):\n                exact_matches += 1\n            # Try fuzzy matching\n            elif self._check_fuzzy_date_pattern(value_str):\n                fuzzy_matches += 1\n                \n        return exact_matches, fuzzy_matches, pattern_counts\n    \n    def _check_exact_date_pattern(self, value: str, pattern_counts: Counter) -> bool:\n        """Check exact date patterns"""\n        for pattern_regex, format_str, pattern_name in self.DATE_PATTERNS:\n            if re.match(pattern_regex, value):\n                if format_str:\n                    try:\n                        datetime.strptime(value, format_str)\n                        pattern_counts[pattern_name] += 1\n                        return True\n                    except ValueError:\n                        continue\n                else:\n                    # Handle special formats (Korean, Japanese, Chinese)\n                    pattern_counts[pattern_name] += 1\n                    return True\n        return False\n    \n    def _check_fuzzy_date_pattern(self, value: str) -> bool:\n        """Check fuzzy date patterns"""\n        # Year + month/day pattern\n        if re.search(r'\d{4}', value) and re.search(r'\d{1,2}', value):\n            return True\n        \n        # Month name patterns\n        month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',\n                      'jul', 'aug', 'sep', 'oct', 'nov', 'dec']\n        if any(month in value.lower() for month in month_names):\n            return True\n            \n        return False\n    \n    def _create_result(self, confidence: float, reason: str) -> TypeInferenceResult:\n        """Create TypeInferenceResult for date type"""\n        return TypeInferenceResult(\n            type=DataType.DATE.value,\n            confidence=confidence,\n            reason=reason\n        )\n    \n    def _create_result_with_metadata(self, confidence: float, reason: str, \n                                   metadata: dict) -> TypeInferenceResult:\n        """Create TypeInferenceResult with metadata"""\n        return TypeInferenceResult(\n            type=DataType.DATE.value,\n            confidence=confidence,\n            reason=reason,\n            metadata=metadata\n        )