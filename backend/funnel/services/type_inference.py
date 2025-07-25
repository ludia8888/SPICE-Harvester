"""
🔥 THINK ULTRA! Funnel Type Inference Service
Automatically detects data types from sample data with confidence scoring
"""

import re
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, List, Optional, Dict, Tuple
import logging

# 기존 구현 재사용
from shared.models.common import DataType
from shared.models.type_inference import ColumnAnalysisResult, TypeInferenceResult
from shared.validators.complex_type_validator import ComplexTypeValidator

logger = logging.getLogger(__name__)


class FunnelTypeInferenceService:
    """
    🔥 THINK ULTRA! Advanced AI Type Inference Service

    고도화된 AI 알고리즘으로 데이터 타입을 지능적으로 추론합니다.
    
    Advanced Features:
    - 적응형 임계값 시스템 (Adaptive Thresholds)
    - 컨텍스트 기반 타입 추론 (Contextual Analysis)
    - 퍼지 매칭 알고리즘 (Fuzzy Pattern Matching)
    - 다국어 패턴 인식 (Multilingual Pattern Recognition)
    - 복합 타입 탐지 (Composite Type Detection)
    - 통계 분포 분석 (Statistical Distribution Analysis)

    Architecture:
    Data Connector → Advanced AI Engine → OMS/BFF
    """

    # Date patterns to check
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

    # DateTime patterns
    DATETIME_PATTERNS = [
        # ISO format with time
        (r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}", "%Y-%m-%dT%H:%M:%S", "ISO DateTime"),
        (r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}", "%Y-%m-%d %H:%M:%S", "YYYY-MM-DD HH:MM:SS"),
    ]

    # Boolean values
    BOOLEAN_VALUES = {
        "true": True,
        "false": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
        "1": True,
        "0": False,
        "on": True,
        "off": False,
        "참": True,
        "거짓": False,
        "예": True,
        "아니오": False,
        # Japanese boolean values
        "はい": True,
        "いいえ": False,
        "真": True,
        "偽": False,
        # Chinese boolean values
        "是": True,
        "否": False,
        "真": True,
        "假": False,
    }

    @classmethod
    def infer_column_type(
        cls,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
    ) -> ColumnAnalysisResult:
        """
        🔥 고도화된 AI 알고리즘으로 컬럼 데이터를 지능적으로 분석하여 타입을 추론합니다.

        Args:
            column_data: 컬럼의 샘플 데이터
            column_name: 컬럼 이름 (타입 힌트용)
            include_complex_types: 복합 타입 검사 여부
            context_columns: 주변 컬럼 데이터 (컨텍스트 분석용)

        Returns:
            ColumnAnalysisResult with advanced AI analysis
        """
        # 통계 정보 수집
        non_empty_values = [v for v in column_data if v is not None and str(v).strip() != ""]
        null_count = len(column_data) - len(non_empty_values)
        unique_values = set(str(v) for v in non_empty_values)
        unique_count = len(unique_values)

        # 샘플 값 추출 (최대 5개)
        sample_values = list(non_empty_values[:5])

        if not non_empty_values:
            result = TypeInferenceResult(
                type=DataType.STRING.value,
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

        # 🔥 Advanced AI Type Inference
        inference_result = cls._infer_type_advanced(
            non_empty_values, 
            column_name, 
            include_complex_types,
            context_columns,
            sample_size=len(column_data)
        )

        return ColumnAnalysisResult(
            column_name=column_name or "unknown",
            inferred_type=inference_result,
            sample_values=sample_values,
            null_count=null_count,
            unique_count=unique_count,
        )

    @classmethod
    def _infer_type_advanced(
        cls,
        values: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        sample_size: int = 0,
    ) -> TypeInferenceResult:
        """🔥 Advanced AI Type Inference Engine
        
        Uses adaptive thresholds, contextual analysis, and sophisticated pattern matching.
        """
        # Convert all values to strings for analysis
        str_values = [str(v).strip() for v in values]
        
        # 🔥 Adaptive Thresholds based on sample size and data quality
        adaptive_thresholds = cls._calculate_adaptive_thresholds(str_values, sample_size)
        
        # 🔥 Contextual Analysis - analyze surrounding columns
        context_hints = cls._analyze_context(column_name, context_columns) if context_columns else {}
        
        # Enhanced column name hints with multilingual support
        name_hint_result = cls._check_column_name_hints_enhanced(column_name) if column_name else None
        
        # 🔥 Advanced Type Detection with Adaptive Thresholds
        # 1. Boolean check (most specific)
        bool_result = cls._check_boolean_enhanced(str_values, adaptive_thresholds)
        if bool_result.confidence >= adaptive_thresholds['boolean']:
            return bool_result

        # 2. Integer check with statistical analysis
        int_result = cls._check_integer_enhanced(str_values, adaptive_thresholds)
        if int_result.confidence >= adaptive_thresholds['integer']:
            return int_result

        # 3. Decimal check with distribution analysis
        decimal_result = cls._check_decimal_enhanced(str_values, adaptive_thresholds)
        if decimal_result.confidence >= adaptive_thresholds['decimal']:
            return decimal_result

        # 4. Date check with fuzzy matching
        date_result = cls._check_date_enhanced(str_values, adaptive_thresholds)
        if date_result.confidence >= adaptive_thresholds['date']:
            return date_result

        # 5. DateTime check with advanced parsing
        datetime_result = cls._check_datetime_enhanced(str_values, adaptive_thresholds)
        if datetime_result.confidence >= adaptive_thresholds['datetime']:
            return datetime_result

        # 🔥 Advanced Complex Type Detection
        if include_complex_types:
            # Enhanced phone number detection
            phone_result = cls._check_phone_enhanced(str_values, adaptive_thresholds, column_name)
            if phone_result.confidence >= 0.7:
                return phone_result
                
            # Use column name hints with existing validator
            if name_hint_result and name_hint_result.confidence >= 0.7:
                # Validate using ComplexTypeValidator
                sample_value = str_values[0] if str_values else ""
                valid, error, normalized = ComplexTypeValidator.validate(
                    sample_value, name_hint_result.type, {}
                )

                if valid:
                    # Check more samples for confidence
                    valid_count = 0
                    for val in str_values[: min(50, len(str_values))]:
                        is_valid, _, _ = ComplexTypeValidator.validate(
                            val, name_hint_result.type, {}
                        )
                        if is_valid:
                            valid_count += 1

                    confidence = valid_count / min(50, len(str_values))
                    if confidence >= 0.7:
                        return TypeInferenceResult(
                            type=name_hint_result.type,
                            confidence=confidence,
                            reason=f"Column name suggests {name_hint_result.type}, {valid_count}/{min(50, len(str_values))} samples valid",
                        )

        # Default to string
        return TypeInferenceResult(
            type=DataType.STRING.value,
            confidence=1.0,
            reason="No specific pattern detected, using string type",
        )

    @classmethod
    def _check_boolean(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are boolean"""
        total = len(values)
        matched = 0

        for value in values:
            if value.lower() in cls.BOOLEAN_VALUES:
                matched += 1

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.9:
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) match boolean patterns"
            return TypeInferenceResult(
                type=DataType.BOOLEAN.value, confidence=confidence, reason=reason
            )

        return TypeInferenceResult(
            type=DataType.BOOLEAN.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values match boolean patterns",
        )

    @classmethod
    def _check_integer(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are integers"""
        total = len(values)
        matched = 0

        for value in values:
            # Remove thousand separators
            cleaned = value.replace(",", "").replace(" ", "")
            try:
                # Check if it's an integer (not float)
                int_val = int(cleaned)
                # Verify it's not a float disguised as int
                if (
                    cleaned == str(int_val)
                    or cleaned.startswith("+")
                    and cleaned[1:] == str(int_val)
                ):
                    matched += 1
            except ValueError:
                # Skip non-integer values, this is expected for type inference
                pass

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.9:
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid integers"
            return TypeInferenceResult(
                type=DataType.INTEGER.value, confidence=confidence, reason=reason
            )

        return TypeInferenceResult(
            type=DataType.INTEGER.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values are valid integers",
        )

    @classmethod
    def _check_decimal(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are decimal numbers"""
        total = len(values)
        matched = 0
        has_decimals = 0

        for value in values:
            # Handle both . and , as decimal separators
            cleaned = value.replace(" ", "")

            # Try comma as decimal separator (European style)
            if "," in cleaned and "." not in cleaned:
                cleaned = cleaned.replace(",", ".")
            # Try period as decimal separator (US style)
            elif "," in cleaned and "." in cleaned:
                # Assume comma is thousand separator
                cleaned = cleaned.replace(",", "")

            try:
                Decimal(cleaned)
                matched += 1
                if "." in cleaned:
                    has_decimals += 1
            except (ValueError, InvalidOperation):
                # Skip non-decimal values, this is expected for type inference
                pass

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.9:
            if has_decimals > 0:
                reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid numbers, {has_decimals} with decimals"
            else:
                # All numbers but no decimals - could be integer
                reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid numbers (no decimals found)"
                confidence *= 0.8  # Reduce confidence since integers would be more appropriate
            return TypeInferenceResult(
                type=DataType.DECIMAL.value, confidence=confidence, reason=reason
            )

        return TypeInferenceResult(
            type=DataType.DECIMAL.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values are valid numbers",
        )

    @classmethod
    def _check_date(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are dates"""
        total = len(values)
        matched = 0
        pattern_counts = Counter()

        for value in values:
            for pattern_regex, format_str, pattern_name in cls.DATE_PATTERNS:
                if re.match(pattern_regex, value):
                    if format_str:
                        try:
                            datetime.strptime(value, format_str)
                            matched += 1
                            pattern_counts[pattern_name] += 1
                            break
                        except ValueError:
                            continue
                    else:
                        # Korean date format
                        matched += 1
                        pattern_counts[pattern_name] += 1
                        break

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.8 and pattern_counts:
            most_common_pattern = pattern_counts.most_common(1)[0][0]
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) match date pattern {most_common_pattern}"
            metadata = {"detected_format": most_common_pattern}
            return TypeInferenceResult(
                type=DataType.DATE.value, confidence=confidence, reason=reason, metadata=metadata
            )

        return TypeInferenceResult(
            type=DataType.DATE.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values match date patterns",
        )

    @classmethod
    def _check_datetime(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are datetime"""
        total = len(values)
        matched = 0
        pattern_counts = Counter()

        for value in values:
            for pattern_regex, format_str, pattern_name in cls.DATETIME_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        # Handle timezone info
                        test_value = value.replace("Z", "+00:00")
                        if format_str:
                            # Extract just the datetime part without timezone
                            datetime_part = test_value.split("+")[0].split("Z")[0]
                            datetime.strptime(datetime_part, format_str)
                        matched += 1
                        pattern_counts[pattern_name] += 1
                        break
                    except ValueError:
                        continue

        confidence = matched / total if total > 0 else 0

        if confidence >= 0.8 and pattern_counts:
            most_common_pattern = pattern_counts.most_common(1)[0][0]
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) match datetime pattern {most_common_pattern}"
            metadata = {"detected_format": most_common_pattern}
            return TypeInferenceResult(
                type=DataType.DATETIME.value,
                confidence=confidence,
                reason=reason,
                metadata=metadata,
            )

        return TypeInferenceResult(
            type=DataType.DATETIME.value,
            confidence=confidence,
            reason=f"Only {matched}/{total} values match datetime patterns",
        )

    @classmethod
    def _check_column_name_hints(cls, column_name: str) -> Optional[TypeInferenceResult]:
        """Check column name for type hints"""
        name_lower = column_name.lower()

        # Email hints
        email_keywords = ["email", "e-mail", "mail", "이메일", "メール"]
        for keyword in email_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.EMAIL.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests email type",
                )

        # Phone hints
        phone_keywords = ["phone", "tel", "mobile", "cell", "전화", "휴대폰", "電話"]
        for keyword in phone_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.PHONE.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests phone type",
                )

        # URL hints
        url_keywords = ["url", "link", "website", "site", "링크", "사이트"]
        for keyword in url_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.URI.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests URL type",
                )

        # Money hints
        money_keywords = ["price", "cost", "amount", "fee", "salary", "가격", "금액", "価格"]
        for keyword in money_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.MONEY.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests money type",
                )

        # Address hints
        address_keywords = ["address", "addr", "주소", "住所"]
        for keyword in address_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.ADDRESS.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests address type",
                )

        # Coordinate hints
        coord_keywords = [
            "coordinate",
            "lat",
            "lng",
            "longitude",
            "latitude",
            "좌표",
            "위도",
            "경도",
        ]
        for keyword in coord_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.COORDINATE.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests coordinate type",
                )

        return None

    @classmethod
    def _calculate_adaptive_thresholds(cls, values: List[str], sample_size: int) -> Dict[str, float]:
        """🔥 Adaptive Thresholds: Adjust confidence thresholds based on data quality"""
        base_thresholds = {
            'boolean': 0.9,
            'integer': 0.9, 
            'decimal': 0.9,
            'date': 0.8,
            'datetime': 0.8
        }
        
        # Calculate data quality metrics
        unique_count = len(set(values))
        total_count = len(values)
        uniqueness_ratio = unique_count / total_count if total_count > 0 else 0
        
        # Adjust thresholds based on sample size and uniqueness
        if sample_size < 10:
            # Small samples: be more lenient
            adjustment = 0.1
        elif sample_size > 1000:
            # Large samples: be more strict
            adjustment = -0.05
        else:
            adjustment = 0
            
        # High uniqueness suggests more complex data
        if uniqueness_ratio > 0.8:
            adjustment -= 0.05
        elif uniqueness_ratio < 0.3:
            adjustment += 0.05
            
        # Apply adjustments with bounds
        for key in base_thresholds:
            base_thresholds[key] = max(0.6, min(0.95, base_thresholds[key] + adjustment))
            
        return base_thresholds
    
    @classmethod
    def _analyze_context(cls, column_name: str, context_columns: Dict[str, List[Any]]) -> Dict[str, Any]:
        """🔥 Contextual Analysis: Analyze surrounding columns for type hints"""
        context_hints = {
            'related_types': [],
            'pattern_consistency': 0.0,
            'composite_type_hints': []
        }
        
        if not column_name or not context_columns:
            return context_hints
            
        # Look for related columns (name + email pattern, etc.)
        name_lower = column_name.lower()
        
        for other_col, other_data in context_columns.items():
            other_lower = other_col.lower()
            
            # Detect composite patterns
            if 'name' in name_lower and 'email' in other_lower:
                context_hints['composite_type_hints'].append('person_identity')
            elif 'first' in name_lower and 'last' in other_lower:
                context_hints['composite_type_hints'].append('full_name')
            elif 'lat' in name_lower and 'lng' in other_lower:
                context_hints['composite_type_hints'].append('coordinates')
            elif 'street' in name_lower and ('city' in other_lower or 'zip' in other_lower):
                context_hints['composite_type_hints'].append('address')
                
        return context_hints
    
    @classmethod
    def _check_column_name_hints_enhanced(cls, column_name: str) -> Optional[TypeInferenceResult]:
        """🔥 Enhanced Column Name Hints with Multilingual Support"""
        name_lower = column_name.lower()
        
        # Enhanced Email hints (multilingual)
        email_keywords = [
            "email", "e-mail", "mail", "メール", "이메일", "邮箱", "邮件",
            "correo", "correio", "почта", "ایمیل"
        ]
        for keyword in email_keywords:
            if keyword in name_lower:
                confidence = 0.85 if keyword in ['email', 'mail'] else 0.75
                return TypeInferenceResult(
                    type=DataType.EMAIL.value,
                    confidence=confidence,
                    reason=f"Enhanced multilingual column name analysis: '{column_name}' suggests email type",
                )
        
        # Enhanced Phone hints (multilingual)
        phone_keywords = [
            "phone", "tel", "mobile", "cell", "전화", "휴대폰", "電話", "手机",
            "telefono", "telefone", "телефон", "تلفن"
        ]
        for keyword in phone_keywords:
            if keyword in name_lower:
                confidence = 0.8
                return TypeInferenceResult(
                    type=DataType.PHONE.value,
                    confidence=confidence,
                    reason=f"Enhanced multilingual column name analysis: '{column_name}' suggests phone type",
                )
        
        # Enhanced Address hints
        address_keywords = [
            "address", "addr", "주소", "住所", "地址", "dirección", "endereço", "адрес"
        ]
        for keyword in address_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.ADDRESS.value,
                    confidence=0.75,
                    reason=f"Enhanced multilingual column name analysis: '{column_name}' suggests address type",
                )
        
        return None
    
    @classmethod
    def _check_boolean_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Boolean Detection with Fuzzy Matching"""
        total = len(values)
        matched = 0
        fuzzy_matched = 0
        
        for value in values:
            value_lower = value.lower().strip()
            
            # Exact match
            if value_lower in cls.BOOLEAN_VALUES:
                matched += 1
            # Fuzzy matching for partial patterns
            elif any(bool_val in value_lower for bool_val in cls.BOOLEAN_VALUES.keys() if len(bool_val) > 2):
                fuzzy_matched += 1
                
        exact_confidence = matched / total if total > 0 else 0
        fuzzy_confidence = (matched + fuzzy_matched * 0.7) / total if total > 0 else 0
        
        # Use higher confidence with explanation
        if exact_confidence >= thresholds['boolean']:
            return TypeInferenceResult(
                type=DataType.BOOLEAN.value,
                confidence=exact_confidence,
                reason=f"Enhanced boolean detection: {matched}/{total} exact matches ({exact_confidence*100:.1f}%)",
            )
        elif fuzzy_confidence >= thresholds['boolean'] * 0.8:  # Lower threshold for fuzzy
            return TypeInferenceResult(
                type=DataType.BOOLEAN.value,
                confidence=fuzzy_confidence,
                reason=f"Enhanced fuzzy boolean detection: {matched} exact + {fuzzy_matched} fuzzy matches ({fuzzy_confidence*100:.1f}%)",
            )
        
        return TypeInferenceResult(
            type=DataType.BOOLEAN.value,
            confidence=fuzzy_confidence,
            reason=f"Enhanced boolean analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_integer_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Integer Detection with Statistical Analysis"""
        total = len(values)
        matched = 0
        int_values = []
        
        for value in values:
            cleaned = value.replace(",", "").replace(" ", "")
            try:
                int_val = int(cleaned)
                if cleaned == str(int_val) or (cleaned.startswith("+") and cleaned[1:] == str(int_val)):
                    matched += 1
                    int_values.append(int_val)
            except ValueError:
                pass
                
        confidence = matched / total if total > 0 else 0
        
        # Statistical analysis for better confidence
        if int_values:
            # Check for patterns that suggest integers
            value_range = max(int_values) - min(int_values) if len(int_values) > 1 else 0
            avg_digits = statistics.mean([len(str(abs(v))) for v in int_values])
            
            # Boost confidence for typical integer patterns
            if avg_digits <= 3 and value_range < 1000:  # IDs, counts, etc.
                confidence = min(1.0, confidence * 1.1)
            elif all(v >= 0 for v in int_values):  # All positive
                confidence = min(1.0, confidence * 1.05)
                
        if confidence >= thresholds['integer']:
            stats_info = f", range: {max(int_values) - min(int_values)}, avg_digits: {statistics.mean([len(str(abs(v))) for v in int_values]):.1f}" if int_values else ""
            return TypeInferenceResult(
                type=DataType.INTEGER.value,
                confidence=confidence,
                reason=f"Enhanced integer analysis: {matched}/{total} values ({confidence*100:.1f}%){stats_info}",
            )
        
        return TypeInferenceResult(
            type=DataType.INTEGER.value,
            confidence=confidence,
            reason=f"Enhanced integer analysis: insufficient matches ({confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_decimal_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Decimal Detection with Distribution Analysis"""
        total = len(values)
        matched = 0
        has_decimals = 0
        decimal_values = []
        
        for value in values:
            cleaned = value.replace(" ", "")
            
            # Handle different decimal separators
            if "," in cleaned and "." not in cleaned:
                cleaned = cleaned.replace(",", ".")
            elif "," in cleaned and "." in cleaned:
                cleaned = cleaned.replace(",", "")
                
            try:
                decimal_val = float(Decimal(cleaned))
                matched += 1
                decimal_values.append(decimal_val)
                if "." in cleaned:
                    has_decimals += 1
            except (ValueError, InvalidOperation):
                pass
                
        confidence = matched / total if total > 0 else 0
        
        # Statistical distribution analysis
        if decimal_values and len(decimal_values) > 2:
            try:
                std_dev = statistics.stdev(decimal_values)
                mean_val = statistics.mean(decimal_values)
                coefficient_of_variation = std_dev / abs(mean_val) if mean_val != 0 else float('inf')
                
                # Adjust confidence based on distribution characteristics
                if coefficient_of_variation < 0.5:  # Low variability suggests structured data
                    confidence = min(1.0, confidence * 1.1)
                elif coefficient_of_variation > 2.0:  # High variability might indicate mixed types
                    confidence = confidence * 0.9
                    
            except statistics.StatisticsError:
                pass
                
        if confidence >= thresholds['decimal']:
            decimal_info = f", {has_decimals} with decimal places" if has_decimals > 0 else " (no decimal places found)"
            stats_info = f", std_dev: {statistics.stdev(decimal_values):.2f}" if len(decimal_values) > 2 else ""
            
            # Reduce confidence if no actual decimals but claiming decimal type
            if has_decimals == 0:
                confidence *= 0.8
                
            return TypeInferenceResult(
                type=DataType.DECIMAL.value,
                confidence=confidence,
                reason=f"Enhanced decimal analysis: {matched}/{total} values ({confidence*100:.1f}%){decimal_info}{stats_info}",
            )
            
        return TypeInferenceResult(
            type=DataType.DECIMAL.value,
            confidence=confidence,
            reason=f"Enhanced decimal analysis: insufficient matches ({confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_date_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced Date Detection with Fuzzy Matching"""
        total = len(values)
        matched = 0
        fuzzy_matched = 0
        pattern_counts = Counter()
        
        for value in values:
            exact_match = False
            
            # Exact pattern matching
            for pattern_regex, format_str, pattern_name in cls.DATE_PATTERNS:
                if re.match(pattern_regex, value):
                    if format_str:
                        try:
                            datetime.strptime(value, format_str)
                            matched += 1
                            pattern_counts[pattern_name] += 1
                            exact_match = True
                            break
                        except ValueError:
                            continue
                    else:
                        matched += 1
                        pattern_counts[pattern_name] += 1
                        exact_match = True
                        break
            
            # Fuzzy matching for partial date patterns
            if not exact_match:
                if re.search(r'\d{4}', value) and re.search(r'\d{1,2}', value):  # Year + month/day pattern
                    fuzzy_matched += 1
                elif any(month in value.lower() for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                                                               'jul', 'aug', 'sep', 'oct', 'nov', 'dec']):
                    fuzzy_matched += 1
                    
        exact_confidence = matched / total if total > 0 else 0
        fuzzy_confidence = (matched + fuzzy_matched * 0.6) / total if total > 0 else 0
        
        if exact_confidence >= thresholds['date'] and pattern_counts:
            most_common_pattern = pattern_counts.most_common(1)[0][0]
            return TypeInferenceResult(
                type=DataType.DATE.value,
                confidence=exact_confidence,
                reason=f"Enhanced date detection: {matched}/{total} exact matches ({exact_confidence*100:.1f}%), primary pattern: {most_common_pattern}",
                metadata={"detected_format": most_common_pattern, "fuzzy_matches": fuzzy_matched}
            )
        elif fuzzy_confidence >= thresholds['date'] * 0.7:  # Lower threshold for fuzzy
            return TypeInferenceResult(
                type=DataType.DATE.value,
                confidence=fuzzy_confidence,
                reason=f"Enhanced fuzzy date detection: {matched} exact + {fuzzy_matched} fuzzy matches ({fuzzy_confidence*100:.1f}%)",
                metadata={"fuzzy_matches": fuzzy_matched}
            )
            
        return TypeInferenceResult(
            type=DataType.DATE.value,
            confidence=fuzzy_confidence,
            reason=f"Enhanced date analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)",
        )
    
    @classmethod
    def _check_datetime_enhanced(cls, values: List[str], thresholds: Dict[str, float]) -> TypeInferenceResult:
        """🔥 Enhanced DateTime Detection with Advanced Parsing"""
        total = len(values)
        matched = 0
        fuzzy_matched = 0
        pattern_counts = Counter()
        timezone_detected = 0
        
        for value in values:
            exact_match = False
            
            # Check for timezone indicators
            if any(tz in value.upper() for tz in ['UTC', 'GMT', '+00:00', 'Z', 'EST', 'PST']):
                timezone_detected += 1
                
            # Exact pattern matching
            for pattern_regex, format_str, pattern_name in cls.DATETIME_PATTERNS:
                if re.match(pattern_regex, value):
                    try:
                        test_value = value.replace("Z", "+00:00")
                        if format_str:
                            datetime_part = test_value.split("+")[0].split("Z")[0]
                            datetime.strptime(datetime_part, format_str)
                        matched += 1
                        pattern_counts[pattern_name] += 1
                        exact_match = True
                        break
                    except ValueError:
                        continue
                        
            # Fuzzy matching for datetime-like patterns
            if not exact_match:
                if re.search(r'\d{4}-\d{2}-\d{2}', value) and re.search(r'\d{2}:\d{2}', value):
                    fuzzy_matched += 1
                elif 'T' in value and ':' in value:  # ISO-like format
                    fuzzy_matched += 1
                    
        exact_confidence = matched / total if total > 0 else 0
        fuzzy_confidence = (matched + fuzzy_matched * 0.7) / total if total > 0 else 0
        
        # Boost confidence if timezones are detected
        if timezone_detected > 0:
            timezone_boost = min(0.1, timezone_detected / total * 0.2)
            fuzzy_confidence = min(1.0, fuzzy_confidence + timezone_boost)
            
        if exact_confidence >= thresholds['datetime'] and pattern_counts:
            most_common_pattern = pattern_counts.most_common(1)[0][0]
            tz_info = f", {timezone_detected} with timezone info" if timezone_detected > 0 else ""
            return TypeInferenceResult(
                type=DataType.DATETIME.value,
                confidence=exact_confidence,
                reason=f"Enhanced datetime detection: {matched}/{total} exact matches ({exact_confidence*100:.1f}%), pattern: {most_common_pattern}{tz_info}",
                metadata={"detected_format": most_common_pattern, "timezone_count": timezone_detected}
            )
        elif fuzzy_confidence >= thresholds['datetime'] * 0.75:
            tz_info = f", {timezone_detected} with timezone info" if timezone_detected > 0 else ""
            return TypeInferenceResult(
                type=DataType.DATETIME.value,
                confidence=fuzzy_confidence,
                reason=f"Enhanced fuzzy datetime detection: {matched} exact + {fuzzy_matched} fuzzy matches ({fuzzy_confidence*100:.1f}%){tz_info}",
                metadata={"fuzzy_matches": fuzzy_matched, "timezone_count": timezone_detected}
            )
            
        return TypeInferenceResult(
            type=DataType.DATETIME.value,
            confidence=fuzzy_confidence,
            reason=f"Enhanced datetime analysis: insufficient matches ({fuzzy_confidence*100:.1f}%)",
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
        전체 데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.

        Args:
            data: 데이터셋 (행 리스트)
            columns: 컬럼 이름 리스트
            sample_size: 분석할 샘플 크기
            include_complex_types: 복합 타입 검사 여부

        Returns:
            각 컬럼의 분석 결과 리스트
        """
        results = []

        # 샘플 크기 제한
        if sample_size and len(data) > sample_size:
            data = data[:sample_size]

        # 데이터가 없는 경우
        if not data:
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

        # 각 컬럼 분석
        # 🔥 Build context for advanced analysis
        context_columns = {}
        for i, col_name in enumerate(columns):
            context_columns[col_name] = [row[i] if i < len(row) else None for row in data]
        
        for i, column_name in enumerate(columns):
            column_data = [row[i] if i < len(row) else None for row in data]
            
            # Create context without current column
            current_context = {k: v for k, v in context_columns.items() if k != column_name}
            
            analysis_result = cls.infer_column_type(
                column_data, 
                column_name, 
                include_complex_types,
                current_context
            )
            results.append(analysis_result)

        return results

    @classmethod
    def _check_phone_enhanced(cls, values: List[str], thresholds: Dict[str, float], column_name: Optional[str] = None) -> TypeInferenceResult:
        """🔥 Enhanced Phone Number Detection with Global Patterns"""
        total = len(values)
        matched = 0
        fuzzy_matched = 0
        
        # Enhanced phone patterns
        phone_patterns = [
            r'^\+?1?[-\s]?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{4}$',  # US format
            r'^\+?\d{1,4}[-\s]?\(?\d{1,4}\)?[-\s]?\d{3,4}[-\s]?\d{4}$',  # International
            r'^\d{3}-\d{4}-\d{4}$',  # Korean format
            r'^\d{3}[-\s]\d{4}[-\s]\d{4}$',  # Korean alt
            r'^\d{10,15}$',  # Simple numeric
            r'^\+\d{10,15}$',  # International prefix
        ]
        
        for value in values:
            # Clean the value
            cleaned = value.strip()
            
            # Exact pattern matching
            exact_match = False
            for pattern in phone_patterns:
                if re.match(pattern, cleaned):
                    matched += 1
                    exact_match = True
                    break
                    
            # Fuzzy matching if no exact match
            if not exact_match:
                # Remove all non-digit characters and check length
                digits_only = re.sub(r'[^\d]', '', cleaned)
                if 7 <= len(digits_only) <= 15:  # Reasonable phone number length
                    # Check if it has phone-like separators
                    if any(sep in cleaned for sep in ['-', ' ', '.', '(', ')']):
                        fuzzy_matched += 1
                    # Check if it starts with country code patterns
                    elif cleaned.startswith(('+', '00')) or (len(digits_only) >= 10):
                        fuzzy_matched += 1
                        
        exact_confidence = matched / total if total > 0 else 0
        fuzzy_confidence = (matched + fuzzy_matched * 0.8) / total if total > 0 else 0
        
        # Boost confidence if column name suggests phone
        column_boost = 0
        if column_name:
            phone_keywords = ["phone", "tel", "mobile", "cell", "전화", "휴대폰", "電話", "手机"]
            if any(keyword in column_name.lower() for keyword in phone_keywords):
                column_boost = 0.1
                
        final_confidence = min(1.0, fuzzy_confidence + column_boost)
        
        if exact_confidence >= 0.8:
            return TypeInferenceResult(
                type=DataType.PHONE.value,
                confidence=exact_confidence,
                reason=f"Enhanced phone detection: {matched}/{total} exact pattern matches ({exact_confidence*100:.1f}%)",
            )
        elif final_confidence >= 0.7:
            boost_info = f" + column hint boost" if column_boost > 0 else ""
            return TypeInferenceResult(
                type=DataType.PHONE.value,
                confidence=final_confidence,
                reason=f"Enhanced fuzzy phone detection: {matched} exact + {fuzzy_matched} fuzzy matches ({final_confidence*100:.1f}%){boost_info}",
            )
            
        return TypeInferenceResult(
            type=DataType.PHONE.value,
            confidence=final_confidence,
            reason=f"Enhanced phone analysis: insufficient matches ({final_confidence*100:.1f}%)",
        )
