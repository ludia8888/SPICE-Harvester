"""
🔥 THINK ULTRA! Funnel Type Inference Service
Automatically detects data types from sample data with confidence scoring
"""

import re
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from decimal import Decimal, InvalidOperation
from collections import Counter

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# 기존 구현 재사용
from shared.models.common import DataType
from shared.validators.complex_type_validator import ComplexTypeValidator
from funnel.models import TypeInferenceResult, ColumnAnalysisResult


class FunnelTypeInferenceService:
    """
    🔥 THINK ULTRA! Funnel 서비스의 Type Inference
    
    Data Connector에서 수집된 데이터를 분석하여 타입을 자동으로 추론합니다.
    기존 shared의 validator와 완벽한 정합성을 유지합니다.
    
    Architecture:
    Data Connector → Funnel (Type Inference) → OMS/BFF
    """
    
    # Date patterns to check
    DATE_PATTERNS = [
        # ISO formats
        (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d', 'YYYY-MM-DD'),
        (r'^\d{4}/\d{2}/\d{2}$', '%Y/%m/%d', 'YYYY/MM/DD'),
        # US formats
        (r'^\d{2}/\d{2}/\d{4}$', '%m/%d/%Y', 'MM/DD/YYYY'),
        (r'^\d{2}-\d{2}-\d{4}$', '%m-%d-%Y', 'MM-DD-YYYY'),
        # European formats
        (r'^\d{2}/\d{2}/\d{4}$', '%d/%m/%Y', 'DD/MM/YYYY'),
        (r'^\d{2}\.\d{2}\.\d{4}$', '%d.%m.%Y', 'DD.MM.YYYY'),
        # Korean format
        (r'^\d{4}년\s*\d{1,2}월\s*\d{1,2}일$', None, 'YYYY년 MM월 DD일'),
    ]
    
    # DateTime patterns
    DATETIME_PATTERNS = [
        # ISO format with time
        (r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}', '%Y-%m-%dT%H:%M:%S', 'ISO DateTime'),
        (r'^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', '%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH:MM:SS'),
    ]
    
    # Boolean values
    BOOLEAN_VALUES = {
        'true': True, 'false': False,
        'yes': True, 'no': False,
        'y': True, 'n': False,
        '1': True, '0': False,
        'on': True, 'off': False,
        '참': True, '거짓': False,
        '예': True, '아니오': False,
    }
    
    @classmethod
    def infer_column_type(
        cls, 
        column_data: List[Any], 
        column_name: Optional[str] = None,
        include_complex_types: bool = False
    ) -> ColumnAnalysisResult:
        """
        컬럼 데이터를 분석하여 타입을 추론합니다.
        
        Args:
            column_data: 컬럼의 샘플 데이터
            column_name: 컬럼 이름 (타입 힌트용)
            include_complex_types: 복합 타입 검사 여부
            
        Returns:
            ColumnAnalysisResult with complete analysis
        """
        # 통계 정보 수집
        non_empty_values = [v for v in column_data if v is not None and str(v).strip() != '']
        null_count = len(column_data) - len(non_empty_values)
        unique_values = set(str(v) for v in non_empty_values)
        unique_count = len(unique_values)
        
        # 샘플 값 추출 (최대 5개)
        sample_values = list(non_empty_values[:5])
        
        if not non_empty_values:
            result = TypeInferenceResult(
                type=DataType.STRING.value,
                confidence=1.0,
                reason="All values are empty, defaulting to string type"
            )
            return ColumnAnalysisResult(
                column_name=column_name or "unknown",
                inferred_type=result,
                sample_values=[],
                null_count=null_count,
                unique_count=0
            )
        
        # 타입 추론
        inference_result = cls._infer_type(non_empty_values, column_name, include_complex_types)
        
        return ColumnAnalysisResult(
            column_name=column_name or "unknown",
            inferred_type=inference_result,
            sample_values=sample_values,
            null_count=null_count,
            unique_count=unique_count
        )
    
    @classmethod
    def _infer_type(
        cls, 
        values: List[Any], 
        column_name: Optional[str] = None,
        include_complex_types: bool = False
    ) -> TypeInferenceResult:
        """실제 타입 추론 로직"""
        # Convert all values to strings for analysis
        str_values = [str(v).strip() for v in values]
        
        # Check for column name hints
        name_hint_result = cls._check_column_name_hints(column_name) if column_name else None
        
        # MVP: Basic type detection
        # 1. Boolean check (most specific)
        bool_result = cls._check_boolean(str_values)
        if bool_result.confidence >= 0.9:
            return bool_result
        
        # 2. Integer check
        int_result = cls._check_integer(str_values)
        if int_result.confidence >= 0.9:
            return int_result
        
        # 3. Decimal check
        decimal_result = cls._check_decimal(str_values)
        if decimal_result.confidence >= 0.9:
            return decimal_result
        
        # 4. Date check
        date_result = cls._check_date(str_values)
        if date_result.confidence >= 0.8:
            return date_result
        
        # 5. DateTime check
        datetime_result = cls._check_datetime(str_values)
        if datetime_result.confidence >= 0.8:
            return datetime_result
        
        # Complex type checks (if enabled)
        if include_complex_types:
            # Use column name hints with existing validator
            if name_hint_result and name_hint_result.confidence >= 0.7:
                # Validate using ComplexTypeValidator
                sample_value = str_values[0] if str_values else ""
                valid, error, normalized = ComplexTypeValidator.validate(
                    sample_value, 
                    name_hint_result.type, 
                    {}
                )
                
                if valid:
                    # Check more samples for confidence
                    valid_count = 0
                    for val in str_values[:min(50, len(str_values))]:
                        is_valid, _, _ = ComplexTypeValidator.validate(val, name_hint_result.type, {})
                        if is_valid:
                            valid_count += 1
                    
                    confidence = valid_count / min(50, len(str_values))
                    if confidence >= 0.7:
                        return TypeInferenceResult(
                            type=name_hint_result.type,
                            confidence=confidence,
                            reason=f"Column name suggests {name_hint_result.type}, {valid_count}/{min(50, len(str_values))} samples valid"
                        )
        
        # Default to string
        return TypeInferenceResult(
            type=DataType.STRING.value,
            confidence=1.0,
            reason="No specific pattern detected, using string type"
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
            return TypeInferenceResult(type=DataType.BOOLEAN.value, confidence=confidence, reason=reason)
        
        return TypeInferenceResult(
            type=DataType.BOOLEAN.value, 
            confidence=confidence, 
            reason=f"Only {matched}/{total} values match boolean patterns"
        )
    
    @classmethod
    def _check_integer(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are integers"""
        total = len(values)
        matched = 0
        
        for value in values:
            # Remove thousand separators
            cleaned = value.replace(',', '').replace(' ', '')
            try:
                # Check if it's an integer (not float)
                int_val = int(cleaned)
                # Verify it's not a float disguised as int
                if cleaned == str(int_val) or cleaned.startswith('+') and cleaned[1:] == str(int_val):
                    matched += 1
            except ValueError:
                pass
        
        confidence = matched / total if total > 0 else 0
        
        if confidence >= 0.9:
            reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid integers"
            return TypeInferenceResult(type=DataType.INTEGER.value, confidence=confidence, reason=reason)
        
        return TypeInferenceResult(
            type=DataType.INTEGER.value, 
            confidence=confidence, 
            reason=f"Only {matched}/{total} values are valid integers"
        )
    
    @classmethod
    def _check_decimal(cls, values: List[str]) -> TypeInferenceResult:
        """Check if values are decimal numbers"""
        total = len(values)
        matched = 0
        has_decimals = 0
        
        for value in values:
            # Handle both . and , as decimal separators
            cleaned = value.replace(' ', '')
            
            # Try comma as decimal separator (European style)
            if ',' in cleaned and '.' not in cleaned:
                cleaned = cleaned.replace(',', '.')
            # Try period as decimal separator (US style)
            elif ',' in cleaned and '.' in cleaned:
                # Assume comma is thousand separator
                cleaned = cleaned.replace(',', '')
            
            try:
                decimal_val = Decimal(cleaned)
                matched += 1
                if '.' in cleaned:
                    has_decimals += 1
            except (ValueError, InvalidOperation):
                pass
        
        confidence = matched / total if total > 0 else 0
        
        if confidence >= 0.9:
            if has_decimals > 0:
                reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid numbers, {has_decimals} with decimals"
            else:
                # All numbers but no decimals - could be integer
                reason = f"{matched}/{total} values ({confidence*100:.0f}%) are valid numbers (no decimals found)"
                confidence *= 0.8  # Reduce confidence since integers would be more appropriate
            return TypeInferenceResult(type=DataType.DECIMAL.value, confidence=confidence, reason=reason)
        
        return TypeInferenceResult(
            type=DataType.DECIMAL.value, 
            confidence=confidence, 
            reason=f"Only {matched}/{total} values are valid numbers"
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
                type=DataType.DATE.value, 
                confidence=confidence, 
                reason=reason, 
                metadata=metadata
            )
        
        return TypeInferenceResult(
            type=DataType.DATE.value, 
            confidence=confidence, 
            reason=f"Only {matched}/{total} values match date patterns"
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
                        test_value = value.replace('Z', '+00:00')
                        if format_str:
                            # Extract just the datetime part without timezone
                            datetime_part = test_value.split('+')[0].split('Z')[0]
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
                metadata=metadata
            )
        
        return TypeInferenceResult(
            type=DataType.DATETIME.value, 
            confidence=confidence, 
            reason=f"Only {matched}/{total} values match datetime patterns"
        )
    
    @classmethod
    def _check_column_name_hints(cls, column_name: str) -> Optional[TypeInferenceResult]:
        """Check column name for type hints"""
        name_lower = column_name.lower()
        
        # Email hints
        email_keywords = ['email', 'e-mail', 'mail', '이메일', 'メール']
        for keyword in email_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.EMAIL.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests email type"
                )
        
        # Phone hints
        phone_keywords = ['phone', 'tel', 'mobile', 'cell', '전화', '휴대폰', '電話']
        for keyword in phone_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.PHONE.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests phone type"
                )
        
        # URL hints
        url_keywords = ['url', 'link', 'website', 'site', '링크', '사이트']
        for keyword in url_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.URI.value,
                    confidence=0.8,
                    reason=f"Column name '{column_name}' suggests URL type"
                )
        
        # Money hints
        money_keywords = ['price', 'cost', 'amount', 'fee', 'salary', '가격', '금액', '価格']
        for keyword in money_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.MONEY.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests money type"
                )
        
        # Address hints
        address_keywords = ['address', 'addr', '주소', '住所']
        for keyword in address_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.ADDRESS.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests address type"
                )
        
        # Coordinate hints
        coord_keywords = ['coordinate', 'lat', 'lng', 'longitude', 'latitude', '좌표', '위도', '경도']
        for keyword in coord_keywords:
            if keyword in name_lower:
                return TypeInferenceResult(
                    type=DataType.COORDINATE.value,
                    confidence=0.7,
                    reason=f"Column name '{column_name}' suggests coordinate type"
                )
        
        return None
    
    @classmethod
    def analyze_dataset(
        cls, 
        data: List[List[Any]], 
        columns: List[str],
        sample_size: Optional[int] = 1000,
        include_complex_types: bool = False
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
                    reason="No data available, defaulting to string type"
                )
                results.append(ColumnAnalysisResult(
                    column_name=col,
                    inferred_type=result,
                    sample_values=[],
                    null_count=0,
                    unique_count=0
                ))
            return results
        
        # 각 컬럼 분석
        for i, column_name in enumerate(columns):
            column_data = [row[i] if i < len(row) else None for row in data]
            analysis_result = cls.infer_column_type(column_data, column_name, include_complex_types)
            results.append(analysis_result)
        
        return results