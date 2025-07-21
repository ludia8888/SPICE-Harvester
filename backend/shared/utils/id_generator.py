"""
ID Generator 유틸리티
다국어 레이블로부터 안전하고 고유한 ID를 생성합니다.
"""

import re
import time
import unicodedata
from datetime import datetime
from typing import Any, Dict, Optional, Union

import logging

logger = logging.getLogger(__name__)


class IDGenerationError(Exception):
    """ID 생성 관련 예외"""
    pass


def _normalize_korean_to_roman(text: str) -> str:
    """한국어를 로마자로 변환 (간단한 매핑)"""
    # 기본적인 한글 자모 -> 로마자 매핑
    korean_to_roman = {
        'ㄱ': 'g', 'ㄴ': 'n', 'ㄷ': 'd', 'ㄹ': 'r', 'ㅁ': 'm',
        'ㅂ': 'b', 'ㅅ': 's', 'ㅇ': '', 'ㅈ': 'j', 'ㅊ': 'ch',
        'ㅋ': 'k', 'ㅌ': 't', 'ㅍ': 'p', 'ㅎ': 'h',
        'ㅏ': 'a', 'ㅑ': 'ya', 'ㅓ': 'eo', 'ㅕ': 'yeo', 'ㅗ': 'o',
        'ㅛ': 'yo', 'ㅜ': 'u', 'ㅠ': 'yu', 'ㅡ': 'eu', 'ㅣ': 'i',
        '가': 'ga', '나': 'na', '다': 'da', '라': 'ra', '마': 'ma',
        '바': 'ba', '사': 'sa', '아': 'a', '자': 'ja', '차': 'cha',
        '카': 'ka', '타': 'ta', '파': 'pa', '하': 'ha',
        '제품': 'product', '간단한': 'simple', '테스트': 'test',
        '관리': 'management', '시스템': 'system', '서비스': 'service'
    }
    
    result = text
    for korean, roman in korean_to_roman.items():
        result = result.replace(korean, roman)
    
    # 남은 한글은 제거하거나 기본 변환
    result = re.sub(r'[가-힣]', '', result)
    
    return result


def _extract_text_from_label(label: Union[str, Dict[str, Any], Any]) -> tuple[str, bool]:
    """
    레이블에서 텍스트 추출
    
    Returns:
        tuple: (추출된 텍스트, 한국어 포함 여부)
    """
    has_korean = False
    
    if isinstance(label, str):
        has_korean = bool(re.search(r'[가-힣]', label))
        return label, has_korean
    
    elif isinstance(label, dict):
        # MultiLingualText 형식 처리
        if 'en' in label and label['en']:
            # 영어가 있으면 영어 우선
            text = label['en']
        elif 'ko' in label and label['ko']:
            # 한국어만 있으면 한국어 사용
            text = label['ko']
            has_korean = True
        else:
            # 첫 번째 사용 가능한 값 사용
            for key, value in label.items():
                if value:
                    text = value
                    has_korean = bool(re.search(r'[가-힣]', text))
                    break
            else:
                text = ""
        
        return text, has_korean
    
    elif hasattr(label, 'get_value'):
        # MultiLingualText 객체 처리
        text = label.get_value('en') or label.get_value('ko') or ""
        has_korean = bool(re.search(r'[가-힣]', text))
        return text, has_korean
    
    else:
        # 기타 타입은 문자열로 변환
        text = str(label) if label else ""
        has_korean = bool(re.search(r'[가-힣]', text))
        return text, has_korean


def _clean_and_format_id(text: str, preserve_camel_case: bool = False) -> str:
    """텍스트를 ID 형식으로 정리"""
    if not text:
        return ""
    
    # 1. Unicode 정규화
    text = unicodedata.normalize('NFKD', text)
    
    # 2. 특수문자 제거 (알파벳, 숫자, 공백만 유지)
    text = re.sub(r'[^\w\s]', '', text)
    
    if preserve_camel_case:
        # CamelCase 보존: 공백만 제거
        text = ''.join(word.capitalize() for word in text.split())
    else:
        # 일반적인 정리: 소문자 + 첫 글자 대문자
        text = ''.join(word.capitalize() for word in text.split())
    
    # 3. 숫자로 시작하는 경우 접두어 추가
    if text and text[0].isdigit():
        text = f"Class{text}"
    
    # 4. 길이 제한 (너무 긴 ID 방지)
    if len(text) > 50:
        text = text[:50]
    
    return text


def _generate_timestamp() -> str:
    """고유성을 위한 타임스탬프 생성"""
    return datetime.now().strftime("%Y%m%d%H%M%S")


def _generate_short_timestamp() -> str:
    """짧은 타임스탬프 생성"""
    return datetime.now().strftime("%m%d%H%M")


def generate_ontology_id(
    label: Union[str, Dict[str, Any], Any],
    preserve_camel_case: bool = True,
    handle_korean: bool = True,
    default_fallback: str = "UnnamedClass"
) -> str:
    """
    온톨로지 ID 생성 (고급 옵션)
    
    Args:
        label: 레이블 (문자열, 딕셔너리, 또는 MultiLingualText 객체)
        preserve_camel_case: CamelCase 형식 보존 여부
        handle_korean: 한국어 특수 처리 여부
        default_fallback: 기본 폴백 값
        
    Returns:
        생성된 ID
        
    Examples:
        >>> generate_ontology_id({"en": "Simple Product", "ko": "간단한 제품"})
        'SimpleProduct'
        >>> generate_ontology_id({"ko": "제품 관리"}, handle_korean=True)
        'ProductManagement'
    """
    try:
        logger.debug(f"Generating ontology ID for label: {label}")
        
        # 1. 텍스트 추출
        text, has_korean = _extract_text_from_label(label)
        
        if not text:
            logger.warning(f"No valid text found in label: {label}, using fallback")
            return default_fallback
        
        # 2. 한국어 처리
        if has_korean and handle_korean:
            text = _normalize_korean_to_roman(text)
            logger.debug(f"Korean text normalized: {text}")
        
        # 3. ID 형식으로 정리
        cleaned_id = _clean_and_format_id(text, preserve_camel_case)
        
        if not cleaned_id:
            logger.warning(f"Failed to generate valid ID from text: {text}, using fallback")
            return default_fallback
        
        logger.debug(f"Generated ontology ID: {cleaned_id}")
        return cleaned_id
        
    except Exception as e:
        logger.error(f"Error generating ontology ID for label {label}: {e}")
        return default_fallback


def generate_simple_id(
    label: Union[str, Dict[str, Any], Any],
    use_timestamp_for_korean: bool = True,
    default_fallback: str = "UnnamedClass"
) -> str:
    """
    간단한 ID 생성 (타임스탬프 포함)
    
    Args:
        label: 레이블 (문자열, 딕셔너리, 또는 MultiLingualText 객체)
        use_timestamp_for_korean: 한국어의 경우 타임스탬프 추가 여부
        default_fallback: 기본 폴백 값
        
    Returns:
        생성된 ID
        
    Examples:
        >>> generate_simple_id({"en": "Simple Product"})
        'SimpleProduct'
        >>> generate_simple_id({"ko": "간단한 제품"}, use_timestamp_for_korean=True)
        'SimpleProduct20250721123456'
    """
    try:
        logger.debug(f"Generating simple ID for label: {label}")
        
        # 1. 텍스트 추출
        text, has_korean = _extract_text_from_label(label)
        
        if not text:
            logger.warning(f"No valid text found in label: {label}, using fallback")
            if use_timestamp_for_korean:
                return f"{default_fallback}{_generate_short_timestamp()}"
            return default_fallback
        
        # 2. 한국어 처리
        if has_korean:
            text = _normalize_korean_to_roman(text)
            logger.debug(f"Korean text normalized: {text}")
        
        # 3. ID 형식으로 정리
        cleaned_id = _clean_and_format_id(text, preserve_camel_case=True)
        
        if not cleaned_id:
            logger.warning(f"Failed to generate valid ID from text: {text}, using fallback")
            cleaned_id = default_fallback
        
        # 4. 한국어의 경우 타임스탬프 추가로 고유성 보장
        if has_korean and use_timestamp_for_korean:
            timestamp = _generate_timestamp()
            cleaned_id = f"{cleaned_id}{timestamp}"
            logger.debug(f"Added timestamp for Korean text: {cleaned_id}")
        
        logger.debug(f"Generated simple ID: {cleaned_id}")
        return cleaned_id
        
    except Exception as e:
        logger.error(f"Error generating simple ID for label {label}: {e}")
        if use_timestamp_for_korean:
            return f"{default_fallback}{_generate_short_timestamp()}"
        return default_fallback


def generate_unique_id(
    label: Union[str, Dict[str, Any], Any],
    prefix: str = "",
    suffix: str = "",
    max_length: int = 64,
    force_unique: bool = True
) -> str:
    """
    고유 ID 생성 (확장 가능한 버전)
    
    Args:
        label: 레이블
        prefix: 접두어
        suffix: 접미어
        max_length: 최대 길이
        force_unique: 강제 고유성 (타임스탬프 추가)
        
    Returns:
        생성된 고유 ID
    """
    try:
        # 기본 ID 생성
        base_id = generate_ontology_id(label)
        
        # 접두어/접미어 추가
        full_id = f"{prefix}{base_id}{suffix}"
        
        # 강제 고유성
        if force_unique:
            timestamp = _generate_short_timestamp()
            full_id = f"{full_id}{timestamp}"
        
        # 길이 제한
        if len(full_id) > max_length:
            # 타임스탬프 보존하며 중간 부분 축소
            if force_unique:
                prefix_suffix_len = len(prefix) + len(suffix) + len(timestamp)
                max_base_len = max_length - prefix_suffix_len
                if max_base_len > 0:
                    base_id = base_id[:max_base_len]
                    full_id = f"{prefix}{base_id}{suffix}{timestamp}"
                else:
                    full_id = full_id[:max_length]
            else:
                full_id = full_id[:max_length]
        
        return full_id
        
    except Exception as e:
        logger.error(f"Error generating unique ID: {e}")
        return f"GeneratedClass{_generate_short_timestamp()}"


# 편의 함수들
def generate_class_id(label: Union[str, Dict[str, Any], Any]) -> str:
    """클래스 ID 생성"""
    return generate_ontology_id(label)


def generate_property_id(label: Union[str, Dict[str, Any], Any]) -> str:
    """속성 ID 생성"""
    return generate_simple_id(label, use_timestamp_for_korean=False)


def generate_relationship_id(label: Union[str, Dict[str, Any], Any]) -> str:
    """관계 ID 생성"""
    return generate_simple_id(label, use_timestamp_for_korean=False)


# 검증 함수
def validate_generated_id(id_string: str) -> bool:
    """생성된 ID의 유효성 검증"""
    if not id_string:
        return False
    
    # 1. 첫 글자는 문자여야 함
    if not id_string[0].isalpha():
        return False
    
    # 2. 알파벳과 숫자만 포함
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9]*$', id_string):
        return False
    
    # 3. 길이 제한
    if len(id_string) > 64:
        return False
    
    return True


if __name__ == "__main__":
    # 테스트
    test_cases = [
        {"en": "Simple Product", "ko": "간단한 제품"},
        {"ko": "제품 관리 시스템"},
        {"en": "User Management"},
        "Simple String",
        "",
        None,
        {"en": "Test-Class@123!"},
    ]
    
    print("=== ID Generator Tests ===")
    for i, test_case in enumerate(test_cases):
        print(f"\nTest {i+1}: {test_case}")
        
        ontology_id = generate_ontology_id(test_case)
        simple_id = generate_simple_id(test_case)
        unique_id = generate_unique_id(test_case)
        
        print(f"  Ontology ID: {ontology_id}")
        print(f"  Simple ID: {simple_id}")
        print(f"  Unique ID: {unique_id}")
        print(f"  Valid: {validate_generated_id(ontology_id)}")