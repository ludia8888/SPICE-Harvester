#!/usr/bin/env python3
"""
🔥 ULTRA! 속성/관계 제약조건 및 기본값 추출 로직

TerminusDB 스키마에서 제약조건과 기본값을 추출하고 검증하는 유틸리티
"""

import json
import logging
from enum import Enum
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class ConstraintType(Enum):
    """제약조건 타입들"""
    
    # 값 범위 제약조건
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    
    # 문자열 제약조건
    MIN_LENGTH = "min_length" 
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    FORMAT = "format"
    
    # 배열/컬렉션 제약조건
    MIN_ITEMS = "min_items"
    MAX_ITEMS = "max_items"
    UNIQUE_ITEMS = "unique_items"
    
    # 타입 제약조건
    ENUM_VALUES = "enum_values"
    DATA_TYPE = "data_type"
    
    # 관계 제약조건
    MIN_CARDINALITY = "min_cardinality"
    MAX_CARDINALITY = "max_cardinality"
    TARGET_TYPES = "target_types"
    
    # 필수/선택 제약조건
    REQUIRED = "required"
    NULLABLE = "nullable"
    
    # 고유성 제약조건
    UNIQUE = "unique"
    PRIMARY_KEY = "primary_key"


class DefaultValueType(Enum):
    """기본값 타입들"""
    
    STATIC = "static"          # 고정값
    COMPUTED = "computed"      # 계산값 (함수)
    TIMESTAMP = "timestamp"    # 현재 시간
    UUID = "uuid"             # UUID 생성
    SEQUENCE = "sequence"     # 시퀀스 번호
    REFERENCE = "reference"   # 다른 필드 참조
    

class ConstraintExtractor:
    """제약조건 및 기본값 추출기"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def extract_property_constraints(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """속성에서 제약조건 추출"""
        constraints = {}
        
        # 기본 필드에서 제약조건 추출
        if property_data.get("required"):
            constraints[ConstraintType.REQUIRED.value] = True
        
        if property_data.get("nullable") is not None:
            constraints[ConstraintType.NULLABLE.value] = property_data["nullable"]
        
        if property_data.get("unique"):
            constraints[ConstraintType.UNIQUE.value] = True
        
        # constraints 필드에서 상세 제약조건 추출
        prop_constraints = property_data.get("constraints", {})
        
        # 🔥 ULTRA! 값 범위 제약조건 (0도 유효한 값이므로 None 체크 필요)
        if "min" in prop_constraints:
            constraints[ConstraintType.MIN_VALUE.value] = prop_constraints["min"]
        elif "minimum" in prop_constraints:
            constraints[ConstraintType.MIN_VALUE.value] = prop_constraints["minimum"]
        
        if "max" in prop_constraints:
            constraints[ConstraintType.MAX_VALUE.value] = prop_constraints["max"]
        elif "maximum" in prop_constraints:
            constraints[ConstraintType.MAX_VALUE.value] = prop_constraints["maximum"]
        
        # 🔥 ULTRA! 문자열 제약조건 (0도 유효한 값이므로 None 체크 필요)
        if "minLength" in prop_constraints:
            constraints[ConstraintType.MIN_LENGTH.value] = prop_constraints["minLength"]
        elif "min_length" in prop_constraints:
            constraints[ConstraintType.MIN_LENGTH.value] = prop_constraints["min_length"]
        
        if "maxLength" in prop_constraints:
            constraints[ConstraintType.MAX_LENGTH.value] = prop_constraints["maxLength"] 
        elif "max_length" in prop_constraints:
            constraints[ConstraintType.MAX_LENGTH.value] = prop_constraints["max_length"]
        
        if "pattern" in prop_constraints:
            constraints[ConstraintType.PATTERN.value] = prop_constraints["pattern"]
        
        if "format" in prop_constraints:
            constraints[ConstraintType.FORMAT.value] = prop_constraints["format"]
        
        # 🔥 ULTRA! 배열/컬렉션 제약조건 (0도 유효한 값이므로 None 체크 필요)
        if "minItems" in prop_constraints:
            constraints[ConstraintType.MIN_ITEMS.value] = prop_constraints["minItems"]
        elif "min_items" in prop_constraints:
            constraints[ConstraintType.MIN_ITEMS.value] = prop_constraints["min_items"]
        
        if "maxItems" in prop_constraints:
            constraints[ConstraintType.MAX_ITEMS.value] = prop_constraints["maxItems"]
        elif "max_items" in prop_constraints:
            constraints[ConstraintType.MAX_ITEMS.value] = prop_constraints["max_items"]
        
        if "uniqueItems" in prop_constraints:
            constraints[ConstraintType.UNIQUE_ITEMS.value] = prop_constraints["uniqueItems"]
        elif "unique_items" in prop_constraints:
            constraints[ConstraintType.UNIQUE_ITEMS.value] = prop_constraints["unique_items"]
        
        # 🔥 ULTRA! 타입 제약조건
        if "enum" in prop_constraints:
            constraints[ConstraintType.ENUM_VALUES.value] = prop_constraints["enum"]
        elif "enum_values" in prop_constraints:
            constraints[ConstraintType.ENUM_VALUES.value] = prop_constraints["enum_values"]
        
        if "type" in property_data:
            constraints[ConstraintType.DATA_TYPE.value] = property_data["type"]
        
        self.logger.info(f"✅ Extracted {len(constraints)} constraints for property '{property_data.get('name', 'unknown')}'")
        return constraints
    
    def extract_relationship_constraints(self, relationship_data: Dict[str, Any]) -> Dict[str, Any]:
        """관계에서 제약조건 추출"""
        constraints = {}
        
        # 기본 필드에서 제약조건 추출
        if relationship_data.get("required"):
            constraints[ConstraintType.REQUIRED.value] = True
        
        # constraints 필드에서 상세 제약조건 추출
        rel_constraints = relationship_data.get("constraints", {})
        
        # 🔥 ULTRA! 관계 카디널리티 제약조건
        if "min_cardinality" in rel_constraints:
            constraints[ConstraintType.MIN_CARDINALITY.value] = rel_constraints["min_cardinality"]
        
        if "max_cardinality" in rel_constraints:
            constraints[ConstraintType.MAX_CARDINALITY.value] = rel_constraints["max_cardinality"]
        
        # 🔥 ULTRA! 다중 타겟 타입 (Union relationships)
        if "target_types" in rel_constraints:
            constraints[ConstraintType.TARGET_TYPES.value] = rel_constraints["target_types"]
        
        # 카디널리티 정보도 제약조건으로 포함
        if "cardinality" in relationship_data:
            cardinality = relationship_data["cardinality"]
            
            # 카디널리티에서 암시적 제약조건 추출
            if cardinality in ["1:1", "n:1"]:
                # 단일 관계 - 최대 1개
                constraints[ConstraintType.MAX_CARDINALITY.value] = 1
            elif cardinality in ["1:n", "n:m", "many"]:
                # 다중 관계 - 최소 제한 없음 (기본적으로)
                pass
            elif cardinality == "required":
                # 필수 관계 - 최소 1개
                constraints[ConstraintType.MIN_CARDINALITY.value] = 1
        
        self.logger.info(f"✅ Extracted {len(constraints)} constraints for relationship '{relationship_data.get('predicate', 'unknown')}'")
        return constraints
    
    def extract_default_value(self, field_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """필드에서 기본값 정보 추출"""
        if "default" not in field_data and "defaultValue" not in field_data:
            return None
        
        default_value = field_data.get("default") or field_data.get("defaultValue")
        
        # 🔥 ULTRA! 기본값 타입 분석
        default_info = {
            "value": default_value,
            "type": DefaultValueType.STATIC.value
        }
        
        # 문자열 기본값에서 특수 패턴 탐지
        if isinstance(default_value, str):
            if default_value.lower() in ["now()", "current_timestamp", "current_date"]:
                default_info["type"] = DefaultValueType.TIMESTAMP.value
            elif default_value.lower() in ["uuid()", "generate_uuid"]:
                default_info["type"] = DefaultValueType.UUID.value
            elif default_value.lower().startswith("sequence("):
                default_info["type"] = DefaultValueType.SEQUENCE.value
            elif default_value.startswith("{{") and default_value.endswith("}}"):
                # 필드 참조 패턴: {{other_field}}
                default_info["type"] = DefaultValueType.REFERENCE.value
                default_info["reference_field"] = default_value[2:-2]
            elif default_value.startswith("function:"):
                # 계산 함수 패턴: function:calculate_total
                default_info["type"] = DefaultValueType.COMPUTED.value
                default_info["function"] = default_value[9:]  # "function:" 제거
        
        self.logger.info(f"✅ Extracted default value for '{field_data.get('name', 'unknown')}': {default_info}")
        return default_info
    
    def validate_constraint_compatibility(self, constraints: Dict[str, Any], field_type: str) -> List[str]:
        """제약조건과 필드 타입의 호환성 검증"""
        warnings = []
        
        # 🔥 ULTRA! 타입별 제약조건 호환성 체크
        numeric_types = ["integer", "int", "decimal", "float", "double", "number"]
        string_types = ["string", "text"]
        collection_types = ["list", "set", "array"]
        
        # 숫자 타입 검증
        if field_type.lower() in numeric_types:
            if ConstraintType.MIN_LENGTH.value in constraints:
                warnings.append(f"min_length constraint is not applicable to numeric type '{field_type}'")
            if ConstraintType.MAX_LENGTH.value in constraints:
                warnings.append(f"max_length constraint is not applicable to numeric type '{field_type}'")
            if ConstraintType.PATTERN.value in constraints:
                warnings.append(f"pattern constraint is not applicable to numeric type '{field_type}'")
        
        # 문자열 타입 검증
        elif field_type.lower() in string_types:
            if ConstraintType.MIN_VALUE.value in constraints:
                warnings.append(f"min_value constraint is not applicable to string type '{field_type}'")
            if ConstraintType.MAX_VALUE.value in constraints:
                warnings.append(f"max_value constraint is not applicable to string type '{field_type}'")
        
        # 컬렉션 타입 검증
        if any(field_type.lower().startswith(ct) for ct in collection_types):
            # 컬렉션 타입에는 min/max_items가 적용 가능
            pass
        else:
            # 단일 값 타입에는 items 제약조건이 적용되지 않음
            if ConstraintType.MIN_ITEMS.value in constraints:
                warnings.append(f"min_items constraint is not applicable to non-collection type '{field_type}'")
            if ConstraintType.MAX_ITEMS.value in constraints:
                warnings.append(f"max_items constraint is not applicable to non-collection type '{field_type}'")
        
        # 값 범위 검증
        min_val = constraints.get(ConstraintType.MIN_VALUE.value)
        max_val = constraints.get(ConstraintType.MAX_VALUE.value)
        if min_val is not None and max_val is not None:
            try:
                if float(min_val) > float(max_val):
                    warnings.append(f"min_value ({min_val}) cannot be greater than max_value ({max_val})")
            except (ValueError, TypeError):
                warnings.append(f"Invalid numeric values for min_value ({min_val}) or max_value ({max_val})")
        
        # 길이 범위 검증
        min_len = constraints.get(ConstraintType.MIN_LENGTH.value)
        max_len = constraints.get(ConstraintType.MAX_LENGTH.value)
        if min_len is not None and max_len is not None:
            try:
                if int(min_len) > int(max_len):
                    warnings.append(f"min_length ({min_len}) cannot be greater than max_length ({max_len})")
                if int(min_len) < 0:
                    warnings.append(f"min_length ({min_len}) cannot be negative")
            except (ValueError, TypeError):
                warnings.append(f"Invalid length values for min_length ({min_len}) or max_length ({max_len})")
        
        if warnings:
            self.logger.warning(f"⚠️ Found {len(warnings)} constraint compatibility issues for type '{field_type}'")
        
        return warnings
    
    def extract_all_constraints(self, class_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """클래스 데이터에서 모든 제약조건 추출"""
        all_constraints = {}
        
        # 🔥 ULTRA! 속성 제약조건 추출
        properties = class_data.get("properties", [])
        for prop in properties:
            prop_name = prop.get("name")
            if prop_name:
                constraints = self.extract_property_constraints(prop)
                default_value = self.extract_default_value(prop)
                
                all_constraints[prop_name] = {
                    "type": "property",
                    "field_type": prop.get("type", "unknown"),
                    "constraints": constraints,
                    "default_value": default_value,
                    "validation_warnings": self.validate_constraint_compatibility(
                        constraints, prop.get("type", "unknown")
                    )
                }
        
        # 🔥 ULTRA! 관계 제약조건 추출
        relationships = class_data.get("relationships", [])
        for rel in relationships:
            rel_predicate = rel.get("predicate")
            if rel_predicate:
                constraints = self.extract_relationship_constraints(rel)
                
                all_constraints[rel_predicate] = {
                    "type": "relationship", 
                    "target_class": rel.get("target"),
                    "cardinality": rel.get("cardinality"),
                    "constraints": constraints,
                    "default_value": self.extract_default_value(rel),
                    "validation_warnings": []  # 관계는 타입 호환성 검사 생략
                }
        
        self.logger.info(f"✅ Extracted constraints for {len(all_constraints)} fields from class '{class_data.get('id', 'unknown')}'")
        return all_constraints
    
    def generate_constraint_summary(self, all_constraints: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """제약조건 요약 생성"""
        summary = {
            "total_fields": len(all_constraints),
            "properties": 0,
            "relationships": 0,
            "required_fields": 0,
            "fields_with_defaults": 0,
            "fields_with_constraints": 0,
            "validation_warnings": 0,
            "constraint_types": {}
        }
        
        for field_name, field_info in all_constraints.items():
            # 필드 타입별 카운팅
            if field_info["type"] == "property":
                summary["properties"] += 1
            else:
                summary["relationships"] += 1
            
            # 필수 필드 카운팅
            if field_info["constraints"].get(ConstraintType.REQUIRED.value):
                summary["required_fields"] += 1
            
            # 기본값이 있는 필드 카운팅
            if field_info.get("default_value"):
                summary["fields_with_defaults"] += 1
            
            # 제약조건이 있는 필드 카운팅
            if field_info["constraints"]:
                summary["fields_with_constraints"] += 1
            
            # 검증 경고 카운팅
            summary["validation_warnings"] += len(field_info.get("validation_warnings", []))
            
            # 제약조건 타입별 카운팅
            for constraint_type in field_info["constraints"].keys():
                if constraint_type not in summary["constraint_types"]:
                    summary["constraint_types"][constraint_type] = 0
                summary["constraint_types"][constraint_type] += 1
        
        return summary


# 편의 함수들
def extract_property_constraints(property_data: Dict[str, Any]) -> Dict[str, Any]:
    """속성 제약조건 추출 편의 함수"""
    extractor = ConstraintExtractor()
    return extractor.extract_property_constraints(property_data)


def extract_relationship_constraints(relationship_data: Dict[str, Any]) -> Dict[str, Any]:
    """관계 제약조건 추출 편의 함수"""
    extractor = ConstraintExtractor()
    return extractor.extract_relationship_constraints(relationship_data)


def extract_all_constraints(class_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """모든 제약조건 추출 편의 함수"""
    extractor = ConstraintExtractor()
    return extractor.extract_all_constraints(class_data)


if __name__ == "__main__":
    # 테스트 예제
    print("🔥 제약조건 및 기본값 추출 테스트")
    
    test_class_data = {
        "id": "TestClass",
        "properties": [
            {
                "name": "title",
                "type": "string",
                "required": True,
                "constraints": {
                    "min_length": 1,
                    "max_length": 255,
                    "pattern": "^[a-zA-Z0-9\\s]+$"
                }
            },
            {
                "name": "score",
                "type": "integer", 
                "required": False,
                "default": 0,
                "constraints": {
                    "min": 0,
                    "max": 100
                }
            },
            {
                "name": "status",
                "type": "string",
                "required": True,
                "default": "pending",
                "constraints": {
                    "enum": ["pending", "active", "inactive"]
                }
            }
        ],
        "relationships": [
            {
                "predicate": "belongs_to",
                "target": "Category",
                "cardinality": "n:1",
                "required": True,
                "constraints": {
                    "min_cardinality": 1,
                    "max_cardinality": 1
                }
            }
        ]
    }
    
    extractor = ConstraintExtractor()
    constraints = extractor.extract_all_constraints(test_class_data)
    summary = extractor.generate_constraint_summary(constraints)
    
    print("\n제약조건 추출 결과:")
    print(json.dumps(constraints, indent=2, ensure_ascii=False))
    
    print("\n제약조건 요약:")
    print(json.dumps(summary, indent=2, ensure_ascii=False))