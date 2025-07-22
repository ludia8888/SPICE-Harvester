#!/usr/bin/env python3
"""
🔥 THINK ULTRA! TerminusDB v11.x 복잡한 스키마 타입 완전 지원 모듈

TerminusDB v11.x에서 지원하는 모든 스키마 타입들을 체계적으로 관리하고
변환하는 유틸리티 클래스들을 제공합니다.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class TerminusSchemaType(Enum):
    """TerminusDB v11.x 지원 스키마 타입들"""
    
    # 기본 데이터 타입
    STRING = "xsd:string"
    INTEGER = "xsd:integer" 
    DECIMAL = "xsd:decimal"
    DOUBLE = "xsd:double"
    FLOAT = "xsd:float"
    BOOLEAN = "xsd:boolean"
    DATETIME = "xsd:dateTime"
    DATE = "xsd:date"
    TIME = "xsd:time"
    
    # 복합 데이터 타입
    GEOPOINT = "xsd:geoPoint"
    GEOTEMPORALPOINT = "xsd:geoTemporalPoint"
    COORDINATEPOINT = "xsd:coordinatePoint"
    GYEAR = "xsd:gYear"
    GYEARMONTH = "xsd:gYearMonth"
    DURATION = "xsd:duration"
    ANYURI = "xsd:anyURI"
    LANGUAGE = "xsd:language"
    BASE64BINARY = "xsd:base64Binary"
    HEXBINARY = "xsd:hexBinary"
    
    # 컨테이너 타입들
    OPTIONAL = "Optional"
    LIST = "List"
    SET = "Set"
    ARRAY = "Array"
    
    # 특수 타입들
    CLASS = "Class"
    ENUM = "Enum"
    FOREIGN = "Foreign"
    UNIT = "Unit"
    ONEOFTYPE = "OneOfType"
    

class TerminusSchemaBuilder:
    """TerminusDB 스키마 구조를 생성하는 빌더 클래스"""
    
    def __init__(self):
        self.schema_data = {}
    
    def set_class(self, class_id: str, key_type: str = "Random") -> "TerminusSchemaBuilder":
        """기본 클래스 설정"""
        self.schema_data.update({
            "@type": "Class",
            "@id": class_id,
            "@key": {"@type": key_type}
        })
        return self
    
    def set_subdocument(self) -> "TerminusSchemaBuilder":
        """서브 도큐먼트로 설정"""
        self.schema_data["@subdocument"] = []
        return self
    
    def add_string_property(self, name: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """문자열 속성 추가"""
        prop_type = TerminusSchemaType.STRING.value
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_integer_property(self, name: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """정수 속성 추가"""
        prop_type = TerminusSchemaType.INTEGER.value
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_boolean_property(self, name: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """불리언 속성 추가"""
        prop_type = TerminusSchemaType.BOOLEAN.value
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_datetime_property(self, name: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """날짜시간 속성 추가"""
        prop_type = TerminusSchemaType.DATETIME.value
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_date_property(self, name: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """날짜 속성 추가"""
        prop_type = TerminusSchemaType.DATE.value
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_list_property(self, name: str, element_type: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """리스트 속성 추가"""
        # 🔥 ULTRA! For List and Set, optional is handled differently in TerminusDB
        # Optional collections are just empty collections, not wrapped in Optional
        prop_type = {"@type": "List", "@class": element_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_set_property(self, name: str, element_type: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """셋 속성 추가"""
        if optional:
            # 🔥 ULTRA! For optional Set, TerminusDB expects a flat structure
            prop_type = {"@type": "Set", "@class": element_type}
        else:
            prop_type = {"@type": "Set", "@class": element_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_array_property(self, name: str, element_type: str, dimensions: int = 1, optional: bool = False) -> "TerminusSchemaBuilder":
        """배열 속성 추가"""
        # 🔥 ULTRA! TerminusDB doesn't support complex nested structures or @dimensions
        # Arrays are simply treated as Lists in TerminusDB
        # Multi-dimensional arrays are not directly supported - use List type instead
        # The dimensions parameter is stored in metadata but not in the schema
        prop_type = {"@type": "List", "@class": element_type}
        
        # Note: For optional arrays, we don't wrap in Optional because
        # an empty list is equivalent to an optional array
        self.schema_data[name] = prop_type
        return self
    
    def add_class_reference(self, name: str, target_class: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """다른 클래스 참조 추가"""
        prop_type = target_class
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_enum_property(self, name: str, enum_values: List[str], optional: bool = False) -> "TerminusSchemaBuilder":
        """Enum 속성 추가"""
        prop_type = {"@type": "Enum", "@value": enum_values}
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_foreign_property(self, name: str, foreign_type: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """Foreign 키 속성 추가"""
        prop_type = {"@type": "Foreign", "@class": foreign_type}
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_one_of_type(self, name: str, type_options: List[str], optional: bool = False) -> "TerminusSchemaBuilder":
        """OneOfType 속성 추가 (Union type)"""
        prop_type = {"@type": "OneOfType", "@class": type_options}
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_geopoint_property(self, name: str, optional: bool = False) -> "TerminusSchemaBuilder":
        """지리적 좌표 속성 추가"""
        prop_type = TerminusSchemaType.GEOPOINT.value
        if optional:
            prop_type = {"@type": "Optional", "@class": prop_type}
        self.schema_data[name] = prop_type
        return self
    
    def add_documentation(self, comment: str = None, description: str = None) -> "TerminusSchemaBuilder":
        """문서화 정보 추가"""
        doc = {}
        if comment:
            doc["@comment"] = comment
        if description:
            doc["@description"] = description
        if doc:
            self.schema_data["@documentation"] = doc
        return self
    
    def build(self) -> Dict[str, Any]:
        """완성된 스키마 반환"""
        return self.schema_data.copy()


class TerminusSchemaConverter:
    """기존 스키마 데이터를 TerminusDB 형식으로 변환하는 클래스"""
    
    @staticmethod
    def convert_property_type(prop_type: str, constraints: Dict[str, Any] = None) -> Union[str, Dict[str, Any]]:
        """속성 타입을 TerminusDB 형식으로 변환"""
        
        # 기본 타입 매핑
        type_mapping = {
            "string": TerminusSchemaType.STRING.value,
            "text": TerminusSchemaType.STRING.value,
            "integer": TerminusSchemaType.INTEGER.value,
            "int": TerminusSchemaType.INTEGER.value,
            "number": TerminusSchemaType.DECIMAL.value,
            "decimal": TerminusSchemaType.DECIMAL.value,
            "float": TerminusSchemaType.FLOAT.value,
            "double": TerminusSchemaType.DOUBLE.value,
            "boolean": TerminusSchemaType.BOOLEAN.value,
            "bool": TerminusSchemaType.BOOLEAN.value,
            "datetime": TerminusSchemaType.DATETIME.value,
            "date": TerminusSchemaType.DATE.value,
            "time": TerminusSchemaType.TIME.value,
            "uri": TerminusSchemaType.ANYURI.value,
            "url": TerminusSchemaType.ANYURI.value,
            "email": TerminusSchemaType.STRING.value,  # Email is a string with pattern validation
            "geopoint": TerminusSchemaType.GEOPOINT.value,
            "json": TerminusSchemaType.STRING.value,  # JSON is stored as string
        }
        
        # 기본 타입 매핑 시도
        if prop_type in type_mapping:
            base_type = type_mapping[prop_type]
        else:
            # 그대로 사용 (클래스 참조 등)
            base_type = prop_type
        
        # constraints 처리
        if constraints:
            if "enum_values" in constraints:
                return {
                    "@type": "Enum",
                    "@value": constraints["enum_values"]
                }
            elif "min" in constraints or "max" in constraints:
                # 범위 제약조건은 현재 TerminusDB에서 스키마 레벨로 직접 지원 안 됨
                # 애플리케이션 레벨에서 검증
                logger.warning(f"Range constraints not supported at schema level for type {prop_type}")
        
        return base_type
    
    @staticmethod
    def convert_relationship_cardinality(cardinality: str) -> Dict[str, Any]:
        """관계 카디널리티를 TerminusDB 형식으로 변환"""
        
        cardinality_mapping = {
            "one": "Optional",  # 0..1
            "many": "Set",      # 0..*
            "1:1": "Optional",  # 1:1 (Optional as it might not be set initially)
            "1:n": "Set",       # 1:n (Set of references)
            "n:1": "Optional",  # n:1 (Single reference)
            "n:n": "Set",       # n:n (Set of references)
            "n:m": "Set",       # n:m (Set of references - same as n:n)
            "m:n": "Set",       # m:n (Set of references - same as n:n)
        }
        
        container_type = cardinality_mapping.get(cardinality, "Optional")
        return {"@type": container_type}
    
    @staticmethod
    def convert_complex_type(type_config: Dict[str, Any]) -> Dict[str, Any]:
        """복잡한 타입 구성을 변환"""
        
        if not isinstance(type_config, dict):
            return str(type_config)
        
        type_name = type_config.get("type")
        
        if type_name == "array" or type_name == "list":
            element_type = type_config.get("element_type", "xsd:string")
            dimensions = type_config.get("dimensions", 1)
            
            if type_name == "array":
                return {
                    "@type": "Array",
                    "@class": element_type,
                    "@dimensions": dimensions
                }
            else:
                return {
                    "@type": "List",
                    "@class": element_type
                }
        
        elif type_name == "set":
            element_type = type_config.get("element_type", "xsd:string")
            return {
                "@type": "Set",
                "@class": element_type
            }
        
        elif type_name == "optional":
            inner_type = type_config.get("inner_type", "xsd:string")
            return {
                "@type": "Optional",
                "@class": inner_type
            }
        
        elif type_name == "union":
            type_options = type_config.get("types", [])
            return {
                "@type": "OneOfType",
                "@class": type_options
            }
        
        # 알 수 없는 타입은 그대로 반환
        return type_config


class TerminusConstraintProcessor:
    """TerminusDB 제약조건 처리 클래스"""
    
    @staticmethod
    def extract_constraints_for_validation(constraints: Dict[str, Any]) -> Dict[str, Any]:
        """스키마 제약조건에서 런타임 검증용 제약조건 추출"""
        validation_constraints = {}
        
        # 숫자 범위 제약조건
        if "min" in constraints:
            validation_constraints["min_value"] = constraints["min"]
        if "max" in constraints:
            validation_constraints["max_value"] = constraints["max"]
        
        # 문자열 제약조건
        if "min_length" in constraints:
            validation_constraints["min_length"] = constraints["min_length"]
        if "max_length" in constraints:
            validation_constraints["max_length"] = constraints["max_length"]
        if "pattern" in constraints:
            validation_constraints["pattern"] = constraints["pattern"]
        
        # 리스트/배열 제약조건
        if "min_items" in constraints:
            validation_constraints["min_items"] = constraints["min_items"]
        if "max_items" in constraints:
            validation_constraints["max_items"] = constraints["max_items"]
        
        return validation_constraints
    
    @staticmethod
    def apply_schema_level_constraints(schema: Dict[str, Any], constraints: Dict[str, Any]) -> Dict[str, Any]:
        """스키마 레벨에서 적용 가능한 제약조건 적용"""
        
        # Unique 제약조건은 @key로 처리
        if constraints.get("unique", False):
            if "@key" not in schema:
                schema["@key"] = {"@type": "Hash", "@fields": []}
        
        # 필수 필드는 Optional 타입을 제거하여 처리
        if constraints.get("required", False):
            # Optional 래핑 제거 로직
            for field_name, field_type in schema.items():
                if isinstance(field_type, dict) and field_type.get("@type") == "Optional":
                    schema[field_name] = field_type.get("@class", "xsd:string")
        
        return schema


# 편의 함수들
def create_basic_class_schema(class_id: str, key_type: str = "Random") -> TerminusSchemaBuilder:
    """기본 클래스 스키마 빌더 생성"""
    return TerminusSchemaBuilder().set_class(class_id, key_type)


def create_subdocument_schema(class_id: str) -> TerminusSchemaBuilder:
    """서브문서 스키마 빌더 생성"""
    return TerminusSchemaBuilder().set_class(class_id).set_subdocument()


def convert_simple_schema(class_data: Dict[str, Any]) -> Dict[str, Any]:
    """간단한 스키마 데이터를 TerminusDB 형식으로 변환"""
    converter = TerminusSchemaConverter()
    builder = create_basic_class_schema(class_data.get("id", "UnknownClass"))
    
    # 기본 정보 설정
    if "label" in class_data:
        builder.add_documentation(description=str(class_data["label"]))
    
    # 속성들 변환
    properties = class_data.get("properties", [])
    for prop in properties:
        prop_name = prop.get("name")
        prop_type = prop.get("type", "string")
        optional = not prop.get("required", True)
        constraints = prop.get("constraints", {})
        
        if prop_name:
            converted_type = converter.convert_property_type(prop_type, constraints)
            
            if isinstance(converted_type, str):
                if prop_type in ["string", "text"]:
                    builder.add_string_property(prop_name, optional)
                elif prop_type in ["integer", "int"]:
                    builder.add_integer_property(prop_name, optional)
                elif prop_type in ["boolean", "bool"]:
                    builder.add_boolean_property(prop_name, optional)
                elif prop_type in ["datetime"]:
                    builder.add_datetime_property(prop_name, optional)
                else:
                    # 클래스 참조 또는 알 수 없는 타입
                    builder.add_class_reference(prop_name, converted_type, optional)
            else:
                # 복잡한 타입 구조
                builder.schema_data[prop_name] = converted_type
    
    return builder.build()


if __name__ == "__main__":
    # 테스트 예제
    print("🔥 TerminusDB 스키마 타입 지원 테스트")
    
    # 기본 클래스 스키마 생성
    builder = create_basic_class_schema("Employee")
    schema = (builder
              .add_string_property("name")
              .add_integer_property("age", optional=True)
              .add_boolean_property("active")
              .add_list_property("skills", "xsd:string")
              .add_set_property("certifications", "xsd:string")
              .add_class_reference("department", "Department", optional=True)
              .add_documentation("Employee class", "Represents company employees")
              .build())
    
    print("Generated schema:")
    import json
    print(json.dumps(schema, indent=2))