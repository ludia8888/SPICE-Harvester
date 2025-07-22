#!/usr/bin/env python3
"""
🔥 ULTRA! TerminusSchemaBuilder 기본 기능 테스트

새로운 복잡한 스키마 타입 지원 확인
"""

import json
import sys
from pathlib import Path

# Add the parent directory to sys.path to import our modules
sys.path.insert(0, str(Path(__file__).parent))

from oms.utils.terminus_schema_types import (
    TerminusSchemaBuilder, 
    TerminusSchemaConverter, 
    TerminusConstraintProcessor,
    create_basic_class_schema,
    create_subdocument_schema,
    convert_simple_schema
)


def test_basic_schema_builder():
    """기본 스키마 빌더 테스트"""
    print("🔧 기본 스키마 빌더 테스트...")
    
    builder = create_basic_class_schema("TestClass")
    schema = (builder
              .add_string_property("name", optional=False)
              .add_integer_property("age", optional=True)
              .add_boolean_property("active", optional=False)
              .add_documentation("Test class", "A class for testing")
              .build())
    
    print("✅ 기본 스키마 생성:")
    print(json.dumps(schema, indent=2))
    
    # 검증
    assert schema["@type"] == "Class"
    assert schema["@id"] == "TestClass"
    assert "name" in schema
    assert "age" in schema
    assert "active" in schema
    assert schema["name"] == "xsd:string"  # Required field
    assert schema["age"]["@type"] == "Optional"  # Optional field
    assert schema["active"] == "xsd:boolean"  # Required field
    
    print("✅ 기본 스키마 빌더 테스트 통과!")
    return True


def test_complex_types():
    """복잡한 타입들 테스트"""
    print("\n🔥 복잡한 타입들 테스트...")
    
    builder = create_basic_class_schema("ComplexClass")
    
    # 🔥 ULTRA! 복잡한 타입들 추가
    schema = (builder
              # Enum 타입
              .add_enum_property("status", ["active", "inactive", "pending"])
              # List 타입
              .add_list_property("tags", "xsd:string")
              # Set 타입
              .add_set_property("categories", "xsd:string")
              # Array 타입 (다차원)
              .add_array_property("matrix", "xsd:integer", dimensions=2)
              # OneOfType (Union)
              .add_one_of_type("flexible", ["xsd:string", "xsd:integer"])
              # Foreign 키
              .add_foreign_property("owner", "User")
              # GeoPoint
              .add_geopoint_property("location")
              .build())
    
    print("✅ 복잡한 타입 스키마:")
    print(json.dumps(schema, indent=2))
    
    # 검증
    assert schema["status"]["@type"] == "Enum"
    assert schema["tags"]["@type"] == "List"
    assert schema["categories"]["@type"] == "Set"
    assert schema["matrix"]["@type"] == "Array"
    assert schema["matrix"]["@dimensions"] == 2
    assert schema["flexible"]["@type"] == "OneOfType"
    assert schema["owner"]["@type"] == "Foreign"
    assert schema["location"] == "xsd:geoPoint"
    
    print("✅ 복잡한 타입 테스트 통과!")
    return True


def test_schema_converter():
    """스키마 컨버터 테스트"""
    print("\n🔧 스키마 컨버터 테스트...")
    
    converter = TerminusSchemaConverter()
    
    # 기본 타입 변환 테스트
    assert converter.convert_property_type("string") == "xsd:string"
    assert converter.convert_property_type("integer") == "xsd:integer"
    assert converter.convert_property_type("boolean") == "xsd:boolean"
    
    # Enum 제약조건 테스트
    enum_result = converter.convert_property_type("string", {"enum_values": ["a", "b", "c"]})
    assert enum_result["@type"] == "Enum"
    assert enum_result["@value"] == ["a", "b", "c"]
    
    # 관계 카디널리티 변환 테스트
    set_config = converter.convert_relationship_cardinality("many")
    assert set_config["@type"] == "Set"
    
    optional_config = converter.convert_relationship_cardinality("one")
    assert optional_config["@type"] == "Optional"
    
    # 복잡한 타입 변환 테스트
    list_config = converter.convert_complex_type({
        "type": "list",
        "element_type": "xsd:string"
    })
    assert list_config["@type"] == "List"
    assert list_config["@class"] == "xsd:string"
    
    union_config = converter.convert_complex_type({
        "type": "union",
        "types": ["xsd:string", "xsd:integer"]
    })
    assert union_config["@type"] == "OneOfType"
    assert union_config["@class"] == ["xsd:string", "xsd:integer"]
    
    print("✅ 스키마 컨버터 테스트 통과!")
    return True


def test_constraint_processor():
    """제약조건 프로세서 테스트"""
    print("\n🔧 제약조건 프로세서 테스트...")
    
    processor = TerminusConstraintProcessor()
    
    # 검증용 제약조건 추출 테스트
    constraints = {
        "min": 0,
        "max": 100,
        "min_length": 1,
        "max_length": 255,
        "pattern": "^[a-zA-Z]+$",
        "min_items": 1,
        "max_items": 10
    }
    
    validation_constraints = processor.extract_constraints_for_validation(constraints)
    
    assert validation_constraints["min_value"] == 0
    assert validation_constraints["max_value"] == 100
    assert validation_constraints["min_length"] == 1
    assert validation_constraints["max_length"] == 255
    assert validation_constraints["pattern"] == "^[a-zA-Z]+$"
    assert validation_constraints["min_items"] == 1
    assert validation_constraints["max_items"] == 10
    
    # 스키마 레벨 제약조건 적용 테스트
    schema = {"@type": "Class", "@id": "TestClass"}
    unique_constraints = {"unique": True}
    
    updated_schema = processor.apply_schema_level_constraints(schema, unique_constraints)
    assert "@key" in updated_schema
    assert updated_schema["@key"]["@type"] == "Hash"
    
    print("✅ 제약조건 프로세서 테스트 통과!")
    return True


def test_subdocument_schema():
    """서브문서 스키마 테스트"""
    print("\n📄 서브문서 스키마 테스트...")
    
    subdoc_builder = create_subdocument_schema("EmbeddedDocument")
    schema = (subdoc_builder
              .add_string_property("name")
              .add_integer_property("value")
              .build())
    
    print("✅ 서브문서 스키마:")
    print(json.dumps(schema, indent=2))
    
    # 검증
    assert schema["@type"] == "Class"
    assert "@subdocument" in schema
    assert schema["@subdocument"] == []
    
    print("✅ 서브문서 스키마 테스트 통과!")
    return True


def test_simple_schema_conversion():
    """간단한 스키마 변환 테스트"""
    print("\n🔄 간단한 스키마 변환 테스트...")
    
    class_data = {
        "id": "SimpleClass",
        "label": "Simple Test Class",
        "properties": [
            {
                "name": "title",
                "type": "string",
                "required": True,
                "constraints": {}
            },
            {
                "name": "count",
                "type": "integer",
                "required": False,
                "constraints": {}
            }
        ]
    }
    
    converted_schema = convert_simple_schema(class_data)
    
    print("✅ 변환된 스키마:")
    print(json.dumps(converted_schema, indent=2))
    
    # 검증
    assert converted_schema["@type"] == "Class"
    assert converted_schema["@id"] == "SimpleClass"
    assert "title" in converted_schema
    assert "count" in converted_schema
    
    print("✅ 간단한 스키마 변환 테스트 통과!")
    return True


def main():
    """모든 테스트 실행"""
    print("=" * 80)
    print("🔥 ULTRA! TerminusSchemaBuilder 기능 테스트")
    print("=" * 80)
    
    tests = [
        test_basic_schema_builder,
        test_complex_types,
        test_schema_converter,
        test_constraint_processor,
        test_subdocument_schema,
        test_simple_schema_conversion
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
                print(f"❌ {test.__name__} 실패")
        except Exception as e:
            failed += 1
            print(f"❌ {test.__name__} 오류: {e}")
            import traceback
            print(traceback.format_exc())
    
    print("\n" + "=" * 80)
    print(f"📊 테스트 결과: {passed} 통과, {failed} 실패")
    print("=" * 80)
    
    if failed == 0:
        print("🎉 모든 TerminusSchemaBuilder 기능이 정상 작동합니다!")
        return 0
    else:
        print("❌ 일부 기능에서 문제가 발견되었습니다")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)