#!/usr/bin/env python3
"""
🔥 ULTRA! 제약조건 및 기본값 추출 통합 테스트

create_ontology_class 메서드에서 제약조건 추출 기능이 제대로 작동하는지 테스트
"""

import json
import sys
from pathlib import Path

# Add the parent directory to sys.path to import our modules
sys.path.insert(0, str(Path(__file__).parent))

from oms.utils.constraint_extractor import ConstraintExtractor


def test_comprehensive_constraint_extraction():
    """포괄적인 제약조건 추출 테스트"""
    print("🔥 포괄적인 제약조건 추출 테스트")
    
    # 복잡한 클래스 데이터
    complex_class_data = {
        "id": "Product",
        "label": "Product Class",
        "description": "Comprehensive product management class",
        "properties": [
            {
                "name": "title",
                "type": "string",
                "required": True,
                "default": "New Product",
                "constraints": {
                    "min_length": 1,
                    "max_length": 255,
                    "pattern": "^[a-zA-Z0-9\\s\\-_]+$"
                },
                "label": "Product Title",
                "description": "The title of the product"
            },
            {
                "name": "price",
                "type": "decimal",
                "required": True,
                "default": "function:calculate_default_price",
                "constraints": {
                    "min": 0.01,
                    "max": 999999.99
                },
                "label": "Price",
                "description": "Product price in USD"
            },
            {
                "name": "category",
                "type": "string",
                "required": True,
                "default": "general",
                "constraints": {
                    "enum": ["electronics", "clothing", "books", "general"]
                },
                "label": "Category"
            },
            {
                "name": "tags",
                "type": "list<string>",
                "required": False,
                "constraints": {
                    "min_items": 0,
                    "max_items": 10,
                    "unique_items": True
                },
                "label": "Tags",
                "description": "Product tags for categorization"
            },
            {
                "name": "created_at",
                "type": "datetime",
                "required": False,
                "default": "now()",
                "label": "Created Date"
            },
            {
                "name": "reference_code",
                "type": "string",
                "required": False,
                "default": "{{id}}_{{category}}",  # 필드 참조
                "constraints": {
                    "pattern": "^[A-Z0-9_]+$"
                },
                "unique": True,
                "label": "Reference Code"
            }
        ],
        "relationships": [
            {
                "predicate": "belongs_to_brand",
                "target": "Brand",
                "cardinality": "n:1",
                "required": True,
                "constraints": {
                    "min_cardinality": 1,
                    "max_cardinality": 1
                },
                "label": "Brand",
                "description": "The brand this product belongs to"
            },
            {
                "predicate": "has_variants",
                "target": "ProductVariant", 
                "cardinality": "1:n",
                "required": False,
                "constraints": {
                    "min_cardinality": 0,
                    "max_cardinality": 50
                },
                "label": "Product Variants",
                "description": "Different variants of this product"
            },
            {
                "predicate": "related_products",
                "target": "Product",
                "cardinality": "union",
                "required": False,
                "constraints": {
                    "target_types": ["Product", "Bundle", "Accessory"],
                    "max_cardinality": 10
                },
                "label": "Related Products",
                "description": "Products related to this one"
            }
        ]
    }
    
    # 제약조건 추출 실행
    extractor = ConstraintExtractor()
    constraints = extractor.extract_all_constraints(complex_class_data)
    summary = extractor.generate_constraint_summary(constraints)
    
    print("\n📊 추출된 제약조건:")
    for field_name, field_info in constraints.items():
        field_type = field_info["type"]
        constraint_count = len(field_info["constraints"])
        default_value = field_info.get("default_value")
        warnings = len(field_info.get("validation_warnings", []))
        
        print(f"  • {field_name} ({field_type}):")
        print(f"    - 제약조건: {constraint_count}개")
        print(f"    - 기본값: {default_value['type'] if default_value else 'None'}")
        print(f"    - 경고: {warnings}개")
        
        if field_info["constraints"]:
            print(f"    - 상세: {list(field_info['constraints'].keys())}")
    
    print(f"\n📈 요약:")
    print(f"  총 필드: {summary['total_fields']}")
    print(f"  속성: {summary['properties']}, 관계: {summary['relationships']}")
    print(f"  필수 필드: {summary['required_fields']}")
    print(f"  기본값 있는 필드: {summary['fields_with_defaults']}")
    print(f"  제약조건 있는 필드: {summary['fields_with_constraints']}")
    print(f"  검증 경고: {summary['validation_warnings']}")
    
    print(f"\n🔧 제약조건 타입별 분포:")
    for constraint_type, count in summary['constraint_types'].items():
        print(f"  - {constraint_type}: {count}개")
    
    # 검증
    assert summary['total_fields'] == 9  # 6 properties + 3 relationships
    assert summary['properties'] == 6
    assert summary['relationships'] == 3
    assert summary['required_fields'] > 0
    assert summary['fields_with_defaults'] > 0
    assert summary['fields_with_constraints'] > 0
    
    print("\n✅ 포괄적인 제약조건 추출 테스트 성공!")
    return True


def test_default_value_types():
    """기본값 타입 분석 테스트"""
    print("\n🔧 기본값 타입 분석 테스트")
    
    test_properties = [
        {
            "name": "static_default",
            "type": "string",
            "default": "fixed_value"
        },
        {
            "name": "timestamp_default", 
            "type": "datetime",
            "default": "now()"
        },
        {
            "name": "uuid_default",
            "type": "string", 
            "default": "uuid()"
        },
        {
            "name": "computed_default",
            "type": "decimal",
            "default": "function:calculate_total"
        },
        {
            "name": "reference_default",
            "type": "string",
            "default": "{{user_id}}_{{timestamp}}"
        }
    ]
    
    class_data = {"id": "TestClass", "properties": test_properties}
    
    extractor = ConstraintExtractor()
    constraints = extractor.extract_all_constraints(class_data)
    
    # 기본값 타입별 검증
    expected_types = {
        "static_default": "static",
        "timestamp_default": "timestamp", 
        "uuid_default": "uuid",
        "computed_default": "computed",
        "reference_default": "reference"
    }
    
    for prop_name, expected_type in expected_types.items():
        field_info = constraints[prop_name]
        default_info = field_info.get("default_value")
        
        assert default_info is not None, f"{prop_name} should have default value"
        assert default_info["type"] == expected_type, f"{prop_name} should have type {expected_type}"
        
        print(f"  ✅ {prop_name}: {default_info['type']}")
        
        # 추가 정보 검증
        if expected_type == "reference":
            assert "reference_field" in default_info
            print(f"    - 참조 필드: {default_info['reference_field']}")
        elif expected_type == "computed":
            assert "function" in default_info 
            print(f"    - 계산 함수: {default_info['function']}")
    
    print("✅ 기본값 타입 분석 테스트 성공!")
    return True


def test_constraint_validation():
    """제약조건 검증 테스트"""
    print("\n⚠️ 제약조건 검증 테스트")
    
    # 호환성 문제가 있는 제약조건들 
    problematic_properties = [
        {
            "name": "numeric_with_length",
            "type": "integer",
            "constraints": {
                "min_length": 1,  # 숫자 타입에는 적용 불가
                "max_length": 10  # 숫자 타입에는 적용 불가
            }
        },
        {
            "name": "string_with_numeric",
            "type": "string",
            "constraints": {
                "min_value": 0,   # 문자열 타입에는 적용 불가
                "max_value": 100  # 문자열 타입에는 적용 불가
            }
        },
        {
            "name": "invalid_range",
            "type": "integer",
            "constraints": {
                "min": 100,
                "max": 50  # min > max 오류
            }
        }
    ]
    
    class_data = {"id": "ProblematicClass", "properties": problematic_properties}
    
    extractor = ConstraintExtractor()
    constraints = extractor.extract_all_constraints(class_data)
    summary = extractor.generate_constraint_summary(constraints)
    
    print(f"  검증 경고 총 개수: {summary['validation_warnings']}")
    
    warning_found = False
    for field_name, field_info in constraints.items():
        warnings = field_info.get("validation_warnings", [])
        if warnings:
            warning_found = True
            print(f"  • {field_name}:")
            for warning in warnings:
                print(f"    - ⚠️ {warning}")
    
    assert warning_found, "Should have validation warnings for incompatible constraints"
    print("✅ 제약조건 검증 테스트 성공!")
    return True


def main():
    """모든 테스트 실행"""
    print("=" * 80)
    print("🔥 ULTRA! 제약조건 및 기본값 추출 통합 테스트")
    print("=" * 80)
    
    tests = [
        test_comprehensive_constraint_extraction,
        test_default_value_types,
        test_constraint_validation
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
        print("🎉 모든 제약조건 추출 기능이 정상 작동합니다!")
        return 0
    else:
        print("❌ 일부 기능에서 문제가 발견되었습니다")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)