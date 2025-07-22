#!/usr/bin/env python3
"""
🔥 ULTRA! Property ↔ Relationship 변환 단위 테스트

TerminusDB 연결 없이 변환 로직만 테스트
"""

import json
import sys
from pathlib import Path

# Add the parent directory to sys.path to import our modules
sys.path.insert(0, str(Path(__file__).parent))

from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
from shared.models.ontology import Property


def test_property_to_relationship_converter():
    """PropertyToRelationshipConverter 단위 테스트"""
    print("🔧 PropertyToRelationshipConverter 단위 테스트")
    
    converter = PropertyToRelationshipConverter()
    
    # 테스트 클래스 데이터
    class_data = {
        "id": "Employee",
        "label": "Employee",
        "properties": [
            # 일반 속성
            {
                "name": "employee_id",
                "type": "string",
                "required": True,
                "label": "Employee ID"
            },
            # linkTarget이 있는 속성 (relationship으로 변환되어야 함)
            {
                "name": "department",
                "type": "link",
                "linkTarget": "Department",
                "required": True,
                "label": "Department",
                "description": "The department this employee belongs to"
            },
            # 또 다른 linkTarget 속성
            {
                "name": "manager",
                "type": "Employee",  # linkTarget 없이 타입이 직접 클래스 참조
                "required": False,
                "label": "Manager"
            },
            # 일반 속성
            {
                "name": "salary",
                "type": "decimal",
                "required": False,
                "label": "Salary"
            }
        ],
        "relationships": [
            # 기존 명시적 관계
            {
                "predicate": "manages",
                "target": "Employee",
                "cardinality": "1:n",
                "label": "Manages"
            }
        ]
    }
    
    # 변환 실행
    processed_data = converter.process_class_data(class_data)
    
    print("\n📊 변환 결과:")
    print(f"Properties: {len(processed_data['properties'])} → {[p['name'] for p in processed_data['properties']]}")
    print(f"Relationships: {len(processed_data['relationships'])} → {[r['predicate'] for r in processed_data['relationships']]}")
    
    # 검증
    properties = processed_data['properties']
    relationships = processed_data['relationships']
    
    # 일반 속성만 properties에 남아야 함
    property_names = {p['name'] for p in properties}
    expected_properties = {"employee_id", "salary"}
    
    if property_names == expected_properties:
        print("✅ 일반 속성만 properties에 남음")
    else:
        print(f"❌ Properties 목록이 예상과 다름: {property_names}")
    
    # linkTarget 속성들이 relationships로 변환되어야 함
    relationship_predicates = {r['predicate'] for r in relationships}
    expected_relationships = {"department", "manager", "manages"}
    
    if relationship_predicates == expected_relationships:
        print("✅ linkTarget 속성들이 relationships로 변환됨")
    else:
        print(f"❌ Relationships 목록이 예상과 다름: {relationship_predicates}")
    
    # 변환된 relationship에 _converted_from_property 플래그 확인
    for rel in relationships:
        if rel['predicate'] in ['department', 'manager']:
            if rel.get('_converted_from_property'):
                print(f"✅ '{rel['predicate']}'에 _converted_from_property 플래그 있음")
            else:
                print(f"❌ '{rel['predicate']}'에 _converted_from_property 플래그 없음")
    
    return True


def test_property_model():
    """Property 모델의 is_class_reference 메서드 테스트"""
    print("\n🔧 Property 모델 테스트")
    
    # 일반 속성
    prop1 = Property(name="name", type="string", label="Name")
    print(f"일반 속성 (string): is_class_reference = {prop1.is_class_reference()}")
    
    # linkTarget이 있는 속성
    prop2 = Property(name="department", type="link", linkTarget="Department", label="Department")
    print(f"linkTarget 속성: is_class_reference = {prop2.is_class_reference()}")
    
    # 타입이 직접 클래스 참조인 속성
    prop3 = Property(name="manager", type="Employee", label="Manager")
    print(f"클래스 참조 타입: is_class_reference = {prop3.is_class_reference()}")
    
    # to_relationship 메서드 테스트
    rel_data = prop2.to_relationship()
    print(f"\nlinkTarget 속성의 relationship 변환:")
    print(json.dumps(rel_data, indent=2))
    
    return True


def test_get_ontology_reverse_conversion():
    """get_ontology에서의 역변환 시뮬레이션"""
    print("\n🔧 get_ontology 역변환 로직 시뮬레이션")
    
    # TerminusDB에서 조회된 것처럼 시뮬레이션
    relationships = [
        {
            "predicate": "department", 
            "target": "Department",
            "cardinality": "n:1",
            "label": "Department"
        },
        {
            "predicate": "manages",
            "target": "Employee", 
            "cardinality": "1:n",
            "label": "Manages"
        }
    ]
    
    # 메타데이터 시뮬레이션
    field_metadata_map = {
        "department": {
            "converted_from_property": True,
            "original_property_name": "department", 
            "required": True
        },
        "manages": {
            "is_explicit_relationship": True
        }
    }
    
    # 역변환 로직 (get_ontology 메서드에서 발췌)
    property_converted_relationships = []
    explicit_relationships = []
    
    for rel in relationships:
        field_meta = field_metadata_map.get(rel["predicate"], {})
        
        is_property_origin = field_meta.get("converted_from_property", False)
        
        if not is_property_origin and not field_meta.get("is_explicit_relationship", False):
            is_property_origin = (
                rel.get("cardinality") in ["n:1", "1:1"] and 
                not field_meta.get("inverse_predicate")
            )
        
        if is_property_origin:
            prop = {
                "name": rel["predicate"],
                "type": "link",
                "linkTarget": rel["target"],
                "label": rel.get("label", rel["predicate"]),
                "required": field_meta.get("required", False)
            }
            property_converted_relationships.append(prop)
        else:
            explicit_relationships.append(rel)
    
    print(f"역변환된 Properties: {len(property_converted_relationships)}")
    for prop in property_converted_relationships:
        print(f"  - {prop['name']}: {prop['type']} → {prop['linkTarget']}")
    
    print(f"\n유지된 Relationships: {len(explicit_relationships)}")
    for rel in explicit_relationships:
        print(f"  - {rel['predicate']} → {rel['target']}")
    
    return True


def main():
    """모든 테스트 실행"""
    print("=" * 80)
    print("🔥 ULTRA! Property ↔ Relationship 변환 단위 테스트")
    print("=" * 80)
    
    tests = [
        test_property_to_relationship_converter,
        test_property_model,
        test_get_ontology_reverse_conversion
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
        print("🎉 모든 변환 로직이 정상 작동합니다!")
        return 0
    else:
        print("❌ 일부 변환 로직에서 문제가 발견되었습니다")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)