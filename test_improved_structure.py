#!/usr/bin/env python3
"""
🔥 THINK ULTRA! 개선된 구조 테스트
요구사항 구조 완전 지원 검증
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

def _check_link_type():
    """type="link" 구조 테스트"""
    print("🔍 Test 1: type='link' 구조")
    
    from shared.models.ontology import Property
    
    # 요구사항 구조: type="link"
    link_prop = Property(
        name="customer",
        type="link",  # 명시적 link 타입
        linkTarget="Customer",  # 대상 클래스
        label="Customer",
        cardinality="n:1"
    )
    
    print(f"✅ Property 생성 성공:")
    print(f"   name: {link_prop.name}")
    print(f"   type: {link_prop.type}")
    print(f"   linkTarget: {link_prop.linkTarget}")
    print(f"   is_class_reference(): {link_prop.is_class_reference()}")
    
    # 변환 테스트
    rel_data = link_prop.to_relationship()
    print(f"✅ 관계 변환:")
    print(f"   {rel_data}")
    
    return rel_data["target"] == "Customer" and rel_data["cardinality"] == "n:1"


def test_link_type():
    """type="link" 구조 테스트"""
    assert _check_link_type()

def _check_array_relationship():
    """배열 관계 구조 테스트"""
    print(f"\n🔍 Test 2: 배열 관계 구조")
    
    from shared.models.ontology import Property
    
    # 요구사항 구조: 배열 관계
    array_prop = Property(
        name="items",
        type="array",
        label="Items",
        items={
            "type": "link",
            "linkTarget": "Product"
        }
    )
    
    print(f"✅ 배열 Property 생성 성공:")
    print(f"   name: {array_prop.name}")
    print(f"   type: {array_prop.type}")
    print(f"   items: {array_prop.items}")
    print(f"   is_class_reference(): {array_prop.is_class_reference()}")
    
    # 변환 테스트
    rel_data = array_prop.to_relationship()
    print(f"✅ 배열 관계 변환:")
    print(f"   {rel_data}")
    
    return rel_data["target"] == "Product" and rel_data["cardinality"] == "1:n"


def test_array_relationship():
    """배열 관계 구조 테스트"""
    assert _check_array_relationship()

def _check_backward_compatibility():
    """기존 방식 호환성 테스트"""
    print(f"\n🔍 Test 3: 기존 방식 호환성")
    
    from shared.models.ontology import Property
    
    # 기존 방식 (클래스명을 type에)
    old_prop = Property(
        name="category",
        type="Category",  # 기존 방식
        label="Category",
        isRelationship=True,
        cardinality="n:1"
    )
    
    print(f"✅ 기존 방식 여전히 작동:")
    print(f"   name: {old_prop.name}")
    print(f"   type: {old_prop.type}")
    print(f"   is_class_reference(): {old_prop.is_class_reference()}")
    
    rel_data = old_prop.to_relationship()
    print(f"✅ 기존 방식 변환:")
    print(f"   {rel_data}")
    
    return rel_data["target"] == "Category"


def test_backward_compatibility():
    """기존 방식 호환성 테스트"""
    assert _check_backward_compatibility()

def _check_complete_order_example():
    """완전한 Order 예시 테스트"""
    print(f"\n🔍 Test 4: 완전한 Order 클래스 예시")
    
    from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
    
    # 요구사항 구조 완전 구현
    order_data = {
        "id": "Order",
        "type": "Class",
        "label": "Order",
        "properties": [
            {
                "name": "order_id",
                "type": "STRING",
                "label": "Order ID",
                "required": True
            },
            # type="link" 방식
            {
                "name": "customer",
                "type": "link",
                "linkTarget": "Customer",
                "label": "Customer",
                "cardinality": "n:1"
            },
            # 배열 관계
            {
                "name": "items",
                "type": "array",
                "label": "Order Items",
                "items": {
                    "type": "link",
                    "linkTarget": "Product"
                }
            },
            # 기존 방식 (호환성)
            {
                "name": "shipping_address",
                "type": "Address",
                "label": "Shipping Address",
                "isRelationship": True,
                "cardinality": "n:1"
            }
        ]
    }
    
    converter = PropertyToRelationshipConverter()
    result = converter.process_class_data(order_data)
    
    print(f"✅ 변환 결과:")
    print(f"   입력 properties: {len(order_data['properties'])}")
    print(f"   결과 properties: {len(result.get('properties', []))}")
    print(f"   결과 relationships: {len(result.get('relationships', []))}")
    
    # 관계 세부 검증
    relationships = result.get('relationships', [])
    rel_map = {r["predicate"]: r for r in relationships}
    
    print(f"\n📋 생성된 관계들:")
    for pred, rel in rel_map.items():
        print(f"   {pred} → {rel['target']} ({rel['cardinality']})")
    
    # 검증
    success = (
        len(result.get('properties', [])) == 1 and  # order_id만 남아야 함
        len(relationships) == 3 and  # customer, items, shipping_address
        rel_map.get("customer", {}).get("target") == "Customer" and
        rel_map.get("items", {}).get("target") == "Product" and
        rel_map.get("shipping_address", {}).get("target") == "Address"
    )
    
    return success


def test_complete_order_example():
    """완전한 Order 예시 테스트"""
    assert _check_complete_order_example()

if __name__ == "__main__":
    print("🔥 THINK ULTRA! 개선된 구조 완전 테스트")
    print("=" * 60)
    
    results = []
    results.append(_check_link_type())
    results.append(_check_array_relationship())
    results.append(_check_backward_compatibility())
    results.append(_check_complete_order_example())
    
    passed = sum(results)
    total = len(results)
    
    print(f"\n📊 최종 결과: {passed}/{total} 테스트 통과")
    
    if passed == total:
        print("🎉 완벽! 요구사항 구조 100% 지원!")
        print("✅ type='link' 지원")
        print("✅ 배열 관계 지원") 
        print("✅ 기존 방식 호환성")
        print("✅ 완전한 Order 예시 작동")
    else:
        print("❌ 일부 테스트 실패")
        for i, result in enumerate(results, 1):
            status = "✅" if result else "❌"
            print(f"   Test {i}: {status}")
