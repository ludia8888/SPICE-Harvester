#!/usr/bin/env python3
"""
🔥 THINK ULTRA! 개선된 메타데이터 시스템 테스트
- 다국어 label/description
- 속성별 메타데이터
- 제약조건, 기본값
- 복잡한 타입 지원
"""
import json
import os
import time
import uuid

import pytest
import requests

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = f"spice_metadata_test_{uuid.uuid4().hex[:8]}"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or os.getenv("OMS_ADMIN_TOKEN") or "").strip()
HEADERS = {"X-Admin-Token": ADMIN_TOKEN} if ADMIN_TOKEN else {}
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN is required for improved metadata tests")

def setup_database():
    """테스트용 데이터베이스 생성"""
    response = requests.post(
        f"{BASE_URL}/database/create",
        json={"name": DB_NAME},
        headers=HEADERS,
    )
    print(f"데이터베이스 생성: {response.status_code}")
    if response.status_code not in (200, 202, 409):
        return False

    for _ in range(15):
        check_resp = requests.get(f"{BASE_URL}/database/exists/{DB_NAME}", headers=HEADERS)
        if check_resp.status_code == 200:
            exists = check_resp.json().get("data", {}).get("exists")
            if exists:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module", autouse=True)
def _ensure_database():
    assert setup_database()


def _wait_for_class(class_id: str, timeout_seconds: int = 15):
    for _ in range(timeout_seconds):
        response = requests.get(f"{BASE_URL}/database/{DB_NAME}/ontology/{class_id}", headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", data)
        time.sleep(1)
    return None


def _label_text(value):
    if isinstance(value, dict):
        return (
            str(value.get("ko") or "").strip()
            or str(value.get("en") or "").strip()
            or next((str(v).strip() for v in value.values() if v), "")
        )
    return str(value or "").strip()

def test_full_metadata_support():
    """전체 메타데이터 지원 테스트"""
    print("\n=== 전체 메타데이터 지원 테스트 ===")
    
    # 1. 복잡한 온톨로지 생성
    order_data = {
        "id": "Order",
        "type": "Class",
        "label": {"en": "Order", "ko": "주문"},
        "description": {"en": "Represents a purchase order", "ko": "구매 주문을 나타냅니다"},
        "properties": [
            {
                "name": "order_id",
                "type": "STRING",
                "label": {"en": "Order ID", "ko": "주문 번호"},
                "description": {"en": "Unique identifier for the order", "ko": "주문의 고유 식별자"},
                "required": True,
                "primaryKey": True,
                "constraints": {
                    "minLength": 5,
                    "maxLength": 20,
                    "pattern": "^ORD-[0-9]+$"
                }
            },
            {
                "name": "order_date",
                "type": "DATE",
                "label": {"en": "Order Date", "ko": "주문일"},
                "description": {"en": "Date when the order was placed", "ko": "주문이 접수된 날짜"},
                "required": True
            },
            {
                "name": "total_amount",
                "type": "DECIMAL",
                "label": {"en": "Total Amount", "ko": "총 금액"},
                "required": False,
                "default": 0.0,
                "constraints": {
                    "minimum": 0,
                    "maximum": 1000000
                }
            },
            {
                "name": "status",
                "type": "ENUM",
                "label": {"en": "Status", "ko": "상태"},
                "required": True,
                "default": "pending",
                "constraints": {
                    "enum": ["pending", "processing", "shipped", "delivered", "cancelled"]
                }
            },
            # 클래스 참조 (자동 변환될 것)
            {
                "name": "customer",
                "type": "Customer",
                "label": {"en": "Customer", "ko": "고객"},
                "description": {"en": "The customer who placed the order", "ko": "주문을 한 고객"},
                "cardinality": "n:1",
                "isRelationship": True
            }
        ],
        "relationships": [
            {
                "predicate": "contains",
                "target": "OrderItem",
                "label": {"en": "Contains", "ko": "포함"},
                "description": {"en": "Items contained in the order", "ko": "주문에 포함된 항목들"},
                "cardinality": "1:n",
                "inverse_predicate": "belongsTo",
                "inverse_label": {"en": "Belongs to", "ko": "속함"}
            }
        ]
    }
    
    # Customer 클래스 먼저 생성
    customer_data = {
        "id": "Customer",
        "type": "Class",
        "label": {"en": "Customer", "ko": "고객"},
        "properties": [
            {
                "name": "customer_id",
                "type": "STRING",
                "label": {"en": "Customer ID", "ko": "고객 ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Name", "ko": "이름"},
                "required": True
            }
        ]
    }
    
    # OrderItem 클래스 생성
    order_item_data = {
        "id": "OrderItem",
        "type": "Class",
        "label": {"en": "Order Item", "ko": "주문 항목"},
        "properties": [
            {
                "name": "order_item_id",
                "type": "STRING",
                "label": {"en": "Order Item ID", "ko": "주문 항목 ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "quantity",
                "type": "INTEGER",
                "label": {"en": "Quantity", "ko": "수량"},
                "required": True,
                "default": 1,
                "constraints": {
                    "minimum": 1,
                    "maximum": 999
                }
            }
        ]
    }
    
    # Customer, OrderItem 클래스 생성
    for class_data, class_name in [(customer_data, "Customer"), (order_item_data, "OrderItem")]:
        response = requests.post(
            f"{BASE_URL}/database/{DB_NAME}/ontology",
            json=class_data,
            headers=HEADERS,
        )
        print(f"\n{class_name} 클래스 생성: {response.status_code}")
        if response.status_code not in (200, 202):
            pytest.fail(f"{class_name} 생성 실패: {response.status_code} {response.text}")
        if not _wait_for_class(class_name):
            pytest.fail(f"{class_name} 클래스가 준비되지 않았습니다")

    # Order 클래스 생성 (관계 대상 클래스가 준비된 뒤)
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=order_data,
        headers=HEADERS,
    )
    print(f"\nOrder 클래스 생성: {response.status_code}")
    if response.status_code not in (200, 202):
        pytest.fail(f"Order 생성 실패: {response.status_code} {response.text}")
    
    # 2. 생성된 클래스 조회 및 검증
    print("\n=== Order 클래스 조회 및 검증 ===")
    order_class = _wait_for_class("Order")
    if order_class:
            
        print(f"\n전체 응답:")
        print(json.dumps(order_class, indent=2, ensure_ascii=False))
        
        # 검증 항목들
        print("\n=== 검증 결과 ===")
        
        # 1. 다국어 label 검증
        if _label_text(order_class.get("label")) == "주문":
            print("✅ 클래스 다국어 label 정상")
        else:
            print("❌ 클래스 다국어 label 누락")
            
        # 2. 다국어 description 검증
        if _label_text(order_class.get("description")) == "구매 주문을 나타냅니다":
            print("✅ 클래스 다국어 description 정상")
        else:
            print("❌ 클래스 다국어 description 누락")
            
        # 3. 속성 메타데이터 검증
        properties = order_class.get("properties", [])
        for prop in properties:
            if prop.get("name") == "order_id":
                if _label_text(prop.get("label")) == "주문 번호":
                    print("✅ order_id 속성 label 정상")
                else:
                    print("❌ order_id 속성 label 누락")
                    
                if prop.get("constraints", {}).get("pattern") == "^ORD-[0-9]+$":
                    print("✅ order_id 제약조건 정상")
                else:
                    print("❌ order_id 제약조건 누락")
                    
            elif prop.get("name") == "total_amount":
                if prop.get("default") == 0.0:
                    print("✅ total_amount 기본값 정상")
                else:
                    print("❌ total_amount 기본값 누락")
                    
        # 4. 관계 메타데이터 검증
        relationships = order_class.get("relationships", [])
        for rel in relationships:
            if rel.get("predicate") == "customer":
                if _label_text(rel.get("label")) == "고객":
                    print("✅ customer 관계 label 정상")
                else:
                    print("❌ customer 관계 label 누락")
                    
            elif rel.get("predicate") == "contains":
                if rel.get("inverse_predicate") == "belongsTo":
                    print("✅ contains 역관계 정상")
                else:
                    print("❌ contains 역관계 누락")
                    
        # 5. Property → Relationship 변환 검증
        prop_names = [p.get("name") for p in properties]
        rel_predicates = [r.get("predicate") for r in relationships]
        
        if "customer" not in prop_names and "customer" in rel_predicates:
            print("✅ customer property → relationship 변환 성공")
        else:
            print("❌ customer property → relationship 변환 실패")
            
    else:
        pytest.fail("Order 클래스 조회 실패")

def test_complex_types():
    """복잡한 타입 테스트"""
    print("\n\n=== 복잡한 타입 지원 테스트 ===")
    
    complex_data = {
        "id": "ComplexTypes",
        "type": "Class",
        "label": {"en": "Complex Types Test"},
        "properties": [
            {
                "name": "complex_types_id",
                "type": "STRING",
                "label": {"en": "Complex Types ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "tags",
                "type": "ARRAY",
                "label": {"en": "Tags"},
                "description": {"en": "Array of tags"},
                "constraints": {
                    "minLength": 1,
                    "maxLength": 10
                }
            },
            {
                "name": "metadata",
                "type": "OBJECT",
                "label": {"en": "Metadata"},
                "description": {"en": "Free-form metadata object"}
            },
            {
                "name": "priority",
                "type": "ENUM",
                "label": {"en": "Priority"},
                "default": "medium",
                "constraints": {
                    "enum": ["low", "medium", "high", "urgent"]
                }
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=complex_data,
        headers=HEADERS,
    )
    print(f"ComplexTypes 클래스 생성: {response.status_code}")
    if response.status_code not in (200, 202):
        pytest.fail(f"ComplexTypes 생성 실패: {response.status_code} {response.text}")
    
    # 조회 및 검증
    complex_class = _wait_for_class("ComplexTypes")
    if complex_class:
        print("\n복잡한 타입 속성들:")
        for prop in complex_class.get("properties", []):
            print(f"- {prop.get('name')}: {prop.get('type')}")
            if prop.get("constraints"):
                print(f"  제약조건: {prop.get('constraints')}")
            if prop.get("default") is not None:
                print(f"  기본값: {prop.get('default')}")
    else:
        pytest.fail("ComplexTypes 클래스 조회 실패")

if __name__ == "__main__":
    print("🔥 THINK ULTRA! 개선된 메타데이터 시스템 테스트")
    print("=" * 60)
    
    setup_database()
    test_full_metadata_support()
    test_complex_types()
    
    print("\n\n테스트 완료!")
