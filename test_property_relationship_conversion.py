#!/usr/bin/env python3
"""
온톨로지 속성 생성 방식 통합 테스트
Property → Relationship 자동 변환 및 ObjectProperty 저장 확인
"""
import requests
import json
import os

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = "spice_relationship_test"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or os.getenv("OMS_ADMIN_TOKEN") or "").strip()
HEADERS = {"X-Admin-Token": ADMIN_TOKEN} if ADMIN_TOKEN else {}
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN is required for relationship conversion tests")

def setup_database():
    """테스트용 데이터베이스 생성"""
    # 기존 DB 삭제
    try:
        requests.delete(f"{BASE_URL}/database/{DB_NAME}", headers=HEADERS)
    except:
        pass
    
    # 새 DB 생성
    response = requests.post(
        f"{BASE_URL}/database/create",
        json={"name": DB_NAME},
        headers=HEADERS,
    )
    print(f"데이터베이스 생성: {response.status_code}")

def test_property_to_relationship_conversion():
    """Property → Relationship 자동 변환 테스트"""
    print("\n=== Property → Relationship 자동 변환 테스트 ===")
    
    # 1. Customer 클래스 생성 (참조 대상)
    customer_data = {
        "id": "Customer",
        "type": "Class",
        "label": {"en": "Customer", "ko": "고객"},
        "properties": [
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Name", "ko": "이름"},
                "required": True
            },
            {
                "name": "email",
                "type": "EMAIL",
                "label": {"en": "Email", "ko": "이메일"}
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=customer_data,
        headers=HEADERS,
    )
    print(f"\nCustomer 클래스 생성: {response.status_code}")
    if response.status_code != 200:
        print(f"오류: {response.text}")
        
    # 2. Product 클래스 생성 (참조 대상)
    product_data = {
        "id": "Product",
        "type": "Class",
        "label": {"en": "Product", "ko": "제품"},
        "properties": [
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Product Name", "ko": "제품명"},
                "required": True
            },
            {
                "name": "price",
                "type": "DECIMAL",
                "label": {"en": "Price", "ko": "가격"}
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=product_data,
        headers=HEADERS,
    )
    print(f"Product 클래스 생성: {response.status_code}")
    
    # 2-1. OrderStatus 클래스 생성 (참조 대상)
    status_data = {
        "id": "OrderStatus",
        "type": "Class",
        "label": {"en": "Order Status", "ko": "주문 상태"},
        "properties": [
            {
                "name": "status",
                "type": "STRING",
                "label": {"en": "Status", "ko": "상태"},
                "required": True
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=status_data,
        headers=HEADERS,
    )
    print(f"OrderStatus 클래스 생성: {response.status_code}")
    
    # 2-2. Department 클래스 생성 (참조 대상)
    department_data = {
        "id": "Department",
        "type": "Class",
        "label": {"en": "Department", "ko": "부서"},
        "properties": [
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Department Name", "ko": "부서명"},
                "required": True
            },
            {
                "name": "code",
                "type": "STRING",
                "label": {"en": "Department Code", "ko": "부서 코드"}
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=department_data,
        headers=HEADERS,
    )
    print(f"Department 클래스 생성: {response.status_code}")
    
    # 2-3. Project 클래스 생성 (참조 대상)
    project_data = {
        "id": "Project",
        "type": "Class",
        "label": {"en": "Project", "ko": "프로젝트"},
        "properties": [
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Project Name", "ko": "프로젝트명"},
                "required": True
            },
            {
                "name": "start_date",
                "type": "DATE",
                "label": {"en": "Start Date", "ko": "시작일"}
            },
            {
                "name": "budget",
                "type": "DECIMAL",
                "label": {"en": "Budget", "ko": "예산"}
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=project_data
    )
    print(f"Project 클래스 생성: {response.status_code}")
    
    # 3. Order 클래스 생성 - 클래스 참조 속성 포함
    order_data = {
        "id": "OrderWithRelations",
        "type": "Class",
        "label": {"en": "Order with Relations", "ko": "관계가 있는 주문"},
        "description": {"en": "Order demonstrating property to relationship conversion"},
        "properties": [
            # 일반 속성
            {
                "name": "order_id",
                "type": "STRING",
                "label": {"en": "Order ID", "ko": "주문 번호"},
                "required": True
            },
            # 클래스 참조 속성 (자동으로 ObjectProperty로 변환되어야 함)
            {
                "name": "customer",
                "type": "Customer",  # 클래스 참조
                "label": {"en": "Customer", "ko": "고객"},
                "required": True,
                "isRelationship": True,
                "cardinality": "n:1"
            },
            # 명시적 linkTarget 사용
            {
                "name": "items",
                "type": "link",
                "linkTarget": "Product",
                "label": {"en": "Order Items", "ko": "주문 항목"},
                "cardinality": "n:m"
            }
        ],
        # 명시적 관계도 정의 (중복 테스트)
        "relationships": [
            {
                "predicate": "hasStatus",
                "target": "OrderStatus",
                "label": {"en": "Has Status", "ko": "상태"},
                "cardinality": "n:1"
            }
        ]
    }
    
    print(f"\n전송 데이터:\n{json.dumps(order_data, indent=2, ensure_ascii=False)}")
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=order_data,
        headers=HEADERS,
    )
    
    print(f"\nOrderWithRelations 클래스 생성: {response.status_code}")
    if response.status_code == 200:
        result = response.json()
        print(f"\n생성 결과:")
        print(f"- Properties 개수: {len(result.get('properties', []))}")
        print(f"- Relationships 개수: {len(result.get('relationships', []))}")
        
        # 변환 확인
        print("\n[Properties]")
        for prop in result.get('properties', []):
            print(f"  - {prop['name']}: {prop['type']}")
            
        print("\n[Relationships]")
        for rel in result.get('relationships', []):
            print(f"  - {rel.get('predicate')} → {rel.get('target')} (cardinality: {rel.get('cardinality')})")
    else:
        print(f"오류: {response.text}")

def test_class_validation():
    """클래스 생성 후 스키마 검증"""
    print("\n\n=== 클래스 스키마 검증 ===")
    
    # OrderWithRelations 클래스 조회
    response = requests.get(f"{BASE_URL}/database/{DB_NAME}/ontology/OrderWithRelations", headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        print(f"\nOrderWithRelations 클래스 조회 성공")
        print(f"\n전체 응답 데이터:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        # data가 API 응답 구조인지 확인
        if "data" in data:
            actual_data = data["data"]
        else:
            actual_data = data
            
        print(f"\nProperties: {len(actual_data.get('properties', []))}")
        print(f"Relationships: {len(actual_data.get('relationships', []))}")
        
        # ObjectProperty로 변환되었는지 확인
        if 'customer' not in [p['name'] for p in actual_data.get('properties', [])]:
            print("✅ 'customer' 속성이 property에서 제거됨 (relationship으로 변환됨)")
        else:
            print("❌ 'customer' 속성이 아직 property에 있음")
            
        if 'customer' in [r.get('predicate') for r in actual_data.get('relationships', [])]:
            print("✅ 'customer' 관계가 relationships에 추가됨")
        else:
            print("❌ 'customer' 관계가 relationships에 없음")
    else:
        print(f"클래스 조회 실패: {response.text}")

def test_mixed_approach():
    """혼합 접근 방식 테스트 - Property와 Relationship 동시 사용"""
    print("\n\n=== 혼합 접근 방식 테스트 ===")
    
    # Employee 클래스 생성
    employee_data = {
        "id": "Employee", 
        "type": "Class",
        "label": {"en": "Employee", "ko": "직원"},
        "properties": [
            # 일반 속성
            {
                "name": "employee_id",
                "type": "STRING",
                "label": {"en": "Employee ID", "ko": "직원 ID"},
                "required": True
            },
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Name", "ko": "이름"},
                "required": True
            },
            # 클래스 참조 (자동 변환)
            {
                "name": "department",
                "type": "Department",
                "label": {"en": "Department", "ko": "부서"},
                "cardinality": "n:1"
            },
            {
                "name": "manager",
                "type": "Employee",  # 자기 참조
                "label": {"en": "Manager", "ko": "상사"},
                "cardinality": "n:1"
            }
        ],
        # 명시적 관계
        "relationships": [
            {
                "predicate": "worksOn",
                "target": "Project",
                "label": {"en": "Works On", "ko": "참여 프로젝트"},
                "cardinality": "n:m"
            }
        ]
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=employee_data,
        headers=HEADERS,
    )
    
    print(f"\nEmployee 클래스 생성: {response.status_code}")
    if response.status_code == 200:
        result = response.json()
        print(f"\n최종 구조:")
        print(f"- 일반 속성: {[p['name'] for p in result.get('properties', [])]}")
        print(f"- 관계: {[r['predicate'] for r in result.get('relationships', [])]}")
    else:
        print(f"오류: {response.text}")

def test_query_with_relationships():
    """관계를 포함한 쿼리 테스트"""
    print("\n\n=== 관계를 포함한 쿼리 테스트 ===")
    
    # WOQL 쿼리로 관계 확인
    query = {
        "query": {
            "@type": "Triple",
            "subject": {"@type": "Variable", "name": "Class"},
            "predicate": {"@type": "Variable", "name": "Predicate"},
            "object": {"@type": "Variable", "name": "Target"}
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology/query",
        json=query,
        headers=HEADERS,
    )
    
    if response.status_code == 200:
        print("✅ 관계 쿼리 성공")
        results = response.json()
        print(f"쿼리 결과 개수: {len(results.get('data', []))}")
    else:
        print(f"쿼리 실패: {response.text}")

if __name__ == "__main__":
    print("🔥 THINK ULTRA! 온톨로지 속성 생성 방식 통합 테스트")
    print("=" * 60)
    
    # 데이터베이스 셋업
    setup_database()
    
    # 테스트 실행
    test_property_to_relationship_conversion()
    test_class_validation()
    test_mixed_approach()
    test_query_with_relationships()
    
    print("\n\n테스트 완료!")
    print("TerminusDB 대시보드에서 확인: http://localhost:6363/dashboard/")
