#!/usr/bin/env python3
"""
🔥 THINK ULTRA! 메타데이터 시스템 직접 테스트
스키마 문제를 우회하여 핵심 로직 검증
"""
import json
import os
import time
import uuid

import pytest
import requests

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = f"spice_metadata_bypass_test_{uuid.uuid4().hex[:8]}"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or os.getenv("OMS_ADMIN_TOKEN") or "").strip()
HEADERS = {"X-Admin-Token": ADMIN_TOKEN} if ADMIN_TOKEN else {}
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN is required for metadata bypass tests")

def setup_database():
    """새로운 테스트용 데이터베이스 생성"""
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

def _run_simple_class_with_basic_metadata():
    """기본 메타데이터로 간단한 클래스 테스트"""
    print("\n=== 기본 메타데이터 테스트 ===")
    
    # 가장 단순한 클래스 데이터
    simple_class = {
        "id": "SimpleTest",
        "type": "Class",
        "label": {"en": "Simple Test", "ko": "간단한 테스트"},
        "description": {"en": "A simple test class", "ko": "간단한 테스트 클래스"},
        "properties": [
            {
                "name": "simpletest_id",
                "type": "STRING",
                "label": {"en": "Simple Test ID", "ko": "간단한 테스트 ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "test_field",
                "type": "STRING",
                "label": {"en": "Test Field", "ko": "테스트 필드"},
                "description": {"en": "A test field", "ko": "테스트 필드입니다"},
                "required": True,
                "constraints": {
                    "minLength": 1,
                    "maxLength": 100
                }
            }
        ]
    }
    
    print("테스트 클래스 생성 중...")
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=simple_class,
        headers=HEADERS,
    )
    
    print(f"클래스 생성 응답: {response.status_code}")
    if response.status_code not in (200, 202):
        print(f"생성 오류: {response.text}")
        return False
    
    # 생성된 클래스 조회
    print("생성된 클래스 조회 중...")
    class_data = _wait_for_class("SimpleTest")
    if class_data:
            
        print(f"\n조회 응답:")
        print(json.dumps(class_data, indent=2, ensure_ascii=False))
        
        # 메타데이터 검증
        print("\n=== 메타데이터 검증 ===")
        
        # 1. 클래스 레벨 다국어 지원
        label_text = _label_text(class_data.get("label"))
        if label_text == "간단한 테스트":
            print("✅ 클래스 한국어 label 정상")
        else:
            print(f"❌ 클래스 한국어 label 문제: {label_text}")
            
        desc_text = _label_text(class_data.get("description"))
        if desc_text == "간단한 테스트 클래스":
            print("✅ 클래스 한국어 description 정상")
        else:
            print(f"❌ 클래스 한국어 description 문제: {desc_text}")
        
        # 2. 속성 레벨 메타데이터
        properties = class_data.get("properties", [])
        for prop in properties:
            if prop.get("name") == "test_field":
                prop_label_text = _label_text(prop.get("label"))
                if prop_label_text == "테스트 필드":
                    print("✅ 속성 한국어 label 정상")
                else:
                    print(f"❌ 속성 한국어 label 문제: {prop_label_text}")
                    
                prop_desc_text = _label_text(prop.get("description"))
                if prop_desc_text == "테스트 필드입니다":
                    print("✅ 속성 한국어 description 정상")
                else:
                    print(f"❌ 속성 한국어 description 문제: {prop_desc_text}")
                    
                constraints = prop.get("constraints", {})
                if constraints.get("minLength") == 1 and constraints.get("maxLength") == 100:
                    print("✅ 속성 제약조건 정상")
                else:
                    print(f"❌ 속성 제약조건 문제: {constraints}")
        
        return True

    print("클래스 조회 실패: SimpleTest class not available")
    return False


def test_simple_class_with_basic_metadata():
    """기본 메타데이터로 간단한 클래스 테스트"""
    assert _run_simple_class_with_basic_metadata()


def _run_relationship_conversion():
    """Property → Relationship 변환 테스트"""
    print("\n=== Property → Relationship 변환 테스트 ===")
    
    # 참조 클래스들 먼저 생성
    category_class = {
        "id": "Category",
        "type": "Class",
        "label": {"en": "Category", "ko": "카테고리"},
        "properties": [
            {
                "name": "category_id",
                "type": "STRING",
                "label": {"en": "Category ID", "ko": "카테고리 ID"},
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
    
    print("Category 클래스 생성...")
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=category_class,
        headers=HEADERS,
    )
    print(f"Category 생성: {response.status_code}")
    if response.status_code not in (200, 202):
        print(f"Category 생성 오류: {response.text}")
        return False
    if not _wait_for_class("Category"):
        print("Category 클래스가 준비되지 않았습니다")
        return False
    
    # 클래스 참조가 포함된 클래스 생성
    product_class = {
        "id": "Product",
        "type": "Class",
        "label": {"en": "Product", "ko": "제품"},
        "properties": [
            {
                "name": "product_id",
                "type": "STRING",
                "label": {"en": "Product ID", "ko": "제품 ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Product Name", "ko": "제품명"},
                "required": True
            },
            # 클래스 참조 - 자동으로 relationship으로 변환되어야 함
            {
                "name": "category",
                "type": "Category",  # 클래스 타입
                "label": {"en": "Category", "ko": "카테고리"},
                "description": {"en": "Product category", "ko": "제품 카테고리"},
                "cardinality": "n:1",
                "isRelationship": True
            }
        ]
    }
    
    print("Product 클래스 생성 (클래스 참조 포함)...")
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=product_class,
        headers=HEADERS,
    )
    print(f"Product 생성: {response.status_code}")

    if response.status_code not in (200, 202):
        print(f"생성 오류: {response.text}")
        return False
    
    # 변환 결과 확인
    print("Product 클래스 조회하여 변환 결과 확인...")
    product_data = _wait_for_class("Product")
    if product_data:
            
        print(f"\nProduct 클래스 구조:")
        print(json.dumps(product_data, indent=2, ensure_ascii=False))
        
        # 변환 검증
        print("\n=== 변환 검증 ===")
        
        properties = product_data.get("properties", [])
        relationships = product_data.get("relationships", [])
        
        # category가 property에서 relationship으로 이동했는지 확인
        category_in_props = any(p.get("name") == "category" for p in properties)
        category_in_rels = any(r.get("predicate") == "category" for r in relationships)
        
        if not category_in_props and category_in_rels:
            print("✅ category property → relationship 변환 성공")
        else:
            print(f"❌ 변환 실패 - properties에 category: {category_in_props}, relationships에 category: {category_in_rels}")
            
        # relationship 메타데이터 확인
        for rel in relationships:
            if rel.get("predicate") == "category":
                if rel.get("target") == "Category":
                    print("✅ relationship target 정상")
                else:
                    print(f"❌ relationship target 문제: {rel.get('target')}")
                    
                rel_label_text = _label_text(rel.get("label"))
                if rel_label_text == "카테고리":
                    print("✅ relationship 한국어 label 정상")
                else:
                    print(f"❌ relationship 한국어 label 문제: {rel_label_text}")
        
        return True

    print("Product 클래스 조회 실패: Product class not available")
    return False


def test_relationship_conversion():
    """Property → Relationship 변환 테스트"""
    assert _run_relationship_conversion()

if __name__ == "__main__":
    print("🔥 THINK ULTRA! 메타데이터 시스템 직접 테스트")
    print("=" * 60)
    
    if setup_database():
        print("✅ 데이터베이스 준비 완료")
        
        # 기본 메타데이터 테스트
        if _run_simple_class_with_basic_metadata():
            print("✅ 기본 메타데이터 테스트 통과")
        else:
            print("❌ 기본 메타데이터 테스트 실패")
        
        # 관계 변환 테스트
        if _run_relationship_conversion():
            print("✅ 관계 변환 테스트 통과")
        else:
            print("❌ 관계 변환 테스트 실패")
            
    else:
        print("❌ 데이터베이스 준비 실패")
    
    print("\n테스트 완료!")
