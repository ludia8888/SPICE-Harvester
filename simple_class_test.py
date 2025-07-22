#!/usr/bin/env python3
"""간단한 클래스 생성 테스트"""
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = "spice_test_ontology"

def test_simple_class():
    """가장 간단한 클래스 생성 테스트"""
    print("\n=== 가장 간단한 클래스 생성 테스트 ===")
    
    # 1. 속성이 없는 클래스
    simple_data = {
        "id": "SimpleClass",
        "type": "Class",
        "label": {"en": "Simple Class"}
    }
    
    print(f"전송 데이터:\n{json.dumps(simple_data, indent=2, ensure_ascii=False)}")
    
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=simple_data)
    print(f"응답 상태: {response.status_code}")
    print(f"응답 내용: {response.text}\n")
    
    # 2. STRING 속성만 있는 클래스
    string_data = {
        "id": "StringClass",
        "type": "Class",
        "label": {"en": "String Class"},
        "properties": [
            {
                "name": "test_string",
                "type": "STRING",
                "label": {"en": "Test String"},
                "required": False
            }
        ]
    }
    
    print(f"전송 데이터:\n{json.dumps(string_data, indent=2, ensure_ascii=False)}")
    
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=string_data)
    print(f"응답 상태: {response.status_code}")
    print(f"응답 내용: {response.text}\n")
    
    # 3. INTEGER 속성만 있는 클래스
    integer_data = {
        "id": "IntegerClass", 
        "type": "Class",
        "label": {"en": "Integer Class"},
        "properties": [
            {
                "name": "test_integer",
                "type": "INTEGER",
                "label": {"en": "Test Integer"},
                "required": False
            }
        ]
    }
    
    print(f"전송 데이터:\n{json.dumps(integer_data, indent=2, ensure_ascii=False)}")
    
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=integer_data)
    print(f"응답 상태: {response.status_code}")
    print(f"응답 내용: {response.text}\n")

def test_each_property_type():
    """각 속성 타입을 개별적으로 테스트"""
    print("\n=== 각 속성 타입 개별 테스트 ===")
    
    # DATE만 테스트
    date_data = {
        "id": "DateClass",
        "type": "Class",
        "label": {"en": "Date Class"},
        "properties": [
            {
                "name": "test_date",
                "type": "DATE",
                "label": {"en": "Test Date"},
                "required": False
            }
        ]
    }
    
    print(f"DATE 타입 테스트:")
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=date_data)
    print(f"상태: {response.status_code}, 응답: {response.text[:200]}...\n")
    
    # MONEY만 테스트
    money_data = {
        "id": "MoneyClass",
        "type": "Class",
        "label": {"en": "Money Class"},
        "properties": [
            {
                "name": "test_money",
                "type": "MONEY",
                "label": {"en": "Test Money"},
                "required": False
            }
        ]
    }
    
    print(f"MONEY 타입 테스트:")
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=money_data)
    print(f"상태: {response.status_code}, 응답: {response.text[:200]}...\n")

if __name__ == "__main__":
    print("간단한 클래스 생성 테스트 시작...")
    
    # 데이터베이스 확인
    try:
        response = requests.get(f"{BASE_URL}/database/exists/{DB_NAME}")
        if not response.json().get("exists"):
            print(f"데이터베이스 '{DB_NAME}' 생성 중...")
            requests.post(f"{BASE_URL}/database/create", json={"db_name": DB_NAME})
    except:
        pass
    
    test_simple_class()
    test_each_property_type()