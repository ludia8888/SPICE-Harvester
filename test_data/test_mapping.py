#!/usr/bin/env python3
"""
SPICE HARVESTER 매핑 엔진 테스트 스크립트
목업 데이터로 전체 워크플로우 검증
"""

import asyncio
import csv
import json
import os
from typing import List, Dict, Any

# 테스트 시나리오 1: 고객 데이터 -> 기존 Person 온톨로지 매핑
def create_test_scenario_1():
    """고객 CSV 데이터를 기존 Person 온톨로지에 매핑"""
    
    # 소스 스키마 (CSV 컬럼)
    source_schema = [
        {"name": "customer_id", "type": "xsd:string"},
        {"name": "full_name", "type": "xsd:string"},
        {"name": "email_address", "type": "xsd:string"},
        {"name": "phone_number", "type": "xsd:string"},
        {"name": "birth_date", "type": "xsd:date"},
        {"name": "registration_date", "type": "xsd:dateTime"},
        {"name": "city", "type": "xsd:string"},
        {"name": "country", "type": "xsd:string"},
        {"name": "total_purchases", "type": "xsd:decimal"},
        {"name": "vip_status", "type": "xsd:boolean"},
    ]
    
    # 타겟 온톨로지 스키마
    target_schema = [
        {"name": "id", "type": "xsd:string"},
        {"name": "name", "type": "xsd:string"},
        {"name": "email", "type": "xsd:string"},
        {"name": "phone", "type": "xsd:string"},
        {"name": "dateOfBirth", "type": "xsd:date"},
        {"name": "createdAt", "type": "xsd:dateTime"},
        {"name": "address", "type": "xsd:string"},
        {"name": "isVIP", "type": "xsd:boolean"},
        {"name": "totalSpent", "type": "xsd:decimal"},
    ]
    
    # 샘플 데이터
    sample_data = []
    with open('customers.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sample_data.append(row)
    
    # 타겟 샘플 데이터 (시뮬레이션)
    target_sample_data = [
        {"id": "P001", "name": "김영수", "email": "youngsoo@example.com", "phone": "010-1111-2222", 
         "dateOfBirth": "1980-05-20", "createdAt": "2019-01-15", "address": "서울시 강남구", 
         "isVIP": True, "totalSpent": 500000},
        {"id": "P002", "name": "이미경", "email": "mikyung@gmail.com", "phone": "010-2222-3333",
         "dateOfBirth": "1985-08-15", "createdAt": "2019-03-20", "address": "서울시 서초구",
         "isVIP": False, "totalSpent": 150000},
        {"id": "P003", "name": "박상호", "email": "sangho.park@naver.com", "phone": "010-3333-4444",
         "dateOfBirth": "1990-12-25", "createdAt": "2019-05-10", "address": "경기도 성남시",
         "isVIP": True, "totalSpent": 380000},
    ]
    
    return {
        "name": "Customer to Person Mapping",
        "source_schema": source_schema,
        "target_schema": target_schema,
        "sample_data": sample_data,
        "target_sample_data": target_sample_data,
    }

# 테스트 시나리오 2: 제품 데이터 -> 새로운 온톨로지 생성
def create_test_scenario_2():
    """제품 CSV 데이터로 새로운 온톨로지 생성"""
    
    # 소스 스키마 (CSV 컬럼)
    source_schema = [
        {"name": "productSKU", "type": "xsd:string"},
        {"name": "productName", "type": "xsd:string"},
        {"name": "categoryName", "type": "xsd:string"},
        {"name": "unitPrice", "type": "xsd:decimal"},
        {"name": "stockQuantity", "type": "xsd:integer"},
        {"name": "manufacturer", "type": "xsd:string"},
        {"name": "releaseDate", "type": "xsd:date"},
        {"name": "isActive", "type": "xsd:boolean"},
    ]
    
    # 타겟 온톨로지 스키마 (Product 온톨로지)
    target_schema = [
        {"name": "sku", "type": "xsd:string"},
        {"name": "product_name", "type": "xsd:string"},
        {"name": "category", "type": "xsd:string"},
        {"name": "price", "type": "xsd:decimal"},
        {"name": "inventory_count", "type": "xsd:integer"},
        {"name": "brand", "type": "xsd:string"},
        {"name": "launch_date", "type": "xsd:date"},
        {"name": "status", "type": "xsd:string"},
    ]
    
    # 샘플 데이터
    sample_data = []
    with open('products.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sample_data.append(row)
    
    # 타겟 샘플 데이터 (기존 제품 데이터 시뮬레이션)
    target_sample_data = [
        {"sku": "PRD-001", "product_name": "갤럭시 S23", "category": "스마트폰", 
         "price": 1350000, "inventory_count": 25, "brand": "Samsung", 
         "launch_date": "2023-02-01", "status": "active"},
        {"sku": "PRD-002", "product_name": "아이폰 14", "category": "스마트폰",
         "price": 1450000, "inventory_count": 18, "brand": "Apple",
         "launch_date": "2022-09-16", "status": "active"},
    ]
    
    return {
        "name": "Product Data Mapping",
        "source_schema": source_schema,
        "target_schema": target_schema,
        "sample_data": sample_data,
        "target_sample_data": target_sample_data,
    }

async def test_mapping_engine():
    """매핑 엔진 테스트"""
    import httpx
    
    base_url = "http://localhost:8002/api/v1"
    
    # 테스트 시나리오 준비
    scenarios = [
        create_test_scenario_1(),
        create_test_scenario_2(),
    ]
    
    for scenario in scenarios:
        print(f"\n{'='*60}")
        print(f"Testing: {scenario['name']}")
        print(f"{'='*60}")
        
        # 매핑 제안 요청
        async with httpx.AsyncClient() as client:
            # 먼저 데이터베이스 생성
            db_response = await client.post(
                f"{base_url}/databases",
                json={
                    "name": "test_mapping_db",
                    "description": "Test database for mapping validation"
                }
            )
            
            if db_response.status_code not in [200, 409]:  # 409는 이미 존재
                print(f"❌ 데이터베이스 생성 실패: {db_response.status_code}")
                continue
            
            response = await client.post(
                f"{base_url}/database/test_mapping_db/suggest-mappings",
                json={
                    "source_schema": scenario["source_schema"],
                    "target_schema": scenario["target_schema"],
                    "sample_data": scenario["sample_data"][:5],  # 샘플 5개만
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                
                print(f"\n✅ 매핑 제안 성공!")
                print(f"전체 신뢰도: {result['overall_confidence']:.2f}")
                print(f"매핑된 필드: {result['statistics']['mapped_fields']}/{result['statistics']['total_source_fields']}")
                print(f"높은 신뢰도: {result['statistics']['high_confidence_mappings']}")
                print(f"중간 신뢰도: {result['statistics']['medium_confidence_mappings']}")
                
                print("\n매핑 제안:")
                for mapping in result['mappings']:
                    print(f"\n  {mapping['source_field']} → {mapping['target_field']}")
                    print(f"  신뢰도: {mapping['confidence']:.2f} ({mapping['match_type']})")
                    print(f"  이유: {', '.join(mapping['reasons'])}")
                
                if result['unmapped_source_fields']:
                    print(f"\n매핑되지 않은 소스 필드: {', '.join(result['unmapped_source_fields'])}")
                
                if result['unmapped_target_fields']:
                    print(f"매핑되지 않은 타겟 필드: {', '.join(result['unmapped_target_fields'])}")
                    
            else:
                print(f"\n❌ 매핑 제안 실패: {response.status_code}")
                print(response.text)

async def test_full_workflow():
    """전체 워크플로우 테스트"""
    print("\n" + "="*80)
    print("SPICE HARVESTER 전체 워크플로우 테스트")
    print("="*80)
    
    # 1. 매핑 엔진 테스트
    await test_mapping_engine()
    
    # 2. 타입 추론 테스트
    print("\n\n타입 추론 테스트:")
    print("-"*40)
    
    # CSV 데이터 읽기
    with open('customers.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = [row for row in reader]
    
    # 데이터 분석 요청 시뮬레이션
    data_array = rows[:5]  # 처음 5개 행만
    
    print(f"분석할 컬럼: {', '.join(headers)}")
    print(f"샘플 데이터 수: {len(data_array)}")
    
    # 간단한 타입 추론 결과 시뮬레이션
    print("\n추론된 타입:")
    type_mapping = {
        "customer_id": "xsd:string (ID 패턴)",
        "full_name": "xsd:string (이름 패턴)",
        "email_address": "xsd:string (이메일 패턴)",
        "phone_number": "xsd:string (전화번호 패턴)",
        "birth_date": "xsd:date (날짜 패턴)",
        "registration_date": "xsd:dateTime",
        "city": "xsd:string",
        "country": "xsd:string",
        "total_purchases": "xsd:decimal",
        "vip_status": "xsd:boolean",
    }
    
    for col, dtype in type_mapping.items():
        print(f"  - {col}: {dtype}")

if __name__ == "__main__":
    # 테스트 데이터 디렉토리로 이동
    os.chdir('/Users/isihyeon/Desktop/SPICE HARVESTER/test_data')
    
    # 비동기 함수 실행
    asyncio.run(test_full_workflow())