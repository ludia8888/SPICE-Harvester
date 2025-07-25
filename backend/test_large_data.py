"""
대용량 데이터 처리 테스트
"""

import asyncio
import httpx
import time
import json
from typing import List, Dict
import random

async def test_large_data_processing():
    print("🔍 대용량 데이터 처리 테스트")
    
    async with httpx.AsyncClient(timeout=300.0) as client:
        # 테스트용 데이터베이스 생성
        db_name = f"large_data_db_{int(time.time())}"
        print(f"\n1️⃣ 테스트 데이터베이스 생성: {db_name}")
        
        response = await client.post(
            "http://localhost:8000/api/v1/database/create",
            json={"name": db_name, "description": "대용량 데이터 테스트"}
        )
        if response.status_code != 201:
            print(f"❌ 데이터베이스 생성 실패: {response.text}")
            return False
        
        # 1. 대용량 속성을 가진 온톨로지 생성
        print("\n2️⃣ 대용량 속성 온톨로지 생성 (100개 속성)")
        
        large_properties = []
        for i in range(100):
            prop_type = random.choice(["xsd:string", "xsd:integer", "xsd:decimal", "xsd:boolean", "xsd:dateTime"])
            large_properties.append({
                "name": f"property_{i}",
                "label": f"속성 {i}",
                "type": prop_type,
                "description": f"테스트 속성 {i}번 - 대용량 데이터 처리 검증용",
                "required": i < 10,  # 처음 10개만 필수
                "constraints": {
                    "min_length": 1 if prop_type == "xsd:string" else None,
                    "max_length": 1000 if prop_type == "xsd:string" else None,
                    "min": 0 if prop_type in ["xsd:integer", "xsd:decimal"] else None,
                    "max": 999999 if prop_type in ["xsd:integer", "xsd:decimal"] else None
                }
            })
        
        start_time = time.time()
        response = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json={
                "id": "LargePropertyClass",
                "label": "대용량 속성 클래스",
                "description": "100개의 속성을 가진 테스트 클래스",
                "properties": large_properties
            }
        )
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            print(f"✅ 성공 (소요시간: {elapsed:.2f}초)")
        else:
            print(f"❌ 실패: {response.status_code} - {response.text[:200]}")
            return False
        
        # 2. 복잡한 관계를 가진 온톨로지 생성
        print("\n3️⃣ 복잡한 관계 온톨로지 생성 (50개 관계)")
        
        # 먼저 대상 클래스들 생성
        target_classes = []
        for i in range(10):
            response = await client.post(
                f"http://localhost:8000/api/v1/ontology/{db_name}/create",
                json={
                    "id": f"TargetClass_{i}",
                    "label": f"대상 클래스 {i}",
                    "description": f"관계의 대상이 되는 클래스 {i}"
                }
            )
            if response.status_code == 200:
                target_classes.append(f"TargetClass_{i}")
        
        # 복잡한 관계들 생성
        relationships = []
        for i in range(50):
            target = random.choice(target_classes)
            cardinality = random.choice(["1:1", "1:n", "n:1", "n:n"])
            relationships.append({
                "predicate": f"has_relation_{i}",
                "label": f"관계 {i}",
                "target": target,
                "cardinality": cardinality,
                "description": f"복잡한 관계 {i} - {cardinality} 관계",
                "constraints": {
                    "min_cardinality": 0,
                    "max_cardinality": 100 if "n" in cardinality else 1
                }
            })
        
        start_time = time.time()
        response = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json={
                "id": "ComplexRelationshipClass",
                "label": "복잡한 관계 클래스",
                "description": "50개의 관계를 가진 테스트 클래스",
                "relationships": relationships
            }
        )
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            print(f"✅ 성공 (소요시간: {elapsed:.2f}초)")
        else:
            print(f"❌ 실패: {response.status_code} - {response.text[:200]}")
        
        # 3. 깊은 상속 구조 생성
        print("\n4️⃣ 깊은 상속 구조 생성 (10단계)")
        
        parent_class = None
        for level in range(10):
            class_id = f"InheritanceLevel_{level}"
            response = await client.post(
                f"http://localhost:8000/api/v1/ontology/{db_name}/create",
                json={
                    "id": class_id,
                    "label": f"상속 레벨 {level}",
                    "description": f"상속 구조 테스트 - 레벨 {level}",
                    "parent_class": parent_class,
                    "properties": [
                        {
                            "name": f"level_{level}_prop",
                            "label": f"레벨 {level} 속성",
                            "type": "xsd:string"
                        }
                    ]
                }
            )
            if response.status_code == 200:
                parent_class = class_id
                print(f"  → 레벨 {level} 생성 완료")
            else:
                print(f"  → 레벨 {level} 실패: {response.status_code}")
        
        # 4. 대량 온톨로지 목록 조회
        print("\n5️⃣ 대량 온톨로지 목록 조회")
        
        start_time = time.time()
        response = await client.get(f"http://localhost:8000/api/v1/ontology/{db_name}/list")
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            ontology_count = len(data.get("data", {}).get("ontologies", []))
            print(f"✅ 성공: {ontology_count}개 온톨로지 조회 (소요시간: {elapsed:.2f}초)")
        else:
            print(f"❌ 실패: {response.status_code}")
        
        # 5. 복잡한 쿼리 테스트
        print("\n6️⃣ 복잡한 쿼리 실행")
        
        # 대량 필터 쿼리
        filters = []
        for i in range(20):
            filters.append({
                "field": f"property_{i}",
                "operator": "eq",
                "value": f"test_value_{i}"
            })
        
        start_time = time.time()
        response = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/query",
            json={
                "class_id": "LargePropertyClass",
                "filters": filters,
                "limit": 100,
                "offset": 0
            }
        )
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            print(f"✅ 복잡한 쿼리 성공 (소요시간: {elapsed:.2f}초)")
        else:
            print(f"❌ 복잡한 쿼리 실패: {response.status_code}")
        
        # 6. 대용량 JSON-LD 변환 테스트
        print("\n7️⃣ 대용량 JSON-LD 변환 테스트")
        
        # 복잡한 구조의 온톨로지 조회
        start_time = time.time()
        response = await client.get(f"http://localhost:8000/api/v1/ontology/{db_name}/ComplexRelationshipClass")
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            json_size = len(json.dumps(data))
            print(f"✅ JSON-LD 변환 성공 (크기: {json_size/1024:.1f}KB, 소요시간: {elapsed:.2f}초)")
        else:
            print(f"❌ JSON-LD 변환 실패: {response.status_code}")
        
        # 정리
        print("\n8️⃣ 테스트 데이터베이스 정리")
        try:
            await client.delete(f"http://localhost:8000/api/v1/database/{db_name}")
            print("✅ 정리 완료")
        except:
            pass
        
        print("\n✅ 대용량 데이터 처리 테스트 완료!")
        return True

if __name__ == "__main__":
    asyncio.run(test_large_data_processing())