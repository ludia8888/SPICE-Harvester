"""
단일 요청 성능 분석
"""

import asyncio
import httpx
import time

async def test_single_requests():
    """단일 요청 성능 테스트"""
    print("🔍 단일 요청 성능 분석")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # 1. 데이터베이스 목록 조회
        print("\n1. 데이터베이스 목록 조회")
        start = time.time()
        response = await client.get("http://localhost:8000/api/v1/database/list")
        elapsed = time.time() - start
        print(f"   시간: {elapsed:.3f}초, 상태: {response.status_code}")
        
        # 2. 간단한 온톨로지 생성
        print("\n2. 간단한 온톨로지 생성 (속성 없음)")
        start = time.time()
        response = await client.post(
            "http://localhost:8000/api/v1/ontology/load_test_db/create",
            json={
                "id": "SimpleClass",
                "label": "간단한 클래스",
                "description": "속성이 없는 클래스"
            }
        )
        elapsed = time.time() - start
        print(f"   시간: {elapsed:.3f}초, 상태: {response.status_code}")
        if response.status_code != 200:
            print(f"   에러: {response.text[:200]}")
        
        # 3. 복잡한 온톨로지 생성 (속성 3개)
        print("\n3. 복잡한 온톨로지 생성 (속성 3개)")
        start = time.time()
        response = await client.post(
            "http://localhost:8000/api/v1/ontology/load_test_db/create",
            json={
                "id": "ComplexClass",
                "label": "복잡한 클래스",
                "description": "속성이 있는 클래스",
                "properties": [
                    {"name": "prop1", "label": "속성 1", "type": "xsd:string"},
                    {"name": "prop2", "label": "속성 2", "type": "xsd:integer"},
                    {"name": "prop3", "label": "속성 3", "type": "xsd:boolean"}
                ]
            }
        )
        elapsed = time.time() - start
        print(f"   시간: {elapsed:.3f}초, 상태: {response.status_code}")
        if response.status_code != 200:
            print(f"   에러: {response.text[:200]}")
        
        # 4. BFF 데이터베이스 목록 조회
        print("\n4. BFF 데이터베이스 목록 조회")
        start = time.time()
        response = await client.get("http://localhost:8002/api/v1/databases")
        elapsed = time.time() - start
        print(f"   시간: {elapsed:.3f}초, 상태: {response.status_code}")
        
        # 5. TerminusDB 직접 호출 시간 측정
        print("\n5. TerminusDB 직접 호출")
        start = time.time()
        response = await client.get("http://localhost:6364/api/")
        elapsed = time.time() - start
        print(f"   시간: {elapsed:.3f}초, 상태: {response.status_code}")

if __name__ == "__main__":
    asyncio.run(test_single_requests())