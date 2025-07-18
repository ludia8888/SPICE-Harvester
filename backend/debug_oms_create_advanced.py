#!/usr/bin/env python3
"""
OMS create-advanced 디버깅
"""

import httpx
import asyncio
import json
import traceback

OMS_BASE_URL = "http://localhost:8000"


async def test_create_advanced_debug():
    """create-advanced 상세 디버깅"""
    
    # 1. 테스트 DB 생성
    test_db = f"debug_db_{int(asyncio.get_event_loop().time() * 1000) % 1000000}"
    print(f"1. 테스트 DB 생성: {test_db}")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{OMS_BASE_URL}/api/v1/database/create",
            json={"name": test_db, "description": "디버깅용"}
        )
        if response.status_code != 200:
            print(f"DB 생성 실패: {response.status_code}")
            return
    
    # 2. create-advanced 호출
    print("\n2. create-advanced 호출")
    
    test_ontology = {
        "id": "DebugClass",
        "label": {"ko": "디버그 클래스"},
        "description": {"ko": "디버깅용"},
        "properties": [
            {
                "name": "test_prop",
                "type": "xsd:string",
                "label": {"ko": "테스트 속성"}
            }
        ],
        "relationships": []
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{OMS_BASE_URL}/api/v1/ontology/{test_db}/create-advanced",
                json=test_ontology,
                params={
                    "auto_generate_inverse": False,
                    "validate_relationships": False,
                    "check_circular_references": False
                }
            )
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text}")
            
            # 헤더 정보
            print(f"\nResponse Headers:")
            for key, value in response.headers.items():
                print(f"  {key}: {value}")
                
    except Exception as e:
        print(f"에러 발생: {type(e).__name__}: {e}")
        traceback.print_exc()
    
    # 3. 정리
    print(f"\n3. DB 삭제")
    async with httpx.AsyncClient() as client:
        await client.delete(f"{OMS_BASE_URL}/api/v1/database/{test_db}")


if __name__ == "__main__":
    asyncio.run(test_create_advanced_debug())