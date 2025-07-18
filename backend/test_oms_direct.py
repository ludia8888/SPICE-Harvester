#!/usr/bin/env python3
"""
OMS 직접 테스트 - 고급 관계 관리 기능
"""

import httpx
import asyncio
import json
from datetime import datetime

OMS_BASE_URL = "http://localhost:8000"
TEST_DB_NAME = f"test_direct_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# 간단한 테스트 온톨로지
TEST_ONTOLOGY = {
    "id": "TestClass",
    "label": {"ko": "테스트 클래스"},
    "description": {"ko": "테스트용"},
    "properties": [],
    "relationships": []
}


async def create_test_db():
    """테스트 DB 생성"""
    print(f"1. 테스트 DB 생성: {TEST_DB_NAME}")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{OMS_BASE_URL}/api/v1/database/create",
            json={
                "name": TEST_DB_NAME,
                "description": "OMS 직접 테스트"
            }
        )
        print(f"   Status: {response.status_code}")
        if response.status_code != 200:
            print(f"   Response: {response.text}")
        else:
            print("   ✅ DB 생성 성공")
        return response.status_code == 200


async def test_create_advanced():
    """create-advanced 엔드포인트 테스트"""
    print(f"\n2. create-advanced 엔드포인트 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{OMS_BASE_URL}/api/v1/ontology/{TEST_DB_NAME}/create-advanced",
            json=TEST_ONTOLOGY,
            params={
                "auto_generate_inverse": True,
                "validate_relationships": True,
                "check_circular_references": True
            }
        )
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:200]}...")
        return response.status_code == 200


async def test_validate_relationships():
    """validate-relationships 엔드포인트 테스트"""
    print(f"\n3. validate-relationships 엔드포인트 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{OMS_BASE_URL}/api/v1/ontology/{TEST_DB_NAME}/validate-relationships",
            json=TEST_ONTOLOGY
        )
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:200]}...")
        return response.status_code == 200


async def test_detect_circular():
    """detect-circular-references 엔드포인트 테스트"""
    print(f"\n4. detect-circular-references 엔드포인트 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{OMS_BASE_URL}/api/v1/ontology/{TEST_DB_NAME}/detect-circular-references",
            json={}
        )
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:200]}...")
        return response.status_code == 200


async def test_analyze_network():
    """analyze-network 엔드포인트 테스트"""
    print(f"\n5. analyze-network 엔드포인트 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{OMS_BASE_URL}/api/v1/ontology/{TEST_DB_NAME}/analyze-network"
        )
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:200]}...")
        return response.status_code == 200


async def cleanup():
    """테스트 DB 삭제"""
    print(f"\n6. 정리: DB 삭제")
    
    async with httpx.AsyncClient() as client:
        response = await client.delete(
            f"{OMS_BASE_URL}/api/v1/database/{TEST_DB_NAME}"
        )
        print(f"   Status: {response.status_code}")


async def main():
    print("🔥 OMS 직접 테스트 시작")
    print("=" * 60)
    
    # DB 생성
    if not await create_test_db():
        print("❌ DB 생성 실패")
        return
    
    # 엔드포인트 테스트
    await test_create_advanced()
    await test_validate_relationships()
    await test_detect_circular()
    await test_analyze_network()
    
    # 정리
    await cleanup()
    
    print("\n" + "=" * 60)
    print("테스트 완료")


if __name__ == "__main__":
    asyncio.run(main())