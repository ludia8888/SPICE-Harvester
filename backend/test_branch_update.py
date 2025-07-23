#!/usr/bin/env python3
"""
브랜치에서 온톨로지 업데이트 테스트
브랜치 DB에서 작업하는 방법 확인
"""

import asyncio
import httpx
import json
from datetime import datetime

# 테스트 설정
BFF_URL = "http://localhost:8002"
OMS_URL = "http://localhost:8000"
API_KEY = "test-api-key"


async def test_branch_update():
    """브랜치 업데이트 테스트"""
    print("\n=== 브랜치 업데이트 테스트 ===\n")
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        headers = {"X-API-Key": API_KEY}
        db_name = f"test_branch_{int(datetime.now().timestamp())}"
        
        try:
            # 1. 테스트 데이터베이스 생성
            print(f"1. 테스트 DB 생성: {db_name}")
            response = await client.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Branch update test"},
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 2. Main에 온톨로지 생성
            print("\n2. Main에 온톨로지 생성")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology",
                json={
                    "label": "TestClass",
                    "description": "Test class",
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 3. 브랜치 생성
            print("\n3. Feature 브랜치 생성")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={
                    "branch_name": "feature",
                    "from_branch": "main",
                    "description": "Feature branch"
                },
            )
            print(f"   상태: {response.status_code}")
            
            # 4. 브랜치 DB 확인
            print("\n4. 브랜치 DB 확인")
            branch_db = f"{db_name}_branch_feature"
            
            # 브랜치 DB의 온톨로지 목록 확인
            response = await client.get(
                f"{BFF_URL}/api/v1/database/{branch_db}/ontology",
                headers=headers
            )
            print(f"   브랜치 DB 온톨로지 목록 상태: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"   온톨로지 수: {len(data.get('ontologies', []))}")
            
            # 5. 브랜치 DB에 새 온톨로지 생성 시도
            print("\n5. 브랜치 DB에 새 온톨로지 생성")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{branch_db}/ontology",
                json={
                    "label": "BranchClass",
                    "description": "Branch specific class",
                    "properties": [
                        {"name": "branch_prop", "type": "STRING", "label": "Branch Property"},
                    ]
                },
                headers=headers
            )
            print(f"   새 온톨로지 생성 상태: {response.status_code}")
            if response.status_code != 200:
                print(f"   오류: {response.text[:200]}")
            
            # 6. Main DB 온톨로지 업데이트
            print("\n6. Main DB 온톨로지 업데이트")
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology/TestClass",
                json={
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Updated Name"},
                        {"name": "new_prop", "type": "INTEGER", "label": "New Property"},
                    ]
                },
                headers=headers
            )
            print(f"   Main 업데이트 상태: {response.status_code}")
            
            # 7. diff 테스트
            print("\n7. Diff 테스트")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/diff",
                params={"from_ref": "main", "to_ref": "feature"},
            )
            print(f"   Diff 상태: {response.status_code}")
            if response.status_code != 200:
                print(f"   오류: {response.text[:200]}")
            
            print("\n=== 테스트 완료 ===")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            # 테스트 DB 삭제
            try:
                await client.delete(f"{BFF_URL}/api/v1/databases/{db_name}", headers=headers)
                await client.delete(f"{BFF_URL}/api/v1/databases/{branch_db}", headers=headers)
                print(f"\n테스트 DB 삭제됨")
            except:
                pass


if __name__ == "__main__":
    asyncio.run(test_branch_update())