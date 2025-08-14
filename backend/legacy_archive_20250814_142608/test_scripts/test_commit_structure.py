#!/usr/bin/env python3
"""
커밋 구조 확인 테스트
커밋 히스토리의 실제 구조를 확인
"""

import asyncio
import httpx
import json
from datetime import datetime

# 테스트 설정
OMS_URL = "http://localhost:8000"
BFF_URL = "http://localhost:8002"
API_KEY = "test-api-key"


async def test_commit_structure():
    """커밋 구조 확인"""
    print("\n=== 커밋 구조 확인 테스트 ===\n")
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        headers = {"X-API-Key": API_KEY}
        db_name = f"test_structure_{int(datetime.now().timestamp())}"
        
        try:
            # 1. 테스트 데이터베이스 생성
            print(f"1. 테스트 DB 생성: {db_name}")
            response = await client.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Commit structure test"},
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 2. 온톨로지 생성으로 커밋 생성
            print("\n2. 온톨로지 생성")
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
            
            # 3. 커밋 히스토리 상세 확인
            print("\n3. 커밋 히스토리 상세 구조")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/history",
                params={"limit": 10},
            )
            
            if response.status_code == 200:
                history = response.json()
                commits = history.get("commits", [])
                print(f"   총 커밋 수: {len(commits)}")
                
                if commits:
                    print("\n   첫 번째 커밋 전체 구조:")
                    print(json.dumps(commits[0], indent=2))
                    
                    print("\n   모든 커밋의 키 확인:")
                    for i, commit in enumerate(commits):
                        print(f"\n   커밋 #{i+1} 키들: {list(commit.keys())}")
                        # identifier가 없으면 다른 필드 확인
                        commit_id = commit.get("identifier") or commit.get("id") or commit.get("commit") or commit.get("@id")
                        print(f"   커밋 ID 후보: {commit_id}")
            
            # 4. 직접 log API 호출
            print("\n4. 직접 log API 호출")
            response = await client.get(
                f"{OMS_URL}/api/log/{client._base_url.host}/{db_name}",
                params={"limit": 10},
            )
            
            if response.status_code == 200:
                log_data = response.json()
                print(f"   Log API 응답 타입: {type(log_data)}")
                if isinstance(log_data, list) and log_data:
                    print("\n   첫 번째 로그 엔트리:")
                    print(json.dumps(log_data[0], indent=2))
            else:
                print(f"   Log API 응답 실패: {response.status_code}")
            
            print("\n=== 테스트 완료 ===")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            # 테스트 DB 삭제
            try:
                await client.delete(
                    f"{BFF_URL}/api/v1/databases/{db_name}",
                    headers=headers
                )
                print(f"\n테스트 DB 삭제됨: {db_name}")
            except:
                pass


if __name__ == "__main__":
    asyncio.run(test_commit_structure())