#!/usr/bin/env python3
"""
Three-way Merge 간단한 테스트
공통 조상 API 테스트에 집중
"""

import asyncio
import httpx
import json
from datetime import datetime

# 테스트 설정
BFF_URL = "http://localhost:8002"
OMS_URL = "http://localhost:8000"
API_KEY = "test-api-key"


async def test_common_ancestor():
    """공통 조상 찾기 API 테스트"""
    print("\n=== 공통 조상 찾기 테스트 ===\n")
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        headers = {"X-API-Key": API_KEY}
        db_name = f"test_ancestor_{int(datetime.now().timestamp())}"
        
        try:
            # 1. 테스트 데이터베이스 생성
            print(f"1. 테스트 DB 생성: {db_name}")
            response = await client.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Common ancestor test"},
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 2. 초기 커밋들 생성
            print("\n2. 초기 커밋 생성")
            
            # 첫 번째 커밋
            response = await client.post(
                f"{OMS_URL}/api/v1/version/{db_name}/commit",
                json={"message": "Initial commit", "author": "test"},
            )
            print(f"   커밋 1 상태: {response.status_code}")
            
            # 두 번째 커밋
            response = await client.post(
                f"{OMS_URL}/api/v1/version/{db_name}/commit",
                json={"message": "Second commit", "author": "test"},
            )
            print(f"   커밋 2 상태: {response.status_code}")
            
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
            print(f"   브랜치 생성 상태: {response.status_code}")
            
            # 4. 각 브랜치에서 추가 커밋
            print("\n4. 각 브랜치에서 커밋 추가")
            
            # feature 브랜치에서 커밋
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
                json={"branch_name": "feature"},
            )
            
            response = await client.post(
                f"{OMS_URL}/api/v1/version/{db_name}/commit",
                json={"message": "Feature commit", "author": "test"},
            )
            print(f"   Feature 브랜치 커밋 상태: {response.status_code}")
            
            # main 브랜치로 돌아가서 커밋
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
                json={"branch_name": "main"},
            )
            
            response = await client.post(
                f"{OMS_URL}/api/v1/version/{db_name}/commit",
                json={"message": "Main branch commit", "author": "test"},
            )
            print(f"   Main 브랜치 커밋 상태: {response.status_code}")
            
            # 5. 커밋 히스토리 확인
            print("\n5. 커밋 히스토리 확인")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/history",
                params={"limit": 10},
            )
            if response.status_code == 200:
                history = response.json()
                commits = history.get("commits", [])
                print(f"   총 커밋 수: {len(commits)}")
                for commit in commits[:5]:
                    print(f"   - {commit.get('identifier', '')[:8]}: {commit.get('message', '')}")
            
            # 6. 공통 조상 찾기
            print("\n6. 공통 조상 찾기 테스트")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/common-ancestor",
                params={"branch1": "main", "branch2": "feature"},
            )
            print(f"   상태: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                data = result.get("data", {})
                ancestor = data.get("common_ancestor")
                print(f"   공통 조상: {ancestor[:8] if ancestor else 'None'}")
                
                if ancestor and data.get("ancestor_info"):
                    info = data.get("ancestor_info", {})
                    print(f"   - 메시지: {info.get('message', '')}")
                    print(f"   - 작성자: {info.get('author', '')}")
            else:
                print(f"   오류: {response.text}")
            
            # 7. 병합 시뮬레이션 테스트
            print("\n7. 병합 시뮬레이션 (공통 조상 포함)")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/merge/simulate",
                json={
                    "source_branch": "feature",
                    "target_branch": "main",
                    "strategy": "auto"
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                data = result.get("data", {}).get("merge_preview", {})
                
                print(f"   병합 가능: {data.get('statistics', {}).get('mergeable', False)}")
                print(f"   변경사항: {data.get('statistics', {}).get('changes_to_apply', 0)}")
                print(f"   충돌: {data.get('statistics', {}).get('conflicts_detected', 0)}")
            
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
    asyncio.run(test_common_ancestor())