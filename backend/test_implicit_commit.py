#!/usr/bin/env python3
"""
암시적 커밋 테스트
TerminusDB v11.x에서 실제 데이터 변경으로 커밋 생성
"""

import asyncio
import httpx
import json
from datetime import datetime

# 테스트 설정
BFF_URL = "http://localhost:8002"
OMS_URL = "http://localhost:8000"
API_KEY = "test-api-key"


async def test_implicit_commits():
    """암시적 커밋 생성 테스트"""
    print("\n=== 암시적 커밋 테스트 ===\n")
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        headers = {"X-API-Key": API_KEY}
        db_name = f"test_implicit_{int(datetime.now().timestamp())}"
        
        try:
            # 1. 테스트 데이터베이스 생성
            print(f"1. 테스트 DB 생성: {db_name}")
            response = await client.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Implicit commit test"},
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 2. 온톨로지 생성 (이것이 첫 번째 암시적 커밋을 생성해야 함)
            print("\n2. 온톨로지 생성으로 첫 번째 커밋")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology",
                json={
                    "label": "Product",
                    "description": "Test product",
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "DECIMAL", "label": "Price"},
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 3. 커밋 히스토리 확인
            print("\n3. 첫 번째 커밋 히스토리 확인")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/history",
                params={"limit": 10},
            )
            if response.status_code == 200:
                history = response.json()
                commits = history.get("commits", [])
                print(f"   총 커밋 수: {len(commits)}")
                for i, commit in enumerate(commits):
                    print(f"   커밋 #{i+1}: {commit.get('identifier', '')[:8]} - {commit.get('message', '')}")
            
            # 4. 직접 문서 API로 데이터 추가 (암시적 커밋 생성)
            print("\n4. 문서 API로 데이터 추가 (암시적 커밋)")
            response = await client.post(
                f"{OMS_URL}/api/document/{db_name}",
                params={
                    "graph_type": "instance",
                    "message": "Add test product data",
                    "author": "test_user"
                },
                json=[{
                    "@type": "Product",
                    "@id": "Product/test1",
                    "name": "Test Product 1",
                    "price": 99.99
                }]
            )
            print(f"   상태: {response.status_code}")
            
            # 5. 온톨로지 업데이트 (두 번째 암시적 커밋)
            print("\n5. 온톨로지 업데이트로 두 번째 커밋")
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology/Product",
                json={
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Product Name"},  # 레이블 변경
                        {"name": "price", "type": "DECIMAL", "label": "Price"},
                        {"name": "status", "type": "STRING", "label": "Status"},  # 새 필드 추가
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 6. 브랜치 생성
            print("\n6. Feature 브랜치 생성")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={
                    "branch_name": "feature",
                    "from_branch": "main",
                    "description": "Feature branch"
                },
            )
            print(f"   상태: {response.status_code}")
            
            # 7. 브랜치 체크아웃 후 변경
            print("\n7. Feature 브랜치에서 변경")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
                json={"branch_name": "feature"},
            )
            print(f"   체크아웃 상태: {response.status_code}")
            
            # Feature 브랜치에서 데이터 추가
            response = await client.post(
                f"{OMS_URL}/api/document/{db_name}",
                params={
                    "graph_type": "instance",
                    "message": "Add product in feature branch",
                    "author": "feature_developer"
                },
                json=[{
                    "@type": "Product",
                    "@id": "Product/feature1",
                    "name": "Feature Product",
                    "price": 199.99
                }]
            )
            print(f"   데이터 추가 상태: {response.status_code}")
            
            # 8. Main 브랜치로 돌아가서 변경
            print("\n8. Main 브랜치에서 변경")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
                json={"branch_name": "main"},
            )
            
            response = await client.post(
                f"{OMS_URL}/api/document/{db_name}",
                params={
                    "graph_type": "instance",
                    "message": "Add product in main branch",
                    "author": "main_developer"
                },
                json=[{
                    "@type": "Product",
                    "@id": "Product/main1",
                    "name": "Main Product",
                    "price": 299.99
                }]
            )
            print(f"   데이터 추가 상태: {response.status_code}")
            
            # 9. 전체 커밋 히스토리 확인
            print("\n9. 전체 커밋 히스토리 확인")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/history",
                params={"limit": 20},
            )
            if response.status_code == 200:
                history = response.json()
                commits = history.get("commits", [])
                print(f"   총 커밋 수: {len(commits)}")
                for i, commit in enumerate(commits):
                    commit_id = commit.get("identifier", '')
                    message = commit.get("message", '')
                    author = commit.get("author", '')
                    print(f"   커밋 #{i+1}: {commit_id[:8]} - '{message}' by {author}")
            
            # 10. 공통 조상 찾기
            print("\n10. 공통 조상 찾기")
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
    asyncio.run(test_implicit_commits())