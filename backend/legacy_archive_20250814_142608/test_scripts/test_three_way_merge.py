#!/usr/bin/env python3
"""
Three-way Merge 테스트
공통 조상 기반 충돌 감지 테스트
"""

import asyncio
import httpx
import json
from datetime import datetime

# 테스트 설정
BFF_URL = "http://localhost:8002"
OMS_URL = "http://localhost:8000"
API_KEY = "test-api-key"


async def test_three_way_merge():
    """Three-way merge 기능 테스트"""
    print("\n=== Three-way Merge 테스트 시작 ===\n")
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        headers = {"X-API-Key": API_KEY}
        db_name = f"test_3way_merge_{int(datetime.now().timestamp())}"
        
        try:
            # 1. 테스트 데이터베이스 생성
            print(f"1. 테스트 DB 생성: {db_name}")
            response = await client.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Three-way merge test"},
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 2. 초기 온톨로지 생성
            print("\n2. 초기 온톨로지 생성")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology",
                json={
                    "label": "Product",
                    "description": "Test product",
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "DECIMAL", "label": "Price"},
                        {"name": "status", "type": "STRING", "label": "Status"},
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 3. 초기 커밋 (공통 조상이 될 커밋)
            print("\n3. 초기 커밋 생성")
            response = await client.post(
                f"{OMS_URL}/api/v1/version/{db_name}/commit",
                json={"message": "Initial commit", "author": "test"},
            )
            print(f"   상태: {response.status_code}")
            
            # 4. feature 브랜치 생성
            print("\n4. Feature 브랜치 생성")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={
                    "branch_name": "feature",
                    "from_branch": "main",
                    "description": "Feature branch"
                },
            )
            print(f"   상태: {response.status_code}")
            
            # 5. feature 브랜치에서 변경
            print("\n5. Feature 브랜치에서 price 변경")
            # 브랜치 체크아웃
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
                json={"branch_name": "feature"},
            )
            
            # price 프로퍼티 업데이트
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology/Product",
                json={
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "MONEY", "label": "Price Updated"},  # 변경
                        {"name": "status", "type": "STRING", "label": "Status"},
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 커밋
            response = await client.post(
                f"{OMS_URL}/api/v1/version/{db_name}/commit",
                json={"message": "Update price type to MONEY", "author": "test"},
            )
            
            # 6. main 브랜치로 돌아가서 다른 변경
            print("\n6. Main 브랜치에서 status 변경")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
                json={"branch_name": "main"},
            )
            
            # status 프로퍼티 업데이트
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology/Product",
                json={
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "DECIMAL", "label": "Price"},
                        {"name": "status", "type": "ENUM", "label": "Status Updated"},  # 변경
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 커밋
            response = await client.post(
                f"{OMS_URL}/api/v1/version/{db_name}/commit",
                json={"message": "Update status type to ENUM", "author": "test"},
            )
            
            # 7. 공통 조상 찾기
            print("\n7. 공통 조상 찾기")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/common-ancestor",
                params={"branch1": "main", "branch2": "feature"},
            )
            print(f"   상태: {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                ancestor = result.get("data", {}).get("common_ancestor")
                print(f"   공통 조상: {ancestor[:8] if ancestor else 'None'}")
            
            # 8. 병합 시뮬레이션 (Three-way merge)
            print("\n8. Three-way 병합 시뮬레이션")
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
                conflicts = data.get("conflicts", [])
                
                print(f"\n   병합 통계:")
                print(f"   - 적용할 변경사항: {data.get('statistics', {}).get('changes_to_apply', 0)}")
                print(f"   - 감지된 충돌: {len(conflicts)}")
                print(f"   - 자동 병합 가능: {data.get('statistics', {}).get('mergeable', False)}")
                
                if conflicts:
                    print(f"\n   충돌 상세:")
                    for i, conflict in enumerate(conflicts):
                        print(f"\n   충돌 #{i+1}:")
                        print(f"   - 경로: {conflict.get('path', {}).get('human_readable', 'Unknown')}")
                        print(f"   - 유형: {conflict.get('type')}")
                        print(f"   - Three-way 병합: {conflict.get('is_three_way', False)}")
                        print(f"   - 자동 해결 가능: {conflict.get('resolution', {}).get('auto_resolvable', False)}")
                        
                        if conflict.get('is_three_way'):
                            print(f"   - 공통 조상: {conflict.get('common_ancestor', 'None')[:8]}")
                
            # 9. 충돌이 없는 병합 테스트 (다른 필드 변경)
            print("\n\n9. 충돌 없는 Three-way 병합 테스트")
            
            # 새로운 브랜치 생성
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={
                    "branch_name": "feature2",
                    "from_branch": "main",
                    "description": "Feature2 branch"
                },
            )
            
            # feature2에서 description만 변경
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/checkout",
                json={"branch_name": "feature2"},
            )
            
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology/Product",
                json={
                    "description": "Updated product description",  # description만 변경
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "DECIMAL", "label": "Price"},
                        {"name": "status", "type": "ENUM", "label": "Status Updated"},
                    ]
                },
                headers=headers
            )
            
            # 병합 시뮬레이션
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/merge/simulate",
                json={
                    "source_branch": "feature2",
                    "target_branch": "main",
                    "strategy": "auto"
                },
                headers=headers
            )
            
            if response.status_code == 200:
                result = response.json()
                data = result.get("data", {}).get("merge_preview", {})
                conflicts = data.get("conflicts", [])
                print(f"   충돌 없는 병합 가능: {len(conflicts) == 0}")
            
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
    asyncio.run(test_three_way_merge())