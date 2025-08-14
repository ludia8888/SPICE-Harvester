#!/usr/bin/env python3
"""
최종 Three-way Merge 테스트
공통 조상을 사용한 실제 충돌 감지 테스트
"""

import asyncio
import httpx
import json
from datetime import datetime

# 테스트 설정
BFF_URL = "http://localhost:8002"
OMS_URL = "http://localhost:8000"
API_KEY = "test-api-key"


async def test_three_way_final():
    """최종 Three-way merge 테스트"""
    print("\n=== 최종 Three-way Merge 테스트 ===\n")
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        headers = {"X-API-Key": API_KEY}
        db_name = f"test_3way_final_{int(datetime.now().timestamp())}"
        
        try:
            # 1. 테스트 데이터베이스 생성
            print(f"1. 테스트 DB 생성: {db_name}")
            response = await client.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Final three-way merge test"},
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 2. 초기 온톨로지 생성
            print("\n2. 초기 온톨로지 생성")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology",
                json={
                    "label": "Product",
                    "description": "Base product",
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "DECIMAL", "label": "Price"},
                        {"name": "status", "type": "STRING", "label": "Status"},
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 3. 공통 조상이 될 커밋 확인
            print("\n3. 공통 조상 커밋 확인")
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/history",
                params={"limit": 10},
            )
            if response.status_code == 200:
                history = response.json()
                commits = history.get("commits", [])
                print(f"   현재 커밋 수: {len(commits)}")
                if commits:
                    base_commit = commits[0].get("identifier")
                    print(f"   베이스 커밋: {base_commit[:8]}")
            
            # 4. Feature 브랜치 생성
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
            
            # 5. 공통 조상 확인
            print("\n5. 공통 조상 확인")
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
            
            # 6. Main 브랜치에서 변경
            print("\n6. Main 브랜치에서 변경")
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology/Product",
                json={
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Product Name"},  # 레이블 변경
                        {"name": "price", "type": "MONEY", "label": "Price"},  # 타입 변경
                        {"name": "status", "type": "STRING", "label": "Status"},
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 7. Feature 브랜치에서 변경 (브랜치 DB 직접 사용)
            print("\n7. Feature 브랜치에서 변경")
            feature_db = f"{db_name}_branch_feature"
            
            # Feature 브랜치 DB에 직접 온톨로지 업데이트
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{feature_db}/ontology/Product",
                json={
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "DECIMAL", "label": "Product Price"},  # 다른 레이블 변경
                        {"name": "status", "type": "ENUM", "label": "Status"},  # 다른 타입 변경
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 8. 충돌 없는 변경 테스트 위한 브랜치
            print("\n8. 충돌 없는 변경을 위한 브랜치 생성")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={
                    "branch_name": "feature_clean",
                    "from_branch": "main",
                    "description": "Clean feature branch"
                },
            )
            print(f"   브랜치 생성 상태: {response.status_code}")
            
            # Feature_clean 브랜치에서 description만 변경
            clean_db = f"{db_name}_branch_feature_clean"
            response = await client.put(
                f"{BFF_URL}/api/v1/database/{clean_db}/ontology/Product",
                json={
                    "description": "Updated product description",  # description만 변경
                },
                headers=headers
            )
            print(f"   Description 변경 상태: {response.status_code}")
            
            # 9. 병합 시뮬레이션 (충돌 있는 경우)
            print("\n9. 병합 시뮬레이션 - 충돌 있는 경우")
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
                print(f"   - 변경사항: {data.get('statistics', {}).get('changes_to_apply', 0)}")
                print(f"   - 충돌: {len(conflicts)}")
                print(f"   - 자동 병합 가능: {data.get('statistics', {}).get('mergeable', False)}")
                
                if conflicts:
                    print(f"\n   충돌 상세:")
                    for i, conflict in enumerate(conflicts[:3]):  # 최대 3개만 표시
                        print(f"\n   충돌 #{i+1}:")
                        print(f"   - 타입: {conflict.get('type')}")
                        print(f"   - Three-way: {conflict.get('is_three_way', False)}")
                        print(f"   - 자동 해결 가능: {conflict.get('auto_resolvable', False)}")
                        if conflict.get('suggested_resolution'):
                            print(f"   - 제안된 해결책: {conflict.get('suggested_resolution')}")
            else:
                print(f"   오류: {response.status_code}")
                if response.text:
                    print(f"   응답: {response.text}")
            
            # 10. 병합 시뮬레이션 (충돌 없는 경우)
            print("\n10. 병합 시뮬레이션 - 충돌 없는 경우")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/merge/simulate",
                json={
                    "source_branch": "feature_clean",
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
                print(f"   충돌 없는 병합 가능: {len(conflicts) == 0}")
            
            print("\n=== 테스트 완료 ===")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            # 테스트 DB 삭제
            try:
                # Main DB 삭제
                await client.delete(
                    f"{BFF_URL}/api/v1/databases/{db_name}",
                    headers=headers
                )
                # 브랜치 DB들 삭제
                for branch in ["feature", "feature_clean"]:
                    try:
                        await client.delete(
                            f"{BFF_URL}/api/v1/databases/{db_name}_branch_{branch}",
                            headers=headers
                        )
                    except:
                        pass
                print(f"\n테스트 DB 삭제됨")
            except:
                pass


if __name__ == "__main__":
    asyncio.run(test_three_way_final())