#!/usr/bin/env python3
"""
Three-way Merge 간단한 테스트
브랜치 DB 업데이트 없이 merge 로직만 테스트
"""

import asyncio
import httpx
import json
from datetime import datetime

# 테스트 설정
BFF_URL = "http://localhost:8002"
OMS_URL = "http://localhost:8000"
API_KEY = "test-api-key"


async def test_three_way_simplified():
    """Three-way merge 간단한 테스트"""
    print("\n=== Three-way Merge 간단한 테스트 ===\n")
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        headers = {"X-API-Key": API_KEY}
        db_name = f"test_3way_simple_{int(datetime.now().timestamp())}"
        
        try:
            # 1. 테스트 데이터베이스 생성
            print(f"1. 테스트 DB 생성: {db_name}")
            response = await client.post(
                f"{BFF_URL}/api/v1/databases",
                json={"name": db_name, "description": "Simple three-way merge test"},
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 2. 초기 온톨로지 생성 (공통 조상이 될 상태)
            print("\n2. 초기 온톨로지 생성")
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/ontology",
                json={
                    "label": "Product",
                    "description": "Base product",
                    "properties": [
                        {"name": "name", "type": "STRING", "label": "Name"},
                        {"name": "price", "type": "DECIMAL", "label": "Price"},
                    ]
                },
                headers=headers
            )
            print(f"   상태: {response.status_code}")
            
            # 3. Feature 브랜치 생성
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
            
            # 4. Develop 브랜치 생성
            print("\n4. Develop 브랜치 생성")
            response = await client.post(
                f"{OMS_URL}/api/v1/branch/{db_name}/create",
                json={
                    "branch_name": "develop",
                    "from_branch": "main",
                    "description": "Develop branch"
                },
            )
            print(f"   상태: {response.status_code}")
            
            # 5. 공통 조상 확인
            print("\n5. 공통 조상 확인")
            
            # Feature vs Main
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/common-ancestor",
                params={"branch1": "feature", "branch2": "main"},
            )
            if response.status_code == 200:
                result = response.json()
                ancestor = result.get("data", {}).get("common_ancestor")
                print(f"   Feature-Main 공통 조상: {ancestor[:8] if ancestor else 'None'}")
            
            # Feature vs Develop
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/common-ancestor",
                params={"branch1": "feature", "branch2": "develop"},
            )
            if response.status_code == 200:
                result = response.json()
                ancestor = result.get("data", {}).get("common_ancestor")
                print(f"   Feature-Develop 공통 조상: {ancestor[:8] if ancestor else 'None'}")
            
            # 6. Diff 테스트
            print("\n6. Diff 테스트")
            
            # Main vs Feature (변경사항 없음)
            response = await client.get(
                f"{OMS_URL}/api/v1/version/{db_name}/diff",
                params={"from_ref": "main", "to_ref": "feature"},
            )
            print(f"   Main-Feature diff 상태: {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                changes = result.get("changes", [])
                print(f"   변경사항 수: {len(changes)}")
            
            # 7. 병합 시뮬레이션
            print("\n7. 병합 시뮬레이션")
            
            # Feature -> Main (변경사항 없음)
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/merge/simulate",
                json={
                    "source_branch": "feature",
                    "target_branch": "main",
                    "strategy": "auto"
                },
                headers=headers
            )
            print(f"   Feature->Main 병합 시뮬레이션 상태: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                data = result.get("data", {}).get("merge_preview", {})
                stats = data.get("statistics", {})
                print(f"   - 변경사항: {stats.get('changes_to_apply', 0)}")
                print(f"   - 충돌: {stats.get('conflicts_detected', 0)}")
                print(f"   - 자동 병합 가능: {stats.get('mergeable', False)}")
            
            # Develop -> Feature (변경사항 없음)
            response = await client.post(
                f"{BFF_URL}/api/v1/database/{db_name}/merge/simulate",
                json={
                    "source_branch": "develop",
                    "target_branch": "feature",
                    "strategy": "auto"
                },
                headers=headers
            )
            print(f"\n   Develop->Feature 병합 시뮬레이션 상태: {response.status_code}")
            
            # 8. Three-way merge 로직 테스트
            print("\n8. Three-way merge 충돌 테스트")
            
            # 직접 _detect_merge_conflicts 호출 (가상의 변경사항으로)
            from bff.routers.merge_conflict import _detect_merge_conflicts
            
            # 테스트 케이스: 같은 필드를 다르게 변경
            source_changes = [
                {
                    "path": "Product/price",
                    "new_value": 149.99,
                    "old_value": 99.99
                }
            ]
            target_changes = [
                {
                    "path": "Product/price", 
                    "new_value": 199.99,
                    "old_value": 99.99
                }
            ]
            
            # 공통 조상 있을 때
            conflicts = await _detect_merge_conflicts(
                source_changes,
                target_changes,
                common_ancestor="dummy_ancestor"
            )
            
            print(f"   충돌 감지됨: {len(conflicts)}")
            if conflicts:
                conflict = conflicts[0]
                print(f"   - 경로: {conflict['path']}")
                print(f"   - Three-way: {conflict.get('is_three_way', False)}")
                print(f"   - 충돌 타입: {conflict.get('type')}")
            
            print("\n=== 테스트 완료 ===")
            
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            # 테스트 DB 삭제
            try:
                await client.delete(f"{BFF_URL}/api/v1/databases/{db_name}", headers=headers)
                # 브랜치 DB들도 삭제
                for branch in ["feature", "develop"]:
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
    asyncio.run(test_three_way_simplified())