#!/usr/bin/env python3
"""
Three-way Merge 로직 테스트
실제 DB 없이 로직만 테스트
"""

import asyncio
import sys
sys.path.append('.')

from bff.routers.merge_conflict import _detect_merge_conflicts


async def test_three_way_logic():
    """Three-way merge 로직 테스트"""
    print("\n=== Three-way Merge 로직 테스트 ===\n")
    
    # 테스트 케이스 1: 충돌 없는 경우 (각각 다른 필드 변경)
    print("1. 충돌 없는 경우")
    source_changes = [
        {
            "path": "Product/name",
            "new_value": "Updated Product Name",
            "old_value": "Product Name"
        }
    ]
    target_changes = [
        {
            "path": "Product/price", 
            "new_value": 199.99,
            "old_value": 99.99
        }
    ]
    
    conflicts = await _detect_merge_conflicts(source_changes, target_changes)
    print(f"   충돌 수: {len(conflicts)}")
    
    # 테스트 케이스 2: 충돌 있는 경우 (같은 필드 다른 값으로 변경)
    print("\n2. 충돌 있는 경우 (공통 조상 없음)")
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
    
    conflicts = await _detect_merge_conflicts(source_changes, target_changes)
    print(f"   충돌 수: {len(conflicts)}")
    for conflict in conflicts:
        print(f"   - 경로: {conflict['path']}")
        print(f"   - 타입: {conflict['type']}")
        print(f"   - Three-way: {conflict.get('is_three_way', False)}")
    
    # 테스트 케이스 3: Three-way merge (공통 조상 있음)
    print("\n3. Three-way merge 테스트")
    
    # Mock common ancestor
    common_ancestor = "abc123"
    
    # Source만 변경한 경우 (자동 해결 가능)
    conflicts = await _detect_merge_conflicts(
        source_changes,
        target_changes,
        common_ancestor=common_ancestor
    )
    
    print(f"   충돌 수: {len(conflicts)}")
    for conflict in conflicts:
        print(f"   - Three-way: {conflict.get('is_three_way', False)}")
        print(f"   - 자동 해결 가능: {conflict.get('auto_resolvable', False)}")
        print(f"   - 제안된 해결: {conflict.get('suggested_resolution', 'None')}")
    
    print("\n=== 테스트 완료 ===")


if __name__ == "__main__":
    asyncio.run(test_three_way_logic())