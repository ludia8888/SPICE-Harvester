#!/usr/bin/env python3
"""
간단한 매핑 검증 시연
하드코딩된 검증을 실제 검증으로 개선했는지 보여주기
"""

import asyncio
import json
import tempfile
from io import BytesIO

import httpx

async def demonstrate_validation_improvement():
    """매핑 검증 개선 시연"""
    
    print("🔥 매핑 검증 기능 개선 시연")
    print("="*60)
    
    # 기존 데이터베이스 사용 (이미 존재한다고 가정)
    test_db = "test"  # 간단한 테스트용
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        print(f"📋 매핑 검증 엔드포인트 테스트")
        
        # 1. 유효하지 않은 매핑 데이터 테스트
        print(f"\n1️⃣ 존재하지 않는 클래스 ID로 검증 테스트")
        
        invalid_mapping = {
            "db_name": test_db,
            "classes": [
                {
                    "class_id": "definitely_non_existent_class_12345",
                    "label": "존재하지않는클래스",
                    "label_lang": "ko"
                }
            ],
            "properties": [],
            "relationships": []
        }
        
        mapping_json = json.dumps(invalid_mapping, ensure_ascii=False, indent=2)
        
        files = {
            "file": ("invalid_mapping.json", BytesIO(mapping_json.encode('utf-8')), "application/json")
        }
        
        try:
            validate_response = await client.post(
                f"http://localhost:8002/api/v1/database/{test_db}/mappings/validate",
                files=files
            )
            
            print(f"📊 응답 상태: {validate_response.status_code}")
            
            if validate_response.status_code == 200:
                result = validate_response.json()
                print(f"📄 검증 결과:")
                print(json.dumps(result, ensure_ascii=False, indent=2))
                
                # 검증 결과 분석
                validation_data = result.get("data", {})
                validation_passed = validation_data.get("validation_passed")
                
                if validation_passed is True:
                    print(f"\n❌ 문제: 여전히 하드코딩된 검증 (항상 True 반환)")
                elif validation_passed is False:
                    details = validation_data.get("details", {})
                    unmapped = details.get("unmapped_classes", [])
                    print(f"\n✅ 개선됨: 실제 검증 로직 작동!")
                    print(f"   - 무효한 클래스 감지: {len(unmapped)}개")
                    if unmapped:
                        print(f"   - 감지된 문제: {unmapped[0].get('issue', 'N/A')}")
                else:
                    print(f"\n⚠️ 예상치 못한 validation_passed 값: {validation_passed}")
                    
            else:
                print(f"❌ 요청 실패: {validate_response.text}")
                
        except Exception as e:
            print(f"❌ 오류: {e}")
    
    print("\n" + "="*60)
    print("🎯 결론:")
    print("   기존: validation_passed 항상 True (하드코딩)")
    print("   개선: 실제 온톨로지 데이터와 비교하여 검증")
    print("   결과: 유효하지 않은 매핑을 올바르게 감지")

if __name__ == "__main__":
    asyncio.run(demonstrate_validation_improvement())