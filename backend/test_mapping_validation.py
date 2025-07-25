#!/usr/bin/env python3
"""
매핑 검증 기능 테스트
하드코딩된 검증 로직을 실제 검증으로 개선했는지 확인
"""

import asyncio
import json
import logging
import tempfile
from io import BytesIO

import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BFF_BASE_URL = "http://localhost:8002/api/v1"
OMS_BASE_URL = "http://localhost:8000"

async def test_mapping_validation():
    """매핑 검증 기능 테스트"""
    
    print("🔍 매핑 검증 기능 테스트 시작")
    
    # 1. 테스트 데이터베이스 생성
    test_db = f"validation_test_{int(asyncio.get_event_loop().time())}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            # 1-1. 데이터베이스 생성 (OMS 직접 사용)
            print(f"\n📋 1. 테스트 데이터베이스 생성: {test_db}")
            db_response = await client.post(
                f"{OMS_BASE_URL}/database/{test_db}",
                json={"label": test_db, "description": "Validation test database"}
            )
            if db_response.status_code not in [200, 201]:
                print(f"❌ 데이터베이스 생성 실패: {db_response.status_code} - {db_response.text}")
                return False
            print(f"✅ 데이터베이스 생성 성공")
            
            # 1-2. 테스트용 온톨로지 생성
            print(f"\n📋 2. 테스트 온톨로지 생성")
            ontology_data = {
                "label": {"ko": "테스트 상품", "en": "Test Product"},
                "description": {"ko": "검증 테스트용 상품", "en": "Product for validation test"},
                "properties": [
                    {"name": "name", "type": "string", "description": {"ko": "상품명"}},
                    {"name": "price", "type": "float", "description": {"ko": "가격"}}
                ]
            }
            
            ont_response = await client.post(
                f"{OMS_BASE_URL}/database/{test_db}/ontology",
                json=ontology_data
            )
            if ont_response.status_code not in [200, 201]:
                print(f"❌ 온톨로지 생성 실패: {ont_response.status_code} - {ont_response.text}")
                return False
            
            ont_result = ont_response.json()
            if ont_result.get("status") != "success":
                print(f"❌ 온톨로지 생성 응답 오류: {ont_result}")
                return False
                
            created_ontology = ont_result.get("data", {})
            class_id = created_ontology.get("id")
            if not class_id:
                print(f"❌ 생성된 온톨로지 ID를 찾을 수 없음")
                return False
                
            print(f"✅ 온톨로지 생성 성공 (ID: {class_id})")
            
            # 2. 유효한 매핑 데이터 검증 테스트
            print(f"\n📋 3. 유효한 매핑 데이터 검증 테스트")
            valid_mapping = {
                "db_name": test_db,
                "classes": [
                    {
                        "class_id": class_id,
                        "label": "상품",
                        "label_lang": "ko"
                    }
                ],
                "properties": [],
                "relationships": []
            }
            
            # 임시 파일 생성
            mapping_json = json.dumps(valid_mapping, ensure_ascii=False, indent=2)
            
            # 파일 업로드로 검증
            files = {
                "file": ("valid_mapping.json", BytesIO(mapping_json.encode('utf-8')), "application/json")
            }
            
            validate_response = await client.post(
                f"{BFF_BASE_URL}/database/{test_db}/mappings/validate",
                files=files
            )
            
            if validate_response.status_code != 200:
                print(f"❌ 검증 요청 실패: {validate_response.status_code} - {validate_response.text}")
                return False
            
            validation_result = validate_response.json()
            print(f"📊 검증 결과: {json.dumps(validation_result, ensure_ascii=False, indent=2)}")
            
            # 결과 분석
            if validation_result.get("status") == "success":
                validation_data = validation_result.get("data", {})
                validation_passed = validation_data.get("validation_passed", False)
                
                if validation_passed:
                    print(f"✅ 유효한 매핑 검증 성공")
                else:
                    details = validation_data.get("details", {})
                    print(f"❌ 유효한 매핑이 실패로 검증됨")
                    print(f"   - unmapped_classes: {details.get('unmapped_classes', [])}")
                    print(f"   - unmapped_properties: {details.get('unmapped_properties', [])}")
                    print(f"   - conflicts: {details.get('conflicts', [])}")
                    return False
            else:
                print(f"❌ 검증 응답 상태 오류: {validation_result.get('status')}")
                return False
            
            # 3. 무효한 매핑 데이터 검증 테스트
            print(f"\n📋 4. 무효한 매핑 데이터 검증 테스트")
            invalid_mapping = {
                "db_name": test_db,
                "classes": [
                    {
                        "class_id": "non_existent_class_id",  # 존재하지 않는 클래스 ID
                        "label": "존재하지않는상품",
                        "label_lang": "ko"
                    }
                ],
                "properties": [],
                "relationships": []
            }
            
            invalid_mapping_json = json.dumps(invalid_mapping, ensure_ascii=False, indent=2)
            
            files = {
                "file": ("invalid_mapping.json", BytesIO(invalid_mapping_json.encode('utf-8')), "application/json")
            }
            
            invalid_validate_response = await client.post(
                f"{BFF_BASE_URL}/database/{test_db}/mappings/validate",
                files=files
            )
            
            if invalid_validate_response.status_code != 200:
                print(f"❌ 무효한 매핑 검증 요청 실패: {invalid_validate_response.status_code}")
                return False
            
            invalid_validation_result = invalid_validate_response.json()
            print(f"📊 무효한 매핑 검증 결과: {json.dumps(invalid_validation_result, ensure_ascii=False, indent=2)}")
            
            # 무효한 매핑이 올바르게 실패로 검증되는지 확인
            if invalid_validation_result.get("status") in ["warning", "error"]:
                validation_data = invalid_validation_result.get("data", {})
                validation_passed = validation_data.get("validation_passed", True)
                
                if not validation_passed:
                    details = validation_data.get("details", {})
                    unmapped_classes = details.get("unmapped_classes", [])
                    
                    if len(unmapped_classes) > 0:
                        print(f"✅ 무효한 매핑이 올바르게 실패로 검증됨")
                        print(f"   - 감지된 무효한 클래스: {unmapped_classes[0].get('class_id')}")
                    else:
                        print(f"❌ 무효한 매핑의 문제점이 올바르게 감지되지 않음")
                        return False
                else:
                    print(f"❌ 무효한 매핑이 성공으로 잘못 검증됨")
                    return False
            else:
                print(f"❌ 무효한 매핑 검증 응답 상태 오류: {invalid_validation_result.get('status')}")
                return False
            
            print(f"\n🎉 매핑 검증 기능 테스트 성공!")
            print(f"   ✅ 하드코딩된 'validation_passed: True'가 실제 검증 로직으로 교체됨")
            print(f"   ✅ 유효한 매핑: 올바르게 성공으로 검증")
            print(f"   ✅ 무효한 매핑: 올바르게 실패로 검증") 
            print(f"   ✅ 상세한 검증 오류 정보 제공")
            
            return True
            
        except Exception as e:
            print(f"❌ 테스트 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            # 정리: 테스트 데이터베이스 삭제
            try:
                print(f"\n🧹 테스트 데이터베이스 정리 중...")
                delete_response = await client.delete(f"{OMS_BASE_URL}/database/{test_db}")
                if delete_response.status_code in [200, 204, 404]:
                    print(f"✅ 테스트 데이터베이스 정리 완료")
                else:
                    print(f"⚠️ 테스트 데이터베이스 정리 실패: {delete_response.status_code}")
            except Exception as cleanup_error:
                print(f"⚠️ 정리 중 오류: {cleanup_error}")

async def main():
    """메인 테스트 실행"""
    print("🔥 매핑 검증 기능 개선 테스트")
    print("="*60)
    
    success = await test_mapping_validation()
    
    print("="*60)
    if success:
        print("🎉 전체 테스트 성공: 매핑 검증 기능이 올바르게 개선되었습니다!")
    else:
        print("❌ 테스트 실패: 매핑 검증 기능에 문제가 있습니다.")
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)