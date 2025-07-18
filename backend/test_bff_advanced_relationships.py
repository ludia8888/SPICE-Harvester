#!/usr/bin/env python3
"""
🔥 THINK ULTRA! BFF 고급 관계 관리 기능 테스트
BFF-OMS 고급 관계 관리 연결이 정상 작동하는지 확인
"""

import asyncio
import httpx
import json
from datetime import datetime
from typing import Dict, Any

# BFF 서버 설정
BFF_BASE_URL = "http://localhost:8002"
TEST_DB_NAME = f"test_advanced_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# 테스트 데이터
TEST_ONTOLOGY = {
    "label": {"ko": "고급 테스트 클래스", "en": "Advanced Test Class"},
    "description": {"ko": "고급 관계 관리 테스트용"},
    "properties": [
        {
            "name": "name",
            "type": "xsd:string",
            "label": {"ko": "이름"},
            "required": True
        }
    ],
    "relationships": [
        {
            "predicate": "hasParent",
            "target": "Person",
            "label": {"ko": "부모를 가짐"},
            "cardinality": "n:1"
        }
    ]
}

# 순환 참조 테스트용 온톨로지
CIRCULAR_REF_ONTOLOGY = {
    "label": {"ko": "순환 참조 테스트"},
    "description": {"ko": "순환 참조 감지 테스트"},
    "properties": [],
    "relationships": [
        {
            "predicate": "circularRef",
            "target": "AdvancedTestClass",  # 자기 자신을 참조
            "label": {"ko": "순환 참조"},
            "cardinality": "1:1"
        }
    ]
}


async def test_create_database():
    """테스트용 데이터베이스 생성"""
    print(f"\n1. 테스트 데이터베이스 생성: {TEST_DB_NAME}")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BFF_BASE_URL}/api/v1/databases",
            json={
                "name": TEST_DB_NAME,
                "description": "고급 관계 관리 테스트용 데이터베이스"
            }
        )
        
        if response.status_code == 200:
            print("✅ 데이터베이스 생성 성공")
            return True
        else:
            print(f"❌ 데이터베이스 생성 실패: {response.status_code}")
            print(response.text)
            return False


async def test_create_ontology_advanced():
    """고급 관계 관리 기능을 포함한 온톨로지 생성 테스트"""
    print(f"\n2. 고급 온톨로지 생성 테스트 (auto_generate_inverse=True)")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BFF_BASE_URL}/api/v1/database/{TEST_DB_NAME}/ontology-advanced",
            json=TEST_ONTOLOGY,
            params={
                "auto_generate_inverse": True,
                "validate_relationships": True,
                "check_circular_references": True
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 고급 온톨로지 생성 성공")
            print(f"   - ID: {data.get('data', {}).get('id')}")
            print(f"   - 메타데이터: {json.dumps(data.get('data', {}).get('metadata', {}), indent=2, ensure_ascii=False)}")
            return True
        else:
            print(f"❌ 고급 온톨로지 생성 실패: {response.status_code}")
            print(response.text)
            return False


async def test_validate_relationships():
    """관계 검증 테스트"""
    print(f"\n3. 관계 검증 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BFF_BASE_URL}/api/v1/database/{TEST_DB_NAME}/validate-relationships",
            json=TEST_ONTOLOGY
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 관계 검증 성공")
            summary = data.get('validation_summary', {})
            print(f"   - 생성 가능: {summary.get('can_create')}")
            print(f"   - 총 이슈: {summary.get('total_issues')}")
            print(f"   - 에러: {summary.get('errors')}")
            print(f"   - 경고: {summary.get('warnings')}")
            return True
        else:
            print(f"❌ 관계 검증 실패: {response.status_code}")
            print(response.text)
            return False


async def test_check_circular_references():
    """순환 참조 탐지 테스트"""
    print(f"\n4. 순환 참조 탐지 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BFF_BASE_URL}/api/v1/database/{TEST_DB_NAME}/check-circular-references",
            json=CIRCULAR_REF_ONTOLOGY
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 순환 참조 탐지 성공")
            summary = data.get('cycle_summary', {})
            print(f"   - 안전한 생성 가능: {summary.get('safe_to_create')}")
            print(f"   - 총 순환: {summary.get('total_cycles')}")
            print(f"   - 중요 순환: {summary.get('critical_cycles')}")
            return True
        else:
            print(f"❌ 순환 참조 탐지 실패: {response.status_code}")
            print(response.text)
            return False


async def test_analyze_network():
    """관계 네트워크 분석 테스트"""
    print(f"\n5. 관계 네트워크 분석 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BFF_BASE_URL}/api/v1/database/{TEST_DB_NAME}/relationship-network/analyze"
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 네트워크 분석 성공")
            health = data.get('network_health', {})
            print(f"   - 건강성 점수: {health.get('score')}")
            print(f"   - 등급: {health.get('grade')}")
            stats = data.get('statistics', {})
            print(f"   - 총 온톨로지: {stats.get('total_ontologies')}")
            print(f"   - 총 관계: {stats.get('total_relationships')}")
            return True
        else:
            print(f"❌ 네트워크 분석 실패: {response.status_code}")
            print(response.text)
            return False


async def test_find_paths():
    """관계 경로 탐색 테스트"""
    print(f"\n6. 관계 경로 탐색 테스트")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BFF_BASE_URL}/api/v1/database/{TEST_DB_NAME}/relationship-paths",
            params={
                "start_entity": "AdvancedTestClass",
                "max_depth": 3,
                "path_type": "shortest"
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ 경로 탐색 성공")
            print(f"   - 메시지: {data.get('message')}")
            stats = data.get('statistics', {})
            print(f"   - 발견된 경로: {stats.get('total_paths_found')}")
            return True
        else:
            print(f"❌ 경로 탐색 실패: {response.status_code}")
            print(response.text)
            return False


async def cleanup_database():
    """테스트 데이터베이스 삭제"""
    print(f"\n7. 테스트 데이터베이스 정리")
    
    async with httpx.AsyncClient() as client:
        response = await client.delete(
            f"{BFF_BASE_URL}/api/v1/databases/{TEST_DB_NAME}"
        )
        
        if response.status_code == 200:
            print("✅ 데이터베이스 삭제 성공")
            return True
        else:
            print(f"⚠️  데이터베이스 삭제 실패: {response.status_code}")
            return False


async def main():
    """전체 테스트 실행"""
    print("🔥 BFF 고급 관계 관리 기능 테스트 시작")
    print("=" * 60)
    
    # 결과 추적
    results = {
        "create_db": False,
        "create_advanced": False,
        "validate": False,
        "circular": False,
        "network": False,
        "paths": False
    }
    
    try:
        # 1. 데이터베이스 생성
        results["create_db"] = await test_create_database()
        if not results["create_db"]:
            print("\n❌ 데이터베이스 생성 실패로 테스트 중단")
            return
        
        # 2. 고급 온톨로지 생성
        results["create_advanced"] = await test_create_ontology_advanced()
        
        # 3. 관계 검증
        results["validate"] = await test_validate_relationships()
        
        # 4. 순환 참조 탐지
        results["circular"] = await test_check_circular_references()
        
        # 5. 네트워크 분석
        results["network"] = await test_analyze_network()
        
        # 6. 경로 탐색
        results["paths"] = await test_find_paths()
        
    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")
    
    finally:
        # 정리
        await cleanup_database()
        
        # 결과 요약
        print("\n" + "=" * 60)
        print("📊 테스트 결과 요약")
        print("=" * 60)
        
        total_tests = len(results) - 1  # create_db 제외
        passed_tests = sum(1 for k, v in results.items() if v and k != "create_db")
        
        for test_name, passed in results.items():
            if test_name == "create_db":
                continue
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{test_name:.<30} {status}")
        
        print("=" * 60)
        print(f"총 {total_tests}개 테스트 중 {passed_tests}개 성공")
        
        if passed_tests == total_tests:
            print("\n🎉 모든 고급 관계 관리 기능이 정상 작동합니다!")
        else:
            print(f"\n⚠️  {total_tests - passed_tests}개의 테스트가 실패했습니다.")


if __name__ == "__main__":
    # 이벤트 루프 실행
    asyncio.run(main())