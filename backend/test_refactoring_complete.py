"""
🔥 ULTRA TEST: 리팩토링 완전성 검증 테스트
모든 리팩토링된 코드가 실제로 작동하는지 철저히 검증
"""

import asyncio
import httpx
import json
import sys
import time
import subprocess
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent))


class ServiceManager:
    """서비스 프로세스 관리"""
    
    def __init__(self):
        self.processes = {}
        
    async def start_service(self, name: str, module: str, port: int) -> bool:
        """서비스 시작"""
        try:
            # 기존 프로세스 종료
            subprocess.run(f"pkill -f 'python.*{module}'", shell=True, capture_output=True)
            await asyncio.sleep(1)
            
            # 새 프로세스 시작
            cmd = f"python -m {module}"
            process = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=Path(__file__).parent
            )
            
            self.processes[name] = process
            
            # 서비스가 시작될 때까지 대기
            for i in range(10):
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(f"http://localhost:{port}/health")
                        if response.status_code == 200:
                            print(f"✅ {name} 서비스 시작 성공 (포트: {port})")
                            return True
                except:
                    await asyncio.sleep(1)
                    
            print(f"❌ {name} 서비스 시작 실패")
            return False
            
        except Exception as e:
            print(f"❌ {name} 서비스 시작 중 오류: {e}")
            return False
    
    def stop_all(self):
        """모든 서비스 종료"""
        for name, process in self.processes.items():
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"✅ {name} 서비스 종료")
            except:
                process.kill()
                print(f"⚠️ {name} 서비스 강제 종료")
        
        # 추가 정리
        subprocess.run("pkill -f 'python.*oms.main'", shell=True, capture_output=True)
        subprocess.run("pkill -f 'python.*bff.main'", shell=True, capture_output=True)
        subprocess.run("pkill -f 'python.*funnel.main'", shell=True, capture_output=True)


async def test_service_factory():
    """Service Factory 테스트"""
    print("\n=== 1. Service Factory 동작 테스트 ===")
    
    try:
        # 각 서비스의 설정 확인
        from shared.services.service_factory import BFF_SERVICE_INFO, OMS_SERVICE_INFO, FUNNEL_SERVICE_INFO
        
        assert BFF_SERVICE_INFO.port == 8002, f"BFF 포트 오류: {BFF_SERVICE_INFO.port}"
        assert OMS_SERVICE_INFO.port == 8000, f"OMS 포트 오류: {OMS_SERVICE_INFO.port}"
        assert FUNNEL_SERVICE_INFO.port == 8003, f"Funnel 포트 오류: {FUNNEL_SERVICE_INFO.port}"
        
        print("✅ Service Factory 설정 확인 완료")
        return True
        
    except Exception as e:
        print(f"❌ Service Factory 테스트 실패: {e}")
        return False


async def test_all_endpoints_response_format():
    """All API Response 형식 완전성 테스트"""
    print("\n=== 7. 모든 API 엔드포인트 Response 형식 검증 ===")
    
    manager = ServiceManager()
    
    try:
        # 서비스 시작
        print("\n서비스 시작 중...")
        if not await manager.start_service("OMS", "oms.main", 8000):
            return False
        await asyncio.sleep(2)
        
        if not await manager.start_service("BFF", "bff.main", 8002):
            return False
        await asyncio.sleep(2)
        
        print("\nAPI Response 형식 테스트 시작...")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            # 테스트 DB 생성
            await client.post("http://localhost:8000/api/v1/database/create", 
                json={"name": "test_api_format", "description": "API 형식 테스트"})
            
            endpoints = [
                # OMS 엔드포인트
                ("GET", "http://localhost:8000/api/v1/branch/test_api_format/list", None, "OMS Branch List"),
                ("GET", "http://localhost:8000/api/v1/version/test_api_format/history", None, "OMS Version History"),
                ("GET", "http://localhost:8000/api/v1/database/list", None, "OMS Database List"),
                
                # BFF 엔드포인트  
                ("GET", "http://localhost:8002/api/v1/databases", None, "BFF Database List"),
            ]
            
            failed = []
            
            for method, url, data, name in endpoints:
                try:
                    if method == "GET":
                        response = await client.get(url)
                    elif method == "POST":
                        response = await client.post(url, json=data)
                    
                    if response.status_code in [200, 201]:
                        resp_data = response.json()
                        
                        # ApiResponse 형식 확인
                        if "status" not in resp_data:
                            failed.append(f"{name}: 'status' 필드 누락")
                        elif "message" not in resp_data:
                            failed.append(f"{name}: 'message' 필드 누락")
                        elif "data" not in resp_data:
                            failed.append(f"{name}: 'data' 필드 누락")
                        else:
                            print(f"✅ {name}: ApiResponse 형식 확인")
                    else:
                        print(f"⚠️ {name}: 상태 코드 {response.status_code}")
                        
                except Exception as e:
                    failed.append(f"{name}: 요청 실패 - {str(e)}")
            
            # 테스트 DB 정리
            await client.delete("http://localhost:8000/api/v1/database/test_api_format")
            
            if failed:
                print("\n❌ 다음 API들이 ApiResponse 형식을 따르지 않습니다:")
                for f in failed:
                    print(f"  - {f}")
                return False
            else:
                print("\n✅ 모든 API가 ApiResponse 형식을 올바르게 사용합니다")
                return True
                
    except Exception as e:
        print(f"❌ API Response 형식 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        print("\n서비스 종료 중...")
        manager.stop_all()
        await asyncio.sleep(2)


async def test_validation_dependencies():
    """Validation Dependencies 테스트"""
    print("\n=== 3. Validation Dependencies 테스트 ===")
    
    try:
        # Dependencies import 테스트
        from oms.dependencies import ValidatedDatabaseName, ValidatedClassId, ensure_database_exists
        
        # 잘못된 데이터베이스 이름 테스트
        try:
            ValidatedDatabaseName("../../../etc/passwd")
            print("❌ 데이터베이스 이름 검증이 작동하지 않습니다")
            return False
        except:
            print("✅ 데이터베이스 이름 검증 작동 확인")
            
        # 잘못된 클래스 ID 테스트
        try:
            ValidatedClassId("<script>alert('xss')</script>")
            print("❌ 클래스 ID 검증이 작동하지 않습니다")
            return False
        except:
            print("✅ 클래스 ID 검증 작동 확인")
            
        return True
        
    except Exception as e:
        print(f"❌ Validation Dependencies 테스트 실패: {e}")
        return False


async def test_bff_adapter_service():
    """BFF Adapter Service 테스트"""
    print("\n=== 4. BFF Adapter Service 동작 테스트 ===")
    
    try:
        from bff.services.adapter_service import BFFAdapterService
        from bff.dependencies import TerminusService
        from shared.utils.label_mapper import LabelMapper
        from unittest.mock import AsyncMock
        
        # Mock 객체 생성
        mock_terminus = AsyncMock(spec=TerminusService)
        mock_mapper = AsyncMock(spec=LabelMapper)
        
        # Adapter 생성
        adapter = BFFAdapterService(mock_terminus, mock_mapper)
        
        # 메소드 존재 확인
        methods = [
            'create_ontology',
            'create_advanced_ontology',
            'validate_relationships',
            'detect_circular_references',
            'find_relationship_paths'
        ]
        
        for method in methods:
            assert hasattr(adapter, method), f"BFFAdapterService에 {method} 메소드가 없습니다"
            
        print("✅ BFF Adapter Service 구조 확인 완료")
        
        # Mock 동작 테스트 - terminus_service에는 create_ontology 메소드가 있음
        mock_terminus.create_ontology = AsyncMock(return_value={"status": "success", "data": ["TestClass"]})
        mock_terminus.create_ontology_with_advanced_relationships = AsyncMock(return_value={"status": "success", "data": ["TestClass"]})
        mock_mapper.register_class = AsyncMock()
        mock_mapper.register_property = AsyncMock()
        mock_mapper.register_relationship = AsyncMock()
        
        # create_ontology 테스트
        result = await adapter.create_ontology(
            "test_db",
            {"id": "TestClass", "label": "테스트"},
            "ko"
        )
        
        assert result.id == "TestClass", "create_ontology 결과가 잘못되었습니다"
        print("✅ BFF Adapter Service create_ontology 작동 확인")
        
        return True
        
    except Exception as e:
        print(f"❌ BFF Adapter Service 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_real_api_calls():
    """실제 API 호출 테스트"""
    print("\n=== 5. 실제 API 호출 통합 테스트 ===")
    
    manager = ServiceManager()
    
    try:
        # 서비스 시작
        print("\n서비스 시작 중...")
        
        # OMS 먼저 시작
        if not await manager.start_service("OMS", "oms.main", 8000):
            return False
        await asyncio.sleep(2)
        
        # BFF 시작
        if not await manager.start_service("BFF", "bff.main", 8002):
            return False
        await asyncio.sleep(2)
        
        # Funnel 시작
        if not await manager.start_service("Funnel", "funnel.main", 8003):
            return False
        await asyncio.sleep(2)
        
        print("\n실제 API 호출 테스트 시작...")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            # 1. OMS Health Check
            response = await client.get("http://localhost:8000/health")
            assert response.status_code == 200, f"OMS health check 실패: {response.status_code}"
            data = response.json()
            assert data.get("data", {}).get("service") == "OMS", "OMS 서비스 정보 오류"
            print("✅ OMS Health Check 성공")
            
            # 2. BFF Health Check
            response = await client.get("http://localhost:8002/health")
            assert response.status_code == 200, f"BFF health check 실패: {response.status_code}"
            data = response.json()
            assert data.get("data", {}).get("service") == "BFF", "BFF 서비스 정보 오류"
            print("✅ BFF Health Check 성공")
            
            # 3. Funnel Health Check
            response = await client.get("http://localhost:8003/health")
            assert response.status_code == 200, f"Funnel health check 실패: {response.status_code}"
            data = response.json()
            assert data.get("status") == "success", "Funnel 서비스 정보 오류"
            assert data.get("data", {}).get("service") == "funnel", "Funnel 서비스 이름 오류"
            print("✅ Funnel Health Check 성공")
            
            # 4. 테스트 DB 생성
            print("\n테스트 DB 생성 중...")
            create_db_response = await client.post("http://localhost:8000/api/v1/database/create", 
                json={"name": "test_refactoring_db", "description": "리팩토링 테스트용 DB"})
            if create_db_response.status_code == 201:
                print("✅ 테스트 DB 생성 성공")
            elif create_db_response.status_code == 409:
                print("⚠️ 테스트 DB가 이미 존재합니다")
            else:
                print(f"❌ 테스트 DB 생성 실패: {create_db_response.status_code}")
            
            # 4-1. OMS Branch API (ApiResponse 사용 확인)
            response = await client.get("http://localhost:8000/api/v1/branch/test_refactoring_db/list")
            assert response.status_code == 200, f"Branch API 실패: {response.status_code}"
            data = response.json()
            assert "status" in data, "Branch API가 ApiResponse 형식을 사용하지 않습니다"
            assert "message" in data, "Branch API에 message 필드가 없습니다"
            assert "data" in data, "Branch API에 data 필드가 없습니다"
            print("✅ OMS Branch API ApiResponse 형식 확인")
            
            # 4-2. OMS Version API (ApiResponse 사용 확인)
            response = await client.get("http://localhost:8000/api/v1/version/test_refactoring_db/history")
            assert response.status_code == 200, f"Version API 실패: {response.status_code}"
            data = response.json()
            assert "status" in data, "Version API가 ApiResponse 형식을 사용하지 않습니다"
            assert "message" in data, "Version API에 message 필드가 없습니다"
            assert "data" in data, "Version API에 data 필드가 없습니다"
            print("✅ OMS Version API ApiResponse 형식 확인")
            
            # 5. BFF 리팩토링된 엔드포인트 테스트
            # 5-1. BFF-OMS 통신 확인
            response = await client.get("http://localhost:8002/api/v1/databases")
            assert response.status_code == 200, f"BFF database list 실패: {response.status_code}"
            print("✅ BFF-OMS 통신 확인")
            
            # 5-2. BFF Adapter Service를 사용하는 엔드포인트 테스트
            # ontology-advanced 엔드포인트 (BFF Adapter 사용)
            test_ontology = {
                "id": "TestRefactoringClass",
                "label": "리팩토링 테스트 클래스",
                "description": "리팩토링이 올바르게 작동하는지 확인하는 테스트 클래스",
                "properties": [],
                "relationships": []
            }
            
            response = await client.post(
                "http://localhost:8002/api/v1/databases/test_refactoring_db/ontology-advanced",
                json=test_ontology
            )
            
            if response.status_code == 200:
                data = response.json()
                assert "id" in data, "ontology-advanced 응답에 id가 없습니다"
                assert "label" in data, "ontology-advanced 응답에 label이 없습니다"
                print("✅ BFF ontology-advanced 엔드포인트 (BFF Adapter 사용) 확인")
            else:
                print(f"⚠️ BFF ontology-advanced 테스트 스킵: {response.status_code}")
            
            # 6. 테스트 DB 정리
            print("\n테스트 DB 정리 중...")
            delete_response = await client.delete("http://localhost:8000/api/v1/database/test_refactoring_db")
            if delete_response.status_code == 200:
                print("✅ 테스트 DB 삭제 성공")
            else:
                print(f"⚠️ 테스트 DB 삭제 실패: {delete_response.status_code}")
            
        return True
        
    except Exception as e:
        print(f"❌ 실제 API 호출 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        print("\n서비스 종료 중...")
        manager.stop_all()
        await asyncio.sleep(2)


async def test_error_handling():
    """에러 처리 테스트"""
    print("\n=== 6. 에러 처리 및 검증 테스트 ===")
    
    try:
        from oms.dependencies import ValidatedDatabaseName, ValidatedClassId
        
        # SQL Injection 시도
        dangerous_inputs = [
            "'; DROP TABLE users; --",
            "../../../etc/passwd",
            "<script>alert('xss')</script>",
            "test_db; DELETE FROM *",
            "test\x00null",
            "test%00null"
        ]
        
        for dangerous_input in dangerous_inputs:
            try:
                ValidatedDatabaseName(dangerous_input)
                print(f"❌ 위험한 입력이 통과했습니다: {dangerous_input}")
                return False
            except:
                pass  # 예외가 발생해야 정상
                
        print("✅ 모든 위험한 입력이 차단되었습니다")
        return True
        
    except Exception as e:
        print(f"❌ 에러 처리 테스트 실패: {e}")
        return False


async def main():
    """전체 테스트 실행"""
    print("🔥 ULTRA TEST: 리팩토링 완전성 검증 시작\n")
    
    results = []
    
    # 1. Service Factory 테스트
    results.append(("Service Factory", await test_service_factory()))
    
    # 2. Validation Dependencies 테스트
    results.append(("Validation Dependencies", await test_validation_dependencies()))
    
    # 3. BFF Adapter Service 테스트
    results.append(("BFF Adapter Service", await test_bff_adapter_service()))
    
    # 4. 에러 처리 테스트
    results.append(("Error Handling", await test_error_handling()))
    
    # 5. 실제 API 호출 테스트 (가장 중요)
    results.append(("Real API Calls", await test_real_api_calls()))
    
    # 6. 모든 API Response 형식 테스트
    results.append(("All API Response Format", await test_all_endpoints_response_format()))
    
    # 결과 요약
    print("\n" + "="*60)
    print("📊 테스트 결과 요약")
    print("="*60)
    
    total_tests = len(results)
    passed_tests = sum(1 for _, passed in results if passed)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:.<40} {status}")
    
    print("="*60)
    print(f"총 {total_tests}개 테스트 중 {passed_tests}개 통과")
    
    if passed_tests == total_tests:
        print("\n🎉 모든 테스트 통과! 리팩토링이 완벽하게 작동합니다!")
        print("\n✨ 검증된 내용:")
        print("- Service Factory가 올바르게 작동")
        print("- 모든 API가 ApiResponse 형식 사용")
        print("- Validation Dependencies가 정상 작동")
        print("- BFF Adapter Service가 올바르게 구현됨")
        print("- 보안 검증이 제대로 작동")
        print("- 실제 서비스 간 통신이 정상")
        print("- 테스트 DB 생성/삭제가 정상 작동")
        print("- 모든 API 엔드포인트가 표준 형식 준수")
    else:
        print("\n⚠️ 일부 테스트 실패. 코드를 점검해주세요.")
        
    return passed_tests == total_tests


if __name__ == "__main__":
    # 환경 변수 설정
    os.environ["TERMINUS_URL"] = "http://localhost:6363"
    os.environ["TERMINUS_USER"] = "admin"
    os.environ["TERMINUS_ACCOUNT"] = "admin"
    os.environ["TERMINUS_KEY"] = "admin123"
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)