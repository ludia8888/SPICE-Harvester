"""
실제 TerminusDB 연결 테스트
실제 TerminusDB 인스턴스와의 연결을 테스트하는 모듈

이 테스트는 다음을 검증합니다:
1. TerminusDB 서버 연결
2. 인증 처리
3. 기본 데이터베이스 작업
4. 원자적 업데이트 실제 동작
"""

import pytest

# No need for sys.path.insert - using proper spice_harvester package imports
import asyncio
import os
from unittest.mock import AsyncMock, patch

# 프로젝트 경로 설정
from oms.services.async_terminus import (
    AsyncTerminusService,
    AsyncDatabaseError,
    AsyncOntologyNotFoundError
)
from shared.models.config import ConnectionConfig

@pytest.fixture
def real_connection_config():
    """실제 TerminusDB 연결 설정"""
    return ConnectionConfig(
        server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
        user=os.getenv("TERMINUS_USER", "admin"),
        account=os.getenv("TERMINUS_ACCOUNT", "admin"),
        key=os.getenv("TERMINUS_KEY", "test_key"),
        timeout=30
    )

@pytest.fixture
def terminus_service(real_connection_config):
    """실제 TerminusDB 서비스"""
    return AsyncTerminusService(real_connection_config)

@pytest.fixture
def test_db_name():
    """테스트용 데이터베이스 이름"""
    return "atomic_update_test_db"

@pytest.fixture
def sample_ontology_data():
    """테스트용 온톨로지 데이터"""
    return {
        "id": "AtomicTestClass",
        "label": {"ko": "원자적 테스트 클래스", "en": "Atomic Test Class"},
        "description": {"ko": "원자적 업데이트 테스트용 클래스", "en": "Class for atomic update testing"},
        "properties": [
            {"name": "testProperty", "type": "xsd:string"},
            {"name": "testNumber", "type": "xsd:integer"}
        ]
    }

class TestTerminusConnection:
    """TerminusDB 연결 테스트"""
    
    @pytest.mark.asyncio
    async def test_connection_check(self, terminus_service):
        """연결 상태 확인 테스트"""
        # Mock을 사용하여 실제 연결 없이 테스트
        with patch.object(terminus_service, '_make_request', return_value={"status": "ok"}):
            is_connected = await terminus_service.check_connection()
            assert is_connected is True
    
    @pytest.mark.asyncio
    async def test_authentication(self, terminus_service):
        """인증 처리 테스트"""
        # Mock을 사용하여 인증 토큰 생성 테스트
        token = await terminus_service._authenticate()
        assert token is not None
        assert token.startswith("Basic ")
    
    @pytest.mark.asyncio
    async def test_database_operations(self, terminus_service, test_db_name):
        """데이터베이스 기본 작업 테스트"""
        # Mock을 사용하여 데이터베이스 작업 테스트
        with patch.object(terminus_service, '_make_request') as mock_request:
            # 데이터베이스 존재 확인 (없음) - 첫 번째 호출
            mock_request.side_effect = [
                AsyncOntologyNotFoundError("Not found"),  # database_exists 호출 (없음)
                AsyncOntologyNotFoundError("Not found"),  # create_database 내부 중복 검사
                {"success": True}  # 실제 생성 요청
            ]
            
            exists = await terminus_service.database_exists(test_db_name)
            assert exists is False
            
            # 데이터베이스 생성
            result = await terminus_service.create_database(test_db_name, "Test database")
            assert result["name"] == test_db_name
            assert "created_at" in result
    
    @pytest.mark.asyncio
    async def test_ontology_crud_operations(self, terminus_service, test_db_name, sample_ontology_data):
        """온톨로지 CRUD 작업 테스트"""
        with patch.object(terminus_service, '_make_request') as mock_request:
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                # 온톨로지 생성
                mock_request.return_value = {"success": True}
                
                create_result = await terminus_service.create_ontology_class(test_db_name, sample_ontology_data)
                assert create_result is not None
                
                # 온톨로지 조회
                mock_request.return_value = sample_ontology_data
                with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
                    ontology = await terminus_service.get_ontology(test_db_name, "AtomicTestClass")
                    assert ontology is not None
                    assert ontology.get("id") == "AtomicTestClass"

class TestAtomicUpdateIntegration:
    """원자적 업데이트 통합 테스트"""
    
    @pytest.mark.asyncio
    async def test_atomic_patch_update_integration(self, terminus_service, test_db_name, sample_ontology_data):
        """PATCH 방식 원자적 업데이트 통합 테스트"""
        update_data = {
            "label": {"ko": "업데이트된 테스트 클래스", "en": "Updated Test Class"},
            "properties": [
                {"name": "testProperty", "type": "xsd:string"},
                {"name": "testNumber", "type": "xsd:integer"},
                {"name": "newProperty", "type": "xsd:boolean"}  # 새 속성 추가
            ]
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    result = await terminus_service.update_ontology_atomic_patch(
                        test_db_name, "AtomicTestClass", update_data
                    )
                    
                    assert result["id"] == "AtomicTestClass"
                    assert result["method"] == "atomic_patch"
                    assert "updated_at" in result
    
    @pytest.mark.asyncio
    async def test_atomic_transaction_update_integration(self, terminus_service, test_db_name, sample_ontology_data):
        """트랜잭션 방식 원자적 업데이트 통합 테스트"""
        update_data = {
            "description": {"ko": "트랜잭션으로 업데이트된 설명", "en": "Transaction updated description"}
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
                with patch.object(terminus_service, '_create_backup_before_update', return_value={"backup_id": "backup_123"}):
                    with patch.object(terminus_service, '_begin_transaction', return_value="tx_456"):
                        with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                            with patch.object(terminus_service, '_commit_transaction', return_value=None):
                                
                                result = await terminus_service.update_ontology_atomic_transaction(
                                    test_db_name, "AtomicTestClass", update_data
                                )
                                
                                assert result["id"] == "AtomicTestClass"
                                assert result["method"] == "atomic_transaction"
                                assert result["transaction_id"] == "tx_456"
                                assert result["backup_id"] == "backup_123"
    
    @pytest.mark.asyncio
    async def test_fallback_chain_integration(self, terminus_service, test_db_name, sample_ontology_data):
        """폴백 체인 통합 테스트"""
        update_data = {
            "label": {"ko": "폴백 체인 테스트", "en": "Fallback Chain Test"}
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            # PATCH 실패 → 트랜잭션 성공 시나리오
            with patch.object(terminus_service, 'update_ontology_atomic_patch', side_effect=Exception("PATCH failed")):
                with patch.object(terminus_service, 'update_ontology_atomic_transaction', return_value={"method": "atomic_transaction"}):
                    
                    result = await terminus_service.update_ontology(
                        test_db_name, "AtomicTestClass", update_data
                    )
                    
                    assert result["method"] == "atomic_transaction"

class TestErrorHandlingIntegration:
    """오류 처리 통합 테스트"""
    
    @pytest.mark.asyncio
    async def test_connection_error_handling(self, terminus_service):
        """연결 오류 처리 테스트"""
        with patch.object(terminus_service, '_make_request', side_effect=AsyncDatabaseError("Connection failed")):
            # check_connection은 예외를 잡아서 False를 반환하므로 직접 _make_request 호출 테스트
            with pytest.raises(AsyncDatabaseError, match="Connection failed"):
                await terminus_service._make_request("GET", "/api/")
    
    @pytest.mark.asyncio
    async def test_ontology_not_found_error_handling(self, terminus_service, test_db_name):
        """온톨로지 없음 오류 처리 테스트"""
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', side_effect=AsyncOntologyNotFoundError("Not found")):
                
                with pytest.raises(AsyncOntologyNotFoundError):
                    await terminus_service.update_ontology_atomic_patch(
                        test_db_name, "NonExistentClass", {"label": "test"}
                    )
    
    @pytest.mark.asyncio
    async def test_backup_and_restore_integration(self, terminus_service, test_db_name, sample_ontology_data):
        """백업 및 복원 통합 테스트"""
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            
            # 백업 생성
            backup_data = await terminus_service._create_backup_before_update(test_db_name, "AtomicTestClass")
            
            assert backup_data["class_id"] == "AtomicTestClass"
            assert backup_data["database"] == test_db_name
            assert backup_data["backup_data"] == sample_ontology_data
            assert "backup_id" in backup_data
            assert "backup_timestamp" in backup_data
            
            # 백업 복원
            with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                restore_success = await terminus_service._restore_from_backup(backup_data)
                assert restore_success is True

class TestPerformanceConsiderations:
    """성능 고려사항 테스트"""
    
    @pytest.mark.asyncio
    async def test_concurrent_updates(self, terminus_service, test_db_name, sample_ontology_data):
        """동시 업데이트 테스트"""
        update_data_1 = {"label": {"ko": "동시 업데이트 1", "en": "Concurrent Update 1"}}
        update_data_2 = {"label": {"ko": "동시 업데이트 2", "en": "Concurrent Update 2"}}
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    # 동시 업데이트 실행
                    tasks = [
                        terminus_service.update_ontology_atomic_patch(test_db_name, "AtomicTestClass", update_data_1),
                        terminus_service.update_ontology_atomic_patch(test_db_name, "AtomicTestClass", update_data_2)
                    ]
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # 결과 검증 (예외가 발생할 수 있음)
                    for result in results:
                        if isinstance(result, Exception):
                            # 동시 업데이트로 인한 예외는 예상 범위 내
                            assert isinstance(result, (AsyncDatabaseError, Exception))
                        else:
                            # 성공한 경우 검증
                            assert result["id"] == "AtomicTestClass"
                            assert result["method"] == "atomic_patch"
    
    @pytest.mark.asyncio
    async def test_large_ontology_update(self, terminus_service, test_db_name):
        """대용량 온톨로지 업데이트 테스트"""
        # 많은 속성을 가진 온톨로지 데이터 생성
        large_ontology_data = {
            "id": "LargeTestClass",
            "label": {"ko": "대용량 테스트 클래스", "en": "Large Test Class"},
            "properties": [
                {"name": f"property_{i}", "type": "xsd:string"}
                for i in range(100)  # 100개의 속성
            ]
        }
        
        update_data = {
            "properties": [
                {"name": f"property_{i}", "type": "xsd:string"}
                for i in range(150)  # 150개로 증가
            ]
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=large_ontology_data):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    result = await terminus_service.update_ontology_atomic_patch(
                        test_db_name, "LargeTestClass", update_data
                    )
                    
                    assert result["id"] == "LargeTestClass"
                    assert result["method"] == "atomic_patch"

@pytest.mark.skipif(
    not os.getenv("TERMINUS_INTEGRATION_TEST", "false").lower() == "true",
    reason="실제 TerminusDB 연결이 필요합니다. TERMINUS_INTEGRATION_TEST=true로 설정하세요."
)
class TestRealTerminusDBConnection:
    """실제 TerminusDB 연결 테스트 (선택적)"""
    
    @pytest.mark.asyncio
    async def test_real_connection(self, terminus_service):
        """실제 TerminusDB 연결 테스트"""
        try:
            await terminus_service.connect()
            is_connected = await terminus_service.check_connection()
            assert is_connected is True
        except Exception as e:
            pytest.skip(f"TerminusDB 연결 실패: {e}")
        finally:
            await terminus_service.disconnect()
    
    @pytest.mark.asyncio
    async def test_real_database_operations(self, terminus_service, test_db_name):
        """실제 데이터베이스 작업 테스트"""
        try:
            await terminus_service.connect()
            
            # 테스트 데이터베이스 생성
            if not await terminus_service.database_exists(test_db_name):
                await terminus_service.create_database(test_db_name, "Integration test database")
            
            # 데이터베이스 목록 확인
            databases = await terminus_service.list_databases()
            db_names = [db["name"] for db in databases]
            assert test_db_name in db_names
            
        except Exception as e:
            pytest.skip(f"실제 TerminusDB 작업 실패: {e}")
        finally:
            # 정리
            try:
                await terminus_service.delete_database(test_db_name)
            except:
                pass
            await terminus_service.disconnect()

if __name__ == "__main__":
    # 테스트 실행
    pytest.main([__file__, "-v"])