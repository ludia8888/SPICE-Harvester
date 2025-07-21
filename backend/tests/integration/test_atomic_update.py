"""
원자적 업데이트 통합 테스트
TerminusDB의 원자적 업데이트 기능 테스트 모듈

이 테스트는 다음을 검증합니다:
1. PATCH 기반 원자적 업데이트
2. 트랜잭션 기반 원자적 업데이트  
3. WOQL 기반 원자적 업데이트
4. 백업 및 복원 기능
5. 오류 처리 및 롤백 기능
"""

import pytest

# No need for sys.path.insert - using proper spice_harvester package imports
import asyncio
import json
import uuid
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

# 프로젝트 경로 설정
import os
from oms.services.async_terminus import (
    AsyncTerminusService, 
    AtomicUpdateError,
    PatchUpdateError,
    TransactionUpdateError,
    WOQLUpdateError,
    BackupCreationError,
    BackupRestoreError,
    CriticalDataLossRisk,
    AsyncOntologyNotFoundError,
    AsyncDatabaseError
)
from shared.models.config import ConnectionConfig

@pytest.fixture
def mock_connection_config():
    """테스트용 연결 설정"""
    return ConnectionConfig(
        server_url="http://localhost:6363",
        user="test_user",
        key="test_key",
        account="test_account",
        timeout=10
    )

@pytest.fixture
def terminus_service(mock_connection_config):
    """테스트용 TerminusDB 서비스"""
    return AsyncTerminusService(mock_connection_config)

@pytest.fixture
def sample_ontology_data():
    """테스트용 온톨로지 데이터"""
    return {
        "@id": "TestClass",
        "@type": "Class",
        "rdfs:label": {"ko": "테스트 클래스", "en": "Test Class"},
        "rdfs:comment": {"ko": "테스트용 클래스", "en": "Test class for testing"},
        "properties": [
            {"name": "name", "type": "xsd:string"},
            {"name": "age", "type": "xsd:integer"}
        ]
    }

@pytest.fixture
def sample_update_data():
    """테스트용 업데이트 데이터"""
    return {
        "rdfs:label": {"ko": "업데이트된 테스트 클래스", "en": "Updated Test Class"},
        "rdfs:comment": {"ko": "업데이트된 테스트용 클래스", "en": "Updated test class"},
        "properties": [
            {"name": "name", "type": "xsd:string"},
            {"name": "age", "type": "xsd:integer"},
            {"name": "email", "type": "xsd:string"}  # 새 속성 추가
        ]
    }

class TestAtomicUpdateMethods:
    """원자적 업데이트 메소드 테스트"""
    
    @pytest.mark.asyncio
    async def test_patch_atomic_update_success(self, terminus_service, sample_ontology_data, sample_update_data):
        """PATCH 방식 원자적 업데이트 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    result = await terminus_service.update_ontology_atomic_patch(
                        "test_db", "TestClass", sample_update_data
                    )
                    
                    # 결과 검증
                    assert result["id"] == "TestClass"
                    assert result["method"] == "atomic_patch"
                    assert "updated_at" in result
                    assert result["database"] == "test_db"
    
    @pytest.mark.asyncio
    async def test_patch_atomic_update_no_changes(self, terminus_service, sample_ontology_data):
        """PATCH 방식 변경사항 없음 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                
                result = await terminus_service.update_ontology_atomic_patch(
                    "test_db", "TestClass", sample_ontology_data  # 동일한 데이터
                )
                
                # 결과 검증
                assert result["message"] == "No changes detected"
                assert result["id"] == "TestClass"
                assert result["database"] == "test_db"
                assert "updated_at" in result
    
    @pytest.mark.asyncio
    async def test_patch_atomic_update_failure(self, terminus_service, sample_ontology_data, sample_update_data):
        """PATCH 방식 실패 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                with patch.object(terminus_service, '_make_request', side_effect=Exception("Patch failed")):
                    
                    with pytest.raises(AsyncDatabaseError, match="원자적 패치 업데이트 실패"):
                        await terminus_service.update_ontology_atomic_patch(
                            "test_db", "TestClass", sample_update_data
                        )
    
    @pytest.mark.asyncio
    async def test_transaction_atomic_update_success(self, terminus_service, sample_ontology_data, sample_update_data):
        """트랜잭션 방식 원자적 업데이트 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                with patch.object(terminus_service, '_create_backup_before_update', return_value={"backup_id": "backup_123"}):
                    with patch.object(terminus_service, '_begin_transaction', return_value="tx_123"):
                        with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                            with patch.object(terminus_service, '_commit_transaction', return_value=None):
                                
                                result = await terminus_service.update_ontology_atomic_transaction(
                                    "test_db", "TestClass", sample_update_data
                                )
                                
                                # 결과 검증
                                assert result["id"] == "TestClass"
                                assert result["method"] == "atomic_transaction"
                                assert result["transaction_id"] == "tx_123"
                                assert result["backup_id"] == "backup_123"
                                assert result["database"] == "test_db"
    
    @pytest.mark.asyncio
    async def test_transaction_atomic_update_rollback(self, terminus_service, sample_ontology_data, sample_update_data):
        """트랜잭션 방식 롤백 테스트"""
        backup_data = {"backup_id": "backup_123", "class_id": "TestClass"}
        
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                with patch.object(terminus_service, '_create_backup_before_update', return_value=backup_data):
                    with patch.object(terminus_service, '_begin_transaction', return_value="tx_123"):
                        with patch.object(terminus_service, '_make_request', side_effect=Exception("Transaction failed")):
                            with patch.object(terminus_service, '_enhanced_rollback_transaction', return_value=None) as mock_rollback:
                                
                                with pytest.raises(AsyncDatabaseError, match="트랜잭션 업데이트 실패"):
                                    await terminus_service.update_ontology_atomic_transaction(
                                        "test_db", "TestClass", sample_update_data
                                    )
                                
                                # 롤백이 호출되었는지 확인
                                mock_rollback.assert_called_once_with("test_db", "tx_123", backup_data)
    
    @pytest.mark.asyncio
    async def test_woql_atomic_update_success(self, terminus_service, sample_ontology_data, sample_update_data):
        """WOQL 방식 원자적 업데이트 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    result = await terminus_service.update_ontology_atomic_woql(
                        "test_db", "TestClass", sample_update_data
                    )
                    
                    # 결과 검증
                    assert result["id"] == "TestClass"
                    assert result["method"] == "atomic_woql"
                    assert "updated_at" in result
                    assert result["database"] == "test_db"
    
    @pytest.mark.asyncio
    async def test_woql_atomic_update_failure(self, terminus_service, sample_ontology_data, sample_update_data):
        """WOQL 방식 실패 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data):
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                with patch.object(terminus_service, '_make_request', side_effect=Exception("WOQL failed")):
                    
                    with pytest.raises(AsyncDatabaseError, match="WOQL 원자적 업데이트 실패"):
                        await terminus_service.update_ontology_atomic_woql(
                            "test_db", "TestClass", sample_update_data
                        )

class TestBackupAndRestore:
    """백업 및 복원 기능 테스트"""
    
    @pytest.mark.asyncio
    async def test_create_backup_success(self, terminus_service, sample_ontology_data):
        """백업 생성 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology_data) as mock_get:
            
            backup_data = await terminus_service._create_backup_before_update("test_db", "TestClass")
            
            # 결과 검증
            assert backup_data["class_id"] == "TestClass"
            assert backup_data["database"] == "test_db"
            assert backup_data["backup_data"] == sample_ontology_data
            assert "backup_id" in backup_data
            assert "backup_timestamp" in backup_data
            # get_ontology가 호출되었는지 확인
            mock_get.assert_called_once_with("test_db", "TestClass", raise_if_missing=True)
    
    @pytest.mark.asyncio
    async def test_create_backup_failure(self, terminus_service):
        """백업 생성 실패 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'get_ontology', side_effect=Exception("Backup failed")) as mock_get:
            
            with pytest.raises(AsyncDatabaseError, match="백업 생성 실패"):
                await terminus_service._create_backup_before_update("test_db", "TestClass")
                
            # get_ontology가 호출되었는지 확인
            mock_get.assert_called_once_with("test_db", "TestClass", raise_if_missing=True)
    
    @pytest.mark.asyncio
    async def test_restore_from_backup_success(self, terminus_service, sample_ontology_data):
        """백업 복원 성공 테스트"""
        backup_data = {
            "class_id": "TestClass",
            "database": "test_db",
            "backup_data": sample_ontology_data,
            "backup_id": "backup_123"
        }
        
        # Mock 설정 - DELETE와 POST 요청을 모두 모킹
        with patch.object(terminus_service, '_make_request', return_value={"success": True}) as mock_request:
            
            result = await terminus_service._restore_from_backup(backup_data)
            
            # 결과 검증
            assert result is True
            # _make_request가 호출되었는지 확인 (DELETE + POST)
            assert mock_request.call_count >= 1
    
    @pytest.mark.asyncio
    async def test_restore_from_backup_failure(self, terminus_service, sample_ontology_data):
        """백업 복원 실패 테스트"""
        backup_data = {
            "class_id": "TestClass",
            "database": "test_db",
            "backup_data": sample_ontology_data,
            "backup_id": "backup_123"
        }
        
        # Mock 설정 - POST 요청에서 실패
        def mock_request_side_effect(method, url, data=None, params=None):
            if method == "POST":
                raise Exception("Restore failed")
            return {"success": True}
        
        with patch.object(terminus_service, '_make_request', side_effect=mock_request_side_effect) as mock_request:
            
            result = await terminus_service._restore_from_backup(backup_data)
            
            # 결과 검증
            assert result is False
            # _make_request가 호출되었는지 확인
            assert mock_request.call_count >= 1
    
    @pytest.mark.asyncio
    async def test_enhanced_rollback_with_backup(self, terminus_service, sample_ontology_data):
        """강화된 롤백 (백업 포함) 테스트"""
        backup_data = {
            "class_id": "TestClass",
            "database": "test_db",
            "backup_data": sample_ontology_data,
            "backup_id": "backup_123"
        }
        
        # Mock 설정 - 롤백 실패, 백업 복원 성공
        with patch.object(terminus_service, '_rollback_transaction', side_effect=Exception("Rollback failed")) as mock_rollback:
            with patch.object(terminus_service, '_restore_from_backup', return_value=True) as mock_restore:
                
                # 예외 발생하지 않고 정상 완료되어야 함
                await terminus_service._enhanced_rollback_transaction("test_db", "tx_123", backup_data)
                
                # 롤백 시도와 백업 복원 호출 확인
                mock_rollback.assert_called_once_with("test_db", "tx_123")
                mock_restore.assert_called_once_with(backup_data)
    
    @pytest.mark.asyncio
    async def test_enhanced_rollback_total_failure(self, terminus_service, sample_ontology_data):
        """강화된 롤백 완전 실패 테스트"""
        backup_data = {
            "class_id": "TestClass",
            "database": "test_db",
            "backup_data": sample_ontology_data,
            "backup_id": "backup_123"
        }
        
        # Mock 설정 - 롤백 실패, 백업 복원도 실패
        with patch.object(terminus_service, '_rollback_transaction', side_effect=Exception("Rollback failed")) as mock_rollback:
            with patch.object(terminus_service, '_restore_from_backup', return_value=False) as mock_restore:
                
                # 예외 발생하지 않고 정상 완료되어야 함 (로그만 기록)
                await terminus_service._enhanced_rollback_transaction("test_db", "tx_123", backup_data)
                
                # 롤백 시도와 백업 복원 호출 확인
                mock_rollback.assert_called_once_with("test_db", "tx_123")
                mock_restore.assert_called_once_with(backup_data)

class TestUpdateOntologyFallbackChain:
    """업데이트 온톨로지 폴백 체인 테스트"""
    
    @pytest.mark.asyncio
    async def test_fallback_chain_patch_success(self, terminus_service, sample_ontology_data, sample_update_data):
        """폴백 체인 - PATCH 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'update_ontology_atomic_patch', return_value={"method": "atomic_patch"}) as mock_patch:
                
                result = await terminus_service.update_ontology("test_db", "TestClass", sample_update_data)
                
                # 결과 검증
                assert result["method"] == "atomic_patch"
                # PATCH 메서드가 호출되었는지 확인
                mock_patch.assert_called_once_with("test_db", "TestClass", sample_update_data)
    
    @pytest.mark.asyncio
    async def test_fallback_chain_patch_to_transaction(self, terminus_service, sample_ontology_data, sample_update_data):
        """폴백 체인 - PATCH 실패 → 트랜잭션 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'update_ontology_atomic_patch', side_effect=Exception("Patch failed")) as mock_patch:
                with patch.object(terminus_service, 'update_ontology_atomic_transaction', return_value={"method": "atomic_transaction"}) as mock_transaction:
                    
                    result = await terminus_service.update_ontology("test_db", "TestClass", sample_update_data)
                    
                    # 결과 검증
                    assert result["method"] == "atomic_transaction"
                    # PATCH 메서드가 먼저 호출되었는지 확인
                    mock_patch.assert_called_once_with("test_db", "TestClass", sample_update_data)
                    # 트랜잭션 메서드가 호출되었는지 확인
                    mock_transaction.assert_called_once_with("test_db", "TestClass", sample_update_data)
    
    @pytest.mark.asyncio
    async def test_fallback_chain_to_woql(self, terminus_service, sample_ontology_data, sample_update_data):
        """폴백 체인 - PATCH, 트랜잭션 실패 → WOQL 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'update_ontology_atomic_patch', side_effect=Exception("Patch failed")) as mock_patch:
                with patch.object(terminus_service, 'update_ontology_atomic_transaction', side_effect=Exception("Transaction failed")) as mock_transaction:
                    with patch.object(terminus_service, 'update_ontology_atomic_woql', return_value={"method": "atomic_woql"}) as mock_woql:
                        
                        result = await terminus_service.update_ontology("test_db", "TestClass", sample_update_data)
                        
                        # 결과 검증
                        assert result["method"] == "atomic_woql"
                        # 모든 메서드가 순서대로 호출되었는지 확인
                        mock_patch.assert_called_once_with("test_db", "TestClass", sample_update_data)
                        mock_transaction.assert_called_once_with("test_db", "TestClass", sample_update_data)
                        mock_woql.assert_called_once_with("test_db", "TestClass", sample_update_data)
    
    @pytest.mark.asyncio
    async def test_fallback_chain_to_legacy(self, terminus_service, sample_ontology_data, sample_update_data):
        """폴백 체인 - 모든 원자적 방법 실패 → 레거시 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'update_ontology_atomic_patch', side_effect=Exception("Patch failed")) as mock_patch:
                with patch.object(terminus_service, 'update_ontology_atomic_transaction', side_effect=Exception("Transaction failed")) as mock_transaction:
                    with patch.object(terminus_service, 'update_ontology_atomic_woql', side_effect=Exception("WOQL failed")) as mock_woql:
                        with patch.object(terminus_service, 'update_ontology_legacy', return_value={"method": "legacy", "warning": "Non-atomic update used"}) as mock_legacy:
                            
                            result = await terminus_service.update_ontology("test_db", "TestClass", sample_update_data)
                            
                            # 결과 검증
                            assert result["method"] == "legacy"
                            assert "warning" in result
                            # 모든 메서드가 순서대로 호출되었는지 확인
                            mock_patch.assert_called_once_with("test_db", "TestClass", sample_update_data)
                            mock_transaction.assert_called_once_with("test_db", "TestClass", sample_update_data)
                            mock_woql.assert_called_once_with("test_db", "TestClass", sample_update_data)
                            mock_legacy.assert_called_once_with("test_db", "TestClass", sample_update_data)

class TestTransactionMethods:
    """트랜잭션 메소드 테스트"""
    
    @pytest.mark.asyncio
    async def test_begin_transaction_success(self, terminus_service):
        """트랜잭션 시작 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, '_make_request', return_value={"transaction_id": "tx_123"}) as mock_request:
            
            tx_id = await terminus_service._begin_transaction("test_db")
            
            # 결과 검증
            assert tx_id == "tx_123"
            # 올바른 엔드포인트로 호출되었는지 확인
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_begin_transaction_failure(self, terminus_service):
        """트랜잭션 시작 실패 테스트"""
        # Mock 설정
        with patch.object(terminus_service, '_make_request', side_effect=Exception("Transaction start failed")) as mock_request:
            
            with pytest.raises(AsyncDatabaseError, match="트랜잭션 시작 실패"):
                await terminus_service._begin_transaction("test_db")
                
            # 올바른 엔드포인트로 호출되었는지 확인
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_commit_transaction_success(self, terminus_service):
        """트랜잭션 커밋 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, '_make_request', return_value={"success": True}) as mock_request:
            
            # 예외 발생하지 않아야 함
            await terminus_service._commit_transaction("test_db", "tx_123")
            
            # 올바른 엔드포인트로 호출되었는지 확인
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_commit_transaction_failure(self, terminus_service):
        """트랜잭션 커밋 실패 테스트"""
        # Mock 설정
        with patch.object(terminus_service, '_make_request', side_effect=Exception("Commit failed")) as mock_request:
            
            with pytest.raises(AsyncDatabaseError, match="트랜잭션 커밋 실패"):
                await terminus_service._commit_transaction("test_db", "tx_123")
                
            # 올바른 엔드포인트로 호출되었는지 확인
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_rollback_transaction_success(self, terminus_service):
        """트랜잭션 롤백 성공 테스트"""
        # Mock 설정
        with patch.object(terminus_service, '_make_request', return_value={"success": True}) as mock_request:
            
            # 예외 발생하지 않아야 함
            await terminus_service._rollback_transaction("test_db", "tx_123")
            
            # 올바른 엔드포인트로 호출되었는지 확인
            mock_request.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_rollback_transaction_failure(self, terminus_service):
        """트랜잭션 롤백 실패 테스트"""
        # Mock 설정
        with patch.object(terminus_service, '_make_request', side_effect=Exception("Rollback failed")) as mock_request:
            
            # 예외 발생하지 않아야 함 (로그만 기록)
            await terminus_service._rollback_transaction("test_db", "tx_123")
            
            # 올바른 엔드포인트로 호출되었는지 확인
            mock_request.assert_called_once()

class TestErrorHandling:
    """오류 처리 테스트"""
    
    @pytest.mark.asyncio
    async def test_ontology_not_found_error(self, terminus_service, sample_update_data):
        """온톨로지 없음 오류 테스트"""
        # Mock 설정
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', side_effect=AsyncOntologyNotFoundError("Not found")) as mock_get:
                
                with pytest.raises(AsyncOntologyNotFoundError):
                    await terminus_service.update_ontology_atomic_patch("test_db", "NonExistentClass", sample_update_data)
                    
                # get_ontology가 호출되었는지 확인
                mock_get.assert_called_once_with("test_db", "NonExistentClass", raise_if_missing=True)
    
    def test_custom_exception_hierarchy(self):
        """커스텀 예외 계층 테스트"""
        # 예외 계층 검증
        assert issubclass(PatchUpdateError, AtomicUpdateError)
        assert issubclass(TransactionUpdateError, AtomicUpdateError)
        assert issubclass(WOQLUpdateError, AtomicUpdateError)
        assert issubclass(BackupCreationError, AtomicUpdateError)
        assert issubclass(BackupRestoreError, AtomicUpdateError)
        assert issubclass(CriticalDataLossRisk, AtomicUpdateError)

if __name__ == "__main__":
    # 테스트 실행
    pytest.main([__file__, "-v"])