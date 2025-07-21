"""
원자적 업데이트 성능 및 부하 테스트
대용량 데이터와 높은 부하 상황에서의 원자적 업데이트 성능 검증

이 테스트는 다음을 검증합니다:
1. 대용량 온톨로지 처리 성능
2. 동시 업데이트 부하 테스트
3. 메모리 사용량 및 누수 검증
4. 트랜잭션 처리 성능
5. 백업/복원 성능
"""

import pytest

# No need for sys.path.insert - using proper spice_harvester package imports
import asyncio
import time
import psutil
import gc
from typing import List, Dict, Any
from unittest.mock import AsyncMock, patch

# 프로젝트 경로 설정
import os
from oms.services.async_terminus import (
    AsyncTerminusService,
    AsyncDatabaseError,
    AsyncOntologyNotFoundError
)
from shared.models.config import ConnectionConfig

@pytest.fixture
def performance_connection_config():
    """성능 테스트용 연결 설정"""
    return ConnectionConfig(
        server_url="http://localhost:6363",
        user="admin",
        account="admin",
        key="test_key",
        timeout=60,  # 긴 타임아웃
        retry_attempts=1,  # 재시도 최소화
        retry_delay=0.1
    )

@pytest.fixture
def terminus_service(performance_connection_config):
    """성능 테스트용 TerminusDB 서비스"""
    return AsyncTerminusService(performance_connection_config)

@pytest.fixture
def large_ontology_data():
    """대용량 온톨로지 데이터"""
    return {
        "id": "LargePerformanceClass",
        "label": {"ko": "대용량 성능 테스트 클래스", "en": "Large Performance Test Class"},
        "properties": [
            {"name": f"property_{i}", "type": "xsd:string"}
            for i in range(1000)  # 1000개의 속성
        ],
        "relationships": [
            {"predicate": f"relation_{i}", "target": f"Target_{i % 10}"}
            for i in range(100)  # 100개의 관계
        ]
    }

class TestPerformanceBasics:
    """기본 성능 테스트"""
    
    @pytest.mark.asyncio
    async def test_single_update_performance(self, terminus_service, large_ontology_data):
        """단일 업데이트 성능 테스트"""
        db_name = "performance_test_db"
        
        # 업데이트 데이터 준비
        update_data = {
            "label": {"ko": "업데이트된 대용량 클래스", "en": "Updated Large Class"},
            "properties": [
                {"name": f"property_{i}", "type": "xsd:string"}
                for i in range(1200)  # 1200개로 증가
            ]
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=large_ontology_data):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    # 성능 측정
                    start_time = time.time()
                    memory_before = psutil.Process().memory_info().rss
                    
                    result = await terminus_service.update_ontology_atomic_patch(
                        db_name, "LargePerformanceClass", update_data
                    )
                    
                    end_time = time.time()
                    memory_after = psutil.Process().memory_info().rss
                    
                    # 성능 검증
                    execution_time = end_time - start_time
                    memory_used = memory_after - memory_before
                    
                    print(f"🔍 단일 업데이트 성능:")
                    print(f"   실행 시간: {execution_time:.3f}초")
                    print(f"   메모리 사용량: {memory_used / 1024 / 1024:.2f}MB")
                    
                    # 성능 임계값 검증
                    assert execution_time < 5.0, f"업데이트 시간이 너무 깁니다: {execution_time:.3f}초"
                    assert memory_used < 100 * 1024 * 1024, f"메모리 사용량이 너무 많습니다: {memory_used / 1024 / 1024:.2f}MB"
                    
                    # 결과 검증
                    assert result["id"] == "LargePerformanceClass"
                    assert result["method"] == "atomic_patch"
    
    @pytest.mark.asyncio
    async def test_transaction_performance(self, terminus_service, large_ontology_data):
        """트랜잭션 성능 테스트"""
        db_name = "performance_test_db"
        
        update_data = {
            "description": {"ko": "트랜잭션 성능 테스트", "en": "Transaction Performance Test"}
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=large_ontology_data):
                with patch.object(terminus_service, '_create_backup_before_update', return_value={"backup_id": "backup_123"}):
                    with patch.object(terminus_service, '_begin_transaction', return_value="tx_456"):
                        with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                            with patch.object(terminus_service, '_commit_transaction', return_value=None):
                                
                                # 성능 측정
                                start_time = time.time()
                                
                                result = await terminus_service.update_ontology_atomic_transaction(
                                    db_name, "LargePerformanceClass", update_data
                                )
                                
                                end_time = time.time()
                                execution_time = end_time - start_time
                                
                                print(f"🔍 트랜잭션 성능:")
                                print(f"   실행 시간: {execution_time:.3f}초")
                                
                                # 트랜잭션은 더 오래 걸릴 수 있음
                                assert execution_time < 10.0, f"트랜잭션 시간이 너무 깁니다: {execution_time:.3f}초"
                                
                                # 결과 검증
                                assert result["method"] == "atomic_transaction"

class TestConcurrencyPerformance:
    """동시성 성능 테스트"""
    
    @pytest.mark.asyncio
    async def test_concurrent_updates_performance(self, terminus_service):
        """동시 업데이트 성능 테스트"""
        db_name = "concurrent_test_db"
        
        # 전역 모킹 설정 - 모든 메소드를 한번에 모킹
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                # get_ontology를 전역으로 모킹하여 모든 태스크에 적용
                with patch.object(terminus_service, 'get_ontology') as mock_get_ontology:
                    
                    # 동시 업데이트 데이터 준비
                    update_tasks = []
                    for i in range(20):  # 20개의 동시 업데이트
                        update_data = {
                            "label": {"ko": f"동시 업데이트 {i}", "en": f"Concurrent Update {i}"},
                            "properties": [
                                {"name": f"prop_{i}_{j}", "type": "xsd:string"}
                                for j in range(50)  # 각각 50개의 속성
                            ]
                        }
                        
                        sample_ontology = {
                            "id": f"ConcurrentClass_{i}",
                            "label": {"ko": f"동시 클래스 {i}", "en": f"Concurrent Class {i}"},
                            "properties": [
                                {"name": f"prop_{i}_{j}", "type": "xsd:string"}
                                for j in range(30)  # 기존 30개 속성
                            ]
                        }
                        
                        # 각 태스크에 대해 개별적으로 모킹된 반환값 설정
                        mock_get_ontology.return_value = sample_ontology
                        
                        task = terminus_service.update_ontology_atomic_patch(
                            db_name, f"ConcurrentClass_{i}", update_data
                        )
                        update_tasks.append(task)
                    
                    # 동시 실행
                    start_time = time.time()
                    memory_before = psutil.Process().memory_info().rss
                    
                    results = await asyncio.gather(*update_tasks, return_exceptions=True)
                    
                    end_time = time.time()
                    memory_after = psutil.Process().memory_info().rss
                    
                    # 성능 분석
                    execution_time = end_time - start_time
                    memory_used = memory_after - memory_before
                    
                    success_count = sum(1 for r in results if not isinstance(r, Exception))
                    error_count = len(results) - success_count
                    
                    print(f"🔍 동시 업데이트 성능:")
                    print(f"   총 작업 수: {len(update_tasks)}")
                    print(f"   성공: {success_count}, 실패: {error_count}")
                    print(f"   총 실행 시간: {execution_time:.3f}초")
                    print(f"   평균 작업 시간: {execution_time / len(update_tasks):.3f}초")
                    print(f"   메모리 사용량: {memory_used / 1024 / 1024:.2f}MB")
                    
                    # 성능 임계값 검증
                    assert execution_time < 15.0, f"동시 업데이트 시간이 너무 깁니다: {execution_time:.3f}초"
                    assert success_count >= len(update_tasks) * 0.8, f"성공률이 너무 낮습니다: {success_count}/{len(update_tasks)}"
    
    @pytest.mark.asyncio
    async def test_load_stress_test(self, terminus_service):
        """부하 스트레스 테스트"""
        db_name = "stress_test_db"
        
        # 높은 부하 상황 시뮬레이션
        stress_tasks = []
        for batch in range(5):  # 5개 배치
            for i in range(10):  # 각 배치에 10개 작업
                update_data = {
                    "label": {"ko": f"스트레스 테스트 {batch}-{i}", "en": f"Stress Test {batch}-{i}"},
                    "properties": [
                        {"name": f"stress_prop_{batch}_{i}_{j}", "type": "xsd:string"}
                        for j in range(20)
                    ]
                }
                
                sample_ontology = {
                    "id": f"StressClass_{batch}_{i}",
                    "label": {"ko": f"스트레스 클래스", "en": f"Stress Class"},
                    "properties": []
                }
                
                with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                    with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology):
                        with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                            
                            task = terminus_service.update_ontology_atomic_patch(
                                db_name, f"StressClass_{batch}_{i}", update_data
                            )
                            stress_tasks.append(task)
        
        # 스트레스 테스트 실행
        start_time = time.time()
        cpu_before = psutil.cpu_percent(interval=None)
        memory_before = psutil.Process().memory_info().rss
        
        results = await asyncio.gather(*stress_tasks, return_exceptions=True)
        
        end_time = time.time()
        cpu_after = psutil.cpu_percent(interval=None)
        memory_after = psutil.Process().memory_info().rss
        
        # 스트레스 테스트 분석
        execution_time = end_time - start_time
        memory_used = memory_after - memory_before
        cpu_usage = cpu_after - cpu_before
        
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        error_count = len(results) - success_count
        
        print(f"🔍 부하 스트레스 테스트:")
        print(f"   총 작업 수: {len(stress_tasks)}")
        print(f"   성공: {success_count}, 실패: {error_count}")
        print(f"   총 실행 시간: {execution_time:.3f}초")
        print(f"   처리량: {len(stress_tasks) / execution_time:.2f} 작업/초")
        print(f"   메모리 사용량: {memory_used / 1024 / 1024:.2f}MB")
        print(f"   CPU 사용률 변화: {cpu_usage:.1f}%")
        
        # 스트레스 테스트 임계값 검증
        assert execution_time < 30.0, f"스트레스 테스트 시간이 너무 깁니다: {execution_time:.3f}초"
        assert success_count >= len(stress_tasks) * 0.7, f"스트레스 테스트 성공률이 너무 낮습니다: {success_count}/{len(stress_tasks)}"
        assert memory_used < 500 * 1024 * 1024, f"메모리 사용량이 너무 많습니다: {memory_used / 1024 / 1024:.2f}MB"

class TestMemoryManagement:
    """메모리 관리 테스트"""
    
    @pytest.mark.asyncio
    async def test_memory_leak_detection(self, terminus_service):
        """메모리 누수 탐지 테스트"""
        db_name = "memory_test_db"
        
        # 기준 메모리 사용량 측정
        gc.collect()  # 가비지 컬렉션 실행
        initial_memory = psutil.Process().memory_info().rss
        
        # 반복 업데이트로 메모리 누수 테스트
        for iteration in range(100):  # 100번 반복
            update_data = {
                "label": {"ko": f"메모리 테스트 {iteration}", "en": f"Memory Test {iteration}"},
                "properties": [
                    {"name": f"mem_prop_{iteration}_{j}", "type": "xsd:string"}
                    for j in range(10)
                ]
            }
            
            sample_ontology = {
                "id": f"MemoryClass_{iteration}",
                "label": {"ko": "메모리 클래스", "en": "Memory Class"},
                "properties": []
            }
            
            with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
                with patch.object(terminus_service, 'get_ontology', return_value=sample_ontology):
                    with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                        
                        await terminus_service.update_ontology_atomic_patch(
                            db_name, f"MemoryClass_{iteration}", update_data
                        )
            
            # 10번마다 메모리 체크
            if iteration % 10 == 0:
                gc.collect()
                current_memory = psutil.Process().memory_info().rss
                memory_growth = current_memory - initial_memory
                
                print(f"🔍 메모리 사용량 (반복 {iteration}):")
                print(f"   현재 메모리: {current_memory / 1024 / 1024:.2f}MB")
                print(f"   증가량: {memory_growth / 1024 / 1024:.2f}MB")
                
                # 메모리 증가량 임계값 검증 (100MB 이하)
                assert memory_growth < 100 * 1024 * 1024, f"메모리 누수 의심: {memory_growth / 1024 / 1024:.2f}MB 증가"
        
        # 최종 메모리 체크
        gc.collect()
        final_memory = psutil.Process().memory_info().rss
        total_growth = final_memory - initial_memory
        
        print(f"🔍 최종 메모리 분석:")
        print(f"   초기 메모리: {initial_memory / 1024 / 1024:.2f}MB")
        print(f"   최종 메모리: {final_memory / 1024 / 1024:.2f}MB")
        print(f"   총 증가량: {total_growth / 1024 / 1024:.2f}MB")
        
        # 메모리 누수 검증 (50MB 이하 증가는 정상)
        assert total_growth < 50 * 1024 * 1024, f"메모리 누수 감지: {total_growth / 1024 / 1024:.2f}MB 증가"
    
    @pytest.mark.asyncio
    async def test_backup_restore_memory_usage(self, terminus_service):
        """백업/복원 메모리 사용량 테스트"""
        db_name = "backup_memory_test_db"
        
        # 대용량 온톨로지 데이터
        large_ontology = {
            "id": "LargeBackupClass",
            "label": {"ko": "대용량 백업 클래스", "en": "Large Backup Class"},
            "properties": [
                {"name": f"backup_prop_{i}", "type": "xsd:string"}
                for i in range(500)  # 500개의 속성
            ]
        }
        
        with patch.object(terminus_service, 'get_ontology', return_value=large_ontology):
            # 백업 생성 메모리 사용량 측정
            memory_before_backup = psutil.Process().memory_info().rss
            
            backup_data = await terminus_service._create_backup_before_update(db_name, "LargeBackupClass")
            
            memory_after_backup = psutil.Process().memory_info().rss
            backup_memory_used = memory_after_backup - memory_before_backup
            
            print(f"🔍 백업 메모리 사용량:")
            print(f"   백업 메모리: {backup_memory_used / 1024 / 1024:.2f}MB")
            
            # 백업 복원 메모리 사용량 측정
            with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                memory_before_restore = psutil.Process().memory_info().rss
                
                restore_success = await terminus_service._restore_from_backup(backup_data)
                
                memory_after_restore = psutil.Process().memory_info().rss
                restore_memory_used = memory_after_restore - memory_before_restore
                
                print(f"🔍 복원 메모리 사용량:")
                print(f"   복원 메모리: {restore_memory_used / 1024 / 1024:.2f}MB")
                
                # 메모리 사용량 임계값 검증
                assert backup_memory_used < 20 * 1024 * 1024, f"백업 메모리 사용량이 너무 많습니다: {backup_memory_used / 1024 / 1024:.2f}MB"
                assert restore_memory_used < 20 * 1024 * 1024, f"복원 메모리 사용량이 너무 많습니다: {restore_memory_used / 1024 / 1024:.2f}MB"
                
                # 복원 성공 검증
                assert restore_success is True

class TestScalabilityLimits:
    """확장성 한계 테스트"""
    
    @pytest.mark.asyncio
    async def test_maximum_property_count(self, terminus_service):
        """최대 속성 개수 테스트"""
        db_name = "scalability_test_db"
        
        # 매우 많은 속성을 가진 온톨로지
        max_properties = 5000  # 5000개의 속성
        
        large_ontology = {
            "id": "MaxPropertiesClass",
            "label": {"ko": "최대 속성 클래스", "en": "Max Properties Class"},
            "properties": [
                {"name": f"max_prop_{i}", "type": "xsd:string"}
                for i in range(max_properties)
            ]
        }
        
        update_data = {
            "properties": [
                {"name": f"updated_prop_{i}", "type": "xsd:string"}
                for i in range(max_properties + 100)  # 100개 추가
            ]
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=large_ontology):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    # 최대 속성 처리 성능 측정
                    start_time = time.time()
                    memory_before = psutil.Process().memory_info().rss
                    
                    result = await terminus_service.update_ontology_atomic_patch(
                        db_name, "MaxPropertiesClass", update_data
                    )
                    
                    end_time = time.time()
                    memory_after = psutil.Process().memory_info().rss
                    
                    execution_time = end_time - start_time
                    memory_used = memory_after - memory_before
                    
                    print(f"🔍 최대 속성 처리 성능:")
                    print(f"   속성 개수: {max_properties + 100}")
                    print(f"   실행 시간: {execution_time:.3f}초")
                    print(f"   메모리 사용량: {memory_used / 1024 / 1024:.2f}MB")
                    
                    # 확장성 임계값 검증
                    assert execution_time < 30.0, f"대용량 속성 처리 시간이 너무 깁니다: {execution_time:.3f}초"
                    assert memory_used < 200 * 1024 * 1024, f"대용량 속성 메모리 사용량이 너무 많습니다: {memory_used / 1024 / 1024:.2f}MB"
                    
                    # 결과 검증
                    assert result["id"] == "MaxPropertiesClass"
                    assert result["method"] == "atomic_patch"
    
    @pytest.mark.asyncio
    async def test_deep_relationship_chains(self, terminus_service):
        """깊은 관계 체인 테스트"""
        db_name = "deep_relationship_test_db"
        
        # 깊은 관계 체인을 가진 온톨로지들
        chain_depth = 100
        
        ontology_chain = []
        for i in range(chain_depth):
            ontology = {
                "id": f"ChainClass_{i}",
                "label": {"ko": f"체인 클래스 {i}", "en": f"Chain Class {i}"},
                "relationships": [
                    {"predicate": "next", "target": f"ChainClass_{i+1}"}
                ] if i < chain_depth - 1 else []
            }
            ontology_chain.append(ontology)
        
        # 체인 중간 노드 업데이트
        middle_index = chain_depth // 2
        update_data = {
            "label": {"ko": f"업데이트된 체인 클래스 {middle_index}", "en": f"Updated Chain Class {middle_index}"},
            "properties": [
                {"name": f"chain_prop_{i}", "type": "xsd:string"}
                for i in range(10)
            ]
        }
        
        with patch.object(terminus_service, 'ensure_db_exists', return_value=None):
            with patch.object(terminus_service, 'get_ontology', return_value=ontology_chain[middle_index]):
                with patch.object(terminus_service, '_make_request', return_value={"success": True}):
                    
                    # 깊은 관계 체인 처리 성능 측정
                    start_time = time.time()
                    
                    result = await terminus_service.update_ontology_atomic_patch(
                        db_name, f"ChainClass_{middle_index}", update_data
                    )
                    
                    end_time = time.time()
                    execution_time = end_time - start_time
                    
                    print(f"🔍 깊은 관계 체인 처리 성능:")
                    print(f"   체인 깊이: {chain_depth}")
                    print(f"   업데이트 노드: {middle_index}")
                    print(f"   실행 시간: {execution_time:.3f}초")
                    
                    # 관계 체인 처리 임계값 검증
                    assert execution_time < 10.0, f"깊은 관계 체인 처리 시간이 너무 깁니다: {execution_time:.3f}초"
                    
                    # 결과 검증
                    assert result["id"] == f"ChainClass_{middle_index}"
                    assert result["method"] == "atomic_patch"

if __name__ == "__main__":
    # 성능 테스트 실행
    pytest.main([__file__, "-v", "-s"])  # -s 옵션으로 print 출력 활성화