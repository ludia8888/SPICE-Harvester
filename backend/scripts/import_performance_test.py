#!/usr/bin/env python3
"""
🔥 THINK ULTRA! Import Performance & Memory Efficiency Test

비효율적인 bulk import vs 직접 경로 import 성능 비교
- 메모리 사용량 차이 측정
- Import 시간 차이 측정  
- 실제 로드되는 모듈 수 비교
"""

import sys
import time
import tracemalloc
import importlib
from pathlib import Path

# Add backend directory to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

def measure_import_performance(import_func, description):
    """Import 성능과 메모리 사용량 측정"""
    print(f"\n🔥 {description}")
    print("-" * 50)
    
    # 메모리 추적 시작
    tracemalloc.start()
    
    # Import 시간 측정
    start_time = time.perf_counter()
    modules_before = len(sys.modules)
    
    try:
        result = import_func()
        success = True
        error = None
    except Exception as e:
        result = None
        success = False
        error = str(e)
    
    end_time = time.perf_counter()
    modules_after = len(sys.modules)
    
    # 메모리 사용량 측정
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # 결과 출력
    print(f"✅ 성공: {success}")
    if not success:
        print(f"❌ 오류: {error}")
    print(f"⏱️  Import 시간: {(end_time - start_time) * 1000:.2f}ms")
    print(f"🧠 메모리 사용량: {current / 1024 / 1024:.2f}MB (피크: {peak / 1024 / 1024:.2f}MB)")
    print(f"📦 로드된 모듈 수: {modules_after - modules_before}")
    
    return {
        'success': success,
        'time_ms': (end_time - start_time) * 1000,
        'memory_mb': current / 1024 / 1024,
        'peak_memory_mb': peak / 1024 / 1024,
        'modules_loaded': modules_after - modules_before,
        'error': error
    }

def test_direct_import():
    """직접 경로 import 테스트 (개선된 방식)"""
    from shared.services.elasticsearch_service import ElasticsearchService
    assert ElasticsearchService is not None

def test_bulk_import_simulation():
    """Bulk import 시뮬레이션 (이전 방식)"""
    # 이전에 __init__.py에서 모든 서비스를 import했던 방식을 시뮬레이션
    from shared.services.redis_service import RedisService, create_redis_service
    from shared.services.command_status_service import CommandStatusService, CommandStatus
    from shared.services.sync_wrapper_service import SyncWrapperService  
    from shared.services.websocket_service import (
        WebSocketConnectionManager,
        WebSocketNotificationService,
        get_connection_manager,
        get_notification_service,
    )
    from shared.services.storage_service import StorageService, create_storage_service
    from shared.services.elasticsearch_service import ElasticsearchService, create_elasticsearch_service
    from shared.services.background_task_manager import BackgroundTaskManager
    from shared.services.service_factory import (
        BFF_SERVICE_INFO,
        OMS_SERVICE_INFO, 
        create_fastapi_service,
        run_service
    )
    
    # 하나만 사용하지만 모든 서비스가 로드됨
    assert ElasticsearchService is not None

def test_single_service_need():
    """실제 서비스에서 ElasticsearchService 하나만 필요한 경우"""
    from shared.services.elasticsearch_service import ElasticsearchService
    
    # 실제 서비스 초기화 시뮬레이션
    service = ElasticsearchService(
        host="localhost",
        port=9200,
        username="elastic",
        password="test",
    )
    assert service is not None

def main():
    """메인 테스트 실행"""
    print("🔥 THINK ULTRA! Import Performance & Memory Efficiency Analysis")
    print("=" * 80)
    print("비효율적인 bulk import vs 직접 경로 import 성능 비교")
    
    # 테스트 실행
    results = {}
    
    # 1. 직접 경로 import (개선된 방식)
    results['direct'] = measure_import_performance(
        test_direct_import,
        "직접 경로 Import (개선된 방식): ElasticsearchService만"
    )
    
    print("\n" + "="*50)
    
    # 2. Bulk import 시뮬레이션 (이전 방식)  
    results['bulk'] = measure_import_performance(
        test_bulk_import_simulation,
        "Bulk Import 시뮬레이션 (이전 방식): 모든 서비스 로드"
    )
    
    print("\n" + "="*50)
    
    # 3. 실제 서비스 사용 시나리오
    results['realistic'] = measure_import_performance(
        test_single_service_need,
        "실제 사용 시나리오: ElasticsearchService 초기화"
    )
    
    # 성능 비교 분석
    print("\n" + "🏆" * 20)
    print("성능 비교 분석 결과")
    print("🏆" * 20)
    
    if results['direct']['success'] and results['bulk']['success']:
        time_diff = results['bulk']['time_ms'] - results['direct']['time_ms']
        memory_diff = results['bulk']['memory_mb'] - results['direct']['memory_mb']
        modules_diff = results['bulk']['modules_loaded'] - results['direct']['modules_loaded']
        
        print(f"\n📊 Import 시간 개선:")
        print(f"   직접 경로: {results['direct']['time_ms']:.2f}ms")
        print(f"   Bulk import: {results['bulk']['time_ms']:.2f}ms")
        print(f"   → {time_diff:.2f}ms ({time_diff/results['bulk']['time_ms']*100:.1f}%) 빠름")
        
        print(f"\n🧠 메모리 사용량 개선:")
        print(f"   직접 경로: {results['direct']['memory_mb']:.2f}MB")
        print(f"   Bulk import: {results['bulk']['memory_mb']:.2f}MB")
        print(f"   → {memory_diff:.2f}MB ({memory_diff/results['bulk']['memory_mb']*100:.1f}%) 절약")
        
        print(f"\n📦 로드 모듈 수 개선:")
        print(f"   직접 경로: {results['direct']['modules_loaded']}개")
        print(f"   Bulk import: {results['bulk']['modules_loaded']}개")
        print(f"   → {modules_diff}개 ({modules_diff/results['bulk']['modules_loaded']*100:.1f}%) 적게 로드")
        
        print(f"\n✅ 결론:")
        print(f"   직접 경로 import는 메모리 효율성과 성능 모두에서 개선을 제공합니다.")
        print(f"   특히 하나의 서비스만 필요한 경우 불필요한 모듈 로드를 방지합니다.")
        
    else:
        print("❌ 일부 테스트 실패로 인해 비교 분석을 수행할 수 없습니다.")
    
    print("\n🎯 최종 권장사항:")
    print("   ✅ 항상 직접 경로 import 사용: from shared.services.module_name import ClassName")
    print("   ❌ Bulk import 금지: from shared.services import ClassName")
    print("   ✅ __init__.py는 의도적으로 비워두어 불필요한 import 방지")

if __name__ == "__main__":
    main()
