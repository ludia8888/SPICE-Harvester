"""
Shared services module

ANTI-PATTERN RESOLVED: 직접 경로 임포트 사용 원칙
- 메모리 효율성: 필요한 서비스만 로드
- 의존성 명확성: 각 모듈이 실제 사용하는 서비스만 임포트
- 사이드 이펙트 방지: 불필요한 모듈 초기화 방지

사용법:
❌ from shared.services import ElasticsearchService  # 모든 서비스 로드됨
✅ from shared.services.elasticsearch_service import ElasticsearchService  # 필요한 것만

Common service utilities and factories for SPICE HARVESTER microservices.
모든 임포트는 직접 경로를 사용하세요:
- shared.services.redis_service
- shared.services.elasticsearch_service  
- shared.services.command_status_service
- shared.services.storage_service
- shared.services.background_task_manager
- shared.services.websocket_service
- shared.services.service_factory
"""

# __init__.py를 의도적으로 비워두어 불필요한 bulk import 방지
# 각 서비스는 직접 경로로 임포트하세요.

__all__ = []  # 직접 경로 임포트 강제