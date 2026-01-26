"""
Shared services module

ANTI-PATTERN RESOLVED: 직접 경로 임포트 사용 원칙
- 메모리 효율성: 필요한 서비스만 로드
- 의존성 명확성: 각 모듈이 실제 사용하는 서비스만 임포트
- 사이드 이펙트 방지: 불필요한 모듈 초기화 방지
- 순환 의존성 방지: bulk import 제거

디렉토리 구조 (v2 - 2026-01):
├── registries/    # Entity registries (16 files)
├── pipeline/      # Pipeline execution & transformation (25 files)
├── storage/       # Redis, ES, LakeFS, S3 (6 files)
├── events/        # Event sourcing, outbox, idempotency (9 files)
├── agent/         # LLM gateway, agent tools (4 files)
└── core/          # Core services (19 files)

사용법:
✅ from shared.services.storage.elasticsearch_service import ElasticsearchService
✅ from shared.services.registries.dataset_registry import DatasetRegistry
✅ from shared.services.pipeline.pipeline_executor import PipelineExecutor
✅ from shared.services.core.service_factory import create_app
"""

# 순환 import 방지를 위해 __all__을 비워둡니다.
# 각 서비스는 직접 경로로 임포트하세요.

__all__ = []
