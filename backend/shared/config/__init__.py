"""
Unified Configuration Access Point
SPICE HARVESTER 애플리케이션의 모든 설정을 통합 관리

이 모듈을 통해 모든 설정에 단일 import로 접근할 수 있습니다:

    from shared.config import Config
    
    # Kafka topics
    topic = Config.INSTANCE_EVENTS_TOPIC
    
    # Redis keys
    key = Config.get_command_status_key(command_id)
    
    # Elasticsearch indices
    index = Config.get_instances_index_name(db_name)
    
    # Service URLs
    url = Config.get_oms_url()
"""

from .app_config import AppConfig
from .service_config import ServiceConfig
from .settings import get_settings
from .search_config import (
    get_instances_index_name,
    get_ontologies_index_name,
    sanitize_index_name,
    get_default_index_settings,
)
import logging

# Create unified Config class that inherits from AppConfig and adds other configs
class Config(AppConfig):
    """
    통합 설정 클래스
    
    AppConfig를 기반으로 하여 모든 설정 모듈의 기능을 통합 제공합니다.
    """
    
    # ======================
    # Service Configuration Integration
    # ======================
    
    @staticmethod
    def get_postgres_url() -> str:
        """PostgreSQL 연결 URL"""
        return get_settings().database.postgres_url
    
    @staticmethod
    def get_redis_url() -> str:
        """Redis 연결 URL"""
        return get_settings().database.redis_url
    
    @staticmethod
    def get_elasticsearch_url() -> str:
        """Elasticsearch 연결 URL"""
        return get_settings().database.elasticsearch_url
    
    @staticmethod
    def get_kafka_bootstrap_servers() -> str:
        """Kafka Bootstrap Servers"""
        return get_settings().database.kafka_servers
    
    @staticmethod
    def get_minio_url() -> str:
        """MinIO 연결 URL"""
        return get_settings().storage.minio_endpoint_url
    
    # ======================
    # Search Configuration Integration
    # ======================
    
    @staticmethod
    def get_instances_index_name(db_name: str, version: str = None) -> str:
        """인스턴스 Elasticsearch 인덱스 이름"""
        return get_instances_index_name(db_name, version)
    
    @staticmethod
    def get_ontologies_index_name(db_name: str, version: str = None) -> str:
        """온톨로지 Elasticsearch 인덱스 이름"""
        return get_ontologies_index_name(db_name, version)
    
    @staticmethod
    def sanitize_index_name(name: str) -> str:
        """Elasticsearch 인덱스 이름 정제"""
        return sanitize_index_name(name)
    
    @staticmethod
    def get_default_index_settings() -> dict:
        """기본 인덱스 설정"""
        return get_default_index_settings()
    
    # ======================
    # Configuration Validation & Health Check
    # ======================
    
    @classmethod
    def validate_all_config(cls) -> dict:
        """
        모든 설정의 유효성 검증
        
        Returns:
            검증 결과 딕셔너리
        """
        results = {
            "app_config": cls.validate_config(),
            "service_urls": {},
            "search_config": True  # search_config는 별도 검증 로직이 없음
        }
        
        # 서비스 URL 연결 가능성 검증 (실제 연결 시도는 하지 않음)
        try:
            results["service_urls"] = {
                "postgres": bool(cls.get_postgres_url()),
                "redis": bool(cls.get_redis_url()),
                "elasticsearch": bool(cls.get_elasticsearch_url()),
                "kafka": bool(cls.get_kafka_bootstrap_servers()),
                "minio": bool(cls.get_minio_url())
            }
        except Exception as e:
            logging.getLogger(__name__).warning("Broad exception fallback at shared/config/__init__.py:126", exc_info=True)
            results["service_urls"] = {"error": str(e)}
        
        return results
    
    @classmethod
    def get_full_config_summary(cls) -> dict:
        """
        전체 시스템 설정 요약 반환
        
        Returns:
            완전한 설정 요약
        """
        summary = cls.get_config_summary()  # AppConfig의 요약
        
        # 서비스 URL 추가
        summary["service_urls"] = {
            "postgres": cls.get_postgres_url(),
            "redis": cls.get_redis_url(),
            "elasticsearch": cls.get_elasticsearch_url(),
            "kafka": cls.get_kafka_bootstrap_servers(),
            "minio": cls.get_minio_url()
        }
        
        # 검색 설정 추가
        summary["search_config"] = {
            "default_index_settings": cls.get_default_index_settings(),
            "index_naming": {
                "instances": "get_instances_index_name(db_name, version)",
                "ontologies": "get_ontologies_index_name(db_name, version)"
            }
        }
        
        return summary


# 편의를 위한 직접 접근 exports
__all__ = [
    'Config',
    'AppConfig', 
    'ServiceConfig',
    'get_instances_index_name',
    'get_ontologies_index_name',
    'sanitize_index_name',
    'DEFAULT_INDEX_SETTINGS',
    'get_default_index_settings',
]

def __getattr__(name: str):  # noqa: ANN001
    """
    Lazy, drift-free compatibility exports.

    Important:
    - Do not capture settings-derived values at import-time.
    - Resolve via `Config` / `get_default_index_settings()` on demand.
    """
    if name == "DEFAULT_INDEX_SETTINGS":
        return get_default_index_settings()
    if name == "KAFKA_TOPICS":
        return Config.get_all_topics()
    if name == "DEFAULT_S3_BUCKET":
        return Config.INSTANCE_BUCKET
    if hasattr(Config, name):
        return getattr(Config, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
