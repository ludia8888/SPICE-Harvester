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
from .search_config import (
    get_instances_index_name,
    get_ontologies_index_name,
    sanitize_index_name,
    DEFAULT_INDEX_SETTINGS
)

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
        return ServiceConfig.get_postgres_url()
    
    @staticmethod
    def get_redis_url() -> str:
        """Redis 연결 URL"""
        return ServiceConfig.get_redis_url()
    
    @staticmethod
    def get_elasticsearch_url() -> str:
        """Elasticsearch 연결 URL"""
        return ServiceConfig.get_elasticsearch_url()
    
    @staticmethod
    def get_kafka_bootstrap_servers() -> str:
        """Kafka Bootstrap Servers"""
        return ServiceConfig.get_kafka_bootstrap_servers()
    
    @staticmethod
    def get_terminus_url() -> str:
        """TerminusDB 연결 URL"""
        return ServiceConfig.get_terminus_url()
    
    @staticmethod
    def get_minio_url() -> str:
        """MinIO 연결 URL"""
        return ServiceConfig.get_minio_url()
    
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
        return DEFAULT_INDEX_SETTINGS.copy()
    
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
                "terminus": bool(cls.get_terminus_url()),
                "minio": bool(cls.get_minio_url())
            }
        except Exception as e:
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
            "terminus": cls.get_terminus_url(),
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
    'DEFAULT_INDEX_SETTINGS'
]

# 하위 호환성을 위한 별칭들
KAFKA_TOPICS = Config.get_all_topics()
DEFAULT_S3_BUCKET = Config.INSTANCE_BUCKET

# 모든 주요 설정값들을 최상위에서 접근 가능하도록 함
INSTANCE_EVENTS_TOPIC = Config.INSTANCE_EVENTS_TOPIC
ONTOLOGY_EVENTS_TOPIC = Config.ONTOLOGY_EVENTS_TOPIC
PROJECTION_DLQ_TOPIC = Config.PROJECTION_DLQ_TOPIC
INSTANCE_COMMANDS_TOPIC = Config.INSTANCE_COMMANDS_TOPIC
ONTOLOGY_COMMANDS_TOPIC = Config.ONTOLOGY_COMMANDS_TOPIC

# 설정 검증 (import 시 자동 실행)
_validation_results = Config.validate_all_config()
if not all(_validation_results.values()):
    import warnings
    warnings.warn(
        f"Configuration validation failed: {_validation_results}", 
        UserWarning
    )
