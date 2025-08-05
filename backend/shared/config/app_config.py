"""
Application Configuration
애플리케이션 전체 설정 중앙 관리
"""

import os
from typing import Optional


class AppConfig:
    """
    SPICE HARVESTER 애플리케이션 전체 설정 중앙 관리 클래스
    
    모든 Kafka 토픽, S3 버킷, Redis 키 패턴, 그리고 기타 운영 설정을
    한 곳에서 관리하여 분산 설정으로 인한 불일치 문제를 방지합니다.
    """
    
    # ======================
    # Kafka Topics
    # ======================
    # Event Topics (used by workers to publish events)
    INSTANCE_EVENTS_TOPIC = "instance_events"
    ONTOLOGY_EVENTS_TOPIC = "ontology_events"
    PROJECTION_DLQ_TOPIC = "projection_failures_dlq"
    
    # Command Topics (used by workers to consume commands)
    INSTANCE_COMMANDS_TOPIC = "instance_commands"
    ONTOLOGY_COMMANDS_TOPIC = "ontology_commands"
    
    # Kafka Consumer Groups
    PROJECTION_WORKER_GROUP = "projection-worker-group"
    MESSAGE_RELAY_GROUP = "message-relay-group"
    
    # ======================
    # S3 Storage
    # ======================
    # S3 버킷 이름 (환경변수로 오버라이드 가능)
    INSTANCE_BUCKET = os.getenv("INSTANCE_BUCKET", "instance-events")
    
    # S3 Key Patterns
    @staticmethod
    def get_instance_command_key(db_name: str, command_id: str) -> str:
        """인스턴스 Command S3 키 생성"""
        return f"commands/{db_name}/{command_id}.json"
    
    @staticmethod
    def get_instance_latest_key(db_name: str, instance_id: str) -> str:
        """인스턴스 최신 상태 S3 키 생성 (deprecated - 순수 append-only로 변경됨)"""
        return f"latest/{db_name}/{instance_id}.json"
    
    # ======================
    # Redis Key Patterns
    # ======================
    @staticmethod
    def get_command_status_key(command_id: str) -> str:
        """Command 상태 Redis 키 생성"""
        return f"command:{command_id}:status"
    
    @staticmethod
    def get_command_result_key(command_id: str) -> str:
        """Command 결과 Redis 키 생성"""
        return f"command:{command_id}:result"
    
    @staticmethod
    def get_command_status_pattern() -> str:
        """모든 Command 상태 키 패턴"""
        return "command:*:status"
    
    @staticmethod
    def get_class_label_key(db_name: str, class_id: str) -> str:
        """클래스 라벨 캐시 Redis 키 생성"""
        return f"class_label:{db_name}:{class_id}"
    
    @staticmethod
    def get_user_session_key(user_id: str) -> str:
        """사용자 세션 Redis 키 생성"""
        return f"session:{user_id}"
    
    @staticmethod
    def get_websocket_connection_key(client_id: str) -> str:
        """WebSocket 연결 Redis 키 생성"""
        return f"ws:connection:{client_id}"
    
    # ======================
    # Elasticsearch Integration
    # ======================
    # search_config.py와 통합하여 일관된 인덱스 이름 제공
    @staticmethod
    def get_instances_index_name(db_name: str, version: Optional[str] = None) -> str:
        """인스턴스 Elasticsearch 인덱스 이름 생성"""
        from .search_config import get_instances_index_name
        return get_instances_index_name(db_name, version)
    
    @staticmethod
    def get_ontologies_index_name(db_name: str, version: Optional[str] = None) -> str:
        """온톨로지 Elasticsearch 인덱스 이름 생성"""
        from .search_config import get_ontologies_index_name
        return get_ontologies_index_name(db_name, version)
    
    # ======================
    # Service URLs Integration  
    # ======================
    @staticmethod
    def get_oms_url() -> str:
        """OMS 서비스 URL"""
        from .service_config import ServiceConfig
        return ServiceConfig.get_oms_url()
    
    @staticmethod
    def get_bff_url() -> str:
        """BFF 서비스 URL"""
        from .service_config import ServiceConfig
        return ServiceConfig.get_bff_url()
    
    @staticmethod
    def get_funnel_url() -> str:
        """Funnel 서비스 URL"""
        from .service_config import ServiceConfig
        return ServiceConfig.get_funnel_url()
    
    # ======================
    # Event Sourcing & CQRS
    # ======================
    # Event Store 설정
    EVENT_STORE_RETENTION_DAYS = int(os.getenv("EVENT_STORE_RETENTION_DAYS", "365"))
    
    # Projection Worker 설정
    PROJECTION_BATCH_SIZE = int(os.getenv("PROJECTION_BATCH_SIZE", "1000"))
    PROJECTION_SCROLL_TIMEOUT = os.getenv("PROJECTION_SCROLL_TIMEOUT", "5m")
    
    # Command 처리 설정
    COMMAND_TIMEOUT_SECONDS = int(os.getenv("COMMAND_TIMEOUT_SECONDS", "300"))  # 5분
    COMMAND_RETRY_COUNT = int(os.getenv("COMMAND_RETRY_COUNT", "3"))
    
    # ======================
    # Cache & TTL Settings
    # ======================
    # Redis TTL 설정 (초 단위)
    CLASS_LABEL_CACHE_TTL = int(os.getenv("CLASS_LABEL_CACHE_TTL", "3600"))      # 1시간
    COMMAND_STATUS_CACHE_TTL = int(os.getenv("COMMAND_STATUS_CACHE_TTL", "86400"))  # 24시간
    USER_SESSION_CACHE_TTL = int(os.getenv("USER_SESSION_CACHE_TTL", "7200"))    # 2시간
    WEBSOCKET_CONNECTION_TTL = int(os.getenv("WEBSOCKET_CONNECTION_TTL", "3600"))  # 1시간
    
    # ======================
    # Security Settings
    # ======================
    # 입력 검증 제한
    MAX_SEARCH_QUERY_LENGTH = int(os.getenv("MAX_SEARCH_QUERY_LENGTH", "100"))
    MAX_DB_NAME_LENGTH = int(os.getenv("MAX_DB_NAME_LENGTH", "50"))
    MAX_CLASS_ID_LENGTH = int(os.getenv("MAX_CLASS_ID_LENGTH", "100"))
    MAX_INSTANCE_ID_LENGTH = int(os.getenv("MAX_INSTANCE_ID_LENGTH", "255"))
    
    # WebSocket 보안 설정
    MAX_CLIENT_ID_LENGTH = int(os.getenv("MAX_CLIENT_ID_LENGTH", "50"))
    MAX_USER_ID_LENGTH = int(os.getenv("MAX_USER_ID_LENGTH", "50"))
    
    # ======================
    # Performance Settings
    # ======================
    # 동시 처리 제한
    MAX_CONCURRENT_COMMANDS = int(os.getenv("MAX_CONCURRENT_COMMANDS", "100"))
    MAX_WEBSOCKET_CONNECTIONS = int(os.getenv("MAX_WEBSOCKET_CONNECTIONS", "1000"))
    
    # 데이터 크기 제한
    MAX_ONTOLOGY_SIZE = int(os.getenv("MAX_ONTOLOGY_SIZE", "10485760"))  # 10MB
    MAX_INSTANCE_SIZE = int(os.getenv("MAX_INSTANCE_SIZE", "1048576"))   # 1MB
    
    # ======================
    # Validation Methods
    # ======================
    @classmethod
    def validate_config(cls) -> bool:
        """
        설정값들의 유효성 검증
        
        Returns:
            모든 설정이 유효하면 True, 그렇지 않으면 False
        """
        try:
            # 필수 설정 확인
            assert cls.INSTANCE_EVENTS_TOPIC, "INSTANCE_EVENTS_TOPIC is required"
            assert cls.ONTOLOGY_EVENTS_TOPIC, "ONTOLOGY_EVENTS_TOPIC is required"
            assert cls.INSTANCE_BUCKET, "INSTANCE_BUCKET is required"
            
            # 숫자 설정 유효성 확인
            assert cls.COMMAND_TIMEOUT_SECONDS > 0, "COMMAND_TIMEOUT_SECONDS must be positive"
            assert cls.COMMAND_RETRY_COUNT >= 0, "COMMAND_RETRY_COUNT must be non-negative"
            assert cls.CLASS_LABEL_CACHE_TTL > 0, "CLASS_LABEL_CACHE_TTL must be positive"
            
            # 보안 설정 확인
            assert cls.MAX_SEARCH_QUERY_LENGTH > 0, "MAX_SEARCH_QUERY_LENGTH must be positive"
            assert cls.MAX_DB_NAME_LENGTH > 0, "MAX_DB_NAME_LENGTH must be positive"
            
            return True
            
        except AssertionError as e:
            print(f"Configuration validation failed: {e}")
            return False
    
    @classmethod
    def get_all_topics(cls) -> list[str]:
        """모든 Kafka 토픽 목록 반환"""
        return [
            cls.INSTANCE_EVENTS_TOPIC,
            cls.ONTOLOGY_EVENTS_TOPIC,
            cls.PROJECTION_DLQ_TOPIC,
            cls.INSTANCE_COMMANDS_TOPIC,
            cls.ONTOLOGY_COMMANDS_TOPIC
        ]
    
    @classmethod
    def get_config_summary(cls) -> dict:
        """현재 설정 요약 반환 (디버깅용)"""
        return {
            "kafka_topics": cls.get_all_topics(),
            "s3_bucket": cls.INSTANCE_BUCKET,
            "cache_ttls": {
                "class_label": cls.CLASS_LABEL_CACHE_TTL,
                "command_status": cls.COMMAND_STATUS_CACHE_TTL,
                "user_session": cls.USER_SESSION_CACHE_TTL
            },
            "limits": {
                "search_query": cls.MAX_SEARCH_QUERY_LENGTH,
                "db_name": cls.MAX_DB_NAME_LENGTH,
                "concurrent_commands": cls.MAX_CONCURRENT_COMMANDS
            }
        }


# 편의를 위한 전역 상수들 (하위 호환성)
KAFKA_TOPICS = AppConfig.get_all_topics()
DEFAULT_S3_BUCKET = AppConfig.INSTANCE_BUCKET

# 설정 검증 (import 시 자동 실행)
if not AppConfig.validate_config():
    import warnings
    warnings.warn("AppConfig validation failed. Please check your environment variables.", UserWarning)