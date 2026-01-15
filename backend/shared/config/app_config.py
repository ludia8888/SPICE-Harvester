"""
Application Configuration
애플리케이션 전체 설정 중앙 관리
"""

import re
from operator import attrgetter
from typing import Any, Callable
from typing import Optional

from .settings import get_settings


class _SettingsValue:
    """Descriptor that resolves values from the current settings instance (SSoT)."""

    def __init__(self, path: str):
        self._path = path
        self._getter: Callable[[Any], Any] = attrgetter(path)

    def __get__(self, instance: object, owner: type | None = None):  # noqa: ANN001
        return self._getter(get_settings())


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
    INSTANCE_EVENTS_TOPIC = _SettingsValue("messaging.instance_events_topic")
    ONTOLOGY_EVENTS_TOPIC = _SettingsValue("messaging.ontology_events_topic")
    ACTION_EVENTS_TOPIC = _SettingsValue("messaging.action_events_topic")
    PROJECTION_DLQ_TOPIC = _SettingsValue("messaging.projection_dlq_topic")
    SEARCH_PROJECTION_DLQ_TOPIC = _SettingsValue("messaging.search_projection_dlq_topic")
    CONNECTOR_UPDATES_TOPIC = _SettingsValue("messaging.connector_updates_topic")
    CONNECTOR_UPDATES_DLQ_TOPIC = _SettingsValue("messaging.connector_updates_dlq_topic")
    PIPELINE_JOBS_TOPIC = _SettingsValue("messaging.pipeline_jobs_topic")
    PIPELINE_JOBS_DLQ_TOPIC = _SettingsValue("messaging.pipeline_jobs_dlq_topic")
    PIPELINE_EVENTS_TOPIC = _SettingsValue("messaging.pipeline_events_topic")
    DATASET_INGEST_OUTBOX_DLQ_TOPIC = _SettingsValue("messaging.dataset_ingest_outbox_dlq_topic")
    OBJECTIFY_JOBS_TOPIC = _SettingsValue("messaging.objectify_jobs_topic")
    OBJECTIFY_JOBS_DLQ_TOPIC = _SettingsValue("messaging.objectify_jobs_dlq_topic")
    
    # Command Topics (used by workers to consume commands)
    INSTANCE_COMMANDS_TOPIC = _SettingsValue("messaging.instance_commands_topic")
    ONTOLOGY_COMMANDS_TOPIC = _SettingsValue("messaging.ontology_commands_topic")
    DATABASE_COMMANDS_TOPIC = _SettingsValue("messaging.database_commands_topic")
    ACTION_COMMANDS_TOPIC = _SettingsValue("messaging.action_commands_topic")

    # Command DLQ Topics (poison/non-retryable or max-retry exceeded)
    INSTANCE_COMMANDS_DLQ_TOPIC = _SettingsValue("messaging.instance_commands_dlq_topic")
    ONTOLOGY_COMMANDS_DLQ_TOPIC = _SettingsValue("messaging.ontology_commands_dlq_topic")
    ACTION_COMMANDS_DLQ_TOPIC = _SettingsValue("messaging.action_commands_dlq_topic")
    
    # Kafka Consumer Groups
    PROJECTION_WORKER_GROUP = _SettingsValue("messaging.projection_worker_group")
    MESSAGE_RELAY_GROUP = _SettingsValue("messaging.message_relay_group")
    INSTANCE_WORKER_GROUP = _SettingsValue("messaging.instance_worker_group")
    ONTOLOGY_WORKER_GROUP = _SettingsValue("messaging.ontology_worker_group")
    ACTION_WORKER_GROUP = _SettingsValue("messaging.action_worker_group")
    OBJECTIFY_JOBS_GROUP = _SettingsValue("messaging.objectify_jobs_group")
    SEARCH_PROJECTION_GROUP = _SettingsValue("messaging.search_projection_group")
    
    # ======================
    # S3 Storage
    # ======================
    # S3 버킷 이름 (환경변수로 오버라이드 가능)
    INSTANCE_BUCKET = _SettingsValue("storage.instance_bucket")
    
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
    def get_class_label_key(db_name: str, class_id: str, branch: str = "main") -> str:
        """클래스 라벨 캐시 Redis 키 생성 (branch-aware)."""
        if branch and branch != "main":
            return f"class_label:{db_name}:{branch}:{class_id}"
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
        return get_settings().services.oms_base_url
    
    @staticmethod
    def get_bff_url() -> str:
        """BFF 서비스 URL"""
        return get_settings().services.bff_base_url
    
    @staticmethod
    def get_funnel_url() -> str:
        """Funnel 서비스 URL"""
        return get_settings().services.funnel_base_url
    
    # ======================
    # Event Sourcing & CQRS
    # ======================
    # Event Store 설정
    EVENT_STORE_RETENTION_DAYS = _SettingsValue("event_sourcing.event_store_retention_days")
    
    # Projection Worker 설정
    PROJECTION_BATCH_SIZE = _SettingsValue("event_sourcing.projection_batch_size")
    PROJECTION_SCROLL_TIMEOUT = _SettingsValue("event_sourcing.projection_scroll_timeout")
    
    # Command 처리 설정
    COMMAND_TIMEOUT_SECONDS = _SettingsValue("event_sourcing.command_timeout_seconds")
    COMMAND_RETRY_COUNT = _SettingsValue("event_sourcing.command_retry_count")

    # ======================
    # Ontology Writeback Defaults
    # ======================
    # lakeFS repository ids must match `^[a-z0-9][a-z0-9-]{2,62}$` (no underscores).
    ONTOLOGY_WRITEBACK_REPO = _SettingsValue("writeback.ontology_writeback_repo")
    ONTOLOGY_WRITEBACK_BRANCH_PREFIX = _SettingsValue("writeback.ontology_writeback_branch_prefix")
    ONTOLOGY_WRITEBACK_DATASET_ID = _SettingsValue("writeback.ontology_writeback_dataset_id")

    # Writeback feature flags (ACTION_WRITEBACK_DESIGN.md)
    # NOTE: Defaults are intentionally conservative; enable explicitly per environment.
    WRITEBACK_ENFORCE = _SettingsValue("writeback.writeback_enforce")
    WRITEBACK_ENFORCE_GOVERNANCE = _SettingsValue("writeback.writeback_enforce_governance")
    WRITEBACK_READ_OVERLAY = _SettingsValue("writeback.writeback_read_overlay")
    WRITEBACK_ENABLED_OBJECT_TYPES_RAW = _SettingsValue("writeback.writeback_enabled_object_types")
    WRITEBACK_DATASET_ACL_SCOPE = _SettingsValue("writeback.writeback_dataset_acl_scope")

    @staticmethod
    def _normalize_object_type_id(value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        lowered = raw.lower()
        for prefix in ("object_type:", "object:", "class:"):
            if lowered.startswith(prefix):
                raw = raw.split(":", 1)[1]
                break
        if "@" in raw:
            raw = raw.split("@", 1)[0]
        return raw.strip()

    @classmethod
    def get_writeback_enabled_object_types(cls) -> set[str]:
        raw = cls.WRITEBACK_ENABLED_OBJECT_TYPES_RAW
        if not raw:
            return set()
        parts = [p.strip() for p in raw.split(",")]
        normalized = {cls._normalize_object_type_id(p) for p in parts if cls._normalize_object_type_id(p)}
        return normalized

    @classmethod
    def is_writeback_enabled_object_type(cls, class_id: str) -> bool:
        enabled = cls.get_writeback_enabled_object_types()
        if not enabled:
            return False
        if "*" in enabled or "all" in {e.lower() for e in enabled}:
            return True
        normalized = cls._normalize_object_type_id(class_id)
        return normalized in enabled

    @classmethod
    def get_ontology_writeback_branch(cls, db_name: str) -> str:
        """
        Return a lakeFS-compatible writeback branch id.

        Note:
        - lakeFS branch ids cannot contain `/` and must match `[A-Za-z0-9_-]+`.
        - We therefore avoid Git-like `prefix/{db}` naming and use a safe separator.
        """

        prefix = str(cls.ONTOLOGY_WRITEBACK_BRANCH_PREFIX or "writeback").strip()
        prefix = prefix.rstrip("/").replace("/", "-")
        raw = f"{prefix}-{db_name}"
        return cls.sanitize_lakefs_branch_id(raw)

    @staticmethod
    def sanitize_lakefs_branch_id(value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "writeback"

        # lakeFS branch ids: letters/digits/underscore/dash only (no slashes).
        raw = raw.replace("/", "-")
        raw = re.sub(r"[^A-Za-z0-9_-]", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        raw = re.sub(r"-+", "-", raw).strip("-")

        if not raw:
            raw = "writeback"
        if raw.startswith("-"):
            raw = f"wb{raw}"
        return raw
    
    # ======================
    # Cache & TTL Settings
    # ======================
    # Redis TTL 설정 (초 단위)
    CLASS_LABEL_CACHE_TTL = _SettingsValue("cache.class_label_cache_ttl")
    COMMAND_STATUS_CACHE_TTL = _SettingsValue("cache.command_status_cache_ttl")
    USER_SESSION_CACHE_TTL = _SettingsValue("cache.user_session_cache_ttl")
    WEBSOCKET_CONNECTION_TTL = _SettingsValue("cache.websocket_connection_ttl")
    
    # ======================
    # Security Settings
    # ======================
    # 입력 검증 제한
    MAX_SEARCH_QUERY_LENGTH = _SettingsValue("security.max_search_query_length")
    MAX_DB_NAME_LENGTH = _SettingsValue("security.max_db_name_length")
    MAX_CLASS_ID_LENGTH = _SettingsValue("security.max_class_id_length")
    MAX_INSTANCE_ID_LENGTH = _SettingsValue("security.max_instance_id_length")
    
    # WebSocket 보안 설정
    MAX_CLIENT_ID_LENGTH = _SettingsValue("security.max_client_id_length")
    MAX_USER_ID_LENGTH = _SettingsValue("security.max_user_id_length")
    
    # ======================
    # Performance Settings
    # ======================
    # 동시 처리 제한
    MAX_CONCURRENT_COMMANDS = _SettingsValue("performance.max_concurrent_commands")
    MAX_WEBSOCKET_CONNECTIONS = _SettingsValue("performance.max_websocket_connections")
    
    # 데이터 크기 제한
    MAX_ONTOLOGY_SIZE = _SettingsValue("performance.max_ontology_size")
    MAX_INSTANCE_SIZE = _SettingsValue("performance.max_instance_size")
    
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
        topics = [
            cls.INSTANCE_EVENTS_TOPIC,
            cls.ONTOLOGY_EVENTS_TOPIC,
            cls.ACTION_EVENTS_TOPIC,
            cls.PROJECTION_DLQ_TOPIC,
            cls.SEARCH_PROJECTION_DLQ_TOPIC,
            cls.CONNECTOR_UPDATES_TOPIC,
            cls.CONNECTOR_UPDATES_DLQ_TOPIC,
            cls.PIPELINE_JOBS_TOPIC,
            cls.PIPELINE_JOBS_DLQ_TOPIC,
            cls.PIPELINE_EVENTS_TOPIC,
            cls.DATASET_INGEST_OUTBOX_DLQ_TOPIC,
            cls.OBJECTIFY_JOBS_TOPIC,
            cls.OBJECTIFY_JOBS_DLQ_TOPIC,
            cls.INSTANCE_COMMANDS_TOPIC,
            cls.ONTOLOGY_COMMANDS_TOPIC,
            cls.DATABASE_COMMANDS_TOPIC,
            cls.ACTION_COMMANDS_TOPIC,
            cls.INSTANCE_COMMANDS_DLQ_TOPIC,
            cls.ONTOLOGY_COMMANDS_DLQ_TOPIC,
            cls.ACTION_COMMANDS_DLQ_TOPIC,
        ]
        deduped: list[str] = []
        for name in topics:
            if not name:
                continue
            if name in deduped:
                continue
            deduped.append(name)
        return deduped
    
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


# NOTE: Do not compute/capture config at import-time.
# Use `AppConfig.*` and `AppConfig.get_all_topics()` which resolve via `get_settings()`.
