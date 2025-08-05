"""
Shared services module

Common service utilities and factories for SPICE HARVESTER microservices.
"""

from .redis_service import RedisService, create_redis_service
from .command_status_service import CommandStatusService, CommandStatus
from .sync_wrapper_service import SyncWrapperService
from .websocket_service import (
    WebSocketConnectionManager,
    WebSocketNotificationService,
    get_connection_manager,
    get_notification_service,
)
# StorageService requires boto3 - make import conditional to avoid dependency issues
try:
    from .storage_service import StorageService, create_storage_service
    _STORAGE_AVAILABLE = True
except ImportError:
    StorageService = None
    create_storage_service = None
    _STORAGE_AVAILABLE = False
from .elasticsearch_service import ElasticsearchService, create_elasticsearch_service

# Build __all__ list conditionally based on available dependencies
__all__ = [
    "RedisService",
    "create_redis_service",
    "CommandStatusService",
    "CommandStatus",
    "SyncWrapperService",
    "WebSocketConnectionManager",
    "WebSocketNotificationService",
    "get_connection_manager",
    "get_notification_service",
    "ElasticsearchService",
    "create_elasticsearch_service",
]

# Add storage services if available
if _STORAGE_AVAILABLE:
    __all__.extend(["StorageService", "create_storage_service"])