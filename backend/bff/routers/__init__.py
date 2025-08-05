"""
API 라우터 모듈
도메인별로 분리된 라우터들을 포함
"""

from .database import router as database_router
from .health import router as health_router
from .mapping import router as mapping_router
from .merge_conflict import router as merge_conflict_router
from .ontology import router as ontology_router
from .query import router as query_router
from .instances import router as instances_router
from .instance_async import router as instance_async_router
from .websocket import router as websocket_router

__all__ = [
    "database_router",
    "health_router",
    "mapping_router",
    "merge_conflict_router",
    "ontology_router",
    "query_router",
    "instances_router",
    "instance_async_router",
    "websocket_router",
]
