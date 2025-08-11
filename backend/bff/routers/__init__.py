"""
API 라우터 모듈
도메인별로 분리된 라우터들을 포함
"""

# Import routers with module names that match main.py expectations
from . import database
from . import health
from . import mapping
from . import merge_conflict
from . import ontology
from . import query
from . import instances
from . import instance_async
from . import websocket
from . import tasks
from . import admin
from . import graph

__all__ = [
    "database",
    "health", 
    "mapping",
    "merge_conflict",
    "ontology",
    "query",
    "instances", 
    "instance_async",
    "websocket",
    "tasks",
    "admin",
    "graph"
]
