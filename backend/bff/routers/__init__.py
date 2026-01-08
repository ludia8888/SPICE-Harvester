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
from . import ontology_extensions
from . import query
from . import instances
from . import instance_async
from . import websocket
from . import tasks
from . import admin
from . import command_status
from . import graph
from . import lineage
from . import audit
from . import ai
from . import context7
from . import agent_proxy
from . import summary
from . import pipeline
from . import objectify
from . import governance
from . import object_types
from . import link_types

__all__ = [
    "database",
    "health", 
    "mapping",
    "merge_conflict",
    "ontology",
    "ontology_extensions",
    "query",
    "instances", 
    "instance_async",
    "websocket",
    "tasks",
    "admin",
    "command_status",
    "graph",
    "lineage",
    "audit",
    "ai",
    "context7",
    "agent_proxy",
    "summary",
    "pipeline",
    "objectify",
    "governance",
    "object_types",
    "link_types",
]
