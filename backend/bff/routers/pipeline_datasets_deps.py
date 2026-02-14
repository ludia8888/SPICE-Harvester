"""
Pipeline datasets dependencies (BFF).

Centralizes FastAPI dependency providers for dataset subrouters.
"""

from bff.routers.objectify_job_queue_deps import get_objectify_job_queue

__all__ = ["get_objectify_job_queue"]
