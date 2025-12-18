"""
Backward-compatibility shim for Event Store.

The canonical Event Store implementation lives in `shared.services.event_store`
so every service (OMS/BFF/workers) can depend on it without importing other
service packages.
"""

from shared.services.event_store import EventStore, event_store, get_event_store

__all__ = ["EventStore", "event_store", "get_event_store"]

