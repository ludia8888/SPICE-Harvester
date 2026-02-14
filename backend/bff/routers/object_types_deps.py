"""Object types dependency providers (BFF).

Centralizes FastAPI dependencies used across object-type endpoints to keep
routers focused and support refactoring (Composite/Facade patterns).
"""


from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry

__all__ = [
    "get_dataset_registry",
    "get_objectify_registry",
]
