"""Link types dependency providers.

This module supports router composition (Composite pattern) by centralizing
shared FastAPI dependencies (registries/clients) used across link-type subrouters.
"""


from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry
