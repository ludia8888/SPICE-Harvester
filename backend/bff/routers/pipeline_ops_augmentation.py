"""Pipeline Builder definition augmentation.

Facade for augmentation helpers (Composite + Facade).
"""


from bff.routers.pipeline_ops_augmentation_casts import _augment_definition_with_casts
from bff.routers.pipeline_ops_augmentation_contract import _augment_definition_with_canonical_contract

__all__ = [
    "_augment_definition_with_canonical_contract",
    "_augment_definition_with_casts",
]

