"""Pipeline Builder canonical contract augmentation (BFF facade).

Thin wrapper around shared pipeline augmentation helpers.
"""


from shared.services.pipeline.pipeline_definition_augmentation import (
    augment_definition_with_canonical_contract as _augment_definition_with_canonical_contract,
)

__all__ = ["_augment_definition_with_canonical_contract"]

