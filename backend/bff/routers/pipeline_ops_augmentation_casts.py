"""Pipeline Builder cast augmentation (BFF facade).

Thin wrapper around shared pipeline augmentation helpers.
"""

from __future__ import annotations

from shared.services.pipeline.pipeline_definition_augmentation import (
    augment_definition_with_casts as _augment_definition_with_casts,
)

__all__ = ["_augment_definition_with_casts"]

