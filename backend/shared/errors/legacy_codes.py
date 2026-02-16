"""Backward-compatible alias for legacy error-code imports.

Prefer ``shared.errors.external_codes.ExternalErrorCode`` in runtime modules.
"""

from shared.errors.external_codes import ExternalErrorCode

# Compatibility alias for older imports.
LegacyErrorCode = ExternalErrorCode

__all__ = ["LegacyErrorCode"]
