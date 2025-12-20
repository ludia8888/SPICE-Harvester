"""
Backward-compatible import location.

The sheet import transform logic is domain-neutral and is implemented in shared:
`shared.services.sheet_import_service`.
"""

from shared.services.sheet_import_service import FieldMapping, SheetImportService

__all__ = ["FieldMapping", "SheetImportService"]

