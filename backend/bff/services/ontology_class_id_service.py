"""Shared ontology class id resolution helpers.

Deduplicates class-id validation/generation logic used by ontology services.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import HTTPException, status

from bff.services.ontology_ops_service import _localized_to_string
from shared.security.input_sanitizer import SecurityViolationError, validate_class_id
from shared.utils.id_generator import generate_simple_id


def resolve_or_generate_class_id(payload: Dict[str, Any]) -> str:
    class_id_raw = payload.get("id")
    if class_id_raw:
        try:
            return validate_class_id(str(class_id_raw))
        except (SecurityViolationError, ValueError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return generate_simple_id(
        label=_localized_to_string(payload.get("label", "")),
        use_timestamp_for_korean=True,
        default_fallback="UnnamedClass",
    )
