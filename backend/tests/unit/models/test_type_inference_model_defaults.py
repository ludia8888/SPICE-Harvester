from __future__ import annotations

from shared.models.type_inference import TypeMappingRequest


def test_type_mapping_request_default_target_system_is_foundry() -> None:
    req = TypeMappingRequest(inferred_types={"amount": "number"})
    assert req.target_system == "foundry"
