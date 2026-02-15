from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared.utils.action_runtime_contracts import (
    build_property_type_map_from_properties,
    extract_interfaces_from_metadata,
    extract_required_action_interfaces,
    load_action_target_runtime_contract,
)


@pytest.mark.unit
def test_extract_required_action_interfaces_normalizes_prefix_and_dedupes() -> None:
    spec = {
        "target_interfaces": ["interface:IApproval", "IApproval"],
        "applies_to": {"interfaces": ["interfaces:IAudit"]},
    }
    refs = extract_required_action_interfaces(spec)
    assert refs == ["IApproval", "IAudit"]


@pytest.mark.unit
def test_extract_interfaces_from_metadata_supports_aliases() -> None:
    metadata = {
        "interfaces": ["interface:IApproval"],
        "interfaceRefs": ["IAudit"],
    }
    refs = extract_interfaces_from_metadata(metadata)
    assert refs == ["IApproval", "IAudit"]


@pytest.mark.unit
def test_build_property_type_map_from_properties_handles_models_and_dicts() -> None:
    props = [
        {"name": "receipt", "type": "attachment"},
        SimpleNamespace(name="status", type="xsd:string"),
    ]
    mapping = build_property_type_map_from_properties(props)
    assert mapping == {"receipt": "attachment", "status": "string"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_runtime_contract_returns_none_when_class_missing() -> None:
    class _Resources:
        async def get_resource(
            self,
            db_name: str,  # noqa: ARG002
            *,
            branch: str,  # noqa: ARG002
            resource_type: str,  # noqa: ARG002
            resource_id: str,  # noqa: ARG002
        ):
            return None

    contract = await load_action_target_runtime_contract(
        resources=_Resources(),
        db_name="demo",
        class_id="Ticket",
        branch="main",
    )
    assert contract is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_runtime_contract_extracts_metadata_and_properties() -> None:
    class _Resources:
        async def get_resource(
            self,
            db_name: str,  # noqa: ARG002
            *,
            branch: str,  # noqa: ARG002
            resource_type: str,  # noqa: ARG002
            resource_id: str,  # noqa: ARG002
        ):
            return {
                "metadata": {"interfaces": ["interface:IApproval"], "interfaceRefs": ["IAudit"]},
                "spec": {
                    "properties": [
                        {"name": "receipt", "type": "attachment"},
                        {"name": "status", "type": "xsd:string"},
                    ]
                },
            }

    contract = await load_action_target_runtime_contract(
        resources=_Resources(),
        db_name="demo",
        class_id="Ticket",
        branch="main",
    )
    assert contract is not None
    assert contract.interfaces == ["IApproval", "IAudit"]
    assert contract.field_types == {"receipt": "attachment", "status": "string"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_runtime_contract_reads_object_type_resource_first() -> None:
    class _Resources:
        async def get_resource(
            self,
            db_name: str,  # noqa: ARG002
            *,
            branch: str,  # noqa: ARG002
            resource_type: str,  # noqa: ARG002
            resource_id: str,  # noqa: ARG002
        ):
            return {
                "metadata": {"interfaces": ["interface:IApproval"]},
                "spec": {
                    "required_properties": [
                        {"name": "receipt", "type": "attachment"},
                        {"name": "status", "type": "xsd:string"},
                    ]
                },
            }

    contract = await load_action_target_runtime_contract(
        resources=_Resources(),
        db_name="demo",
        class_id="Ticket",
        branch="main",
    )
    assert contract is not None
    assert contract.interfaces == ["IApproval"]
    assert contract.field_types == {"receipt": "attachment", "status": "string"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_load_action_target_runtime_contract_returns_none_when_resource_missing() -> None:
    class _Resources:
        async def get_resource(
            self,
            db_name: str,  # noqa: ARG002
            *,
            branch: str,  # noqa: ARG002
            resource_type: str,  # noqa: ARG002
            resource_id: str,  # noqa: ARG002
        ):
            return None

    contract = await load_action_target_runtime_contract(
        resources=_Resources(),
        db_name="demo",
        class_id="Ticket",
        branch="main",
    )
    assert contract is None
