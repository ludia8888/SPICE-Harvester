from types import SimpleNamespace

import pytest

from objectify_worker.main import ObjectifyWorker


class _ContractFallbackWorker(ObjectifyWorker):
    async def _fetch_object_type_contract(self, job):  # noqa: ANN003
        _ = job
        return {
            "data": {
                "spec": {
                    "status": "ACTIVE",
                    "properties": [
                        {"name": "account_id", "primaryKey": True, "titleKey": True},
                    ],
                }
            }
        }


@pytest.mark.asyncio
async def test_resolve_object_type_key_contract_supports_camel_case_property_flags() -> None:
    worker = _ContractFallbackWorker()

    async def _fail_job(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError(f"unexpected fail_job call: args={args}, kwargs={kwargs}")

    primary_key, title_key, unique_keys, required_fields, nullable_fields = await worker._resolve_object_type_key_contract(
        job=SimpleNamespace(),
        ontology_payload={},
        prop_map={"account_id": {"type": "xsd:string"}},
        fail_job=_fail_job,
        warnings=[],
    )

    assert primary_key == ["account_id"]
    assert title_key == ["account_id"]
    assert unique_keys == []
    assert required_fields == []
    assert nullable_fields == set()


@pytest.mark.asyncio
async def test_resolve_object_type_key_contract_supports_id_only_property_flags() -> None:
    worker = _ContractFallbackWorker()

    async def _fetch_id_contract(job):  # noqa: ANN001
        _ = job
        return {
            "data": {
                "spec": {
                    "status": "ACTIVE",
                    "properties": [
                        {"id": "account_id", "primaryKey": True, "titleKey": True},
                    ],
                }
            }
        }

    worker._fetch_object_type_contract = _fetch_id_contract  # type: ignore[method-assign]

    async def _fail_job(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError(f"unexpected fail_job call: args={args}, kwargs={kwargs}")

    primary_key, title_key, unique_keys, required_fields, nullable_fields = await worker._resolve_object_type_key_contract(
        job=SimpleNamespace(),
        ontology_payload={},
        prop_map={"account_id": {"type": "xsd:string"}},
        fail_job=_fail_job,
        warnings=[],
    )

    assert primary_key == ["account_id"]
    assert title_key == ["account_id"]
    assert unique_keys == []
    assert required_fields == []
    assert nullable_fields == set()
