from __future__ import annotations

import pytest

from bff.services.pipeline_execution_service import _resolve_output_contract_from_definition
from shared.services.pipeline.dataset_output_semantics import resolve_dataset_write_policy


@pytest.mark.unit
def test_resolve_output_contract_from_definition_merges_declared_metadata() -> None:
    definition = {
        "nodes": [
            {
                "id": "out_1",
                "type": "output",
                "metadata": {
                    "outputName": "orders_out",
                    "outputKind": "dataset",
                    "write_mode": "append_only_new_rows",
                },
            }
        ],
        "outputs": [
            {
                "node_id": "out_1",
                "output_name": "orders_out",
                "output_kind": "dataset",
                "output_metadata": {
                    "primary_key_columns": ["id"],
                    "output_format": "parquet",
                },
            }
        ],
    }

    resolved = _resolve_output_contract_from_definition(
        definition_json=definition,
        node_id="out_1",
        output_name="orders_out",
    )

    assert resolved["output_kind"] == "dataset"
    assert resolved["output_metadata"]["write_mode"] == "append_only_new_rows"
    assert resolved["output_metadata"]["primary_key_columns"] == ["id"]


@pytest.mark.unit
def test_dataset_write_policy_hash_consistent_for_definition_contract() -> None:
    definition = {
        "executionMode": "incremental",
        "nodes": [
            {
                "id": "out_1",
                "type": "output",
                "metadata": {
                    "outputName": "orders_out",
                    "outputKind": "dataset",
                    "write_mode": "append_only_new_rows",
                    "primary_key_columns": ["id"],
                },
            }
        ],
    }

    resolved = _resolve_output_contract_from_definition(
        definition_json=definition,
        node_id="out_1",
        output_name="orders_out",
    )
    policy_a = resolve_dataset_write_policy(
        definition=definition,
        output_metadata=resolved["output_metadata"],
        execution_semantics="incremental",
    )
    policy_b = resolve_dataset_write_policy(
        definition=definition,
        output_metadata=resolved["output_metadata"],
        execution_semantics="incremental",
    )
    assert policy_a.policy_hash == policy_b.policy_hash
