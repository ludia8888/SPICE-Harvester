import pytest

from shared.services.pipeline_executor import PipelineExecutor, PipelineTable


class _DummyDatasetRegistry:
    async def get_dataset(self, *args, **kwargs):
        raise AssertionError("dataset registry should not be consulted when input_overrides are provided")

    async def get_latest_version(self, *args, **kwargs):
        raise AssertionError("dataset registry should not be consulted when input_overrides are provided")

    async def get_dataset_by_name(self, *args, **kwargs):
        raise AssertionError("dataset registry should not be consulted when input_overrides are provided")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_unit_tests_define_inputs_and_expected_outputs():
    """
    CL-025: Unit tests are defined by (test inputs, transform graph, expected outputs).
    """
    from shared.services.pipeline_unit_test_runner import run_unit_tests

    executor = PipelineExecutor(dataset_registry=_DummyDatasetRegistry())
    definition = {
        "nodes": [
            {"id": "in_a", "type": "input", "metadata": {"datasetName": "a"}},
            {"id": "in_b", "type": "input", "metadata": {"datasetName": "b"}},
            {
                "id": "join1",
                "type": "transform",
                "metadata": {
                    "operation": "join",
                    "joinType": "inner",
                    "leftKey": "id",
                    "rightKey": "id",
                },
            },
            {"id": "out1", "type": "output", "metadata": {"datasetName": "out"}},
        ],
        "edges": [
            {"from": "in_a", "to": "join1"},
            {"from": "in_b", "to": "join1"},
            {"from": "join1", "to": "out1"},
        ],
        "parameters": [],
        "unitTests": [
            {
                "name": "join produces expected single row",
                "inputs": {
                    "in_a": {"columns": ["id", "name"], "rows": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]},
                    "in_b": {"columns": ["id", "value"], "rows": [{"id": 1, "value": 10}, {"id": 3, "value": 30}]},
                },
                "target_node_id": "out1",
                "expected": {
                    "columns": ["id", "name", "right_id", "value"],
                    "rows": [{"id": 1, "name": "A", "right_id": 1, "value": 10}],
                },
            }
        ],
    }

    results = await run_unit_tests(executor=executor, definition=definition, db_name="db")
    assert len(results) == 1
    assert results[0].passed is True
    assert results[0].diff == {}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_unit_tests_report_diffs_for_breaking_changes():
    """
    CL-026: Unit tests are effective for breaking-change detection/debugging via clear diffs.
    """
    from shared.services.pipeline_unit_test_runner import run_unit_tests

    executor = PipelineExecutor(dataset_registry=_DummyDatasetRegistry())
    definition = {
        "nodes": [
            {"id": "in_a", "type": "input", "metadata": {"datasetName": "a"}},
            {"id": "in_b", "type": "input", "metadata": {"datasetName": "b"}},
            {
                "id": "join1",
                "type": "transform",
                "metadata": {
                    "operation": "join",
                    "joinType": "inner",
                    "leftKey": "id",
                    "rightKey": "id",
                },
            },
            {"id": "out1", "type": "output", "metadata": {"datasetName": "out"}},
        ],
        "edges": [
            {"from": "in_a", "to": "join1"},
            {"from": "in_b", "to": "join1"},
            {"from": "join1", "to": "out1"},
        ],
        "parameters": [],
        "unitTests": [
            {
                "name": "join diff is reported",
                "inputs": {
                    "in_a": {"columns": ["id", "name"], "rows": [{"id": 1, "name": "A"}]},
                    "in_b": {"columns": ["id", "value"], "rows": [{"id": 1, "value": 10}]},
                },
                "target_node_id": "out1",
                "expected": {
                    "columns": ["id", "name", "right_id", "value"],
                    # Intentionally wrong to force a clear diff.
                    "rows": [{"id": 1, "name": "A", "right_id": 1, "value": 999}],
                },
            }
        ],
    }

    results = await run_unit_tests(executor=executor, definition=definition, db_name="db")
    assert results[0].passed is False
    assert "missing_rows" in results[0].diff
    assert "extra_rows" in results[0].diff
    # The diff should point to the exact mismatching row payload.
    assert {"id": 1, "name": "A", "right_id": 1, "value": 999} in results[0].diff["missing_rows"]
    assert {"id": 1, "name": "A", "right_id": 1, "value": 10} in results[0].diff["extra_rows"]

