from __future__ import annotations

import pytest

from shared.services.pipeline.pipeline_definition_validator import (
    PipelineDefinitionValidationPolicy,
    validate_pipeline_definition,
)
from shared.services.pipeline.pipeline_plan_builder import (
    add_geospatial,
    add_pattern_mining,
    add_split,
    add_stream_join,
    new_plan,
)
from shared.services.pipeline.pipeline_transform_spec import SUPPORTED_TRANSFORMS


@pytest.mark.unit
def test_add_split_expands_to_true_false_filter_nodes() -> None:
    plan = new_plan(goal="split macro", db_name="demo")
    definition = plan["definition_json"]
    definition["nodes"] = [{"id": "in1", "type": "input", "metadata": {"datasetName": "orders"}}]
    definition["edges"] = []

    mutation = add_split(
        plan,
        input_node_id="in1",
        expression="amount > 100",
        true_node_id="split_true",
        false_node_id="split_false",
    )

    nodes = mutation.plan["definition_json"]["nodes"]
    edges = mutation.plan["definition_json"]["edges"]
    by_id = {str(node.get("id")): node for node in nodes}

    assert "split_true" in by_id
    assert "split_false" in by_id
    assert by_id["split_true"]["metadata"]["operation"] == "filter"
    assert by_id["split_true"]["metadata"]["splitBranch"] == "true"
    assert by_id["split_false"]["metadata"]["splitBranch"] == "false"
    assert by_id["split_false"]["metadata"]["expression"] == "not (amount > 100)"
    assert {(str(edge.get("from")), str(edge.get("to"))) for edge in edges} >= {
        ("in1", "split_true"),
        ("in1", "split_false"),
    }


@pytest.mark.unit
def test_validate_pipeline_definition_accepts_advanced_transforms() -> None:
    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": "orders"}},
            {"id": "in2", "type": "input", "metadata": {"datasetName": "inventory"}},
            {
                "id": "geo1",
                "type": "transform",
                "metadata": {
                    "operation": "geospatial",
                    "geospatial": {
                        "mode": "point",
                        "latColumn": "lat",
                        "lonColumn": "lon",
                        "outputColumn": "point",
                    },
                },
            },
            {
                "id": "pm1",
                "type": "transform",
                "metadata": {
                    "operation": "patternMining",
                    "patternMining": {
                        "sourceColumn": "message",
                        "pattern": "ERR-(\\d+)",
                        "outputColumn": "error_code",
                        "matchMode": "extract",
                    },
                },
            },
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "dynamic",
                        "leftEventTimeColumn": "event_time_left",
                        "rightEventTimeColumn": "event_time_right",
                        "allowedLatenessSeconds": 120,
                        "leftCacheExpirationSeconds": 300,
                        "rightCacheExpirationSeconds": 300,
                    },
                },
            },
            {
                "id": "split1",
                "type": "transform",
                "metadata": {
                    "operation": "split",
                    "expression": "amount > 0",
                },
            },
        ],
        "edges": [
            {"from": "in1", "to": "geo1"},
            {"from": "geo1", "to": "pm1"},
            {"from": "pm1", "to": "split1"},
            {"from": "pm1", "to": "sj1"},
            {"from": "in2", "to": "sj1"},
        ],
    }
    result = validate_pipeline_definition(
        definition,
        policy=PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS,
            require_output=False,
            normalize_metadata=True,
        ),
    )
    assert result.errors == []


@pytest.mark.unit
def test_validate_pipeline_definition_rejects_invalid_stream_join_strategy() -> None:
    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": "orders"}},
            {"id": "in2", "type": "input", "metadata": {"datasetName": "inventory"}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "invalid"},
                },
            },
        ],
        "edges": [
            {"from": "in1", "to": "sj1"},
            {"from": "in2", "to": "sj1"},
        ],
    }
    result = validate_pipeline_definition(
        definition,
        policy=PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS,
            require_output=False,
            normalize_metadata=True,
        ),
    )
    assert any("streamJoin has invalid strategy" in err for err in result.errors)


@pytest.mark.unit
def test_validate_pipeline_definition_rejects_invalid_stream_join_time_direction() -> None:
    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": "orders"}},
            {"id": "in2", "type": "input", "metadata": {"datasetName": "inventory"}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {
                        "strategy": "dynamic",
                        "timeDirection": "diagonal",
                        "leftEventTimeColumn": "left_ts",
                        "rightEventTimeColumn": "right_ts",
                        "allowedLatenessSeconds": 30,
                        "leftCacheExpirationSeconds": 300,
                        "rightCacheExpirationSeconds": 300,
                    },
                },
            },
        ],
        "edges": [
            {"from": "in1", "to": "sj1"},
            {"from": "in2", "to": "sj1"},
        ],
    }
    result = validate_pipeline_definition(
        definition,
        policy=PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS,
            require_output=False,
            normalize_metadata=True,
        ),
    )
    assert any("streamJoin has invalid timeDirection" in err for err in result.errors)


@pytest.mark.unit
def test_validate_pipeline_definition_rejects_dynamic_stream_join_without_event_time_fields() -> None:
    definition = {
        "nodes": [
            {"id": "in1", "type": "input", "metadata": {"datasetName": "orders"}},
            {"id": "in2", "type": "input", "metadata": {"datasetName": "inventory"}},
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "dynamic"},
                },
            },
        ],
        "edges": [
            {"from": "in1", "to": "sj1"},
            {"from": "in2", "to": "sj1"},
        ],
    }
    result = validate_pipeline_definition(
        definition,
        policy=PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS,
            require_output=False,
            normalize_metadata=True,
        ),
    )
    assert any("streamJoin dynamic requires leftEventTimeColumn" in err for err in result.errors)
    assert any("streamJoin dynamic requires rightEventTimeColumn" in err for err in result.errors)
    assert any("streamJoin dynamic requires allowedLatenessSeconds" in err for err in result.errors)
    assert any("streamJoin dynamic requires leftCacheExpirationSeconds" in err for err in result.errors)
    assert any("streamJoin dynamic requires rightCacheExpirationSeconds" in err for err in result.errors)


@pytest.mark.unit
def test_validate_pipeline_definition_rejects_left_lookup_with_transformed_right_input() -> None:
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left_ds"}},
            {"id": "right", "type": "input", "metadata": {"datasetName": "right_ds"}},
            {
                "id": "right_filter",
                "type": "transform",
                "metadata": {"operation": "filter", "expression": "id > 0"},
            },
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "left_lookup"},
                },
            },
        ],
        "edges": [
            {"from": "right", "to": "right_filter"},
            {"from": "left", "to": "sj1"},
            {"from": "right_filter", "to": "sj1"},
        ],
    }
    result = validate_pipeline_definition(
        definition,
        policy=PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS,
            require_output=False,
            normalize_metadata=True,
        ),
    )
    assert any("requires right input to be a direct input node" in err for err in result.errors)


@pytest.mark.unit
def test_validate_pipeline_definition_rejects_left_lookup_with_streaming_right_input() -> None:
    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left_ds"}},
            {
                "id": "right",
                "type": "input",
                "metadata": {"datasetName": "right_ds", "read": {"format": "kafka"}},
            },
            {
                "id": "sj1",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "left_lookup"},
                },
            },
        ],
        "edges": [
            {"from": "left", "to": "sj1"},
            {"from": "right", "to": "sj1"},
        ],
    }
    result = validate_pipeline_definition(
        definition,
        policy=PipelineDefinitionValidationPolicy(
            supported_ops=SUPPORTED_TRANSFORMS,
            require_output=False,
            normalize_metadata=True,
        ),
    )
    assert any("requires right input to be batch lookup source" in err for err in result.errors)


@pytest.mark.unit
def test_builder_helpers_add_geospatial_pattern_mining_stream_join() -> None:
    plan = new_plan(goal="advanced transforms", db_name="demo")
    definition = plan["definition_json"]
    definition["nodes"] = [
        {"id": "in1", "type": "input", "metadata": {"datasetName": "orders"}},
        {"id": "in2", "type": "input", "metadata": {"datasetName": "inventory"}},
    ]
    definition["edges"] = []

    geo = add_geospatial(
        plan,
        input_node_id="in1",
        mode="point",
        config={"latColumn": "lat", "lonColumn": "lon", "outputColumn": "point"},
        node_id="geo1",
    )
    pm = add_pattern_mining(
        geo.plan,
        input_node_id="geo1",
        source_column="message",
        pattern="ERR-(\\d+)",
        output_column="error_code",
        match_mode="extract",
        node_id="pm1",
    )
    sj = add_stream_join(
        pm.plan,
        left_node_id="pm1",
        right_node_id="in2",
        left_keys=["id"],
        right_keys=["id"],
        strategy="left_lookup",
        node_id="sj1",
    )

    nodes = {str(node.get("id")): node for node in sj.plan["definition_json"]["nodes"]}
    assert nodes["geo1"]["metadata"]["operation"] == "geospatial"
    assert nodes["pm1"]["metadata"]["operation"] == "patternMining"
    assert nodes["sj1"]["metadata"]["operation"] == "streamJoin"
    assert nodes["sj1"]["metadata"]["joinType"] == "left"
    assert nodes["sj1"]["metadata"]["streamJoin"]["strategy"] == "left_lookup"
    assert nodes["sj1"]["metadata"]["streamJoin"]["timeDirection"] == "backward"
