from __future__ import annotations

from shared.services.pipeline.pipeline_definition_validator import (
    PipelineDefinitionValidationPolicy,
    validate_pipeline_definition,
)
from shared.services.pipeline.pipeline_transform_spec import SUPPORTED_TRANSFORMS


def _spark_policy(*, require_output: bool = True) -> PipelineDefinitionValidationPolicy:
    return PipelineDefinitionValidationPolicy(
        supported_ops=SUPPORTED_TRANSFORMS,
        require_output=require_output,
        normalize_metadata=True,
        require_udf_reference=True,
    )


def test_validate_pipeline_definition_requires_nodes() -> None:
    result = validate_pipeline_definition({}, policy=_spark_policy())
    assert result.errors == ["Pipeline has no nodes"]


def test_validate_pipeline_definition_requires_output_node_when_configured() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {"id": "t", "type": "transform", "metadata": {"operation": "filter", "expression": "x > 0"}},
        ],
        "edges": [{"from": "in", "to": "t"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy(require_output=True))
    assert "Pipeline has no output node" in result.errors


def test_validate_pipeline_definition_detects_missing_edge_nodes() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {"id": "out", "type": "output"},
        ],
        "edges": [{"from": "in", "to": "missing"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert "Pipeline edge references missing node: in->missing" in result.errors


def test_validate_pipeline_definition_normalizes_metadata_fields_to_columns() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {"id": "t", "type": "transform", "metadata": {"operation": "drop", "fields": ["a"]}},
            {"id": "out", "type": "output"},
        ],
        "edges": [{"from": "in", "to": "t"}, {"from": "t", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert result.errors == []


def test_validate_pipeline_definition_reports_missing_columns_for_normalize() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {"id": "t", "type": "transform", "metadata": {"operation": "normalize"}},
            {"id": "out", "type": "output"},
        ],
        "edges": [{"from": "in", "to": "t"}, {"from": "t", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert "normalize missing columns on node t" in result.errors


def test_validate_pipeline_definition_uses_custom_udf_message() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {"id": "t", "type": "transform", "metadata": {"operation": "udf"}},
            {"id": "out", "type": "output"},
        ],
        "edges": [{"from": "in", "to": "t"}, {"from": "t", "to": "out"}],
    }
    policy = PipelineDefinitionValidationPolicy(
        supported_ops=SUPPORTED_TRANSFORMS,
        require_output=True,
        normalize_metadata=True,
        udf_error_message_template="udf not allowed on {node_id}",
    )
    result = validate_pipeline_definition(definition, policy=policy)
    assert "udf not allowed on t" in result.errors


def test_validate_pipeline_definition_udf_requires_udf_id_when_reference_policy_enabled() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {"id": "t", "type": "transform", "metadata": {"operation": "udf"}},
            {"id": "out", "type": "output"},
        ],
        "edges": [{"from": "in", "to": "t"}, {"from": "t", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert "udf requires udfId on node t" in result.errors


def test_validate_pipeline_definition_udf_rejects_inline_code_when_reference_policy_enabled() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {
                "id": "t",
                "type": "transform",
                "metadata": {"operation": "udf", "udfCode": "def transform(row): return row"},
            },
            {"id": "out", "type": "output"},
        ],
        "edges": [{"from": "in", "to": "t"}, {"from": "t", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert "udfCode is not allowed on node t; use udfId (+udfVersion)" in result.errors


def test_validate_pipeline_definition_udf_requires_version_when_pinning_enabled() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {
                "id": "t",
                "type": "transform",
                "metadata": {"operation": "udf", "udfId": "udf-orders"},
            },
            {"id": "out", "type": "output"},
        ],
        "edges": [{"from": "in", "to": "t"}, {"from": "t", "to": "out"}],
    }
    policy = PipelineDefinitionValidationPolicy(
        supported_ops=SUPPORTED_TRANSFORMS,
        require_output=True,
        normalize_metadata=True,
        require_udf_reference=True,
        require_udf_version_pinning=True,
    )
    result = validate_pipeline_definition(definition, policy=policy)
    assert "udfVersion is required on node t" in result.errors


def test_validate_pipeline_definition_rejects_invalid_dataset_output_metadata() -> None:
    definition = {
        "nodes": [
            {"id": "in", "type": "input"},
            {
                "id": "out",
                "type": "output",
                "metadata": {"outputName": "orders", "write_mode": "append_only_new_rows"},
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert "output orders invalid metadata on node out: write_mode=append_only_new_rows requires primary_key_columns" in result.errors


def test_validate_pipeline_definition_rejects_streaming_non_kafka_external_input() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "mode": "streaming",
                        "format": "json",
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert (
        "input node in read.mode=streaming currently supports only read.format=kafka"
        in result.errors
    )


def test_validate_pipeline_definition_rejects_streaming_without_checkpoint_location() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "mode": "streaming",
                        "format": "kafka",
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert (
        "input node in read.mode=streaming requires read.checkpoint_location"
        in result.errors
    )


def test_validate_pipeline_definition_rejects_kafka_json_without_schema() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "format": "kafka",
                        "value_format": "json",
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert (
        "input node in kafka value_format=json requires read.schema/schema_columns"
        in result.errors
    )


def test_validate_pipeline_definition_allows_kafka_avro_with_schema_registry_reference() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "format": "kafka",
                        "value_format": "avro",
                        "schema_registry": {
                            "url": "https://registry.example",
                            "subject": "orders-value",
                            "version": 7,
                        },
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert not any("kafka value_format=avro" in message for message in result.errors)


def test_validate_pipeline_definition_rejects_kafka_avro_without_schema_or_registry() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {"read": {"format": "kafka", "value_format": "avro"}},
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert (
        "input node in kafka value_format=avro requires read.avro_schema or read.schema_registry(url+subject+version)"
        in result.errors
    )


def test_validate_pipeline_definition_rejects_kafka_avro_with_missing_registry_version() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "format": "kafka",
                        "value_format": "avro",
                        "schema_registry": {"url": "https://registry.example", "subject": "orders-value"},
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert (
        "input node in kafka value_format=avro schema registry requires version"
        in result.errors
    )


def test_validate_pipeline_definition_rejects_kafka_avro_with_latest_registry_version() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "format": "kafka",
                        "value_format": "avro",
                        "schema_registry": {
                            "url": "https://registry.example",
                            "subject": "orders-value",
                            "version": "latest",
                        },
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert (
        "input node in kafka value_format=avro schema registry version=latest is not allowed; pin an integer version >= 1"
        in result.errors
    )


def test_validate_pipeline_definition_allows_batch_kafka_without_checkpoint() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "format": "kafka",
                        "options": {"subscribe": "orders"},
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert "read.mode=streaming requires read.checkpoint_location" not in result.errors


def test_validate_pipeline_definition_requires_watermark_for_streaming_semantics() -> None:
    definition = {
        "execution_mode": "streaming",
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "mode": "streaming",
                        "format": "kafka",
                        "checkpoint_location": "/tmp/chk",
                    }
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "orders"}},
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = validate_pipeline_definition(definition, policy=_spark_policy())
    assert "execution_semantics=streaming requires incremental.watermark_column" in result.errors
