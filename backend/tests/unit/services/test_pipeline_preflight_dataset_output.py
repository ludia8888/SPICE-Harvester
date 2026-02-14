from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared.services.pipeline import pipeline_preflight_utils


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_when_dataset_pk_columns_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "value", "type": "xsd:string"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"value": "a"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {"datasetName": "source", "datasetBranch": "main"},
            },
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "append_only_new_rows",
                    "primary_key_columns": ["id"],
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("primary_key_columns missing in output schema" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_accepts_valid_dataset_write_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:string"}, {"name": "value", "type": "xsd:string"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": "1", "value": "a"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {"datasetName": "source", "datasetBranch": "main"},
            },
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "append_only_new_rows",
                    "primary_key_columns": ["id"],
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_ontology_link_missing_required_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:string"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": "1"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "in", "type": "input", "metadata": {"datasetName": "source", "datasetBranch": "main"}},
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "ontology-links",
                    "outputKind": "ontology",
                    "relationship_spec_type": "link",
                    "link_type_id": "owns",
                    "source_class_id": "Customer",
                    "target_class_id": "Order",
                    "predicate": "owns_order",
                    "cardinality": "1:n",
                    "source_key_column": "source_id",
                    "target_key_column": "target_id",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("ontology key columns missing in output schema" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_invalid_ontology_relationship_spec_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:string"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": "1"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "in", "type": "input", "metadata": {"datasetName": "source", "datasetBranch": "main"}},
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "ontology-links",
                    "outputKind": "ontology",
                    "relationship_spec_type": "unsupported",
                    "target_class_id": "Order",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("relationship_spec_type must be one of" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_partitioned_csv_dataset_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:string"}, {"name": "ds", "type": "xsd:string"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": "1", "ds": "2026-01-01"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "in", "type": "input", "metadata": {"datasetName": "source", "datasetBranch": "main"}},
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "csv",
                    "partition_by": ["ds"],
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("output_format=csv does not support partition_by" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_partitioned_json_dataset_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:string"}, {"name": "ds", "type": "xsd:string"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": "1", "ds": "2026-01-01"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "in", "type": "input", "metadata": {"datasetName": "source", "datasetBranch": "main"}},
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "json",
                    "partition_by": ["ds"],
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("output_format=json does not support partition_by" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_streaming_external_non_kafka_input() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "mode": "streaming",
                        "format": "json",
                        "path": "/tmp/events.json",
                    }
                },
            },
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("read.mode=streaming currently supports only read.format=kafka" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_streaming_external_missing_checkpoint() -> None:
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
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("read.mode=streaming requires read.checkpoint_location" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_kafka_json_without_schema() -> None:
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
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("kafka value_format=json requires read.schema/schema_columns" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_allows_kafka_avro_with_schema_registry_reference() -> None:
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
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    messages = [str(issue.get("message") or "") for issue in result.get("blocking_errors") or []]
    assert not any("kafka value_format=avro" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_kafka_avro_without_schema_or_registry() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {"read": {"format": "kafka", "value_format": "avro"}},
            },
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any(
        "kafka value_format=avro requires read.avro_schema or read.schema_registry(url+subject+version)"
        in message
        for message in messages
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_kafka_avro_with_missing_registry_version() -> None:
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
                        },
                    }
                },
            },
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("kafka value_format=avro schema registry requires version" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_kafka_avro_with_latest_registry_version() -> None:
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
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("kafka value_format=avro schema registry version=latest is not allowed" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_allows_batch_kafka_without_checkpoint() -> None:
    definition = {
        "nodes": [
            {
                "id": "in",
                "type": "input",
                "metadata": {
                    "read": {
                        "format": "kafka",
                        "options": {
                            "kafka.bootstrap.servers": "localhost:9092",
                            "subscribe": "orders",
                        },
                    }
                },
            },
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    messages = [str(issue.get("message") or "") for issue in result.get("blocking_errors") or []]
    assert not any("read.mode=streaming requires read.checkpoint_location" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_streaming_without_watermark() -> None:
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
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "target",
                    "outputKind": "dataset",
                    "write_mode": "snapshot_replace",
                    "output_format": "parquet",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("execution_semantics=streaming requires incremental.watermark_column" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_geotemporal_missing_required_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "event_time", "type": "xsd:dateTime"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"event_time": "2026-01-01T00:00:00Z"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "in", "type": "input", "metadata": {"datasetName": "source", "datasetBranch": "main"}},
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "geo_target",
                    "outputKind": "geotemporal",
                    "time_column": "event_time",
                    "geometry_column": "geom",
                    "geometry_format": "wkt",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("geotemporal required columns missing in output schema" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_virtual_dataset_write_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "value", "type": "xsd:string"}]},
                dataset_id="ds-1",
                name="source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"value": "a"}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "in", "type": "input", "metadata": {"datasetName": "source", "datasetBranch": "main"}},
            {
                "id": "out",
                "type": "output",
                "metadata": {
                    "outputName": "virtual_target",
                    "outputKind": "virtual",
                    "query_sql": "select value from source",
                    "refresh_mode": "scheduled",
                    "write_mode": "append_only_new_rows",
                },
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("virtual output does not support dataset write settings" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_left_lookup_with_transformed_right_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        selection = kwargs.get("selection")
        dataset_name = str(getattr(selection, "dataset_name", "") or "")
        if dataset_name == "left_stream":
            schema_columns = [{"name": "id", "type": "xsd:integer"}]
            sample_rows = [{"id": 1}]
        else:
            schema_columns = [{"name": "id", "type": "xsd:integer"}]
            sample_rows = [{"id": 1}]
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": schema_columns},
                dataset_id=f"ds-{dataset_name or 'x'}",
                name=dataset_name or "source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": sample_rows}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left_stream", "datasetBranch": "main"}},
            {"id": "right", "type": "input", "metadata": {"datasetName": "right_lookup", "datasetBranch": "main"}},
            {"id": "right_filter", "type": "transform", "metadata": {"operation": "filter", "expression": "id > 0"}},
            {
                "id": "sj",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "left_lookup"},
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "joined"}},
        ],
        "edges": [
            {"from": "right", "to": "right_filter"},
            {"from": "left", "to": "sj"},
            {"from": "right_filter", "to": "sj"},
            {"from": "sj", "to": "out"},
        ],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("streamJoin strategy=left_lookup requires right input to be a direct input node" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_left_lookup_with_streaming_right_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        selection = kwargs.get("selection")
        dataset_name = str(getattr(selection, "dataset_name", "") or "")
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
                dataset_id=f"ds-{dataset_name or 'x'}",
                name=dataset_name or "source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": 1}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left_stream", "datasetBranch": "main"}},
            {
                "id": "right",
                "type": "input",
                "metadata": {
                    "datasetName": "right_lookup",
                    "datasetBranch": "main",
                    "read": {"format": "kafka"},
                },
            },
            {
                "id": "sj",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "left_lookup"},
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "joined"}},
        ],
        "edges": [
            {"from": "left", "to": "sj"},
            {"from": "right", "to": "sj"},
            {"from": "sj", "to": "out"},
        ],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("streamJoin strategy=left_lookup requires right input to be batch lookup source" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_static_with_transformed_right_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        selection = kwargs.get("selection")
        dataset_name = str(getattr(selection, "dataset_name", "") or "")
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
                dataset_id=f"ds-{dataset_name or 'x'}",
                name=dataset_name or "source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": 1}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left_stream", "datasetBranch": "main"}},
            {"id": "right", "type": "input", "metadata": {"datasetName": "right_lookup", "datasetBranch": "main"}},
            {"id": "right_filter", "type": "transform", "metadata": {"operation": "filter", "expression": "id > 0"}},
            {
                "id": "sj",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "static"},
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "joined"}},
        ],
        "edges": [
            {"from": "right", "to": "right_filter"},
            {"from": "left", "to": "sj"},
            {"from": "right_filter", "to": "sj"},
            {"from": "sj", "to": "out"},
        ],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("streamJoin strategy=static requires right input to be a direct input node" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_static_with_streaming_right_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        selection = kwargs.get("selection")
        dataset_name = str(getattr(selection, "dataset_name", "") or "")
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
                dataset_id=f"ds-{dataset_name or 'x'}",
                name=dataset_name or "source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": 1}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left_stream", "datasetBranch": "main"}},
            {
                "id": "right",
                "type": "input",
                "metadata": {
                    "datasetName": "right_lookup",
                    "datasetBranch": "main",
                    "read": {"format": "kafka"},
                },
            },
            {
                "id": "sj",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "static"},
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "joined"}},
        ],
        "edges": [
            {"from": "left", "to": "sj"},
            {"from": "right", "to": "sj"},
            {"from": "sj", "to": "out"},
        ],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("streamJoin strategy=static requires right input to be batch lookup source" in message for message in messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_compute_pipeline_preflight_blocks_static_non_left_join_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve_dataset_version(*args, **kwargs):
        selection = kwargs.get("selection")
        dataset_name = str(getattr(selection, "dataset_name", "") or "")
        return SimpleNamespace(
            dataset=SimpleNamespace(
                schema_json={"columns": [{"name": "id", "type": "xsd:integer"}]},
                dataset_id=f"ds-{dataset_name or 'x'}",
                name=dataset_name or "source",
                branch="main",
            ),
            version=SimpleNamespace(sample_json={"rows": [{"id": 1}]}),
        )

    monkeypatch.setattr(pipeline_preflight_utils, "resolve_dataset_version", _fake_resolve_dataset_version)

    definition = {
        "nodes": [
            {"id": "left", "type": "input", "metadata": {"datasetName": "left_stream", "datasetBranch": "main"}},
            {"id": "right", "type": "input", "metadata": {"datasetName": "right_lookup", "datasetBranch": "main"}},
            {
                "id": "sj",
                "type": "transform",
                "metadata": {
                    "operation": "streamJoin",
                    "joinType": "full",
                    "leftKeys": ["id"],
                    "rightKeys": ["id"],
                    "streamJoin": {"strategy": "static"},
                },
            },
            {"id": "out", "type": "output", "metadata": {"outputName": "joined"}},
        ],
        "edges": [
            {"from": "left", "to": "sj"},
            {"from": "right", "to": "sj"},
            {"from": "sj", "to": "out"},
        ],
    }
    result = await pipeline_preflight_utils.compute_pipeline_preflight(
        definition=definition,
        db_name="demo",
        dataset_registry=SimpleNamespace(),
        branch="main",
    )
    assert result["has_blocking_errors"] is True
    messages = [str(issue.get("message") or "") for issue in result["blocking_errors"]]
    assert any("streamJoin strategy=static requires joinType=left" in message for message in messages)
