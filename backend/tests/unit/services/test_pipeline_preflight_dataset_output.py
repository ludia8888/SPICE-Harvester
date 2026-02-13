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
