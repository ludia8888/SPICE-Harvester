from __future__ import annotations

import pytest

from shared.services.pipeline.pipeline_plan_builder import (
    PipelinePlanBuilderError,
    add_edge,
    add_cast,
    add_compute_assignments,
    add_compute_column,
    add_udf,
    add_explode,
    add_filter,
    add_input,
    add_external_input,
    add_join,
    add_pivot,
    add_select_expr,
    add_sort,
    add_stream_join,
    add_output,
    add_union,
    configure_input_read,
    delete_edge,
    delete_node,
    new_plan,
    set_node_inputs,
    update_node_metadata,
    update_settings,
    update_output,
    validate_structure,
)


def test_new_plan_has_minimum_shape():
    plan = new_plan(goal="join two datasets", db_name="demo")
    assert plan["goal"] == "join two datasets"
    assert plan["data_scope"]["db_name"] == "demo"
    assert plan["definition_json"]["nodes"] == []
    assert plan["definition_json"]["edges"] == []


def test_add_input_and_output_wires_edges():
    plan = new_plan(goal="select rows", db_name="demo")
    res = add_input(plan, dataset_id="ds-1", node_id="in_1")
    plan = res.plan
    input_id = res.node_id

    out = add_output(plan, input_node_id=input_id, output_name="out_1")
    plan = out.plan

    errors, warnings = validate_structure(plan)
    assert errors == []
    assert warnings == []

    node_ids = {node["id"] for node in plan["definition_json"]["nodes"]}
    assert input_id in node_ids
    assert out.node_id in node_ids
    assert {"from": input_id, "to": out.node_id} in plan["definition_json"]["edges"]


def test_add_output_persists_output_metadata_into_outputs_entry():
    plan = new_plan(goal="output metadata", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan
    out = add_output(
        plan,
        input_node_id="in_1",
        output_name="out_1",
        output_kind="virtual",
        output_metadata={"query_sql": "select 1", "refresh_mode": "on_read"},
    )
    output_entry = out.plan["outputs"][0]
    assert output_entry["output_kind"] == "virtual"
    assert output_entry["output_metadata"]["query_sql"] == "select 1"
    assert output_entry["output_metadata"]["refresh_mode"] == "on_read"


def test_add_output_warns_when_legacy_alias_kind_used():
    plan = new_plan(goal="legacy output kind", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan
    out = add_output(plan, input_node_id="in_1", output_name="out_1", output_kind="unknown")
    assert out.plan["outputs"][0]["output_kind"] == "dataset"
    assert any("output_kind alias" in warning for warning in out.warnings)


def test_add_output_normalizes_dataset_write_metadata_aliases():
    plan = new_plan(goal="dataset output semantics", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan
    out = add_output(
        plan,
        input_node_id="in_1",
        output_name="out_1",
        output_kind="dataset",
        output_metadata={
            "pkSemantics": "append_state",
            "pkColumns": ["id"],
            "outputFormat": "json",
            "partitionBy": "ds",
        },
    )
    output_entry = out.plan["outputs"][0]
    metadata = output_entry["output_metadata"]
    assert metadata["write_mode"] == "append_only_new_rows"
    assert metadata["primary_key_columns"] == ["id"]
    assert metadata["output_format"] == "json"
    assert metadata["partition_by"] == ["ds"]
    assert any("legacy write-mode key" in warning for warning in out.warnings)


def test_add_input_supports_read_config():
    plan = new_plan(goal="read permissive", db_name="demo")
    res = add_input(
        plan,
        dataset_id="ds-1",
        node_id="in_1",
        read={"format": "csv", "mode": "PERMISSIVE", "corrupt_record_column": "_corrupt_record"},
    )
    node = next(node for node in res.plan["definition_json"]["nodes"] if node["id"] == "in_1")
    assert node["metadata"]["read"]["format"] == "csv"
    assert node["metadata"]["read"]["mode"] == "PERMISSIVE"


def test_add_external_input_creates_input_node_without_dataset_selection():
    plan = new_plan(goal="read jdbc", db_name="demo")
    res = add_external_input(
        plan,
        node_id="in_jdbc",
        source_name="orders_db",
        read={"format": "jdbc", "options": {"url": "jdbc:postgresql://db", "dbtable": "public.orders"}},
    )
    node = next(node for node in res.plan["definition_json"]["nodes"] if node["id"] == "in_jdbc")
    assert node["type"] == "input"
    assert "datasetId" not in node["metadata"]
    assert "datasetName" not in node["metadata"]
    assert node["metadata"]["sourceName"] == "orders_db"
    assert node["metadata"]["read"]["format"] == "jdbc"


def test_configure_input_read_patches_input_nodes_only():
    plan = new_plan(goal="read permissive", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan
    patched = configure_input_read(plan, node_id="in_1", read={"format": "csv", "mode": "PERMISSIVE"})
    node = next(node for node in patched.plan["definition_json"]["nodes"] if node["id"] == "in_1")
    assert node["metadata"]["read"]["format"] == "csv"
    assert node["metadata"]["read"]["mode"] == "PERMISSIVE"

    # Non-input nodes should be rejected.
    plan2 = add_filter(plan, input_node_id="in_1", expression="a > 1", node_id="f1").plan
    with pytest.raises(PipelinePlanBuilderError):
        configure_input_read(plan2, node_id="f1", read={"format": "csv"})

def test_add_join_requires_two_inputs_and_keys():
    plan = new_plan(goal="join", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan

    res_join = add_join(
        plan,
        left_node_id="left",
        right_node_id="right",
        left_keys=["id"],
        right_keys=["id"],
        join_type="inner",
    )
    plan = res_join.plan

    errors, _ = validate_structure(plan)
    assert errors == []


def test_add_join_accepts_hints_and_broadcast_flags():
    plan = new_plan(goal="join", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan
    res_join = add_join(
        plan,
        left_node_id="left",
        right_node_id="right",
        left_keys=["id"],
        right_keys=["id"],
        join_type="inner",
        join_hints={"right": "broadcast"},
        broadcast_right=True,
    )
    node = next(node for node in res_join.plan["definition_json"]["nodes"] if node["id"] == res_join.node_id)
    assert node["metadata"]["joinHints"]["right"] == "broadcast"
    assert node["metadata"]["broadcastRight"] is True

def test_add_join_rejects_cross_join():
    plan = new_plan(goal="join", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan

    with pytest.raises(PipelinePlanBuilderError):
        add_join(
            plan,
            left_node_id="left",
            right_node_id="right",
            left_keys=["id"],
            right_keys=["id"],
            join_type="cross",
        )


def test_add_cast_requires_column_and_type():
    plan = new_plan(goal="cast", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan

    with pytest.raises(PipelinePlanBuilderError):
        add_cast(plan, input_node_id="inp", casts=[{"column": "a"}])

    res = add_cast(plan, input_node_id="inp", casts=[{"column": "a", "type": "xsd:integer"}])
    errors, _ = validate_structure(res.plan)
    assert errors == []


def test_compute_column_and_assignments_build_metadata():
    plan = new_plan(goal="compute", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan
    res1 = add_compute_column(plan, input_node_id="inp", target_column="x", formula="a + 1")
    node1 = next(node for node in res1.plan["definition_json"]["nodes"] if node["id"] == res1.node_id)
    assert node1["metadata"]["operation"] == "compute"
    assert node1["metadata"]["targetColumn"] == "x"
    assert node1["metadata"]["formula"] == "a + 1"

    res2 = add_compute_assignments(
        plan,
        input_node_id="inp",
        assignments=[{"column": "x", "expression": "a + 1"}, {"column": "y", "expression": "a + 2"}],
    )
    node2 = next(node for node in res2.plan["definition_json"]["nodes"] if node["id"] == res2.node_id)
    assert node2["metadata"]["operation"] == "compute"
    assert isinstance(node2["metadata"]["assignments"], list)


def test_add_udf_builds_reference_metadata() -> None:
    plan = new_plan(goal="udf", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan
    result = add_udf(
        plan,
        input_node_id="inp",
        udf_id="udf-order-normalize",
        udf_version=3,
    )
    node = next(node for node in result.plan["definition_json"]["nodes"] if node["id"] == result.node_id)
    assert node["metadata"]["operation"] == "udf"
    assert node["metadata"]["udfId"] == "udf-order-normalize"
    assert node["metadata"]["udfVersion"] == 3


def test_add_udf_rejects_invalid_version() -> None:
    plan = new_plan(goal="udf", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan
    with pytest.raises(PipelinePlanBuilderError):
        add_udf(plan, input_node_id="inp", udf_id="udf-order-normalize", udf_version=0)


def test_select_expr_builds_metadata():
    plan = new_plan(goal="select expr", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan
    res = add_select_expr(plan, input_node_id="inp", expressions=["a", "b as bee"])
    node = next(node for node in res.plan["definition_json"]["nodes"] if node["id"] == res.node_id)
    assert node["metadata"]["operation"] == "select"
    assert node["metadata"]["expressions"] == ["a", "b as bee"]


def test_add_sort_supports_desc_prefix_and_dict_form():
    plan = new_plan(goal="sort", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan

    res = add_sort(plan, input_node_id="inp", columns=["a", "-b"])
    node = next(node for node in res.plan["definition_json"]["nodes"] if node["id"] == res.node_id)
    assert node["metadata"]["operation"] == "sort"
    assert node["metadata"]["columns"] == ["a", "-b"]

    res2 = add_sort(plan, input_node_id="inp", columns=[{"column": "a", "direction": "desc"}])
    node2 = next(node for node in res2.plan["definition_json"]["nodes"] if node["id"] == res2.node_id)
    assert node2["metadata"]["columns"] == [{"column": "a", "direction": "desc"}]


def test_add_explode_builds_metadata():
    plan = new_plan(goal="explode", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan
    res = add_explode(plan, input_node_id="inp", column="items")
    node = next(node for node in res.plan["definition_json"]["nodes"] if node["id"] == res.node_id)
    assert node["metadata"]["operation"] == "explode"
    assert node["metadata"]["columns"] == ["items"]


def test_add_union_builds_metadata():
    plan = new_plan(goal="union", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan
    res = add_union(plan, left_node_id="left", right_node_id="right", union_mode="pad")
    node = next(node for node in res.plan["definition_json"]["nodes"] if node["id"] == res.node_id)
    assert node["metadata"]["operation"] == "union"
    assert node["metadata"]["unionMode"] == "pad"


def test_add_stream_join_rejects_left_lookup_with_transformed_right_input():
    plan = new_plan(goal="stream join validation", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan
    right_filtered = add_filter(plan, input_node_id="right", expression="id > 0", node_id="right_filter").plan
    with pytest.raises(PipelinePlanBuilderError, match="requires right input to be a direct input node"):
        add_stream_join(
            right_filtered,
            left_node_id="left",
            right_node_id="right_filter",
            left_keys=["id"],
            right_keys=["id"],
            strategy="left_lookup",
            node_id="sj1",
        )


def test_add_stream_join_rejects_left_lookup_with_streaming_right_input():
    plan = new_plan(goal="stream join validation", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(
        plan,
        dataset_id="right",
        node_id="right",
        read={"format": "kafka"},
    ).plan
    with pytest.raises(PipelinePlanBuilderError, match="requires right input to be batch lookup source"):
        add_stream_join(
            plan,
            left_node_id="left",
            right_node_id="right",
            left_keys=["id"],
            right_keys=["id"],
            strategy="left_lookup",
            node_id="sj1",
        )


def test_add_pivot_builds_metadata():
    plan = new_plan(goal="pivot", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan
    res = add_pivot(
        plan,
        input_node_id="inp",
        index=["customer_id"],
        columns="category",
        values="amount",
        agg="sum",
    )
    node = next(node for node in res.plan["definition_json"]["nodes"] if node["id"] == res.node_id)
    assert node["metadata"]["operation"] == "pivot"
    assert node["metadata"]["pivot"]["index"] == ["customer_id"]
    assert node["metadata"]["pivot"]["columns"] == "category"
    assert node["metadata"]["pivot"]["values"] == "amount"
    assert node["metadata"]["pivot"]["agg"] == "sum"


def test_add_edge_is_idempotent():
    plan = new_plan(goal="edge", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan

    res1 = add_edge(plan, from_node_id="left", to_node_id="right")
    res2 = add_edge(res1.plan, from_node_id="left", to_node_id="right")

    edges = res2.plan["definition_json"]["edges"]
    assert edges.count({"from": "left", "to": "right"}) == 1


def test_delete_edge_is_noop_if_missing_but_warns():
    plan = new_plan(goal="edge", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan
    plan = add_edge(plan, from_node_id="left", to_node_id="right").plan

    removed = delete_edge(plan, from_node_id="left", to_node_id="right")
    assert {"from": "left", "to": "right"} not in removed.plan["definition_json"]["edges"]

    missing = delete_edge(removed.plan, from_node_id="left", to_node_id="right")
    assert missing.warnings


def test_set_node_inputs_replaces_incoming_edges_ordered():
    plan = new_plan(goal="join order", db_name="demo")
    plan = add_input(plan, dataset_id="left", node_id="left").plan
    plan = add_input(plan, dataset_id="right", node_id="right").plan
    join = add_join(plan, left_node_id="left", right_node_id="right", left_keys=["id"], right_keys=["id"])

    swapped = set_node_inputs(join.plan, node_id=join.node_id, input_node_ids=["right", "left"])
    incoming = [
        edge["from"]
        for edge in swapped.plan["definition_json"]["edges"]
        if edge.get("to") == join.node_id
    ]
    assert incoming == ["right", "left"]


def test_update_node_metadata_merges_and_unsets():
    plan = new_plan(goal="filter", db_name="demo")
    plan = add_input(plan, dataset_id="ds", node_id="inp").plan

    res = add_filter(plan, input_node_id="inp", expression="a > 1", node_id="f1")
    plan = res.plan
    assert next(node for node in plan["definition_json"]["nodes"] if node["id"] == "f1")["metadata"]["expression"] == "a > 1"

    patched = update_node_metadata(plan, node_id="f1", set_fields={"expression": "a >= 1"})
    assert next(node for node in patched.plan["definition_json"]["nodes"] if node["id"] == "f1")["metadata"]["expression"] == "a >= 1"

    unset = update_node_metadata(patched.plan, node_id="f1", unset_fields=["expression"])
    assert "expression" not in next(node for node in unset.plan["definition_json"]["nodes"] if node["id"] == "f1")["metadata"]

    with pytest.raises(PipelinePlanBuilderError):
        update_node_metadata(plan, node_id="f1", set_fields={"operation": "not_a_real_op"})


def test_update_settings_patches_definition_settings():
    plan = new_plan(goal="settings", db_name="demo")
    updated = update_settings(plan, set_fields={"spark_conf": {"spark.sql.ansi.enabled": "true"}})
    settings = updated.plan["definition_json"].get("settings")
    assert isinstance(settings, dict)
    assert settings["spark_conf"]["spark.sql.ansi.enabled"] == "true"

    removed = update_settings(updated.plan, unset_fields=["spark_conf"])
    settings2 = removed.plan["definition_json"].get("settings") or {}
    assert "spark_conf" not in settings2


def test_delete_node_removes_outputs_entry():
    plan = new_plan(goal="output delete", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan
    out = add_output(plan, input_node_id="in_1", output_name="out_1")
    plan = out.plan

    deleted = delete_node(plan, node_id=out.node_id)
    node_ids = {node["id"] for node in deleted.plan["definition_json"]["nodes"]}
    assert out.node_id not in node_ids
    assert deleted.plan["outputs"] == []


def test_update_output_renames_and_syncs_output_node_metadata():
    plan = new_plan(goal="output rename", db_name="demo")
    plan = add_input(plan, dataset_id="ds-1", node_id="in_1").plan
    out = add_output(plan, input_node_id="in_1", output_name="out_1")
    plan = out.plan

    updated = update_output(plan, output_name="out_1", set_fields={"output_name": "out_renamed"})
    assert updated.plan["outputs"][0]["output_name"] == "out_renamed"
    assert updated.plan["outputs"][0]["output_metadata"]["outputName"] == "out_renamed"

    output_nodes = [
        node
        for node in updated.plan["definition_json"]["nodes"]
        if node.get("type") == "output"
    ]
    assert len(output_nodes) == 1
    assert output_nodes[0]["metadata"]["outputName"] == "out_renamed"
