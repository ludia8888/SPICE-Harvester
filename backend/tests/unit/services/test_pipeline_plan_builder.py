from __future__ import annotations

import pytest

from shared.services.pipeline_plan_builder import (
    PipelinePlanBuilderError,
    add_edge,
    add_cast,
    add_filter,
    add_input,
    add_join,
    add_output,
    delete_edge,
    delete_node,
    new_plan,
    set_node_inputs,
    update_node_metadata,
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

    output_nodes = [
        node
        for node in updated.plan["definition_json"]["nodes"]
        if node.get("type") == "output"
    ]
    assert len(output_nodes) == 1
    assert output_nodes[0]["metadata"]["outputName"] == "out_renamed"
