from __future__ import annotations

from shared.models.pipeline_plan import PipelinePlan
from shared.models.pipeline_task_spec import PipelineTaskScope, PipelineTaskSpec
from shared.services.pipeline.pipeline_task_spec_policy import clamp_task_spec, validate_plan_against_task_spec


def _plan_with_op(op: str) -> PipelinePlan:
    return PipelinePlan.model_validate(
        {
            "goal": "test",
            "data_scope": {"db_name": "demo", "dataset_ids": ["a", "b"]},
            "definition_json": {
                "nodes": [
                    {"id": "in1", "type": "input", "metadata": {"datasetId": "a"}},
                    {"id": "in2", "type": "input", "metadata": {"datasetId": "b"}},
                    {"id": "t1", "type": "transform", "metadata": {"operation": op}},
                    {"id": "out", "type": "output", "metadata": {"outputName": "out"}},
                ],
                "edges": [
                    {"from": "in1", "to": "t1"},
                    {"from": "in2", "to": "t1"},
                    {"from": "t1", "to": "out"},
                ],
            },
            "outputs": [{"output_name": "out", "output_kind": "unknown"}],
        }
    )


def test_clamp_task_spec_disables_join_for_single_dataset():
    spec = PipelineTaskSpec(scope=PipelineTaskScope.pipeline, allow_join=True, allow_write=True)
    clamped = clamp_task_spec(spec=spec, dataset_count=1)
    assert clamped.allow_write is False
    assert clamped.allow_join is False


def test_policy_rejects_join_when_disallowed():
    plan = _plan_with_op("join")
    spec = PipelineTaskSpec(scope=PipelineTaskScope.pipeline, allow_join=False, allow_advanced_transforms=True)
    errors, warnings = validate_plan_against_task_spec(plan=plan, task_spec=spec)
    assert warnings == []
    assert any("allow_join=false" in err for err in errors)


def test_policy_rejects_advanced_transform_when_disallowed():
    plan = _plan_with_op("groupBy")
    spec = PipelineTaskSpec(scope=PipelineTaskScope.pipeline, allow_join=True, allow_advanced_transforms=False)
    errors, _ = validate_plan_against_task_spec(plan=plan, task_spec=spec)
    assert any("allow_advanced_transforms=false" in err for err in errors)


def test_policy_rejects_report_only_scope():
    plan = _plan_with_op("filter")
    spec = PipelineTaskSpec(scope=PipelineTaskScope.report_only)
    errors, _ = validate_plan_against_task_spec(plan=plan, task_spec=spec)
    assert any("report_only" in err for err in errors)

