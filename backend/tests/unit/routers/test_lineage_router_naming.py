from __future__ import annotations

from bff.routers.lineage import _suggest_remediation_actions


def test_suggest_remediation_actions_uses_graph_target_kind() -> None:
    actions = _suggest_remediation_actions(
        artifacts=[
            {
                "kind": "graph",
                "node_id": "artifact:graph:demo:main:Customer/c1",
                "label": "Customer/c1",
            }
        ]
    )

    assert actions
    assert actions[0]["action"] == "REPLAY_TO_TARGET"
    assert actions[0]["target_kind"] == "graph"
