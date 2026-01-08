from __future__ import annotations

from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, StateGraph

from agent.models import AgentToolCall
from agent.services.agent_runtime import AgentRuntime


class AgentState(TypedDict):
    run_id: str
    actor: str
    steps: List[AgentToolCall]
    step_index: int
    results: List[Dict[str, Any]]
    context: Dict[str, Any]
    dry_run: bool
    request_headers: Dict[str, str]
    request_id: str | None
    failed: bool


def _should_continue(state: AgentState) -> str:
    if state.get("failed"):
        return "end"
    if state.get("step_index", 0) >= len(state.get("steps", [])):
        return "end"
    return "continue"


async def _execute_step(state: AgentState, runtime: AgentRuntime) -> AgentState:
    idx = state.get("step_index", 0)
    steps = state.get("steps", [])
    if idx >= len(steps):
        return state
    tool_call = steps[idx]
    result = await runtime.execute_tool_call(
        run_id=state["run_id"],
        actor=state["actor"],
        step_index=idx,
        tool_call=tool_call,
        context=state.get("context", {}),
        dry_run=bool(state.get("dry_run")),
        request_headers=state.get("request_headers", {}),
        request_id=state.get("request_id"),
    )
    results = list(state.get("results", []))
    results.append(result)
    failed = result.get("status") == "failure"
    return {
        **state,
        "step_index": idx + 1,
        "results": results,
        "failed": failed,
    }


def build_agent_graph(runtime: AgentRuntime):
    graph: StateGraph = StateGraph(AgentState)

    async def execute_step(state: AgentState) -> AgentState:
        return await _execute_step(state, runtime)

    graph.add_node("execute_step", execute_step)
    graph.set_entry_point("execute_step")
    graph.add_conditional_edges(
        "execute_step",
        _should_continue,
        {"continue": "execute_step", "end": END},
    )
    return graph.compile()


async def run_agent_graph(runtime: AgentRuntime, initial_state: AgentState) -> AgentState:
    app = build_agent_graph(runtime)
    return await app.ainvoke(initial_state)
