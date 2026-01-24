# Pipeline Agent (Single Autonomous Loop + MCP)

This document is a backend-facing quick reference for the "natural language ETL" pipeline agent.

For a fuller developer guide, see `docs/PIPELINE_AGENT.md`.

## Current Architecture (as implemented)

- Entry point: `POST /api/v1/agent/pipeline-runs` (BFF)
  - Router: `backend/bff/routers/agent_proxy.py`
  - Runtime (single autonomous loop): `backend/bff/services/pipeline_agent_autonomous_loop.py`
- Tool provider: Pipeline MCP server
  - `backend/mcp/pipeline_mcp_server.py`
  - Deterministic plan builder: `backend/shared/services/pipeline_plan_builder.py`
- Plan registry + deterministic helpers:
  - `backend/bff/routers/pipeline_plans.py` (`context-pack`, `compile`, `preview`, `inspect-preview`, `evaluate-joins`)

## Guarantees

- Single agent loop: BuildPrompt -> Inference -> Tool -> Observation -> ... -> Finish.
- No server-side silent rewrites of plan semantics; any changes must be explicit tool calls.
- Preview is sample-safe and join previews cap output rows via `__preview_meta__` safeguards.
