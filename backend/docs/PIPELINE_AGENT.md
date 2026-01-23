# Pipeline Agent (LangGraph + MCP)

This document describes how the "natural language ETL" pipeline agent works in SPICE-Harvester.

## Goal

- Users describe ETL intentions in natural language.
- The system converts that request into a validated `PipelinePlan` graph, executes a sample-safe preview, and returns artifacts/insights.
- MCP is used to make plan construction deterministic (tool calls) instead of free-form DSL strings.
- LangGraph is used to orchestrate multi-step workflows (profile -> join strategy -> build -> verify -> preview -> repair/cleanse -> mapping).

## High-Level Architecture

- Agent service (`backend/agent`) runs a LangGraph state machine.
- BFF service (`backend/bff`) exposes REST endpoints used as the agent "tools" (plan compile/preview/evaluate/repair/...).
- Pipeline MCP server (`backend/mcp/pipeline_mcp_server.py`) provides deterministic plan-builder + analysis tools.

Key flow (happy path):

1) Context pack (Profiler): build safe dataset summaries + join/key/type hints.
2) Join strategist: select join keys/edges.
3) Plan builder: compile a `PipelinePlan` (MCP-based when enabled).
4) Intent verifier: check that plan matches the goal; suppress known false-positives.
5) Preview + join evaluation: sample-safe execution and quality signals.
6) Optional loops:
   - Repair loop: patch invalid plans.
   - Cleanse loop: apply suggested cleansing actions (if allowed).
   - Specs/mapping: generate ontology mapping specs (if allowed).

## Feature Flags (MCP Planner)

Enable MCP-based planning in your local `.env` (recommended so docker-compose recreates don't reset it):

```bash
PIPELINE_PLAN_MCP_PLANNER_ENABLED=true
PIPELINE_PLAN_MCP_TRANSFORM_ENABLED=true
PIPELINE_PLAN_MCP_REPAIR_ENABLED=true
```

## Determinism Guarantees

- Plan assembly uses plan-builder tool calls (`plan_add_input`, `plan_add_join`, `plan_add_select`, `plan_update_node_metadata`, ...).
- Server-side validation enforces:
  - Node/edge integrity (node ids, edge targets)
  - Basic schema constraints (required metadata for operations)
  - Join key shape (leftKeys/rightKeys lengths)
- Agent loops (repair/transform) must use unique idempotency keys per attempt to avoid 409 conflicts.

## Sampling & Preview Safety

Preview runs are intentionally sample-based and must avoid OOM:

- CSV artifacts are read using head-sampling (first N lines) instead of full-object reads.
- Multi-join previews use a larger input sample to avoid "empty join" false negatives.
- Preview runs cap join output rows (`__preview_meta__.max_output_rows`) to avoid pathological blow-ups on bad keys.

## Common Failure Modes (and Fixes)

- Empty-join false negatives on joins with high-cardinality keys:
  - Fixed by increasing input sample size for join previews (scaled by join count).
- 1:N joins missing from join candidates:
  - Fixed by scoring join candidates to allow PK->FK joins even when one side is non-unique.
- Intent verifier false-positives (e.g. missing output columns when already selected):
  - Fixed with more robust column extraction and suppression rules.
- Agent loops returning HTTP 409 due to idempotency key reuse:
  - Fixed by suffixing step ids with attempt counters for looping steps (verify-intent/preview/evaluate/inspect/cleanse).

## E2E Smoke (API)

Agent entrypoint:

- `POST /api/v1/agent/pipeline-runs` (Agent service, default `http://localhost:8004`)

BFF tool endpoints (Agent service calls these):

- `POST /api/v1/pipeline-plans/context-pack`
- `POST /api/v1/pipeline-plans/join-keys`
- `POST /api/v1/pipeline-plans/compile`
- `POST /api/v1/pipeline-plans/{plan_id}/verify-intent`
- `POST /api/v1/pipeline-plans/{plan_id}/preview`
- `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins`

Expected success signals:

- Agent response: `status=success`, `data.status=success`, `intent_status=pass`
- Preview: `preview.row_count > 0` for join-heavy plans (may be 0 if data truly has no matches)

