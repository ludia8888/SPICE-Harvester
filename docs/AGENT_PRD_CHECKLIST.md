# Agent PRD Checklist (Single Autonomous Loop + Tools)

> Primary references:
> - Runtime spec: `docs/AGENT_PRD.md` (AGENT-RUNTIME-ARCH-001)
> - Pipeline agent dev guide: `docs/PIPELINE_AGENT.md`

This checklist tracks enterprise readiness for the **Pipeline Agent** architecture:
- **Single autonomous loop** (no multi-agent routing/handoffs/parallel sub-agents)
- **Tool-first execution** via MCP (no DSL string generation)
- **No silent rewrites** (repairs happen only through explicit edit/patch tools)

Legacy plan-only control plane (`/api/v1/agent-plans/*`) has been removed from the supported developer surface.

## Status Legend

- `DONE`: implemented + has at least one verification signal (unit test / smoke / documented invariants)
- `PARTIAL`: implemented but missing key guardrails or regression coverage
- `TODO`: not implemented

---

## 0) Core Loop (Runtime Controller)

| ID | Status | Evidence (code/docs/tests) | Notes |
|---|---|---|---|
| LOOP-001 | DONE | `backend/bff/services/pipeline_agent_autonomous_loop.py` | BuildPrompt -> Inference -> Batch Tool Calls -> Observation -> ... -> Finish |
| LOOP-002 | DONE | `backend/bff/services/pipeline_agent_autonomous_loop.py` | Batched tool calls per inference (`tool_calls`, max 12/step) |
| LOOP-003 | DONE | `backend/bff/services/pipeline_agent_autonomous_loop.py` | Append-only JSONL prompt log (prefix-caching friendly) |
| LOOP-004 | DONE | `backend/bff/services/pipeline_agent_autonomous_loop.py` | Deterministic server-side compaction snapshot (no LLM-written summarization) |
| LOOP-005 | PARTIAL | `backend/bff/services/pipeline_agent_autonomous_loop.py` | Step budget exists (`max_steps`), but explicit time/failure budgets + policy are not standardized |
| LOOP-006 | TODO |  | Tool execution concurrency policy (if needed) + deterministic ordering guarantees at runtime |

---

## 1) Tools (MCP) - Analysis + Plan Builder + Repair

| ID | Status | Evidence (code/docs/tests) | Notes |
|---|---|---|---|
| MCP-001 | DONE | `backend/mcp_servers/pipeline_mcp_server.py` | Analysis tools: `context_pack_*` (null/PK-FK/types/join-plan) |
| MCP-002 | DONE | `backend/mcp_servers/pipeline_mcp_server.py`, `backend/shared/services/pipeline_plan_builder.py` | Plan builder tools: `plan_new`, `plan_add_*` (join/filter/groupBy/window/cast/...) |
| MCP-003 | DONE | `backend/mcp_servers/pipeline_mcp_server.py` | Edit/patch tools: node delete, metadata update, edge operations |
| MCP-004 | DONE | `backend/mcp_servers/pipeline_mcp_server.py` | Validation/preview tools: `plan_validate`, `plan_preview`, `plan_evaluate_joins`, `preview_inspect` |
| MCP-005 | PARTIAL | `docs/PIPELINE_AGENT.md` | Tool schema discoverability/documentation quality (examples + failure modes) can be expanded |

---

## 2) Semantics / Safety

| ID | Status | Evidence (code/docs/tests) | Notes |
|---|---|---|---|
| SAF-001 | DONE | `backend/bff/services/pipeline_plan_validation.py` | No silent rewrites (validator must not auto-fix joins/casts/outputs) |
| SAF-002 | DONE | `backend/bff/services/pipeline_agent_autonomous_loop.py`, `backend/shared/utils/llm_safety.py` | PII masking when echoing observations back into prompts/logs |
| SAF-003 | PARTIAL | `backend/shared/services/llm_quota.py`, `backend/bff/routers/pipeline_plans.py` | Tenant/model policy + quota enforcement is wired, but end-to-end quota UX needs hardened messaging |
| SAF-004 | TODO |  | Enterprise-grade redaction policies per tenant + audited override workflows (if required) |

---

## 3) API Surfaces (Frontend Contract)

| ID | Status | Evidence (code/docs/tests) | Notes |
|---|---|---|---|
| API-001 | DONE | `backend/bff/routers/agent_proxy.py` | `POST /api/v1/agent/pipeline-runs` is the primary chat surface |
| API-002 | DONE | `backend/bff/routers/pipeline_plans.py`, `backend/bff/services/pipeline_plan_autonomous_compiler.py` | `POST /api/v1/pipeline-plans/compile` delegates to the same single-loop runtime |
| API-003 | DONE | `docs/API_REFERENCE.md` | API reference regenerated; legacy agent-plans/session endpoints removed |
| API-004 | PARTIAL | `backend/tests/test_openapi_contract_smoke.py` | Smoke coverage should be updated to exercise pipeline agent endpoints directly |

---

## 4) Frontend (Agent Chat UX)

| ID | Status | Evidence (code/docs/tests) | Notes |
|---|---|---|---|
| UI-001 | DONE | `frontend/src/pages/AIAgentPage.tsx`, `frontend/src/api/bff.ts` | Pipeline Agent chat UI calls `/api/v1/agent/pipeline-runs` |
| UI-002 | PARTIAL | `frontend/src/pages/AIAgentPage.tsx` | Conversation persistence/history is intentionally minimal; consider explicit session storage if needed |

---

## 5) Testing / QA

| ID | Status | Evidence (code/docs/tests) | Notes |
|---|---|---|---|
| QA-001 | DONE | `backend/tests/unit` | Backend unit tests green |
| QA-002 | PARTIAL | `scripts/e2e_agent_pipeline_demo.sh`, `docs/PIPELINE_AGENT.md` | E2E demo exists but must be kept aligned with pipeline agent API surface |
| QA-003 | TODO |  | Harsh E2E: multi-dataset join spanning tree + groupBy/aggregate + window ranking + cast repairs (real datasets) |

<!-- DOC_SYNC: 2026-02-13 Foundry pipeline parity + runtime consistency sweep -->
