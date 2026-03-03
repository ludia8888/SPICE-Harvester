---
orphan: true
---

# SPICE gRPC Migration Stage Plan (2026-03-03)

## Why this update
- Current OMS gRPC is an **HTTP bridge** (`gRPC -> ASGITransport -> FastAPI`), not a native domain gRPC runtime.
- Current proto still uses **generic passthrough** (`OmsRequest`/`OmsResponse` with `json_body`), so typed-domain benefits are partial.
- Some internal callers (notably MCP) were still using OMS HTTP directly, making "REST removal now" unsafe.

## Current baseline (confirmed)
- Bridge server: `backend/oms/grpc/server.py`
- Generic proto: `backend/proto/spice/oms/v1/oms_gateway.proto`
- BFF mixed pattern (typed RPC + generic verbs): `backend/bff/services/oms_client.py`
- MCP OMS direct path existed via `mcp_servers/pipeline_mcp_http.py::oms_json` (now switched to gRPC transport in this round).

## Enforced migration order (hard requirement)
1. **Transport boundary hardening (done/ongoing)**
   - Remove direct OMS HTTP usage from internal tools/clients that can move now.
   - Keep BFF as single external REST entry point.
2. **Typed proto expansion**
   - Introduce domain messages per service (Database/Ontology/Query/Action/...).
   - Keep generic RPCs temporarily for fallback compatibility.
3. **Native OMS gRPC handlers**
   - Replace bridge `_dispatch()` path with direct domain service calls.
   - Preserve REST error contract mapping at BFF boundary.
4. **MCP + adapters full migration**
   - Migrate remaining OMS HTTP assumptions (if any) to gRPC clients.
5. **REST business endpoint retirement**
   - Remove OMS business HTTP routes only after 1~4 are green.

## Non-negotiable guardrails
- No direct `http://oms:8000/api/v1/...` from MCP/public tool paths.
- All client-visible contracts remain BFF REST/OpenAPI-first.
- Gate A/Gate B must stay green at each phase boundary.

## Short-term acceptance for this round
- MCP OMS transport uses gRPC bridge client.
- Unit tests cover:
  - gRPC auth enforcement (`UNAUTHENTICATED`/`PERMISSION_DENIED`)
  - gRPC request/response contract mapping
  - MCP OMS helper timeout/error handling through gRPC path

## Deferred items (next round)
- Full typed-domain proto V2 cutover.
- Native gRPC OMS handlers (bridge removal).
- Hard removal of OMS business HTTP routes once MCP/adapters are fully migrated.
