# Frontend Policies (Current State)

> Updated: 2026-01-08

This document reflects the **current frontend implementation** in `frontend/`.

## 1) State Management

- Zustand store: `frontend/src/state/store.ts`
- Current state:
  - `activeNav` (left navigation)
  - `pipelineContext` (selection context)

## 2) API Layer

- BFF client: `frontend/src/api/bff.ts`
- Calls are direct to `/api/v1` via `VITE_API_BASE_URL`.

## 3) Authentication

- **Not implemented in the current frontend**.
- Token handling (X-Admin-Token / Bearer) must be added by the client.

## 4) Command Tracking

- **Not implemented in the current frontend**.
- Writes return `202 + command_id`; clients must poll `/api/v1/commands/{id}/status`.

## 5) Branch / URL Context

- **Not implemented**. There is no URL SSoT or branch-aware navigation in the current UI.

## 6) Rate Limit Handling

- **Not implemented** (no automatic retry/backoff in the current UI).
