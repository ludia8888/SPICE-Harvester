# AGENT PRD 체크리스트 (Traceability)

> 기준 문서: `docs/AGENT_PRD.md`  
> 목적: “요구사항 → 구현/검증 근거”를 1:1로 매핑하고, 빠짐없이 갭을 제거한다.

## 상태 표기

- `DONE`: 구현 + 최소 1개 검증(테스트/리그레션/운영 메트릭) 근거 존재
- `PARTIAL`: 일부 구현 존재(근거 있음)이나 요구사항 전체를 만족하지 못함
- `TODO`: 구현/검증 근거 없음 (백지 또는 설계만)

---

## 1) 인증·권한·테넌시

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| AUTH-001 | DONE | `backend/bff/middleware/auth.py`, `backend/shared/security/user_context.py`, `backend/agent/services/agent_runtime.py`, `backend/tests/unit/middleware/test_middleware_fixes.py`, `backend/tests/unit/services/test_agent_runtime_delegated_auth.py` | |
| AUTH-002 | DONE | `backend/bff/middleware/auth.py`, `backend/shared/services/agent_tool_registry.py`, `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/context_tools.py`, `backend/bff/routers/document_bundles.py`, `backend/tests/unit/middleware/test_middleware_fixes.py`, `backend/bff/tests/test_context_tools_router.py`, `backend/bff/tests/test_document_bundles_router.py` | |
| AUTH-003 | DONE | `backend/bff/middleware/auth.py`, `backend/agent/services/agent_runtime.py`, `backend/tests/unit/middleware/test_middleware_fixes.py`, `backend/tests/unit/services/test_agent_runtime_delegated_auth.py` | |
| AUTH-004 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/shared/services/agent_registry.py`, `backend/shared/services/agent_plan_registry.py`, `backend/shared/services/audit_log_store.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| AUTH-005 | DONE | `backend/shared/services/agent_policy_registry.py`, `backend/shared/services/llm_quota.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/routers/context_tools.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py`, `backend/tests/unit/services/test_llm_quota.py` | |

---

## 2) 세션·대화 상태 관리

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| SESS-001 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| SESS-002 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/middleware/auth.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_events.py`, `backend/bff/tests/test_agent_sessions_llm_usage_metrics.py` | |
| SESS-003 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| SESS-004 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/tests/test_agent_sessions_summarize_remove.py` | |
| SESS-005 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/tests/unit/services/test_agent_session_state_machine.py`, `backend/bff/tests/test_agent_sessions_variables_clarifications.py` | |
| SESS-006 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/agent/routers/agent.py`, `backend/bff/tests/test_agent_sessions_events.py` | |

---

## 3) LLM 모델 게이트웨이 및 모델 선택

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| LLM-001 | DONE | `backend/shared/services/llm_gateway.py`, `backend/shared/config/settings.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` | |
| LLM-002 | DONE | `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/shared/services/llm_gateway.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| LLM-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/shared/services/agent_model_registry.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| LLM-004 | DONE | `backend/shared/services/llm_gateway.py`, `backend/bff/services/agent_plan_compiler.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` | |
| LLM-005 | DONE | `backend/shared/services/agent_model_registry.py`, `backend/shared/services/llm_gateway.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` | |
| LLM-006 | DONE | `backend/shared/services/llm_gateway.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` | |

---

## 4) 에이전트 오케스트레이션 (Closed-loop 실행 엔진)

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| AGT-001 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/bff/services/agent_plan_compiler.py`, `backend/agent/routers/agent.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| AGT-002 | DONE | `backend/agent/services/agent_runtime.py`, `backend/shared/services/event_store.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_events.py` | |
| AGT-003 | DONE | `backend/agent/services/agent_runtime.py`, `backend/agent/services/agent_graph.py`, `backend/bff/routers/agent_sessions.py`, `backend/tests/unit/services/test_agent_graph_parallel.py` | |
| AGT-004 | DONE | `backend/bff/services/agent_plan_validation.py`, `backend/bff/routers/agent_plans.py`, `backend/tests/unit/services/test_agent_graph_simulation_gate.py` | |
| AGT-005 | DONE | `backend/agent/services/agent_policy.py`, `backend/agent/services/agent_graph.py`, `backend/bff/routers/agent_sessions.py`, `backend/tests/unit/services/test_agent_graph_retry.py`, `backend/bff/tests/test_agent_sessions_variables_clarifications.py` | |
| AGT-006 | DONE | `backend/agent/services/agent_graph.py`, `backend/tests/unit/services/test_agent_graph_parallel.py` | |

---

## 5) 툴 프레임워크 (레지스트리·스키마·실행)

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| TOOL-001 | DONE | `backend/shared/services/agent_tool_registry.py`, `backend/shared/policies/agent_tool_allowlist.json`, `backend/bff/services/agent_tool_schemas.py`, `backend/tests/unit/middleware/test_middleware_fixes.py` | |
| TOOL-002 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/middleware/auth.py`, `backend/bff/tests/test_agent_sessions_router.py`, `backend/tests/unit/middleware/test_middleware_fixes.py` | |
| TOOL-003 | DONE | `backend/agent/services/agent_runtime.py`, `backend/bff/middleware/auth.py`, `backend/shared/services/agent_registry.py`, `backend/tests/unit/middleware/test_middleware_fixes.py`, `backend/tests/unit/services/test_agent_runtime_delegated_auth.py` | |
| TOOL-004 | DONE | `backend/agent/services/agent_runtime.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/tests/unit/services/test_agent_runtime_artifacts.py` | |
| TOOL-005 | DONE | `backend/bff/routers/agent_functions.py`, `backend/shared/services/agent_function_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/shared/policies/agent_tool_allowlist.json`, `backend/bff/tests/test_agent_functions_router.py`, `backend/bff/tests/test_agent_sessions_variables_clarifications.py` | |
| TOOL-006 | DONE | `backend/agent/services/agent_runtime.py`, `backend/bff/middleware/auth.py`, `backend/shared/services/audit_log_store.py`, `backend/tests/unit/middleware/test_middleware_fixes.py` | |
| TOOL-007 | DONE | `backend/bff/services/agent_plan_validation.py`, `backend/bff/routers/agent_plans.py`, `backend/bff/routers/pipeline.py`, `backend/tests/unit/services/test_agent_graph_simulation_gate.py` | |

---

## 6) 승인(Human-in-the-loop) 및 정책 엔진

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| APR-001 | DONE | `backend/bff/services/agent_plan_validation.py`, `backend/shared/services/agent_tool_registry.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` | |
| APR-002 | DONE | `backend/bff/services/agent_plan_validation.py`, `backend/shared/config/settings.py`, `backend/bff/tests/test_pipeline_proposal_governance.py` | |
| APR-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_registry.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` | |
| APR-004 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_registry.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` | |
| APR-005 | DONE | `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` | |
| APR-006 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` | |

---

## 7) 변경 격리·리뷰 아티팩트(브랜치/프로포절/PR/CI)

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| CHG-001 | DONE | `backend/bff/routers/pipeline.py`, `backend/bff/routers/ontology_extensions.py`, `backend/shared/config/settings.py`, `backend/bff/tests/test_pipeline_proposal_governance.py` | |
| CHG-002 | DONE | `backend/bff/routers/pipeline.py`, `backend/bff/routers/ontology_extensions.py`, `docs/API_REFERENCE.md`, `backend/tests/test_openapi_contract_smoke.py` | |
| CHG-003 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/middleware/auth.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_events.py` | |
| CHG-004 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/tests/test_agent_sessions_ci_results.py` | |
| CHG-005 | DONE | `backend/bff/routers/pipeline.py`, `backend/shared/services/action_simulation_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_pipeline_router_uploads.py` | |
| CHG-006 | DONE | `backend/shared/policies/agent_tool_allowlist.json`, `backend/bff/routers/agent_sessions.py`, `docs/API_REFERENCE.md`, `backend/tests/test_openapi_contract_smoke.py` | |

---

## 8) 컨텍스트 관리 및 RAG(문서/온톨로지/데이터)

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| CTX-001 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| CTX-002 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/bff/services/agent_plan_compiler.py`, `backend/bff/tests/test_agent_sessions_router.py` | |
| CTX-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/bff/routers/document_bundles.py`, `backend/bff/tests/test_document_bundles_router.py` | |
| CTX-004 | DONE | `backend/bff/routers/document_bundles.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_document_bundles_router.py` | |
| CTX-005 | DONE | `backend/bff/routers/context_tools.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_context_tools_router.py` | |
| CTX-006 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/storage_service.py`, `backend/shared/services/agent_retention_worker.py`, `backend/tests/unit/services/test_agent_retention_worker.py` | |
| CTX-007 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/tests/test_agent_sessions_token_budget.py` | |

---

## 9) 프롬프트 컴파일·툴 설명 주입·출력 규격화

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| PRM-001 | DONE | `backend/bff/services/agent_plan_compiler.py`, `backend/shared/services/llm_gateway.py`, `docs/LLM_NATIVE_CONTROL_PLANE.md`, `backend/bff/tests/test_agent_sessions_router.py` | |
| PRM-002 | DONE | `backend/bff/services/agent_plan_compiler.py`, `backend/shared/services/agent_tool_registry.py`, `backend/tests/unit/errors/test_policy_drift_guards.py` | |
| PRM-003 | DONE | `backend/bff/services/agent_plan_compiler.py`, `backend/shared/services/llm_gateway.py`, `backend/bff/routers/agent_plans.py`, `backend/tests/test_openapi_contract_smoke.py` | |
| PRM-004 | DONE | `backend/bff/services/agent_plan_validation.py`, `backend/bff/services/agent_plan_compiler.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` | |

---

## 10) 감사·투명성·관측(Observability)·비용/토큰 계측

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| OBS-001 | DONE | `backend/shared/services/audit_log_store.py`, `backend/agent/services/agent_runtime.py`, `backend/bff/middleware/auth.py`, `backend/tests/unit/services/test_agent_runtime_artifacts.py` | |
| OBS-002 | DONE | `backend/shared/services/agent_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` | |
| OBS-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_events.py` | |
| OBS-004 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_llm_usage_metrics.py` | |
| OBS-005 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/llm_gateway.py`, `backend/bff/tests/test_agent_sessions_llm_usage_metrics.py` | |
| OBS-006 | DONE | `backend/shared/observability/request_context.py`, `backend/shared/services/service_factory.py`, `backend/agent/services/agent_runtime.py`, `backend/tests/test_openapi_contract_smoke.py` | |
| OBS-007 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/routers/monitoring.py`, `backend/shared/middleware/rate_limiter.py`, `docs/OPERATIONS.md` | |

---

## 11) 프라이버시·데이터 보호·모델 송신 통제

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| SEC-001 | DONE | `backend/shared/services/llm_gateway.py`, `backend/shared/utils/llm_safety.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` | |
| SEC-002 | DONE | `backend/shared/services/llm_gateway.py`, `backend/shared/config/settings.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` | |
| SEC-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/context_tools.py`, `backend/bff/tests/test_context_tools_router.py` | |
| SEC-004 | DONE | `backend/shared/security/data_encryption.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/tests/unit/security/test_data_encryption.py` | |
| SEC-005 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/shared/services/agent_retention_worker.py`, `backend/bff/main.py`, `backend/tests/unit/services/test_agent_retention_worker.py` | |

---

## 12) 외부/내부 통합 API 요구사항

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| INT-001 | DONE | `docs/API_REFERENCE.md`, `backend/bff/routers/agent_sessions.py`, `backend/agent/routers/agent.py`, `backend/tests/test_openapi_contract_smoke.py` | |
| INT-002 | DONE | `backend/bff/services/oms_client.py`, `backend/shared/services/dataset_registry.py`, `backend/shared/services/pipeline_registry.py`, `docs/API_REFERENCE.md` | |
| INT-003 | DONE | `backend/bff/routers/document_bundles.py`, `backend/bff/routers/context7.py`, `backend/bff/tests/test_document_bundles_router.py` | |
| INT-004 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/tests/test_agent_sessions_ci_results.py` | |

---

## 13) 신뢰성·성능·운영(Non-functional)

| ID | Status | Evidence (code/tests/docs) | Gap / Next |
|---|---|---|---|
| NFR-001 | DONE | `backend/shared/services/llm_gateway.py`, `backend/agent/services/agent_runtime.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` | |
| NFR-002 | DONE | `backend/agent/routers/agent.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_events.py` | |
| NFR-003 | DONE | `backend/shared/services/agent_session_registry.py` (active job gate), `backend/agent/services/agent_graph.py`, `backend/tests/unit/services/test_agent_graph_parallel.py` | |
| NFR-004 | DONE | `backend/shared/middleware/rate_limiter.py`, `backend/shared/services/llm_quota.py`, `backend/tests/unit/services/test_llm_quota.py` | |
| NFR-005 | DONE | `backend/shared/services/llm_gateway.py`, `backend/bff/middleware/auth.py`, `backend/shared/services/service_factory.py`, `docs/OPERATIONS.md` | |
| NFR-006 | DONE | `docs/OPERATIONS.md`, `docs/DATA_LINEAGE.md`, `backend/shared/services/event_store.py` | |
| NFR-007 | DONE | `backend/shared/services/agent_tool_registry.py`, `backend/shared/services/agent_model_registry.py`, `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/agent_tools.py`, `backend/bff/routers/agent_models.py`, `backend/bff/routers/agent_policies.py` | |
