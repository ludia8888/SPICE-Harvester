# AGENT PRD 체크리스트 (Traceability)

> 기준 문서: `docs/AGENT_PRD.md`  
> 목적: “요구사항 → 구현/검증 근거”를 1:1로 매핑하고, 빠짐없이 갭을 제거한다.

## 상태 표기

- `DONE`: 구현 + 최소 1개 검증(테스트/리그레션/운영 메트릭) 근거 존재
- `PARTIAL`: 일부 구현 존재(근거 있음)이나 요구사항 전체를 만족하지 못함
- `TODO`: 구현/검증 근거 없음 (백지 또는 설계만)

---

## 1) 인증·권한·테넌시

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| AUTH-001 | DONE | `backend/bff/middleware/auth.py`, `backend/shared/security/user_context.py`, `backend/agent/services/agent_runtime.py`, `backend/tests/unit/middleware/test_middleware_fixes.py`, `backend/tests/unit/services/test_agent_runtime_delegated_auth.py` |  |
| AUTH-002 | PARTIAL | `backend/bff/middleware/auth.py`, `backend/shared/services/agent_tool_registry.py`, `backend/shared/services/agent_policy_registry.py`, `backend/shared/services/agent_session_registry.py`, `backend/tests/unit/middleware/test_middleware_fixes.py` | tool_id allowlist+role gating + session enabled_tools 강제 + ABAC(db_name/branch) 일부 추가됨. 표준 오류 코드(envelope) 정리 + 리소스 스코프 확장(문서/데이터셋 등) 필요. |
| AUTH-003 | PARTIAL | `backend/bff/middleware/auth.py`, `backend/agent/services/agent_runtime.py` | 전 서비스/워커 경로에서 “service token only” 실행이 없도록 정리 + RBAC/ABAC로 최종 보장 필요. |
| AUTH-004 | PARTIAL | `backend/bff/middleware/auth.py`, `backend/bff/routers/agent_proxy.py`, `backend/shared/services/agent_session_registry.py`, `backend/shared/services/agent_plan_registry.py`, `backend/shared/services/agent_registry.py` | 로그/아티팩트/레이트리밋까지 tenant 격리 확대 + cross-service(워커 포함) tenant propagation 완결 필요. |
| AUTH-005 | PARTIAL | `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/agent_policies.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/middleware/auth.py`, `backend/bff/tests/test_agent_sessions_router.py`, `backend/tests/unit/middleware/test_middleware_fixes.py` | 세션 model/tools allowlist + ABAC(db_name/branch) 일부 강제됨. 자동 승인 규칙/데이터·문서 접근 정책(ABAC) 확장 + 세션 정책의 런타임 강제(툴/LLM) 완결 필요. |

---

## 2) 세션·대화 상태 관리

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| SESS-001 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/main.py`, `backend/bff/tests/test_agent_sessions_router.py` |  |
| SESS-002 | PARTIAL | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/shared/services/agent_registry.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py`, `backend/bff/tests/test_agent_sessions_events.py` | tool 호출 req/resp 저장·조회 및 토큰/비용/지연시간 계측을 세션 단위로 완결(조회/집계 API 포함) 필요. |
| SESS-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_router.py` |  |
| SESS-004 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/tests/test_agent_sessions_summarize_remove.py` |  |
| SESS-005 | DONE | `backend/shared/services/agent_session_registry.py`, `backend/tests/unit/services/test_agent_session_state_machine.py` |  |
| SESS-006 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/shared/services/agent_registry.py` |  |

---

## 3) LLM 모델 게이트웨이 및 모델 선택

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| LLM-001 | DONE | `backend/shared/services/llm_gateway.py`, `backend/shared/config/settings.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` |  |
| LLM-002 | PARTIAL | `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/routers/agent_policies.py`, `backend/shared/services/agent_model_registry.py`, `backend/bff/routers/agent_models.py` | 세션 생성/변경 전 과정에서 allowlist 강제(게이트웨이 레벨 포함) + 모델 메타/조직 정책(권한/송신 정책) 런타임 강제 완결 필요. |
| LLM-003 | PARTIAL | `backend/shared/services/agent_session_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_model_registry.py`, `backend/bff/tests/test_agent_sessions_router.py` | 세션 단위 모델 변경 API는 제공됨. LLM 호출 시 세션 정책(마스킹/툴 허용/데이터 정책) 런타임 강제(게이트웨이 포함) 완결 필요. |
| LLM-004 | PARTIAL | `backend/shared/services/llm_gateway.py`(JSON-only), `backend/bff/services/agent_plan_compiler.py`(planner) | native function/tool 호출(복수 호출/병렬 계획) 미구현. |
| LLM-005 | PARTIAL | `backend/shared/services/agent_model_registry.py`, `backend/bff/routers/agent_models.py` | capability 메타 기반 런타임 자동 폴백(네이티브→프롬프트 등) 연결 미구현. |
| LLM-006 | DONE | `backend/shared/services/llm_gateway.py`, `backend/tests/unit/services/test_llm_gateway_resilience.py` |  |

---

## 4) 에이전트 오케스트레이션 (Closed-loop 실행 엔진)

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| AGT-001 | PARTIAL | `backend/bff/routers/agent_plans.py`, `backend/agent/services/agent_graph.py` | 현재는 “Plan → 실행” 중심. 대화형 prompt-to-action session loop는 미구현. |
| AGT-002 | PARTIAL | `backend/agent/services/agent_runtime.py`(AGENT_TOOL_* events) | 이벤트는 있으나 “세션/메시지 이벤트”까지 통합 필요. |
| AGT-003 | PARTIAL | `backend/agent/services/agent_graph.py`(순차), `backend/bff/services/agent_plan_compiler.py`(compile/clarify) | 런타임 re-plan(툴 결과 기반 step 재구성) 미구현. |
| AGT-004 | PARTIAL | `backend/bff/routers/agent_plans.py`(preview), `backend/bff/services/agent_plan_validation.py`(simulate/preview 강제) | 검증 단계 확장(예: CI 확인/코드 프리뷰) 필요. |
| AGT-005 | PARTIAL | `backend/agent/services/agent_policy.py`, `backend/agent/services/agent_runtime.py` | 부분 성공 처리/clarification 전환은 제한적(세션 기반 UX 필요). |
| AGT-006 | TODO | (없음) | 병렬 실행 + 의존성 그래프 + 동시성 제한 미구현. |

---

## 5) 툴 프레임워크 (레지스트리·스키마·실행)

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| TOOL-001 | PARTIAL | `backend/shared/services/agent_tool_registry.py`, `backend/shared/policies/agent_tool_allowlist.json` | tool version / input-output schema / 권한 선언 / retry 정책 메타 확장 필요. |
| TOOL-002 | PARTIAL | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/middleware/auth.py`, `backend/bff/tests/test_agent_sessions_router.py`, `backend/tests/unit/middleware/test_middleware_fixes.py` | 세션 enabled_tools 저장/조회/설정 + 런타임 차단은 추가됨. 모든 툴 실행 경로에서 `X-Agent-Session-ID` 전파를 보장(워커/프록시 포함)하고, 세션 정책 기반 강제를 완결 필요. |
| TOOL-003 | DONE | `backend/bff/middleware/auth.py`, `backend/shared/services/agent_registry.py`, `backend/tests/unit/middleware/test_middleware_fixes.py`, `backend/agent/services/agent_policy.py`, `backend/tests/unit/services/test_agent_policy.py` |  |
| TOOL-004 | PARTIAL | `backend/agent/services/agent_runtime.py`(payload_preview/side_effect_summary) | 표준 응답 스키마(에러 코드 체계/side effect taxonomy) 고도화 필요. |
| TOOL-005 | PARTIAL | `docs/API_REFERENCE.md`(Actions/Graph/Pipeline/Objectify/Commands), `backend/shared/policies/agent_tool_allowlist.json` | Function/SessionVariableUpdate/Clarification 툴은 미구현(또는 allowlist 확장 필요). |
| TOOL-006 | PARTIAL | `backend/agent/services/agent_runtime.py`(mask/audit/events), `backend/shared/utils/llm_safety.py` | hooks는 런타임에 존재하나 tool plugin 레이어/세션 정책 연결 필요. |
| TOOL-007 | PARTIAL | `backend/bff/routers/agent_plans.py`(preview), `backend/bff/routers/actions.py`(simulate) | “프리뷰 결과 분리 저장”은 일부만 충족(범용화 필요). |

---

## 6) 승인(Human-in-the-loop) 및 정책 엔진

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| APR-001 | PARTIAL | `backend/bff/services/agent_plan_validation.py` | 정책 엔진이 risk/allowlist 기반으로만 동작(조직 정책/컨텍스트 기반 확장 필요). |
| APR-002 | PARTIAL | `backend/bff/services/agent_plan_validation.py`, `backend/bff/routers/pipeline.py`(protected branch) | “대규모/비가역” 분류/강제 고도화 필요. |
| APR-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_registry.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` |  |
| APR-004 | DONE | `backend/bff/routers/agent_plans.py`(execute 시 approval 체크), `backend/shared/services/agent_registry.py`(approvals) |  |
| APR-005 | DONE | `backend/shared/services/agent_policy_registry.py`, `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` |  |
| APR-006 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_registry.py`, `backend/bff/tests/test_agent_sessions_approval_flow.py` |  |

---

## 7) 변경 격리·리뷰 아티팩트(브랜치/프로포절/PR/CI)

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| CHG-001 | PARTIAL | `backend/bff/routers/pipeline.py`(branches/proposals), `backend/bff/routers/ontology_extensions.py`(ontology proposals) | 코드 변경(PR)까지 강제되는 “통합 격리”는 미구현. |
| CHG-002 | PARTIAL | `backend/bff/routers/pipeline.py`, `backend/bff/routers/ontology_extensions.py` | PR 생성/연결은 미구현(현재는 pipeline/ontology 중심). |
| CHG-003 | TODO | (없음) | PR/프로포절에 session/tool history 연결 미구현. |
| CHG-004 | TODO | (없음) | CI 결과 수집/표준화/후속 루프 입력 미구현. |
| CHG-005 | PARTIAL | `backend/shared/services/action_simulation_registry.py`, `backend/bff/routers/pipeline.py`(preview/build artifacts) | 세션 단위 재참조 UX/조회 API 미구현. |
| CHG-006 | PARTIAL | pipeline/ontology proposal APIs | cross-domain 표준 링크 스키마(artifact link contract) 미구현. |

---

## 8) 컨텍스트 관리 및 RAG(문서/온톨로지/데이터)

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| CTX-001 | PARTIAL | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py`, `backend/bff/tests/test_agent_sessions_router.py` | 타입별(ref payload) 정규화/커넥터 기반 resolve(데이터셋/온톨로지/문서/파일)까지 확장 필요. |
| CTX-002 | PARTIAL | `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_router.py` | 세션 외 “직접 compile” 경로 포함 전체 프롬프트 컴파일에서 implicit fetch 금지까지 완결 필요. |
| CTX-003 | PARTIAL | `backend/bff/routers/agent_sessions.py`, `backend/shared/services/agent_session_registry.py` | include_mode(full/summary/search)에 따른 실제 컨텐츠 구성(요약/RAG) + token_budget 기반 제어 완결 필요. |
| CTX-004 | PARTIAL | `backend/bff/routers/context7.py` | citation 포함/문서 번들 모델 명확화 필요. |
| CTX-005 | PARTIAL | `backend/bff/routers/context7.py`(suggestions/search) | 온톨로지/데이터 기반 컨텍스트 추출 툴로 정식화 필요. |
| CTX-006 | PARTIAL | `backend/bff/routers/pipeline.py`(file upload) | 바이러스 검사/보존기간/텍스트 추출 정책 미구현. |
| CTX-007 | TODO | (없음) | 토큰 예산/기여량 계산 + 요약/제거 후보 산출 미구현. |

---

## 9) 프롬프트 컴파일·툴 설명 주입·출력 규격화

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| PRM-001 | PARTIAL | `backend/bff/services/agent_plan_compiler.py`, `backend/bff/routers/ai.py` | 세션 정책/툴 활성화 상태를 반영한 컴파일 미구현. |
| PRM-002 | TODO | (없음) | 세션별 활성 툴과 tool 설명/제약 1:1 정합성 강제 미구현. |
| PRM-003 | PARTIAL | `backend/bff/routers/agent_plans.py`(plan-only compile/validate), `backend/shared/services/llm_gateway.py`(JSON) | 통합 “대화 응답 + tool plan” 프로토콜/스트리밍 미구현. |
| PRM-004 | PARTIAL | `backend/bff/services/agent_plan_validation.py`, `backend/agent/services/agent_runtime.py` | schema mismatch 시 “재생성 루프”는 제한적(compile 단계에만 존재). |

---

## 10) 감사·투명성·관측(Observability)·비용/토큰 계측

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| OBS-001 | PARTIAL | `backend/agent/services/agent_runtime.py`(events), `backend/shared/services/audit_log_store.py` | “세션 단위” 감사/추적 + 사용자 검증 컨텍스트 필요. |
| OBS-002 | PARTIAL | `backend/shared/services/agent_registry.py`(approvals), `backend/agent/services/agent_policy.py`(policy) | 변경 아티팩트(PR/프로포절) 링크를 audit에 연결 미구현. |
| OBS-003 | DONE | `backend/bff/routers/agent_sessions.py`, `backend/bff/tests/test_agent_sessions_events.py`, `backend/agent/services/agent_runtime.py` |  |
| OBS-004 | TODO | (없음) | 토큰 사용량 저장/조회 미구현. |
| OBS-005 | TODO | (없음) | 모델/사용자/조직 비용 집계 미구현. |
| OBS-006 | PARTIAL | `backend/shared/observability/request_context.py`, `backend/shared/services/service_factory.py` | 분산 트레이싱/상관관계 id를 agent tool 실행까지 end-to-end 연결 고도화 필요. |
| OBS-007 | PARTIAL | `backend/shared/observability/*`, `backend/shared/middleware/rate_limiter.py` | 성공률/승인 대기/LLM latency 등 AIP 핵심 KPI 대시보드화 미구현. |

---

## 11) 프라이버시·데이터 보호·모델 송신 통제

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| SEC-001 | DONE | `backend/shared/services/llm_gateway.py`(mask_pii_text), `backend/shared/utils/llm_safety.py` |  |
| SEC-002 | TODO | (없음) | provider별 “저장 금지/로그 최소화” 정책 강제 미구현. |
| SEC-003 | TODO | (없음) | 데이터 분류(기밀 등급) 기반 LLM 송신 차단 미구현. |
| SEC-004 | PARTIAL | `docs/SECURITY.md`(운영 문서), 서비스별 TLS 옵션 | 저장 암호화/키 관리/비밀 로테이션 체계 고도화 필요. |
| SEC-005 | TODO | (없음) | retention 설정 + 만료 자동 삭제/비식별화 미구현. |

---

## 12) 외부/내부 통합 API 요구사항

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| INT-001 | PARTIAL | `docs/API_REFERENCE.md`, `backend/bff/routers/agent_sessions.py` | 메시지 스트리밍(또는 long-poll) + context API coverage 정리 + artifact link API 등 남은 군을 완결 필요. |
| INT-002 | PARTIAL | `docs/API_REFERENCE.md`(datasets/ontology/branches 등) | 통합 connector 계층(메타데이터 표준화) 고도화 필요. |
| INT-003 | PARTIAL | `backend/bff/routers/context7.py` | citation contract/문서 번들 모델 확정 필요. |
| INT-004 | TODO | (없음) | CI 결과 수집/표준화(웹훅/폴링) 미구현. |

---

## 13) 신뢰성·성능·운영(Non-functional)

| ID | Status | Evidence (code/docs) | Gap / Next |
|---|---|---|---|
| NFR-001 | PARTIAL | `backend/shared/services/llm_gateway.py`(timeout), `backend/agent/services/agent_runtime.py`(timeout) | 타임아웃 시 세션 상태 복구(세션 모델 필요). |
| NFR-002 | PARTIAL | `docs/API_REFERENCE.md`(commands/tasks/agent runs) | 세션-Job 분리 + UI-friendly progress API 필요. |
| NFR-003 | PARTIAL | `backend/shared/services/event_store.py`(ordering), worker processed_events(idempotency) | 동일 세션 내 동시 실행 제어(락/큐) 미구현. |
| NFR-004 | PARTIAL | `backend/shared/middleware/rate_limiter.py` | 사용자/조직/모델 단위 쿼터 분리 미구현. |
| NFR-005 | PARTIAL | 각 서비스 degraded mode + require_* 설정 | 폴백 전략을 세션/UX에 반영(알림/대안 제안) 필요. |
| NFR-006 | TODO | (없음) | 백업/복구(runbook + 자동화) 미구현. |
| NFR-007 | PARTIAL | `backend/shared/config/settings.py`, tool allowlist admin API | “배포 없이 정책/툴/모델 관리”는 일부(툴만) 구현. |
