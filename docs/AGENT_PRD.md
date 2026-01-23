# Agent PRD: Natural Language Data ETL (Pipeline Agent)

이 문서는 “사용자 자연어 요청을 정확히 수행하는 데이터 ETL 에이전트”의 제품/기술 PRD입니다.
구현 상세/개발자 가이드는 `docs/PIPELINE_AGENT.md`를 우선 참고하세요.

---

## 1) 핵심 문제

기존 방식(LLM이 DSL/definition_json을 직접 생성)은 다음 문제가 반복됩니다.
- 구조 오류(노드/엣지/키 누락)로 컴파일 실패가 잦음
- 작은 변경이 필요한데도 전체 재생성(regen)으로 품질이 흔들림
- 사용자의 “범위”를 과하게 확장(overreach)해서 신뢰를 잃음

---

## 2) 목표(Goal) / 비목표(Non-goal)

### Goal
- 사용자가 어떤 요청을 하든 **의도를 정확히 반영**하고, 필요한 범위 내에서만 ETL 작업을 수행한다.
- 단순 요청은 단순하게(예: “null check만”) / 복잡 요청은 전략적으로(예: join+cast+aggregate+window 등).
- 실패 시 전체 재생성 대신 **부분 patch 기반 repair**로 안정적으로 복구한다.
- 데이터 안전: 샘플 기반 분석(PII 마스킹) + 결정론적 검증/가드레일.

### Non-goal (현재)
- “대규모 Spark build/deploy를 무승인으로 자동 실행”은 목표가 아니다(운영 정책/승인 게이트 필요).
- 온톨로지 자동 구축(무질문/무승인)은 목표가 아니다.

---

## 3) 핵심 철학: MCP = 도구, LangGraph = 오케스트레이션

### MCP: Tool provider
- LLM은 DSL 문자열을 직접 생성하지 않는다.
- 대신 LLM은 “plan-builder tool call script”를 생성한다.
  - 예: `plan_add_join`, `plan_add_filter`, `plan_update_node_metadata`, `plan_delete_node` 등
- 도구(서버)는 노드/엣지/키 정합성과 금지 규칙을 **결정론적으로 강제**한다.

### LangGraph: Multi-agent orchestration
- 하나의 “거대 프롬프트” 대신 역할 기반 노드(Profiler/Strategist/Cleanser/Repair/Mapper)를 그래프로 분해한다.
- TaskSpec(작업 범위 계약)에 따라 “필요한 단계만” 실행한다.

---

## 4) Overreach 방지: PipelineTaskSpec(작업 범위 계약)

Pipeline Agent의 신뢰는 “많이 해주는 것”이 아니라 “정확히 해주는 것”에서 나옵니다.

### TaskSpec 개념
- `scope=report_only`: 분석/리포트만 생성(예: null-check/profile). **plan compile/transform/repair 금지**
- `scope=pipeline`: 계획 생성/검증/preview/repair 가능
- 추가 allow flags로 세부 권한을 제한:
  - `allow_join`, `allow_cleansing`, `allow_advanced_transforms`, `allow_specs`, `allow_write(false 고정)`

### 강제 지점(중요)
- TaskSpec은 LLM이 추론하지만, 서버가 clamp/policy로 “최소 권한”으로 정규화한다.
- Plan validation 단계에서 TaskSpec 위반은 에러로 차단된다(조인 금지인데 join을 하는 등).

관련 코드:
- `backend/shared/models/pipeline_task_spec.py`
- `backend/shared/services/pipeline_task_spec_policy.py`
- `backend/bff/services/pipeline_task_spec_agent.py`
- `backend/bff/services/pipeline_plan_validation.py`

---

## 5) 결정론적 분석(Profiler 확장): PK/FK/type inference

LLM이 조인 전략/키/타입을 “상상”하지 않도록 후보 공간을 결정론적으로 제공합니다.
- Context pack에서 제공:
  - PK 후보(`pk_candidates`)
  - FK 후보(`foreign_key_candidates`)
  - join key 후보(`join_key_candidates`, type/format/overlap 신호 포함)
- MCP 분석 툴 추가:
  - `context_pack_infer_keys`: PK/FK 후보를 planner-friendly 형태로 정리
  - `context_pack_infer_types`: 컬럼 타입 추론 + join-key 캐스트 제안
  - `context_pack_infer_join_plan`: 다중 데이터셋 join chain 후보(스패닝 트리) 제안

관련 코드:
- `backend/bff/services/pipeline_context_pack.py`
- `backend/shared/services/pipeline_type_inference.py`
- `backend/mcp/pipeline_mcp_server.py`

---

## 6) Plan Builder Tools (MCP) + 부분 Repair(패치)

### Plan build
- LLM은 도구 호출 step 리스트를 생성하고, 서버가 순서대로 적용하여 `PipelinePlan`을 만든다.
- 도구는 “구조적으로 유효한 plan”만 생성하도록 설계한다(노드/엣지/키/중복 방지).

### Repair(부분 수정)
- 실패 시 “전체 재생성” 대신 patch/delete/rewire를 먼저 시도한다.
- 최소 변경으로 성공률/일관성을 높인다.

관련 코드:
- `backend/shared/services/pipeline_plan_builder.py`
- `backend/mcp/pipeline_mcp_server.py`
- `backend/bff/services/pipeline_plan_compiler.py`

추가 안정장치(엔터프라이즈 관점):
- LLM이 output/select를 누락하거나 outputs/output 노드를 중복 생성하는 실수를 해도, compiler가 결정론적으로 보정합니다.
- 2-dataset join + join_plan 힌트가 있는 단순 통합 요청은 LLM node_id 실수로 흔들리지 않도록 “결정론 join fast-path”로 plan을 생성할 수 있습니다.
  - `validation_warnings`에 `compiler: deterministic join fast-path`로 표기

---

## 7) 멀티 에이전트 그래프(역할 분해)

Pipeline Agent는 다음 역할 노드로 구성된 오케스트레이션을 목표로 한다(구현은 `backend/agent/services/pipeline_agent_graph.py`).
- Profiler: context pack 생성
- Scope Guard: TaskSpec 확정
- Join Strategist: join 키/전략 수립(필요할 때만)
- Cleansing Strategist: cleansing 힌트 수집(필요할 때만)
- Plan Builder: compile
- Transformer: join/cleansing/advanced 보강
- Intent Verifier: “요청 의도” 정합성 검증 → 부족하면 transform loop 또는 clarify-scope
- Previewer: 샘플 기반 preview 실행
- Join Evaluator: join 품질 평가(coverage/explosion) + 필요 시 전략 재시도
- Inspector/Cleanser: preview 기반 cleansing 제안/적용
- Repairer: preflight/preview 실패에 대한 부분 수정
- Mapper: ontology/canonical 요청 시 spec 생성

핵심 원칙:
- TaskSpec이 허용하지 않는 단계는 실행하지 않는다.
- 단순 요청(리포트)은 plan을 만들지 않는다.

---

## 8) Clarification(재질문) 정책

금지:
- 룰 기반 하드코딩 질문 문구(예: “범위를 확장해도 되나요?” 같은 템플릿 고정 질문)

허용:
- LLM이 현재 상태/오류/TaskSpec을 근거로 “필요한 질문”을 생성
- scope 확장이 필요하면 `clarify-scope`로 사용자에게 선택권 제공

관련 코드:
- `backend/bff/services/pipeline_scope_clarifier.py`
- `backend/bff/routers/pipeline_plans.py` (`POST /api/v1/pipeline-plans/clarify-scope`)

---

## 9) 점진적 전환(Rollout): feature flags

planner/transform/repair는 feature flag로 MCP 기반 구현으로 전환 가능합니다.
- `PIPELINE_PLAN_MCP_PLANNER_ENABLED`
- `PIPELINE_PLAN_MCP_TRANSFORM_ENABLED`
- `PIPELINE_PLAN_MCP_REPAIR_ENABLED`

관련 코드:
- `backend/shared/config/settings.py`
- `backend/docker-compose.yml`

---

## 10) Acceptance test 시나리오(우선순위)

### A. 단순 리포트 요청은 “리포트만”
- 입력: CSV 1개
- 요청: “null check 해줘”
- 기대: context pack 기반 null report만 반환, plan compile/transform/repair/spec 생성 금지

### B. 복잡 통합 요청은 “전략적으로”
- 입력: CSV 6개
- 요청: “최적으로 join 전략 짜서 온톨로지에 맵핑 준비까지 해줘”
- 기대:
  - PK/FK 후보 + 타입/캐스트 제안을 활용해 join chain 구성
  - join 품질 평가(coverage/explosion) 후 필요 시 1회 revise
  - cleansing/type cast를 최소 범위로 적용
  - object/link 분류 및 spec 생성(사용자가 명시적으로 mapping을 요청한 경우에만)

### C. 실패 시 부분 repair로 복구
- 입력: join key mismatch, missing columns 등 실패 조건
- 기대: `plan_update_node_metadata`/`plan_set_node_inputs`/`plan_delete_node` 등으로 최소 수정 후 재시도

---

## 11) 주요 코드 레퍼런스

- Pipeline Agent entrypoint: `backend/agent/routers/pipeline_agent.py`
- Pipeline Agent graph: `backend/agent/services/pipeline_agent_graph.py`
- Pipeline Plans API: `backend/bff/routers/pipeline_plans.py`
- MCP plan compiler/repair: `backend/bff/services/pipeline_plan_compiler.py`
- Plan validation/preflight: `backend/bff/services/pipeline_plan_validation.py`
- Context pack + join/pk/fk 후보: `backend/bff/services/pipeline_context_pack.py`
- Join key agent: `backend/bff/services/pipeline_join_agent.py`
- Cleansing agent: `backend/bff/services/pipeline_cleansing_agent.py`
- Pipeline MCP server: `backend/mcp/pipeline_mcp_server.py`
