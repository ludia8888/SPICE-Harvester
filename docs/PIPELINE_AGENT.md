# Pipeline Agent (LangGraph) + Pipeline Plans (MCP Planner)

이 문서는 “사용자 자연어 요청 → 데이터 분석/ETL 수행”을 위해 추가된 **Pipeline Agent + Pipeline Plans**의 개발자 가이드입니다.

핵심 철학:
- MCP는 **도구 제공**(결정론적 plan-builder/analysis tools)
- LangGraph는 **지능적 오케스트레이션**(멀티 에이전트 워크플로우)
- “과도한 실행(overreach)”을 막기 위해 **TaskSpec(작업 범위 계약)** 으로 scope를 강제합니다.
- 사용자 질문(clarification)은 **룰/하드코딩이 아니라 LLM이 생성**합니다.

---

## 1) 구성요소 한 눈에 보기

### Control plane artifacts
- `PipelineTaskSpec`: 사용자의 의도/범위를 강제하는 계약(최소 권한).
  - 코드: `backend/shared/models/pipeline_task_spec.py`, `backend/shared/services/pipeline_task_spec_policy.py`
- `PipelinePlan`: 파이프라인 “계획” 객체(아직 배포/쓰기 아님). TaskSpec이 plan에 함께 저장됩니다.
  - 코드: `backend/shared/models/pipeline_plan.py`
- `Context Pack`: 데이터셋 샘플/스키마/통계 + join/pk/fk/cast/cleansing 힌트(샘플 기반, PII 마스킹).
  - 코드: `backend/bff/services/pipeline_context_pack.py`

### Tool provider (MCP)
- Pipeline MCP server: plan-builder + 분석(키/타입/preview/eval/inspect) 도구를 제공합니다.
  - 코드: `backend/mcp/pipeline_mcp_server.py`
  - 설정: `mcp-config.json`의 `pipeline` 서버 항목

### Orchestration (LangGraph)
- Pipeline Agent graph: Profiler/Join Strategist/Cleanser/Mapper/Repair 역할로 분리된 상태 머신.
  - 코드: `backend/agent/services/pipeline_agent_graph.py`
  - 실행 엔드포인트: `POST /api/v1/agent/pipeline-runs` (`backend/agent/routers/pipeline_agent.py`)

---

## 2) “TaskSpec”이 overreach를 막는 방식

TaskSpec은 “이 요청에서 무엇을 해도 되는지”를 명시합니다.
- `scope=report_only`: 리포트만 생성(예: null-check). **plan compile/transform/repair 금지**
- `scope=pipeline`: plan compile/preview/repair 가능(단, allow flags에 따라 join/cleansing/advanced/specs가 제한)

검증/강제 지점:
- TaskSpec 추론(LLM): `POST /api/v1/pipeline-plans/task-spec` → `backend/bff/services/pipeline_task_spec_agent.py`
- TaskSpec clamp/policy(결정론): `backend/shared/services/pipeline_task_spec_policy.py`
- Plan validation에서 TaskSpec 정책 강제: `backend/bff/services/pipeline_plan_validation.py`

주의:
- “null check 해줘” 같은 요청은 **절대 join/cleanse/spec 생성으로 확장되지 않아야** 합니다.

---

## 3) Pipeline Plans API (BFF) – 개발자용 흐름

Pipeline plan 관련 REST API는 `backend/bff/routers/pipeline_plans.py`에 있습니다.

대표 흐름(개념):
1) `POST /api/v1/pipeline-plans/context-pack` (Profiler)
2) `POST /api/v1/pipeline-plans/task-spec` (Scope Guard)
3) `POST /api/v1/pipeline-plans/compile` (Planner)
4) 필요 시:
   - `POST /api/v1/pipeline-plans/{plan_id}/transform` (Join/Cleansing/Advanced 단계 보강)
   - `POST /api/v1/pipeline-plans/{plan_id}/verify-intent` (의도 정합성 검증)
   - `POST /api/v1/pipeline-plans/{plan_id}/preview` (샘플 기반 실행)
   - `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins` (커버리지/폭발 평가)
   - `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview` (cleansing 제안)
   - `POST /api/v1/pipeline-plans/{plan_id}/cleanse` (cleansing 적용)
   - `POST /api/v1/pipeline-plans/{plan_id}/repair` (부분 패치 기반 복구)
   - `POST /api/v1/pipeline-plans/{plan_id}/split-outputs` (spec 준비 시 object/link 분류)
   - `POST /api/v1/pipeline-plans/{plan_id}/generate-specs` (mapping/relationship spec 생성)
5) scope 불일치/추가 권한이 필요하면:
   - `POST /api/v1/pipeline-plans/clarify-scope` (LLM이 사용자 질문 생성)

Endpoint index는 `docs/API_REFERENCE.md`를 참고하세요(자동 생성).

---

## 4) MCP 기반 Planner(Plan Builder Tools) – 왜 필요한가

기존: LLM이 `definition_json`(DSL)을 직접 생성 → 오타/구조 오류/키 정합성 실패/수정 어려움이 잦음

현재(MCP Planner): LLM이 “DSL 문자열”이 아니라 **MCP tool calls**로 plan을 조립합니다.
- 예: `plan_add_join(...)`, `plan_add_filter(...)`, `plan_update_node_metadata(...)`
- 서버(도구)가 노드/엣지 구조 및 키 정합성을 **결정론적으로 강제**
- Repair는 “재생성” 대신 **부분 patch/delete**로 최소 변경을 수행

두 가지 실행 모드가 있습니다:
- MCP planner(단발): LLM이 한 번에 tool-call 프로그램을 만들고, 서버가 결정론적 hardening을 수행합니다. (`compile_pipeline_plan_mcp`)
- Autonomous MCP planner loop: LLM이 “다음에 어떤 도구를 호출할지”를 반복적으로 선택하며, 중간에 validate/preview로 스스로 확인하면서 완성합니다. (`compile_pipeline_plan_mcp_autonomous`)
  - `/api/v1/pipeline-plans/compile`에서 `PIPELINE_PLAN_MCP_AUTONOMOUS_ENABLED=true`로 활성화됩니다.

관련 코드:
- MCP autonomous compiler(compile): `backend/bff/services/pipeline_plan_autonomous_compiler.py`
- MCP tool executor(compile/repair): `backend/bff/services/pipeline_plan_compiler.py`
- 결정론 plan builder: `backend/shared/services/pipeline_plan_builder.py`

추가 가드레일(중요):
- `task_spec.allow_specs=false` 인 요청에서 planner가 실수로 `output_kind=object|link` 를 만들면, BFF가 `output_kind=unknown` 으로 **강제(clamp)** 합니다.
  - 목적: “데이터셋 만들어줘” 요청이 ontology/canonical mapping 질문으로 튀는 것을 방지.
  - 코드: `backend/bff/services/pipeline_plan_compiler.py`

### 4.1) Planner hardening: 결정론적 post-process + join fast-path

실전 QA에서 LLM planner가 반복적으로 실수하는 패턴(엔터프라이즈 관점에서 치명적):
- output 노드를 빠뜨림(“데이터셋 만들어줘”인데 output이 없음)
- 동일 output_name 중복 생성(출력 노드/outputs[] 중복 → 실행/의도 검증이 ambiguous)
- goal에 output column list가 있는데 select를 누락(의도 검증에서 false-negative)
- MCP tool 결과 형식 차이(`CallToolResult`/`structuredContent`/`content[text]`)로 500 발생

이를 막기 위해 (MCP planner 단발 모드인) `compile_pipeline_plan_mcp`는 다음을 **결정론적으로 보강**합니다:
- output 자동 주입/정합성 보정: output 노드/outputs[] 누락 시 자동 생성
- outputs/출력 노드 중복 제거: 동일 `outputName`/`output_name` 중복을 dedupe
- goal에 명시된 output 컬럼이 있으면 select를 output 직전에 강제(필요 시 edge rewiring)
- MCP tool 결과 파싱 하드닝: `structuredContent`/`content[text]`/`isError`를 해석해 500 대신 “tool error”로 수렴

추가로, 아래 조건이면 LLM tool script를 건너뛰고 **결정론적 join plan fast-path**로 plan을 생성합니다:
- `task_spec.allow_join=true`
- `task_spec.allow_advanced_transforms=false`
- `dataset_count==2`
- `planner_hints.join_plan`이 있고(또는 context pack 기반으로 추론됨), 선택된 dataset pair를 직접 연결할 수 있을 때

이 fast-path는 node_id 추적 실패로 인한 불안정성을 제거하기 위한 “안전벨트”이며,
`validation_warnings`에 `compiler: deterministic join fast-path`로 표기됩니다.

---

## 5) MCP 분석 툴(Profiler 확장) – PK/FK/type inference

Pipeline MCP server는 context pack을 기반으로 다음 분석 도구를 제공합니다(샘플 기반):
- `context_pack_infer_keys`: PK 후보/ FK 후보를 정리(결정론)
- `context_pack_infer_types`: 컬럼 타입 추론 + join-key 캐스트 제안(결정론)
- `context_pack_infer_join_plan`: 다중 데이터셋을 연결하기 위한 join plan(스패닝 트리) 제안(결정론)

Planner/Join Strategist가 “추측”하지 않도록 BFF MCP planner는 위 결과를 `planner_hints`에 주입합니다:
- `planner_hints.key_inference`
- `planner_hints.type_inference.join_key_cast_suggestions`
- (join-key 선택이 없거나 실패한 경우) `planner_hints.join_plan` 도 context pack 기반으로 보강될 수 있습니다.

관련 코드:
- `backend/mcp/pipeline_mcp_server.py`
- `backend/shared/services/pipeline_type_inference.py`
- `backend/shared/services/pipeline_relationship_inference.py`
- `backend/bff/services/pipeline_plan_compiler.py`

---

## 5.1) Intent verifier 가드레일(의도 과잉/오탐 방지)

`verify-intent`는 LLM 기반이지만, 실전 QA에서 반복되는 오탐(예: “join 했으니 join key를 output에 포함해야 함”, “이미 select한 컬럼이 output에 없다”)을
그대로 실패로 만들면 UX가 무너집니다.

현재는 다음을 결정론적으로 후처리하여 “goal-literal”을 보장합니다:
- join key는 goal의 “조인 조건”일 뿐이며, output requirement가 아니면 `warnings`로만 남깁니다.
- verifier가 “필수 컬럼이 없다”고 주장해도, plan의 select/project가 이미 포함하면 false-positive로 간주해 `warnings`로 내리고 pass 처리합니다.

코드: `backend/bff/services/pipeline_intent_verifier.py`

---

## 6) Pipeline Agent graph (LangGraph) – 역할 분리

`backend/agent/services/pipeline_agent_graph.py`는 다음 역할 노드로 구성됩니다(개념상 멀티 에이전트):
- Profiler: context pack 생성
- Scope Guard: TaskSpec 추론/확정
- Join Strategist: join 키/전략 수집(필요 시)
- Cleansing Strategist: cleansing 힌트 수집(필요 시)
- Plan Builder: compile
- Plan Transformer: join/cleansing/advanced 보강
- Intent Verifier: 의도 정합성 검증 + 부족하면 transform loop 또는 clarify-scope
- Previewer: preview 실행
- Join Evaluator: join 품질 평가(관측/디버깅). 자동 join revision(휴리스틱 재시도) 루프는 사용하지 않음
- Preview Inspector: preview 기반 cleansing 제안
- Cleanser: cleansing 적용
- Mapper: spec 생성(사용자가 ontology/canonical을 요청한 경우만)
- Repairer: preview 실패/검증 실패에 대한 부분 수정

중요: graph는 TaskSpec 기반으로 “필요한 단계만” 실행해야 합니다.
특히 join/cleansing 힌트가 존재한다는 이유만으로 transform을 자동 실행하지 않습니다(불필요한 patch 시도/중복 node_id 오류 방지).
또한 다중 데이터셋에서 join-key 선택 결과가 끊긴(disconnected) 경우, context pack 기반 결정론 분석으로 join plan을 보강해 “연결된 join chain 후보”를 확보합니다.

---

## 7) Feature flags (점진적 전환)

planner/transform/repair는 feature flag로 MCP 기반 구현으로 전환 가능합니다:
- `PIPELINE_PLAN_LLM_ENABLED` (planner API on/off)
- `PIPELINE_PLAN_MCP_AUTONOMOUS_ENABLED` (compile: autonomous tool-call loop; takes precedence over MCP_PLANNER)
- `PIPELINE_PLAN_MCP_PLANNER_ENABLED`
- `PIPELINE_PLAN_MCP_TRANSFORM_ENABLED`
- `PIPELINE_PLAN_MCP_REPAIR_ENABLED`

코드: `backend/shared/config/settings.py`

Docker 예시: `backend/docker-compose.yml`

로컬 Docker에서 빠르게 켜기(예시):
```bash
PIPELINE_PLAN_LLM_ENABLED=true \
PIPELINE_PLAN_MCP_AUTONOMOUS_ENABLED=true \
PIPELINE_PLAN_MCP_PLANNER_ENABLED=true \
PIPELINE_PLAN_MCP_TRANSFORM_ENABLED=true \
PIPELINE_PLAN_MCP_REPAIR_ENABLED=true \
docker compose --env-file .env -f backend/docker-compose.yml up -d --build bff agent
```

---

## 7.1) Docker에서 MCP 설정

Docker 환경에서는 `mcp-config.json` 경로가 호스트/컨테이너에서 달라질 수 있습니다.
개발 편의를 위해 `backend/mcp/mcp-config.docker.json` 을 추가했고, `backend/docker-compose.yml`에서
`/app/mcp-config.json`로 마운트합니다.

관련 파일:
- `backend/mcp/mcp-config.docker.json`
- `backend/docker-compose.yml`

---

## 8) 개발자가 새로운 기능을 추가할 때 체크리스트

### 새로운 plan-builder 툴 추가 시
- `backend/shared/services/pipeline_plan_builder.py`에 결정론적 mutation 추가
- `backend/mcp/pipeline_mcp_server.py`에 tool 등록(list_tools/call_tool)
- `backend/bff/services/pipeline_plan_compiler.py` allowlist(`_PIPELINE_MCP_ALLOWED_TOOLS`) + TaskSpec policy guard 업데이트
- unit test 추가(구조/에러/정합성)

### 새로운 “분석 툴” 추가 시
- 샘플 기반, side-effect free를 기본으로 유지(PII 마스킹 포함)
- 결과는 `planner_hints`로 들어갈 수 있게 “작고 구조화된 payload”로 설계
- 문서(`backend/mcp/README.md`, 이 문서) 업데이트

---

## 9) 테스트/문서 동기화

- Unit tests: `pytest -q backend/tests/unit`
- API Reference 갱신(Endpoint Index): `python scripts/generate_api_reference.py`

### 9.1) E2E smoke test (Agent Chat API)

UI의 agent chat은 `POST /api/v1/agent/pipeline-runs`에 연결됩니다.

아래는 “형태” 예시이며, 실제 `db_name`/`dataset_ids`는 환경에 맞게 교체하세요.

```bash
# null check (report_only)
curl -sS -X POST http://localhost:8004/api/v1/agent/pipeline-runs \\
  -H 'Content-Type: application/json' \\
  -d '{
    "goal": "null check 해줘",
    "data_scope": {"db_name": "<db>", "branch": "main", "dataset_ids": ["<dataset_id>"], "dataset_version_ids": []},
    "include_run_tables": true,
    "max_repairs": 1,
    "max_cleansing": 1,
    "max_transform": 1,
    "apply_specs": false,
    "auto_sync": false
  }'

# 2-dataset join (integrate)
curl -sS -X POST http://localhost:8004/api/v1/agent/pipeline-runs \\
  -H 'Content-Type: application/json' \\
  -d '{
    "goal": "A와 B를 id로 LEFT JOIN해서 id, name, amount 컬럼을 가진 joined_ab 데이터셋 만들어줘",
    "data_scope": {"db_name": "<db>", "branch": "main", "dataset_ids": ["<A_id>", "<B_id>"], "dataset_version_ids": []},
    "include_run_tables": true,
    "max_repairs": 1,
    "max_cleansing": 1,
    "max_transform": 0,
    "apply_specs": false,
    "auto_sync": false
  }'
```

추가: repo에 포함된 synthetic 3PL CSV로 4-way join + groupBy + window(top-N)까지 한 번에 확인할 수 있습니다.

```bash
BASE_BFF=http://localhost:8002/api/v1
BASE_AGENT=http://localhost:8004/api/v1
DB=qa_pipeline_agent_demo

# 1) DB 생성
curl -sS -X POST "$BASE_BFF/databases" -H 'Content-Type: application/json' \\
  -d "{\"name\":\"$DB\",\"description\":\"pipeline agent demo\"}" >/dev/null

# 2) CSV 업로드 (Idempotency-Key 필수)
ORDERS_ID=$(curl -sS -X POST "$BASE_BFF/pipelines/datasets/csv-upload?db_name=$DB&branch=main" \\
  -H "Idempotency-Key: $DB-orders" \\
  -F "file=@test_data/spice_harvester_synthetic_3pl/orders.csv" \\
  -F "dataset_name=orders" | jq -r '.data.dataset.dataset_id')
ORDER_ITEMS_ID=$(curl -sS -X POST "$BASE_BFF/pipelines/datasets/csv-upload?db_name=$DB&branch=main" \\
  -H "Idempotency-Key: $DB-order_items" \\
  -F "file=@test_data/spice_harvester_synthetic_3pl/order_items.csv" \\
  -F "dataset_name=order_items" | jq -r '.data.dataset.dataset_id')
SKUS_ID=$(curl -sS -X POST "$BASE_BFF/pipelines/datasets/csv-upload?db_name=$DB&branch=main" \\
  -H "Idempotency-Key: $DB-skus" \\
  -F "file=@test_data/spice_harvester_synthetic_3pl/skus.csv" \\
  -F "dataset_name=skus" | jq -r '.data.dataset.dataset_id')
PRODUCTS_ID=$(curl -sS -X POST "$BASE_BFF/pipelines/datasets/csv-upload?db_name=$DB&branch=main" \\
  -H "Idempotency-Key: $DB-products" \\
  -F "file=@test_data/spice_harvester_synthetic_3pl/products.csv" \\
  -F "dataset_name=products" | jq -r '.data.dataset.dataset_id')

# 3) Pipeline agent 실행 (category별 매출 Top-N + rank)
curl -sS -X POST "$BASE_AGENT/agent/pipeline-runs" -H 'Content-Type: application/json' \\
  -d "{
    \"goal\": \"orders, order_items, skus, products를 조인해서 category별 매출(revenue=qty*unit_price) Top 10과 rank(row_number) 만들어줘\",
    \"data_scope\": {\"db_name\": \"$DB\", \"branch\": \"main\", \"dataset_ids\": [\"$ORDERS_ID\",\"$ORDER_ITEMS_ID\",\"$SKUS_ID\",\"$PRODUCTS_ID\"]},
    \"preview_limit\": 50,
    \"include_run_tables\": true,
    \"max_transform\": 2
  }" | jq '.data.preview'
```
