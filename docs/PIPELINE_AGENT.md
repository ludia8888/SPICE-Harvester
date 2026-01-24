# Pipeline Agent (Single Autonomous Loop + MCP Tools)

이 문서는 “사용자 자연어 요청 → 데이터 분석/ETL 수행”을 위한 **Pipeline Agent + Pipeline Plans** 개발자 가이드입니다.

핵심 철학:
- MCP는 **도구 제공**(결정론적 analysis + plan-builder tools)
- Pipeline Agent는 **단일 에이전트 루프**(BuildPrompt → Inference → Tool → Observation → … → Finish)
- 서버는 의미를 바꾸는 **무음(heuristic) 수정**을 하지 않습니다. 수리는 반드시 tool call로 명시합니다.

---

## 1) 구성요소 한 눈에 보기

### Entry point (BFF)
- Pipeline Agent 실행: `POST /api/v1/agent/pipeline-runs`
  - 라우터: `backend/bff/routers/agent_proxy.py`
  - 런타임(단일 루프): `backend/bff/services/pipeline_agent_autonomous_loop.py`

### Tool provider (MCP)
- Pipeline MCP server: plan-builder + 분석(키/타입/preview/eval/inspect) 도구 제공
  - 코드: `backend/mcp/pipeline_mcp_server.py`
  - 결정론 plan builder: `backend/shared/services/pipeline_plan_builder.py`

### Pipeline Plans API (BFF, deterministic helpers)
- Plan registry/preview/eval/context pack
  - 라우터: `backend/bff/routers/pipeline_plans.py`
  - 주요 엔드포인트:
    - `POST /api/v1/pipeline-plans/context-pack`
    - `POST /api/v1/pipeline-plans/compile` (autonomous MCP loop 기반 compile)
    - `POST /api/v1/pipeline-plans/{plan_id}/preview`
    - `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview`
    - `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins`

---

## 2) 단일 에이전트 루프 동작 방식

Pipeline Agent는 “여러 에이전트를 조율하는 그래프”가 아니라, 한 요청 안에서 반복적으로 도구를 호출하는 **런타임 컨트롤러**입니다.

고수준 플로우:
1) Snapshot 생성(Goal + data_scope + 현재 context/plan 요약 + 마지막 관측)
2) LLM이 다음 액션 선택(JSON):
   - `call_tool`: MCP tool 호출
   - `clarify`: 필요한 질문 생성
   - `finish`: 결과 종료(리포트 또는 plan)
3) Tool 결과를 관측으로 저장하고 다시 1)로 반복

중요:
- “null check 해줘” 같은 요청은 plan을 만들지 않고 `context_pack_null_report` 등 **분석 툴만** 호출하고 종료해야 합니다.
- “매출 집계용 dataset 만들어줘” 같은 요청은 plan-builder tools로 graph를 만들고 validate/preview까지 확인 후 종료해야 합니다.

---

## 3) No Silent Rewrites

서버는 plan을 자동으로 “고쳐서 통과”시키지 않습니다.
- join input rewiring/reorder, output 자동 주입, select 강제 삽입, cast 자동 삽입 같은 의미 변경 가능 로직은 제거했습니다.
- 실패/오류는 관측(validate/preview 결과)로 표면화하고, LLM이 `plan_update_node_metadata`, `plan_delete_node` 등 edit/patch tools로 직접 수정합니다.

관련 코드:
- Non-mutating validator: `backend/bff/services/pipeline_plan_validation.py`
- Pipeline Agent loop: `backend/bff/services/pipeline_agent_autonomous_loop.py`
- MCP tool server: `backend/mcp/pipeline_mcp_server.py`

---

## 4) MCP 분석 툴(Profiler 확장) – null/PK/FK/type/join-plan

Pipeline MCP server는 context pack 기반으로 결정론적 분석 도구를 제공합니다(샘플 기반, PII 마스킹):
- `context_pack_null_report`: null 비율/핵심 컬럼 결측 요약
- `context_pack_infer_keys`: PK 후보/ FK 후보 정리
- `context_pack_infer_types`: 타입 추론 + join-key cast 제안
- `context_pack_infer_join_plan`: 다중 데이터셋 연결을 위한 join plan(스패닝 트리) 제안

Agent는 “추측” 대신 위 툴을 호출해 근거를 확보하고, 그 결과를 바탕으로 plan-builder tool을 호출합니다.

---

## 5) Feature flags / 설정

- 기본 LLM 모델: `LLM_MODEL=gpt-5` (예: `backend/docker-compose.yml`, `backend/.env.example`)
- Planner on/off: `PIPELINE_PLAN_LLM_ENABLED`

### 5.1) LLM Provider (Real vs Mock)

실제 gpt-5 호출(엔터프라이즈/프로덕션):
- `LLM_PROVIDER=openai_compat`
- `OPENAI_API_KEY=...` (또는 `LLM_API_KEY=...`)

외부 키 없이 E2E/CI 스모크(결정론 mock):
- `LLM_PROVIDER=mock`
- mock payload는 아래 우선순위로 로드됩니다:
  1) `LLM_MOCK_JSON_<TASK>` (예: `LLM_MOCK_JSON_PIPELINE_AGENT_AUTONOMOUS_STEP_V1`)
  2) `LLM_MOCK_DIR/<task>.json` (docker-compose 기본: `../scripts/llm_mocks:/app/llm_mocks:ro`)
  3) `LLM_MOCK_JSON`

Repo에는 파이프라인 에이전트 스모크용 mock 시퀀스가 포함되어 있습니다:
- `scripts/llm_mocks/pipeline_agent_autonomous_step_v1.json`

MCP 설정:
- `mcp-config.json`의 `pipeline` 서버 항목이 필요합니다.
- Docker에서는 `backend/mcp/mcp-config.docker.json`을 `/app/mcp-config.json`로 마운트합니다.

### 5.2) 로컬 포트 충돌 회피(권장)

이미 다른 MinIO/Postgres가 떠있는 개발 머신에서는 기본 포트(9000/5433)가 충돌할 수 있습니다.
예시:

```bash
export POSTGRES_PORT_HOST=5435
export MINIO_PORT_HOST=9100
export MINIO_CONSOLE_PORT_HOST=9101
docker compose -f backend/docker-compose.yml up -d
```


---

## 9) 테스트/문서 동기화

- Unit tests: `pytest -q backend/tests/unit`
- API Reference 갱신(Endpoint Index): `python scripts/generate_api_reference.py`

### 9.1) E2E smoke test (Agent Chat API)

UI의 agent chat은 `POST /api/v1/agent/pipeline-runs`에 연결됩니다.

아래는 “형태” 예시이며, 실제 `db_name`/`dataset_ids`는 환경에 맞게 교체하세요.

```bash
# null check (report_only)
curl -sS -X POST http://localhost:8002/api/v1/agent/pipeline-runs \\
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
curl -sS -X POST http://localhost:8002/api/v1/agent/pipeline-runs \\
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
curl -sS -X POST "$BASE_BFF/agent/pipeline-runs" -H 'Content-Type: application/json' \\
  -d "{
    \"goal\": \"orders, order_items, skus, products를 조인해서 category별 매출(revenue=qty*unit_price) Top 10과 rank(row_number) 만들어줘\",
    \"data_scope\": {\"db_name\": \"$DB\", \"branch\": \"main\", \"dataset_ids\": [\"$ORDERS_ID\",\"$ORDER_ITEMS_ID\",\"$SKUS_ID\",\"$PRODUCTS_ID\"]},
    \"preview_limit\": 50,
    \"include_run_tables\": true,
    \"max_transform\": 2
  }" | jq '.data.preview'
```
