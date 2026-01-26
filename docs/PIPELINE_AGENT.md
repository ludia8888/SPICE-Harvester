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
  - 도구 목록(코드에서 자동 생성): `docs/reference/_generated/PIPELINE_MCP_TOOLS.md`
  - 에이전트 allowlist(코드에서 자동 생성): `docs/reference/_generated/PIPELINE_AGENT_ALLOWED_TOOLS.md`

### Pipeline Plans API (BFF, deterministic helpers)
- Plan registry/preview/eval/context pack
  - 라우터: `backend/bff/routers/pipeline_plans.py`
  - 주요 엔드포인트:
    - `POST /api/v1/pipeline-plans/context-pack`
    - `POST /api/v1/pipeline-plans/compile` (Pipeline Agent 단일 루프 런타임에 위임; API shape만 유지)
    - `POST /api/v1/pipeline-plans/{plan_id}/preview`
    - `POST /api/v1/pipeline-plans/{plan_id}/inspect-preview`
    - `POST /api/v1/pipeline-plans/{plan_id}/evaluate-joins`

---

## 2) 단일 에이전트 루프 동작 방식

Pipeline Agent는 “여러 에이전트를 조율하는 그래프”가 아니라, 한 요청 안에서 반복적으로 도구를 호출하는 **런타임 컨트롤러**입니다.

고수준 플로우:
1) Append-only JSONL 상태 로그에 “현재 상태”를 추가(Goal + data_scope + context/plan 요약 + 마지막 관측)
   - 로그가 너무 커지면 서버가 **결정론 snapshot**으로 compaction(LLM 요약 금지)합니다.
2) LLM이 다음 액션 선택(JSON):
   - `call_tool`: MCP tool 호출
     - latency/cost를 줄이기 위해 한 번의 inference에서 여러 tool call을 `tool_calls`로 배치 실행할 수 있습니다(서버는 순차 실행, max 20; `pipeline_agent_autonomous_loop.py`의 `max_tool_calls_per_step` 참고).
   - `clarify`: 필요한 질문 생성
   - `finish`: 결과 종료(리포트 또는 plan)
3) Tool 결과를 관측으로 저장하고 다시 1)로 반복

중요:
- “null check 해줘” 같은 요청은 plan을 만들지 않고 `context_pack_null_report` 등 **분석 툴만** 호출하고 종료해야 합니다.
- “매출 집계용 dataset 만들어줘” 같은 요청은 plan-builder tools로 graph를 만들고 validate/preview까지 확인 후 종료해야 합니다.

### 2.1) Spark SQL 표현식(Compute/Filter)과 Preview 엔진 정합성

`plan_add_compute(expression=...)` / `plan_add_filter(expression=...)`의 `expression`은 **Spark SQL expression**(Spark `expr` / `filter`)을 그대로 사용합니다.

권장:
- 컬럼을 추가/덮어쓰는 compute는 `plan_add_compute_column(target_column=..., formula=...)`를 우선 사용하세요.
  - `expression="a = b"`는 Spark에서 “비교” 문법이라, assignment로 해석하면 의미가 깨질 수 있습니다.

주의:
- `plan_preview`는 Python 기반의 경량 deterministic executor이며, **Spark SQL 전체 문법/함수와 1:1 정합성이 아닙니다.**
- 특히 `cast(...)`, `case when`, `regexp_*`, 날짜/시간 함수 등 **복잡한 Spark SQL 표현식**은 `plan_preview`에서 “그럴듯하게” 통과할 수 있으나, 실제 Spark worker 실행에서 실패/불일치가 발생할 수 있습니다.

정확한 Spark semantics로 검증이 필요하면:
- `pipeline_create_from_plan`으로 pipeline을 materialize 한 뒤,
- `pipeline_preview_wait`로 Spark worker preview를 실행하여 확인하는 것을 권장합니다.

### 2.2) Ingestion & Parsing (input.read) – permissive parsing / corrupt record 격리

Spark ingestion에서 “깨진 레코드 때문에 전체 파이프라인이 죽는” 리스크를 줄이기 위해,
input node에 `metadata.read`를 설정해 Spark reader 옵션을 제어할 수 있습니다.

설정 방법:
- input 생성 시: `plan_add_input(read=...)`
- 이미 만든 input 수정: `plan_configure_input_read(...)`

예시(깨진 CSV 라인을 `_corrupt_record` 컬럼으로 격리):
```json
{
  "format": "csv",
  "mode": "PERMISSIVE",
  "corrupt_record_column": "_corrupt_record",
  "options": {
    "header": "true"
  }
}
```

주의:
- `read.options`는 Spark DataFrameReader option에 그대로 전달됩니다(문자열로 전달).
- `read.schema`를 주면 schema DDL로 적용됩니다(예: `xsd:integer` → `bigint`).

### 2.3) External Inputs (JDBC/Kafka/File URI)

DatasetRegistry에 저장된 artifact가 아닌, Spark가 직접 읽는 소스도 input node로 표현할 수 있습니다.

- 생성: `plan_add_external_input(read=..., source_name?=...)`
- 보안: 비밀번호/토큰은 plan에 넣지 말고 `read.options_env`로 환경변수에서 주입하세요.

예시(JDBC, password는 env var로 주입):
```json
{
  "format": "jdbc",
  "options": {
    "url": "jdbc:postgresql://host:5432/db",
    "dbtable": "public.orders",
    "user": "readonly"
  },
  "options_env": {
    "password": "JDBC_PASSWORD"
  }
}
```

예시(Kafka batch read):
```json
{
  "format": "kafka",
  "options": {
    "kafka.bootstrap.servers": "broker:9092",
    "subscribe": "orders"
  }
}
```

---

## 3) No Silent Rewrites

서버는 plan을 자동으로 “고쳐서 통과”시키지 않습니다.
- join input rewiring/reorder, output 자동 주입, select 강제 삽입, cast 자동 삽입 같은 의미 변경 가능 로직은 제거했습니다.
- 실패/오류는 관측(validate/preview 결과)로 표면화하고, LLM이 `plan_update_node_metadata`, `plan_delete_node` 등 edit/patch tools로 직접 수정합니다.

관련 코드:
- Non-mutating validator: `backend/bff/services/pipeline_plan_validation.py`
- Pipeline Agent loop: `backend/bff/services/pipeline_agent_autonomous_loop.py`
- MCP tool server: `backend/mcp/pipeline_mcp_server.py`

### 3.1) Pipeline control plane의 deterministic augmentation (투명성)

Pipeline(Pipeline API) 레벨에서는 운영 안전을 위해 일부 **deterministic augmentation**이 존재할 수 있습니다.
예: input schema 기반 cast 삽입, canonical output contract(sys columns / schemaChecks) 주입 등.

원칙:
- Plan 단계에서는 무음 수정 금지(LLM이 tool-call로 명시적으로 수정).
- Pipeline 단계 augmentation은 “운영 런타임 semantics”로 취급되며, 엔드포인트 응답/preview/build 결과로 관측 가능해야 합니다.

---

## 4) MCP 분석 툴(Profiler 확장) – null/PK/FK/type/join-plan

Pipeline MCP server는 context pack 기반으로 결정론적 분석 도구를 제공합니다(샘플 기반, PII 마스킹):
- `context_pack_null_report`: null 비율/핵심 컬럼 결측 요약
- `context_pack_infer_keys`: PK 후보/ FK 후보 정리
- `context_pack_infer_types`: 타입 추론 + join-key cast 제안
- `context_pack_infer_join_plan`: 다중 데이터셋 연결을 위한 join plan(스패닝 트리) 제안

Agent는 “추측” 대신 위 툴을 호출해 근거를 확보하고, 그 결과를 바탕으로 plan-builder tool을 호출합니다.

---

## 4.1) Claim-based Refutation Gate (plan_refute_claims)

Pipeline Agent는 LLM의 자유도를 제한하는 “룰 기반 Verifier” 대신, **반례(witness) 기반 Refuter**를 사용합니다.

- Claim(주장): Agent가 노드별로 “내가 보장하려는 것”을 node.metadata.claims에 선언
- Refuter(반증기): `plan_refute_claims`가 샘플 테이블에서 **반례를 찾을 때만** HARD로 차단
- PASS는 “정답”이 아니라 “아직 반증 못함(not refuted)”

Claim 저장 위치:
- `definition_json.nodes[*].metadata.claims` (list)

지원 Claim kind (v1):
- `PK`: key 컬럼이 NULL이 아니고 유일하다고 주장(중복/NULL 반례로 HARD)
- `JOIN_ASSUMES_RIGHT_PK`: join의 right key가 PK라고 가정(중복/NULL 반례로 HARD)
- `JOIN_FUNCTIONAL_RIGHT`: N:1 join 가정(“left key가 right에서 2개 이상 매칭” 반례로 HARD)
- `CAST_SUCCESS`: cast가 실패 없이 파싱된다고 주장(파싱 실패 값 1개 반례로 HARD)
- `CAST_LOSSLESS`: cast가 “허용된 정규화 범위 내에서” 무손실이라고 주장(HARD 가능)
  - spec에 `allowed_normalization`(예: `["trim","lowercase"]`)을 포함해야 HARD로 반증 가능
  - 지원 normalization (v1): `trim`, `lowercase`, `uppercase`, `collapse_whitespace`, `strip_leading_zeros`, `strip_trailing_zeros`, `normalize_integer`, `normalize_decimal`, `normalize_datetime`, `normalize_date`
- `FILTER_ONLY_NULLS`: “특정 컬럼에서 NULL만 제거했다” 주장(삭제된 row 중 non-null이 1개라도 있으면 HARD)
- `FILTER_MIN_RETAIN_RATE`: “retain_rate >= min_rate” 주장(비율 반례로 HARD)
- `UNION_ROW_LOSSLESS`: “union이 입력 row를 잃지 않는다” 주장(input−output 차집합이 존재하면 HARD)
- `FK` / `ROW_PRESERVE_LEFT` 등은 샘플 기반에서 HARD로 “증명/부정”이 어려워 현재는 SOFT로만 처리합니다.

예시:
```json
{
  "operation": "join",
  "joinType": "left",
  "leftKeys": ["order_id"],
  "rightKeys": ["order_id"],
  "claims": [
    {"id": "orders_items_n_to_1", "kind": "JOIN_FUNCTIONAL_RIGHT", "severity": "HARD"}
  ]
}
```

주의:
- Refuter는 plan을 고치지 않습니다(의미 변형 금지). 오직 “반례 보고서”만 반환합니다.
- Agent는 보고서를 관측으로 받고 `plan_update_node_metadata`/`plan_delete_node` 등으로 부분 수정합니다.

---

## 5) Feature flags / 설정

- 기본 LLM 모델: `LLM_MODEL=gpt-5` (예: `backend/docker-compose.yml`, `backend/.env.example`)
- Planner on/off: `PIPELINE_PLAN_LLM_ENABLED`

### 5.1) LLM Provider (Real vs Mock)

실제 gpt-5 호출(엔터프라이즈/프로덕션):
- `LLM_PROVIDER=openai_compat`
- `LLM_API_KEY=...` (legacy alias: `OPENAI_API_KEY=...`)

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

## 6) Pipeline 실행(산출물 생성): pipeline_* MCP tools

Agent는 기본적으로 read-only(analysis + plan + preview)로 동작합니다.
사용자가 “실제 산출물 생성(build)” 또는 “배포/승격(deploy)”를 명시적으로 요청한 경우에만 아래 도구를 사용합니다.

- `pipeline_create_from_plan`: plan.definition_json으로 Pipeline 생성
- `pipeline_update_from_plan`: plan 수정 후 Pipeline definition 갱신
- `pipeline_preview_wait`: Spark worker preview enqueue + poll + 결과 반환(PII 마스킹)
- `pipeline_build_wait`: Spark worker build enqueue + poll + 결과 반환(PII 마스킹)
- `pipeline_deploy_promote_build`: build 결과를 deploy로 promote(approve 권한 필요)

권장 플로우(예):
1) plan 조립/validate/preview
2) `pipeline_create_from_plan`
3) (필요 시) `pipeline_preview_wait`로 Spark semantics 검증
4) `pipeline_build_wait`로 build 산출물 생성
5) (요청/권한이 있을 때만) `pipeline_deploy_promote_build`

### 6.1) Spark feature parity를 위한 plan-builder tool 확장

Spark에서 자주 쓰는 고급 표현식을 “문자열 DSL”이 아니라 tool-call로 plan에 주입할 수 있도록,
아래 도구를 추가로 제공합니다:
- `plan_add_compute_column`: target_column/formula 기반 compute (assignment/비교 `=` 혼동 방지)
- `plan_add_compute_assignments`: 여러 컬럼을 한 번에 compute
- `plan_add_select_expr`: Spark `selectExpr` 기반 projection
- `plan_add_group_by_expr`: Spark aggregate expressions 기반 groupBy/aggregate (예: approx_percentile)
- `plan_add_window_expr`: Spark window expressions 기반 파생 컬럼 생성
- `plan_configure_input_read`: input.read 설정(ingestion/parsing 옵션)
- `plan_add_external_input`: DatasetRegistry 없이 Spark-native source(JDBC/Kafka/URI) input 추가
- `plan_update_settings`: plan.definition_json.settings 패치 (Spark conf / cast mode / AQE 등)

예: ANSI + AQE 활성화 + STRICT cast mode
```json
{
  "spark_conf": {
    "spark.sql.ansi.enabled": "true",
    "spark.sql.adaptive.enabled": "true"
  },
  "cast_mode": "STRICT"
}
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
