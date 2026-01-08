# SPICE HARVESTER

[English README](README.md)

## 1) What it is (한 문단)

SPICE HARVESTER는 **Event Sourcing + CQRS** 기반으로 아래를 결합한 플랫폼입니다:
- **온톨로지/관계 그래프** (TerminusDB)
- **검색/읽기 프로젝션** (Elasticsearch)
- **데이터 플레인**: raw ingest → pipeline 변환 → objectify로 온톨로지 인스턴스 생성 (lakeFS + Spark pipeline worker)
- **거버넌스 + lineage + audit**로 변경 이력을 추적 가능

Kafka의 **at-least-once** 전달(중복 발행/재전달/재시작/리플레이)이 발생해도 **side effect가 중복 적용되지 않도록** Postgres 기반 정합성 레이어(멱등/순서/OCC)를 강하게 보장합니다.

## 2) Who it’s for / Use cases (누구를 위한가)

- **지식 그래프/온톨로지**를 구축하려는 팀 (product ↔ supplier ↔ customer, 소유/의존/라인리지)
- raw ingest부터 **ETL/정제 → 온톨로지 매핑 → 검색**까지 필요한 데이터 플랫폼 팀
- 변경 이력/감사가 필수인 **데이터 거버넌스/플랫폼** 팀
- “스프레드시트 중심” 운영 데이터를 **스키마 추론 → 매핑 → 임포트**로 온보딩하려는 팀
- 여러 upstream을 통합하며 **재시도/중복**이 정상인 환경에서, 결과 동일성을 보장해야 하는 시스템
- PoC로 시작하되, 나중에 프로덕션으로 올릴 때 **코어 모델을 다시 쓰고 싶지 않은** 팀

## 3) What makes it different (차별점 3가지)

- **Correctness-first**: `processed_events`(멱등 레지스트리) + `aggregate_versions`(순서/스테일 가드) + write-side OCC(`expected_seq`)를 코드/테스트로 고정
- **Lineage-first**: event → artifact(ES/Terminus/S3 등) 연결을 1급으로 다뤄 “왜 이 데이터가 존재하나?”를 설명이 아니라 증명으로 전환
- **Rebuildable**: ES/프로젝션은 “진실”이 아니라 materialized view이며, Event Store replay로 재구축 가능
- **Data plane 분리**: lakeFS + dataset registry로 raw/cleaned 아티팩트를 그래프 스토어와 분리 관리

## 3.1) 기능 전체 목록 (현재 구현)

- **온톨로지/그래프**: 클래스/속성/관계 CRUD, 브랜치/병합/롤백, 질의, 관계 검증(OMS/BFF).
- **관계 모델링**: LinkType + RelationshipSpec(FK/조인테이블/object-backed), dangling 정책, link edits overlay.
- **거버넌스**: 보호 브랜치, proposal/승인, merge 체크, health gate, 객체/백킹 스키마 마이그레이션 계획.
- **데이터 플레인 인제스트**: CSV/Excel/미디어 업로드, Google Sheets 커넥터, Funnel 타입 추론/프로파일링, lakeFS 데이터셋 버전 관리, dataset registry + ingest outbox + reconciler.
- **파이프라인 실행**: preview/build/deploy, Spark 변환(필터/조인/계산/캐스팅/리네임/union/dedupe/groupBy/aggregate/window/pivot), 스키마 계약 + expectations, 파이프라인 스케줄러.
- **Objectify**: mapping spec 버전 관리, 자동/제안 매핑, KeySpec(Primary/Title) 강제, PK 유일성 게이트, edits 마이그레이션.
- **이벤트 소싱 정합성**: S3/MinIO Event Store, message-relay→Kafka, processed_event_registry, sequence allocator, command status(HTTP + WS), admin replay/recompute.
- **조회/프로젝션**: Elasticsearch 프로젝션, graph query federation + label query, optional search-projection worker.
- **Access policy**: dataset 단위 정책에 따라 instance/query/graph 결과가 행/컬럼 마스킹된다.
- **운영/보안**: audit logs, lineage graph, health/config/monitoring, tracing/metrics, rate limiting + 입력 sanitizer, auth token guard.
- **AI/LLM**: 자연어 질의 계획/응답 + Context7 지식베이스 연동.

전체 엔드포인트 목록: `docs/API_REFERENCE.md`

## 4) Architecture

```mermaid
flowchart LR
  Client --> BFF
  BFF --> OMS
  BFF --> Funnel
  BFF --> PG[(Postgres: registry/outbox)]
  BFF --> LFS[(lakeFS + MinIO)]

  OMS -->|append command| EventStore[(S3/MinIO Event Store)]
  EventStore --> Relay[message-relay (S3 tail -> Kafka)]
  Relay --> Kafka[(Kafka)]

  Kafka --> InstanceWorker[instance-worker]
  Kafka --> OntologyWorker[ontology-worker]
  Kafka --> ProjectionWorker[projection-worker]

  InstanceWorker -->|write graph| TerminusDB[(TerminusDB)]
  InstanceWorker -->|append domain events| EventStore
  InstanceWorker --> PG

  OntologyWorker -->|write schema| TerminusDB
  OntologyWorker -->|append domain events| EventStore
  OntologyWorker --> PG

  ProjectionWorker --> ES[(Elasticsearch)]
  ProjectionWorker --> Redis[(Redis)]
  ProjectionWorker --> PG

  LFS --> PipelineWorker[pipeline-worker]
  PipelineWorker --> LFS
  PG --> ObjectifyWorker[objectify-worker]
  ObjectifyWorker --> Kafka
```

**Truth sources (SSoT)**:
- Graph/schema authority: TerminusDB
- Immutable log: S3/MinIO Event Store (commands + domain events)
- Control plane: Postgres (registries, outbox, idempotency, ordering, gates)
- Data plane: lakeFS + MinIO (dataset/artifact versions)

## 5) Reliability Contract (짧게)

1) **Delivery**: Publisher/Kafka는 **at-least-once**. 소비자는 멱등 처리가 계약  
2) **Idempotency key**: 시스템 전체 멱등 키는 `event_id` (같은 `event_id`는 side effect를 최대 1회만 생성)  
3) **Ordering**: aggregate 단위 `sequence_number`가 진실이며, 구버전 이벤트는 무시  
4) **OCC**: write-side 커맨드는 `expected_seq`를 포함하며, 불일치는 **409 충돌**로 감지(조용한 last-write-wins 금지)

자세한 내용: `docs/IDEMPOTENCY_CONTRACT.md`

## 6) Quick Start (5분)

Prereq: Docker + Docker Compose.

```bash
git clone https://github.com/ludia8888/SPICE-Harvester.git
cd SPICE-Harvester

# 선택: 환경변수 템플릿(필요 시 편집)
cp .env.example .env

docker compose -f docker-compose.full.yml up -d
```

Health:

```bash
curl -fsS http://localhost:8000/health
curl -fsS http://localhost:8002/api/v1/health
curl -fsS http://localhost:8003/health
```

## 6.1) 프론트엔드 (Vite + React)

Prereq: Node 20+.

```bash
cd frontend
cp .env.example .env  # 선택
npm ci
npm run dev
```

Dev/preview 프록시:
- `/api/*` 요청은 `VITE_API_PROXY_TARGET`(또는 `BFF_BASE_URL`, 기본값 `http://localhost:8002`)로 프록시됩니다.
- 클라이언트 기본 API base는 `VITE_API_BASE_URL=/api/v1` 입니다.

Preview (빌드 기반) + 프록시:

```bash
npm run build
npm run preview
```

## 7) E2E demo (PoC 시나리오 1개)

시나리오: **Customer + Product(owned_by Customer)** 생성 후, 멀티홉 federation 쿼리로 관계/문서 payload를 함께 조회합니다.

Tip: 아래 예시는 편의를 위해 `jq`를 사용합니다.

BFF의 DB 스코프 리소스는 `/api/v1/databases/{db_name}/...` 형식을 사용합니다.

⚠️ **비동기 Write(202)**  
현재 지원되는 시스템 포지션에서는 core write가 모두 **202 Accepted + command_id**로 반환되며, 아래를 폴링해 완료를 확인해야 합니다.  
`GET /api/v1/commands/{command_id}/status` → `COMPLETED`  
(직접쓰기 `ENABLE_EVENT_SOURCING=false`는 core write 경로에서 지원하지 않습니다.)

Tip: `command_id` 위치가 응답 타입에 따라 다를 수 있습니다. (예: DB/온톨로지 `ApiResponse`는 `.data.command_id`, 인스턴스 `CommandResult`는 `.command_id`)

`/api/v1/databases/{db_name}/instances/...`(Async Instance) API는 “커맨드 제출” API이므로 **항상 202로 취급**하는 것이 안전합니다.

```bash
DB=demo_db_$(date +%s)

# 1) DB 생성 (BFF -> OMS; async 202)
DB_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${DB}\",\"description\":\"demo\"}" | jq -r '.data.command_id')

curl -fsS "http://localhost:8002/api/v1/commands/${DB_CMD}/status" | jq .

# 2) 온톨로지 생성 (BFF -> OMS; async 202)
CUST_ONTO_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/ontology" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"Customer",
    "label":"Customer",
    "properties":[
      {"name":"customer_id","type":"string","required":true},
      {"name":"name","type":"string","required":true}
    ],
    "relationships":[]
  }' | jq -r '.data.command_id')

PROD_ONTO_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/ontology" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"Product",
    "label":"Product",
    "properties":[
      {"name":"product_id","type":"string","required":true},
      {"name":"name","type":"string","required":true}
    ],
    "relationships":[
      {"predicate":"owned_by","target":"Customer","label":"Owned By","cardinality":"n:1"}
    ]
  }' | jq -r '.data.command_id')

curl -fsS "http://localhost:8002/api/v1/commands/${CUST_ONTO_CMD}/status" | jq .
curl -fsS "http://localhost:8002/api/v1/commands/${PROD_ONTO_CMD}/status" | jq .

# 3) 인스턴스 생성 (BFF label API; async 202) + command_id 캡처
CUST_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/instances/Customer/create" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"customer_id":"cust_001","name":"Alice"}}' | jq -r '.command_id')

PROD_CMD=$(curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/instances/Product/create" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"product_id":"prod_001","name":"Shirt","owned_by":"Customer/cust_001"}}' | jq -r '.command_id')

# 4) 비동기 처리 상태 관측 (BFF command status proxy)
curl -fsS "http://localhost:8002/api/v1/commands/${CUST_CMD}/status" | jq .
curl -fsS "http://localhost:8002/api/v1/commands/${PROD_CMD}/status" | jq .

# 5) 멀티홉 쿼리 (BFF federation)
curl -fsS -X POST "http://localhost:8002/api/v1/graph-query/${DB}" \
  -H 'Content-Type: application/json' \
  -d '{
    "start_class":"Product",
    "hops":[{"predicate":"owned_by","target_class":"Customer"}],
    "filters":{"product_id":"prod_001"},
    "limit":10,
    "offset":0,
    "max_nodes":200,
    "max_edges":500,
    "no_cycles":true,
    "include_documents":true
  }' | jq .
```

기대: 응답에는 nodes/edges가 포함되고, 각 노드는 `data_status=FULL|PARTIAL|MISSING`를 제공해 “인덱싱 지연”과 “실제 누락”을 UI에서 구분할 수 있습니다.

## 7.1) 브랜치 기반 what-if (데이터 복사 없음)

브랜치 가상화는 “만약 이 값을 바꾸면?” 같은 시뮬레이션을 **실제 데이터 복사 없이** 수행할 수 있게 합니다.
- 그래프/스키마 조회는 TerminusDB 브랜치에서 수행됩니다(Copy-on-write).
- ES payload는 오버레이로 해석됩니다: 브랜치 인덱스 → (없으면) main 인덱스 폴백(best-effort)
- 브랜치에서 삭제된 엔티티는 tombstone으로 처리되어 “폴백으로 부활”하는 사고를 막습니다(`data_status=MISSING`, `index_status.tombstoned=true`)
- 삭제는 aggregate 단위로 최종 상태이며, 동일 ID를 같은 브랜치에서 재생성하려 하면 OCC로 409가 반환됩니다

```bash
# 1) main에서 브랜치 생성
curl -fsS -X POST "http://localhost:8002/api/v1/databases/${DB}/branches" \
  -H 'Content-Type: application/json' \
  -d '{"name":"feature/whatif","from_branch":"main"}'

# 2) 브랜치에서 federated 조회
curl -fsS -X POST "http://localhost:8002/api/v1/graph-query/${DB}?branch=feature/whatif" \
  -H 'Content-Type: application/json' \
  -d '{"start_class":"Product","hops":[],"filters":{"product_id":"prod_001"},"include_documents":true}' | jq .

# 3) 브랜치에서 업데이트(OCC): expected_seq는 nodes[].index_status.event_sequence에서 가져옵니다
curl -fsS -X PUT "http://localhost:8002/api/v1/databases/${DB}/instances/Product/prod_001/update?branch=feature/whatif&expected_seq=<expected_seq>" \
  -H 'Content-Type: application/json' \
  -d '{"data":{"name":"Shirt (WhatIf)"}}'
```

## 8) Testing (no mocks)

프로덕션 게이트:

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

Pipeline 변환/정제 E2E (전체 스택 필요):

```bash
RUN_PIPELINE_TRANSFORM_E2E=true PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

카오스(파괴적: docker compose stop/start/restart 및 워커 크래시 주입):

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-lite
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-out-of-order
PYTHON_BIN=python3.12 SOAK_SECONDS=600 SOAK_SEED=123 ./backend/run_production_tests.sh --full --chaos-soak
```

### Coverage (pytest-cov + Codecov)

유닛 테스트 중심 커버리지(로컬):

```bash
PYTHONPATH=backend python -m pytest -q \
  backend/tests/unit backend/bff/tests backend/funnel/tests \
  --cov=backend/bff --cov=backend/oms --cov=backend/funnel --cov=backend/shared --cov=backend/instance_worker \
  --cov-branch --cov-report=term --cov-report=xml:coverage.xml
```

CI에서는 `.github/workflows/ci.yml` 가 `coverage.xml` 을 Codecov로 업로드합니다.  
Private repo라면 GitHub Actions secrets에 `CODECOV_TOKEN` 을 설정하세요.

### 성능 테스트 (k6)

풀 스택(docker compose)이 실행 중이어야 합니다.

```bash
make backend-perf-k6-smoke
```

튜닝/정리(테스트 DB cleanup)는 `backend/perf/README.md` 를 참고하세요.

## 9) Limitations / PoC vs Production

- **Saga/보상 트랜잭션 오케스트레이션은 아직 없음**(계획): 실패는 command status로 관측 가능하지만 자동 보상은 미구현
- **Drift 탐지/리컨실 + 백필 파이프라인은 아직 turnkey가 아님**(계획): replay로 재구축 가능하나, 운영용 잡/지표/SLA는 별도 패키징 필요
- **DLQ는 “토픽”은 있으나 기본 운영 워크플로우(알림/재처리/대시보드)는 환경별 구성 필요**
- **보안 모델은 PoC 기본값**: authn/authz, tenant isolation, secrets, rate limits, retention 등을 프로덕션 수준으로 강화해야 함
- **용량/성장 전략 필요**: 특히 Postgres 레지스트리/인덱스 보존/파티셔닝(트래픽 증가 시 병목 지점이 됨)

## 10) Docs

- Index: `docs/README.md`
- Architecture: `docs/ARCHITECTURE.md`
- Reliability contract: `docs/IDEMPOTENCY_CONTRACT.md`
- Ops/runbook: `docs/OPERATIONS.md`, `backend/PRODUCTION_MIGRATION_RUNBOOK.md`
- Production tests: `backend/docs/testing/OMS_PRODUCTION_TEST_README.md`
