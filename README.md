# SPICE HARVESTER

SPICE HARVESTER는 **Event Sourcing + CQRS** 기반의 온톨로지/그래프 데이터 관리 플랫폼입니다.

- **Schema / Graph authority**: TerminusDB (온톨로지 + 관계 그래프)
- **Event log (SSoT)**: S3/MinIO Event Store (Command/Domain Event immutable log)
- **Read model (payload/index)**: Elasticsearch (문서/검색)
- **Correctness layer (필수)**: Postgres (`processed_events`, `aggregate_versions`, write-side seq allocator)
- **Transport**: Kafka (at-least-once)
- **Command status / cache**: Redis

핵심 목표는 하나입니다: **중복 발행/재전달/재시작/리플레이가 있어도 결과가 동일하게 수렴**하도록 “계약(Contract)”을 코드와 테스트로 고정하는 것.

---

## 이 프로젝트로 가능한 것

- 데이터베이스(테넌트) 생성/삭제(비동기 커맨드)
- 온톨로지(클래스/속성/관계) 생성/수정/삭제 + Git-like 버전 컨트롤(브랜치/커밋/diff/merge/rollback)
- 인스턴스 생성/수정/삭제(비동기 커맨드, OCC 지원)
- 멀티홉 그래프 쿼리(관계는 TerminusDB, payload는 ES로 federation)
- Spreadsheet/표 데이터 구조 분석 및 타입 추론(Funnel) + 매핑/임포트 보조(BFF)
- Audit logs / Data lineage(provenance) 기반 운영 관측(선택 기능이지만 코드 경로 존재)

---

## 아키텍처(요약)

```mermaid
flowchart LR
  Client --> BFF
  BFF --> OMS
  OMS -->|append command| EventStore[(S3/MinIO Event Store)]
  EventStore --> Relay[message-relay (S3 tail -> Kafka)]
  Relay --> Kafka[(Kafka)]

  Kafka --> InstanceWorker[instance-worker]
  Kafka --> OntologyWorker[ontology-worker]

  InstanceWorker -->|write graph| TerminusDB[(TerminusDB)]
  InstanceWorker -->|append domain events| EventStore
  InstanceWorker --> PG[(Postgres: processed_events + aggregate_versions)]

  OntologyWorker -->|write schema| TerminusDB
  OntologyWorker -->|append domain events| EventStore
  OntologyWorker --> PG

  Kafka --> ProjectionWorker[projection-worker]
  ProjectionWorker --> ES[(Elasticsearch)]
  ProjectionWorker --> Redis[(Redis)]
  ProjectionWorker --> PG
```

---

## 신뢰성 계약(반드시 읽을 것)

- Delivery semantics: Publisher/Kafka는 **at-least-once**, Consumer는 **멱등 처리**가 계약
- Idempotency key: 시스템 전체 멱등 키는 `event_id` (handler별 `processed_events`로 side-effect 1회 보장)
- Ordering rule: aggregate 단위 `sequence_number`가 진실이며, 구버전은 무시(`aggregate_versions` 단조 증가)
- Write-side OCC: Command는 `expected_seq` 기반으로 충돌을 **409**로 감지 (Postgres seq allocator가 원자 보장)

자세한 내용: `docs/IDEMPOTENCY_CONTRACT.md`

---

## 빠른 시작 (Docker Compose)

### 1) 실행

```bash
git clone https://github.com/ludia8888/SPICE-Harvester.git
cd SPICE-Harvester

docker compose -f docker-compose.full.yml up -d
```

### 2) (선택) 로컬 포트 충돌 회피

`docker-compose.*.yml`은 다음 env로 호스트 포트를 오버라이드할 수 있습니다(기본값은 각 compose 파일의 `:-` 값).

예시 `.env`:

```bash
POSTGRES_PORT_HOST=15433
REDIS_PORT_HOST=16379
ELASTICSEARCH_PORT_HOST=19200
MINIO_PORT_HOST=19000
MINIO_CONSOLE_PORT_HOST=19001
KAFKA_PORT_HOST=19092
KAFKA_UI_PORT_HOST=18080
```

### 3) 헬스체크

```bash
curl -fsS http://localhost:8000/health | jq .
curl -fsS http://localhost:8002/health | jq .
curl -fsS http://localhost:8003/health | jq .
```

---

## 최소 E2E 예시 (curl)

### 1) DB 생성 (BFF)

```bash
curl -fsS -X POST http://localhost:8002/api/v1/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo_db","description":"demo"}' | jq .
```

### 2) 온톨로지 생성 (BFF → OMS)

```bash
curl -fsS -X POST http://localhost:8002/api/v1/database/demo_db/ontology \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"Customer",
    "label":"Customer",
    "properties":[
      {"name":"customer_id","type":"string","required":true},
      {"name":"name","type":"string","required":true}
    ],
    "relationships":[]
  }' | jq .
```

### 3) 인스턴스 생성(비동기) + 상태 조회 (BFF)

```bash
CREATE=$(curl -fsS -X POST http://localhost:8002/api/v1/database/demo_db/instances/Customer/create \
  -H 'Content-Type: application/json' \
  -d '{"data":{"customer_id":"cust_001","name":"Alice"}}')

CMD=$(echo "$CREATE" | jq -r '.command_id')
curl -fsS "http://localhost:8002/api/v1/database/demo_db/instances/command/${CMD}/status" | jq .
```

### 4) 멀티홉 쿼리(그래프 + ES federation)

```bash
curl -fsS -X POST http://localhost:8002/api/v1/graph-query/demo_db \
  -H 'Content-Type: application/json' \
  -d '{
    "start_class":"Customer",
    "hops":[],
    "filters":{"customer_id":"cust_001"},
    "limit":10,
    "offset":0,
    "max_nodes":200,
    "max_edges":500,
    "include_paths":false,
    "no_cycles":true,
    "include_documents":true
  }' | jq .
```

---

## 테스트(무목, 실제 인프라)

프로덕션 게이트(기본):

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full
```

추가 카오스 시나리오(파괴적: docker compose stop/start/restart 포함):

```bash
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-lite
PYTHON_BIN=python3.12 ./backend/run_production_tests.sh --full --chaos-out-of-order
PYTHON_BIN=python3.12 SOAK_SECONDS=600 SOAK_SEED=123 ./backend/run_production_tests.sh --full --chaos-soak
```

---

## 문서

- 문서 인덱스: `docs/README.md`
- 아키텍처: `docs/ARCHITECTURE.md`
- 멱등/순서/OCC 계약: `docs/IDEMPOTENCY_CONTRACT.md`
- 운영/런북: `docs/OPERATIONS.md`, `backend/PRODUCTION_MIGRATION_RUNBOOK.md`
- 백엔드 프로덕션 테스트 가이드: `backend/docs/testing/OMS_PRODUCTION_TEST_README.md`

---

## 주의 사항(운영 관점)

- Postgres(`processed_events`/`aggregate_versions`/seq allocator)는 이제 “선택”이 아니라 **정합성 계약의 핵심 의존성**입니다.
- Kafka/Publisher는 at-least-once이므로, Consumer 멱등/순서 가드가 “마지막 관문”입니다.
- `expected_seq`를 올바르게 사용하면 write-side에서 충돌을 409로 감지할 수 있습니다(충돌 시 재시도/리프레시 정책 필요).

마지막 업데이트: 2025-12-18
