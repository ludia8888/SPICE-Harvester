# SPICE HARVESTER — Architecture (Current, Code-Backed)

> - 기준: 코드 + docker-compose.full.yml
> - 범위: Backend + Infra + Control/Data plane + Event Sourcing
> - 상태: 현재 구현된 구성요소/계약 중심으로 정리

## 0) TL;DR

- **BFF가 결정론적 프론트 API 계약**이며 OMS/Funnel/Registry/Pipeline/Objectify로 라우팅한다. **Agent 서비스는 별도 도구 실행 계층**이다(BFF 통해서만 호출).
- **Event Store(S3/MinIO)가 Write SSoT**이고, Kafka는 transport다.
- **Control Plane은 Postgres**(dataset/pipeline/objectify registry, proposal/approval/deploy, gate results, outbox, processed_event_registry).
- **Ontology 정의는 TerminusDB**, 데이터 아티팩트는 **lakeFS + MinIO**, 검색은 **Elasticsearch**.
- **Action-only writeback**은 lakeFS commit-addressed patchset + ES overlay(derived cache)로 구현되며, writeback 대상 타입에서 overlay 장애는 **명시적 DEGRADED/503**로 처리한다(절대 silent fallback 금지).
- **Outbox + Reconciler**로 dataset/objectify 작업 내구성을 확보한다.
- **Relationship indexing + link edits overlay**로 그래프 관계와 수정을 분리 관리한다.
- **Audit/Lineage는 Postgres에 저장되고 BFF로 조회된다**.

---

## 1) 서비스 토폴로지

```{mermaid}
graph TD
  UI[Web UI / Clients]

  subgraph API
    BFF[BFF FastAPI :8002]
    AGENT[Agent FastAPI :8004]
    OMS[OMS FastAPI :8000]
    FUNNEL[Funnel FastAPI :8003]
  end

  subgraph Workers
    MSG[message-relay]
    OW[ontology-worker]
    IW[instance-worker]
    PW[projection-worker]
    AW[action-worker]
    AOW[action-outbox-worker]
    WMW[writeback-materializer-worker]
    SPW[search-projection-worker]
    PLW[pipeline-worker]
    PLS[pipeline-scheduler]
    OBJ[objectify-worker]
    ING[ingest-reconciler-worker]
    CTS[connector-trigger-service]
    CSW[connector-sync-worker]
  end

  subgraph Infra
    PG[(Postgres)]
    TDB[(TerminusDB)]
    KAF[Kafka]
    RED[Redis]
    ES[(Elasticsearch)]
    S3[(S3/MinIO Event Store)]
    S3A[(S3/MinIO Agent Store)]
    LFS[(lakeFS)]
  end

  UI --> BFF
  BFF --> AGENT
  BFF --> OMS
  BFF --> FUNNEL
  BFF --> LFS

  AGENT --> BFF
  AGENT --> PG
  AGENT --> S3A

  OMS --> TDB
  OMS --> PG
  OMS --> S3

  MSG --> KAF
  S3 --> MSG

  KAF --> OW
  KAF --> IW
  KAF --> PW
  KAF --> AW
  KAF --> SPW
  KAF --> PLW
  KAF --> OBJ
  KAF --> CSW

  OW --> TDB
  IW --> TDB
  PW --> ES
  SPW --> ES
  AW --> PG
  AW --> LFS
  AW --> S3
  AOW --> PG
  AOW --> LFS
  AOW --> S3
  WMW --> LFS
  WMW --> S3

  PLW --> LFS
  PLW --> PG
  OBJ --> LFS
  OBJ --> PG
  PLS --> PG
  PLS --> KAF
  ING --> PG

  CTS --> PG
  CTS --> KAF
  CSW --> BFF

  BFF --> ES
  BFF --> RED
  BFF --> PG
```

**Note**: BFF 내부에서 `dataset_ingest_outbox_worker`와 `objectify_outbox_worker`가 함께 실행된다.
**Note**: `action-worker` / `action-outbox-worker` / `writeback-materializer-worker`는 `docker-compose.full.yml`(및 `backend/docker-compose.yml`)에 포함되어 있다(운영에서는 각 워커를 별도 서비스/디플로이로 상시 구동 권장, `writeback-materializer-worker`는 CronJob/주기 실행도 가능).

---

## 1.1) 서비스 역할 (현재 구현)

- **BFF**: 단일 API gateway, auth/rate limit, dataset ingest + pipeline/objectify orchestration, graph/query/lineage/audit, command status(HTTP+WS), admin tasks, AI/Context7.
- **Agent**: 검증된 AgentPlan step 실행(단일 순차 루프). 외부 호출은 BFF만 사용하며, 에이전트 이벤트는 SSoT Event Store와 분리된 버킷에 기록한다.
- **OMS**: ontology/branch/version/pull request/merge/rollback, async command 등록, 스키마 검증.
- **Funnel**: 타입 추론/프로파일링(스키마/컬럼 분석).
- **message-relay**: S3/MinIO Event Store tail → Kafka publish.
- **ontology-worker**: ontology command 처리 → TerminusDB write + domain event.
- **instance-worker**: instance command 처리 → TerminusDB write + link indexing + domain event.
- **projection-worker**: domain event → ES projection (DLQ 포함).
- **action-worker**: ActionCommand consume → (permission + `submission_criteria` gate) → patchset 계산 → lakeFS writeback commit → ActionApplied emit + queue append.
- **action-outbox-worker**: ActionLog 상태머신(outbox) 복구(emit/queue append 재시도)로 partial failure를 수렴.
- **writeback-materializer-worker**: `writeback_edits_queue`를 스캔하여 `writeback_merged_snapshot`을 생성(재빌드/서버 merge fallback 최적화).
- **search-projection-worker**: 선택적 검색 인덱스 업데이트 (`ENABLE_SEARCH_PROJECTION`).
- **pipeline-worker**: Spark 기반 변환 실행(Preview/Build/Deploy + 계약/expectations).
- **pipeline-scheduler**: 스케줄 파이프라인 실행 트리거 → job queue.
- **objectify-worker**: mapping spec → bulk instance 생성 + 관계 인덱싱 + edits overlay.
- **ingest-reconciler-worker**: dataset ingest/outbox 상태 복구/재발행.
- **connector-trigger-service / connector-sync-worker**: Google Sheets 변경 감지 → BFF ingest 호출.

---

## 1.2) DLQ (Kafka) (현재 구현)

- Kafka consumer는 **poison/non-retryable** 또는 **max-retry 초과** 시 DLQ 토픽에 원본 메시지(`original_topic/partition/offset/value`)와 오류(`error/stage/attempt_count`)를 기록하고 offset을 commit한다.
- 주요 DLQ 토픽:
  - **Commands**: `instance-commands-dlq`, `ontology-commands-dlq`, `action-commands-dlq`
  - **Pipeline/Objectify**: `pipeline-jobs-dlq`, `objectify-jobs-dlq`
  - **Projection/Search**: `projection_failures_dlq`
  - **Connectors**: `connector-updates-dlq`
  - **Outbox**: `dataset-ingest-outbox-dlq`
- 운영 재처리(Replay): `scripts/replay_dlq.py` (DLQ → 원 토픽 publish)

---

## 1.3) Kafka Worker Runtime (Template Method + Partitioned Loop)

- **공통 런타임**: `ProcessedEventKafkaWorker`가 claim/heartbeat/retry/DLQ/commit 흐름을 **Template Method**로 제공한다.
- **전략 훅**: payload 파싱, registry key 추출, side-effect 실행, DLQ publish는 워커가 **Strategy**로 주입한다.
- **Partitioned loop**: `pipeline-worker`/`objectify-worker`는 **파티션 단일 in-flight** + pause/resume를 사용한다.
  - 대기열 버퍼링 및 commit-gate(실패 시 rewind)를 통해 **순서 보장 + 안전한 재시도**를 확보한다.
- **표준 루프**: 나머지 워커는 `run_loop`로 **poll/에러 처리/seek**을 일관화한다.

---

## 2) Event Sourcing Write Flow (실제 코드 기준)

```{mermaid}
sequenceDiagram
  participant Client
  participant BFF
  participant OMS
  participant S3 as EventStore (S3/MinIO)
  participant Relay as message-relay
  participant Kafka
  participant Worker
  participant TerminusDB
  participant ES

  Client->>BFF: Write 요청 (create/update/delete)
  BFF->>OMS: 비동기 write API 호출
  OMS->>S3: Command EventEnvelope append
  S3->>Relay: index tail
  Relay->>Kafka: command publish
  Kafka->>Worker: consume command
  Worker->>TerminusDB: 실제 write 수행
  Worker->>S3: Domain EventEnvelope append
  S3->>Relay: index tail
  Relay->>Kafka: domain event publish
  Kafka->>ES: projection-worker가 검색 인덱스 갱신
```

핵심 포인트:
- **Event Store가 유일한 SSoT**이며, Kafka는 transport로만 사용된다.
- **ProcessedEventRegistry(Postgres)**로 worker idempotency + ordering 보장.
- **CommandStatusService(Redis)**가 async write 상태/결과를 추적.

---

## 2.1) Action-only Writeback Flow (lakeFS patchset + ES overlay)

Action writeback은 `docs/ACTION_WRITEBACK_DESIGN.md` 철학을 따르는 별도 write plane이다:
- **writeback은 Action만 허용**(Direct CRUD는 ingest-only로 제한 가능)
- lakeFS에 **commit-addressed patchset**을 기록하고,
- ES overlay는 **derived cache**이며 read-your-write는 overlay로 보장한다.
- Actor identity는 `X-User-ID`/`X-User-Type` 헤더로부터 결정되며, OMS/worker는 `(principal_type, principal_id)` + `role:*` principal tag로 `permission_policy`/dataset ACL을 평가한다.
- `action-worker`는 `action_type.spec.submission_criteria`를 안전한 boolean evaluator(`backend/shared/utils/safe_bool_expression.py`)로 평가하고, false/평가불가면 ActionLog를 `FAILED`로 종결한다.
- `WRITEBACK_ENFORCE_GOVERNANCE=true`에서는 backing dataset ACL과 writeback dataset ACL이 정렬되지 않으면(Action worker에서 검증 불가 포함) Action을 reject한다.
- `conflict_policy=BASE_WINS` 등으로 `applied_changes`가 **no-op**이 되면, overlay 문서와 writeback queue entry는 생성되지 않을 수 있다
  (이 경우 read는 base로 자연스럽게 fallback되고, 의사결정/충돌 메타는 ActionLog에서 조회한다).

기본 저장소/브랜치(구현 기준):
- lakeFS repo: `ontology-writeback` (`ONTOLOGY_WRITEBACK_REPO`)
- overlay/writeback branch: `writeback-{db_name}` (`ONTOLOGY_WRITEBACK_BRANCH_PREFIX`)
- datasets: `writeback_patchsets`, `writeback_edits_queue`, `writeback_merged_snapshot`

```{mermaid}
sequenceDiagram
  participant Client
  participant BFF
  participant OMS
  participant PG as Postgres(ActionLogs)
  participant S3 as EventStore (S3/MinIO)
  participant Relay as message-relay
  participant Kafka
  participant AW as action-worker
  participant LFS as lakeFS(writeback)
  participant PW as projection-worker

  Client->>BFF: Action submit (intent)
  BFF->>OMS: submit_action_async
  OMS->>PG: ActionLog create (PENDING)
  OMS->>S3: ActionCommand append (action_commands)
  S3->>Relay: index tail
  Relay->>Kafka: command publish
  Kafka->>AW: consume ActionCommand
  AW->>S3: base state replay (conflict check)
  AW->>LFS: write patchset commit (writeback_patchsets)
  AW->>PG: ActionLog update (COMMIT_WRITTEN)
  AW->>S3: ActionApplied append (action_events)
  S3->>Relay: index tail
  Relay->>Kafka: event publish
  Kafka->>PW: consume ActionApplied
  PW->>LFS: read patchset
  PW->>ES: write overlay doc (external_gte, non-noop only)
```

내구성/복구:
- `action_outbox_worker`가 ActionLog 상태머신을 스캔하여 **event emit/queue append 누락을 복구**한다.
- `writeback_edits_queue`는 per-object append-only 인덱스이며, `writeback_materializer_worker`가 `writeback_merged_snapshot`을 생성해 재빌드/서버 merge fallback 비용을 낮춘다.

## 3) 저장소/인프라 역할 (SSoT 분리)

| 저장소 | 역할 | SSoT 여부 |
|---|---|---|
| **S3/MinIO (Event Store)** | Command + Domain 이벤트 로그 (immutable) | Write SSoT |
| **TerminusDB** | Ontology 정의 + 버전/브랜치 | Definition SSoT |
| **Postgres** | registry/outbox/proposal/health/processed_event_registry + action logs | Control Plane SSoT |
| **lakeFS + MinIO** | dataset/artifact 버전 관리 + writeback patchsets/queue/snapshot | Data plane SSoT |
| **Elasticsearch** | CQRS read model/projection | Read cache |
| **Redis** | command status, cache, websocket, rate limit | 보조 |

---

## 4) Control Plane (Registry/Proposal/Approval/Deploy/Health)

### 4.1 Registries & Governance Specs

- `dataset_registry` / `pipeline_registry` / `objectify_registry`가 데이터/파이프라인 실행 이력을 보존.
- BackingDataSource/Version, KeySpec, MappingSpec, Schema Migration Plan을 Postgres에 기록.
- Gate Policy/Result가 스키마/매핑/데이터 검증 결과를 축적한다.
- Dataset/Objectify outbox가 비동기 작업을 내구적으로 트리거한다.
- Action-only writeback은 `spice_action_logs.ontology_action_logs`(ActionLogRegistry)로 상태/감사(SSoT)를 관리한다.
- ActionLog는 단순 감사 로그가 아니라 **온톨로지 객체(1급 객체)**로 취급되며, BFF에서 가상 object type `ActionLog`로도 노출된다
  (`GET /api/v1/databases/{db_name}/class/ActionLog/instance/{action_log_id}`, `GET /api/v1/databases/{db_name}/class/ActionLog/instances`).
- Access policy(행/컬럼 마스킹)는 dataset_registry에 저장되고 BFF 조회 경로에서 적용된다.

### 4.2 Proposal/Approval

- **Pull Request 서비스**가 Postgres(MVCC)로 proposal 상태를 관리.
- 보호된 브랜치에서 direct write는 `409`로 차단된다.

### 4.3 Deploy

- `deployments_v2`에 `ontology_commit_id`, `snapshot_rid`, `gate_policy`, `health_summary` 고정.
- **deploy outbox**가 후속 작업(캐시/알림/프로젝션)을 분리 처리.

### 4.4 Health Gate

- Linter + 관계 검증 결과를 Postgres에 기록.
- 동일 commit/policy 조합은 dedupe 가능.

---

## 5) Data Plane (Dataset / Pipeline / Objectify)

### 5.1 Dataset Ingest

- BFF에서 CSV/Excel/Media 업로드 처리.
- **Idempotency Key 필수**: 동일 payload 재시도 시 중복 방지.
- 업로드 → lakeFS commit → dataset registry 기록 → **outbox 발행** → (event store + lineage 기록).
- ingest-reconciler가 stale ingest를 복구/정리한다.

### 5.2 Pipeline Build/Deploy

- `pipeline-worker`가 Spark 기반 빌드 실행.
- 지원 변환: filter/join/compute/rename/cast/dedupe/groupBy/aggregate/window/pivot/union 등.
- expectations(not_null 등) 기반 데이터 품질 체크를 지원한다.
- 스키마 계약(schema contract) 위반이나 타입 미스매치는 빌드 게이트에서 차단.
- build/preview 결과는 **PipelineRegistry**에 기록.
- Deploy 시 lakeFS merge + dataset version 등록 + lineage 기록.

### 5.3 Objectify

- Mapping spec + dataset version 기준으로 bulk instance 생성.
- Objectify outbox + reconciler로 내구성 확보.

---

## 6) Connector Flow (Google Sheets)

- `connector_trigger_service`가 polling / cursor 변경을 감지.
- 변경 사항은 Kafka 이벤트로 publish.
- `connector_sync_worker`가 BFF를 호출하여 ingest 파이프라인 실행.
- Connector sources + mappings + sync state는 **ConnectorRegistry(Postgres)**에 저장.

---

## 7) Relationship Indexing & Link Edits

- LinkType/RelationshipSpec은 온톨로지 리소스로 저장된다.
- 인덱싱 파이프라인이 FK/조인 테이블 입력을 그래프 엣지로 반영한다.
- dangling policy(WARN/FAIL) 기반으로 인덱싱 상태/통계를 기록한다.
- Link edits(override)는 별도 레이어로 저장되며 reindex 시 반영될 수 있다.

---

## 8) Read/Query Path

- **Elasticsearch projection**이 기본 조회 경로(derived cache)이며, writeback-enabled 타입은 **overlay branch**로 read-your-write를 보장한다.
- Read API는 `base_branch`(Terminus/SSoT)와 `overlay_branch`(ES overlay)를 분리한다. (`branch`는 `base_branch`의 deprecated alias)
- writeback-enabled 타입에서 overlay가 required인데 ES가 불가하면:
  - 가능하면 서버 merge view를 반환하고 `overlay_status=DEGRADED`로 명시한다(현재 단건 instance read에서 제공).
  - 불가하면 `503` + `overlay_status=DEGRADED`로 통일하며, **Terminus-only silent fallback은 금지**한다.
- Graph query는 **WOQL 기반 traversal(Terminus)** + **ES document fetch/overlay enrichment**를 결합한다.
- Access policy에 따라 결과가 마스킹/필터링될 수 있다.

---

## 9) Consistency & Idempotency

- `AggregateSequenceAllocator` (Postgres)로 **expected_seq** 보장.
- `ProcessedEventRegistry`로 worker idempotency + ordering 보장.
- Event Store는 **event_id 기반 idempotency**를 제공.
- Kafka producer는 idempotent 설정으로 retry 안전성을 높인다.

---

## 10) Observability & Security

- Prometheus metrics + OpenTelemetry tracing.
- Error normalization (`shared/errors/error_response.py`).
- Input sanitizer로 SQL/XSS/NoSQL 주입 차단.
- Rate limiting (Redis token bucket + local fallback).
- `/api/v1/monitoring`, `/api/v1/config` 로 상태/설정 모니터링 제공.
- Admin 전용 replay/recompute/trace 작업 엔드포인트가 존재한다 (`backend/bff/routers/admin.py`).
- Audit logs(해시 체인) + lineage graph가 Postgres에 저장되며 BFF API로 조회 가능하다.

---

## 11) Runtime / Local

- 권장 실행: `docker-compose.full.yml`.
- 구성 요소: terminusdb, postgres, kafka, minio, lakefs, bff, oms, funnel, agent, workers.

---

## 12) 코드 네비게이션

- `backend/bff/` : API contract + aggregation
- `backend/oms/` : TerminusDB + ontology control
- `backend/agent/` : agent tool runner(순차 실행) + audit/event logging
- `backend/shared/` : Event Store, registries, models, security
- `backend/*_worker/` : async command execution + projection
- `backend/message_relay/` : S3 event tail → Kafka publish
- `backend/funnel/` : type inference + structure analysis
- `backend/pipeline_worker/` : Spark transforms + dataset artifacts
- `backend/objectify_worker/` : mapping spec → ontology instances
- `backend/connector_trigger_service/`, `backend/connector_sync_worker/` : connector ingest flow

## 12.1) THINK ULTRA Design (현재 코드 기준)

이 섹션은 "실제 코드 기준"으로 전체 시스템의 설계 의도와 실행 경로를 재구성한 문서다.
의도/구현/제약을 분리해 과장 없이 현재 상태를 명확히 기록한다.
Agent 런타임(단일 루프) 설계는 `docs/AGENT_PRD.md` Appendix A (AGENT-RUNTIME-ARCH-001)를 참고한다.

### 12.1.1 설계 불변성 (Invariants)

- **단일 프론트 계약**: 외부 호출은 BFF만 허용한다. Agent/OMS/Funnel은 내부 전용이다.
- **SSoT 분리**: Event Store(S3/MinIO)=Write SSoT, TerminusDB=Ontology SSoT, Postgres=Control Plane SSoT, lakeFS=Data Plane SSoT.
- **Event Sourcing**: 모든 write는 immutable event로 저장되고, Kafka는 transport다.
- **Idempotency/Ordering**: `event_id` + `sequence_number` + ProcessedEventRegistry로 중복/경합을 제거한다.

### 12.1.2 Control Plane 모델 (Postgres 중심)

- **DatasetRegistry**: dataset/version, ingest request/transaction/outbox, backing datasource + version, key spec, gate policy/result, access policy, instance edits, relationship spec/index 결과, link edits, schema migration plan을 관리한다.
- **PipelineRegistry**: pipeline/version/run/artifact, dependencies, permissions, watermarks, UDFs, promotion manifest를 관리한다.
- **ObjectifyRegistry**: mapping spec + objectify job + outbox를 관리한다.
- **ProcessedEventRegistry**: worker idempotency + ordering lease를 보장한다.

### 12.1.3 Event Sourcing & Async Write (OMS/Workers)

1. BFF가 OMS에 async command를 요청한다.
2. OMS는 Command EventEnvelope를 Event Store에 append한다.
3. message-relay가 S3 index를 tail하여 Kafka로 publish한다.
4. Worker가 command를 처리하고 domain event를 다시 Event Store에 append한다.
5. projection-worker가 ES read model을 갱신한다.

### 12.1.4 Data Plane E2E (Ingest → Pipeline → Objectify → Projection)

1. **Ingest**: BFF가 CSV/Excel/Media/Connector 데이터를 받아 lakeFS에 커밋하고 ingest request/outbox를 기록한다.
2. **Schema Gate**: 샘플/스키마 해시를 기록하고 승인/거부를 gate result로 남긴다.
3. **Pipeline**: preview는 local executor, build/deploy는 Spark pipeline-worker가 수행한다.
4. **Contract/Expectations**: schema checks/contract/expectations 실패는 build/deploy를 차단한다.
5. **Objectify**: mapping spec + key spec 기반으로 bulk instance 생성(job) 후 OMS로 write 이벤트를 보낸다.
6. **Auto Objectify**: `auto_sync=true`인 mapping spec만 자동 실행되며 schema hash 불일치는 gate fail로 기록된다.
7. **Projection**: instance event는 TerminusDB에 적용되고 ES로 투사된다.

### 12.1.5 Relationship Indexing & Link Edits

- RelationshipSpec은 join-table/FK/object-backed 링크를 정의한다.
- Objectify/Instance worker가 링크를 인덱싱하고 결과/통계/라인리지를 기록한다.
- dangling policy(FAIL/WARN)는 결과에 반영되며, link edits는 별도 overlay 레이어로 저장된다.

### 12.1.6 Governance & Branching

- 보호된 브랜치는 direct write를 차단하고 proposal/approval 흐름을 요구한다.
- Gate policy/result는 schema/mapping/quality 검증을 기록하고 pipeline/objectify에 적용된다.

### 12.1.7 Read/Query Path & Access Policy

- Label 기반 Query는 BFF가 라벨→내부 ID 변환 후 OMS/TerminusDB 쿼리를 실행한다.
- Graph Query는 TerminusDB traversal + ES 문서 fetch를 결합한다.
- Access policy는 행/컬럼 필터/마스킹을 적용한다.

### 12.1.8 Agent (Pipeline Agent: Single Autonomous Loop)

- Agent service(`backend/agent`)는 `/api/v1/agent/runs` 실행(typed plan step executor) 용도로 유지된다.
- BFF는 Agent service를 `backend/bff/routers/agent_proxy.py`로 proxy하며, `X-Spice-Caller: agent`로 proxy loop를 차단한다.
- Pipeline Agent(자연어 ETL)는 `POST /api/v1/agent/pipeline-runs`로 실행되며, **BFF 내부에서 단일 autonomous loop + MCP tools**로 수행된다.
  - 런타임 컨트롤러: `backend/bff/services/pipeline_agent_autonomous_loop.py`
- Tool provider: `backend/mcp_servers/pipeline_mcp_server.py`
- Pipeline Agent는 “서브 에이전트/핸드오프/라우터/병렬 실행” 없이, 한 요청 안에서 `Inference → Tool → Observation`을 반복한다. (상세: `docs/PIPELINE_AGENT.md`)

### 12.1.9 LLM/Context7 (현재 구현)

- `/api/v1/ai/*`는 **읽기 전용 계획 생성**과 요약만 수행한다 (write 금지).
- LLM Gateway는 JSON schema 검증을 통해 출력 안전성을 강제한다.
- `/api/v1/context7/*`는 search/knowledge/link/ontology analyze를 제공하지만 Agent와 자동 연계는 없다.
- Pipeline Agent의 “단일 autonomous loop + MCP tools”가 자연어 ETL의 우선 경로이다. (상세: `docs/PIPELINE_AGENT.md`)
- (Removed) `POST /api/v1/agent-plans/*` 기반의 legacy plan-only control plane은 제거되었다.

### 12.1.10 현재 미구현/제약 (명시)

- 자연어 → 파이프라인 자동 구성/preview/repair는 Pipeline Agent로 부분 구현되어 있으나, 기본값은 feature flag에 의해 제어된다.
- Pipeline Agent는 control-plane 중심(계획/검증/샘플 preview)이며, Spark build/deploy(대규모 실행) 자동화는 별도 운영 정책/승인 흐름이 필요하다.
- 온톨로지 자동 구축은 human-in-the-loop 없이 동작하지 않는다.
- Agent service는 내부 step executor로 유지되지만, 자연어 ETL은 Pipeline Agent가 담당한다(단일 autonomous loop + MCP tools).

### 12.1.11 코드 크로스체크 앵커 (주요 구현 위치)

- Event Store: `backend/shared/services/event_store.py`
- Idempotency/Ordering: `backend/shared/services/processed_event_registry.py`
- Ingest/Pipeline/Objectify API: `backend/bff/routers/pipeline.py`, `backend/bff/routers/objectify.py`
- Pipeline transforms: `backend/shared/services/pipeline_executor.py`, `backend/shared/services/pipeline_transform_spec.py`
- Objectify + link indexing: `backend/objectify_worker/main.py`, `backend/instance_worker/main.py`
- Agent runtime + proxy: `backend/agent/services/agent_runtime.py`, `backend/bff/routers/agent_proxy.py`
- Pipeline Agent loop: `backend/bff/services/pipeline_agent_autonomous_loop.py`, `backend/bff/routers/agent_proxy.py`
- Pipeline Plans API: `backend/bff/routers/pipeline_plans.py`, `backend/bff/services/pipeline_plan_autonomous_compiler.py`, `backend/bff/services/pipeline_plan_validation.py`
- Pipeline MCP server/tools: `backend/mcp_servers/pipeline_mcp_server.py`, `backend/shared/services/pipeline_plan_builder.py`
- AI/Context7: `backend/bff/routers/ai.py`, `backend/bff/routers/context7.py`

### 12.1.12 Agent 실행 상태 저장 계약 (현재 구현)

Agent 실행(검증/승인/감사 포함)을 재현/감사하기 위한 control-plane 저장소를 Postgres에 둔다.
아래는 현재 구현된 테이블/필드의 요약이다.

- `agent_runs`: `run_id`, `plan_id`, `status`, `risk_level`, `requester`, `delegated_actor`, `context`(JSONB), `plan_snapshot`(JSONB), `started_at`, `finished_at`
- `agent_steps`: `run_id`, `step_id`, `tool_id`, `status`, `command_id`, `task_id`, `input_digest`, `output_digest`, `error`, `metadata`(JSONB), `started_at`, `finished_at`
- `agent_approvals`: `approval_id`, `plan_id`, `step_id?`, `decision`, `approved_by`, `approved_at`, `comment`, `metadata`(JSONB)

---

## 13) Auto-generated Reference (from code)

이 섹션은 코드/compose를 기반으로 자동 생성됩니다. 수정 시 아래 스크립트를 사용하세요:

```bash
python scripts/generate_architecture_reference.py
```

### Compose Inventory (docker-compose.full.yml)

<!-- BEGIN AUTO-GENERATED ARCH: COMPOSE_INVENTORY -->
Source: `docker-compose.full.yml` (with extends resolved).

| Service | Ports | Depends On |
| --- | --- | --- |
| `action-outbox-worker` | - | kafka<br/>postgres<br/>minio<br/>lakefs<br/>otel-collector |
| `action-worker` | - | terminusdb<br/>kafka<br/>postgres<br/>minio<br/>lakefs<br/>message-relay<br/>otel-collector |
| `agent` | ${AGENT_PORT_HOST:-8004}:8004 | bff<br/>postgres<br/>minio<br/>otel-collector |
| `alertmanager` | 19093:9093 | - |
| `bff` | 8002:8002 | db-migrations<br/>oms<br/>postgres<br/>redis<br/>elasticsearch<br/>kafka<br/>minio<br/>lakefs<br/>funnel<br/>otel-collector |
| `connector-sync-worker` | - | bff<br/>kafka<br/>postgres<br/>otel-collector |
| `connector-trigger-service` | - | kafka<br/>postgres<br/>otel-collector |
| `db-migrations` | - | postgres |
| `elasticsearch` | ${ELASTICSEARCH_PORT_HOST:-9200}:9200<br/>${ELASTICSEARCH_TRANSPORT_PORT_HOST:-9300}:9300 | - |
| `funnel` | ${FUNNEL_PORT_HOST:-8003}:8003 | otel-collector |
| `grafana` | 13000:3000 | prometheus |
| `ingest-reconciler-worker` | ${INGEST_RECONCILER_PORT_HOST:-8012}:8012 | postgres<br/>otel-collector |
| `instance-worker` | - | terminusdb<br/>kafka<br/>elasticsearch<br/>minio<br/>message-relay<br/>postgres<br/>otel-collector |
| `jaeger` | 16686:16686 | - |
| `kafka` | ${KAFKA_PORT_HOST:-39092}:9092<br/>${KAFKA_PORT_INTERNAL_HOST:-29092}:29092 | zookeeper |
| `kafka-ui` | ${KAFKA_UI_PORT_HOST:-8080}:8080 | kafka<br/>zookeeper |
| `lakefs` | ${LAKEFS_PORT_HOST:-48080}:8000 | postgres<br/>minio |
| `lakefs-init` | - | lakefs<br/>minio-init |
| `message-relay` | - | kafka<br/>postgres<br/>minio<br/>otel-collector |
| `minio` | ${MINIO_PORT_HOST:-9000}:9000<br/>${MINIO_CONSOLE_PORT_HOST:-9001}:9001 | - |
| `minio-init` | - | minio |
| `objectify-worker` | - | kafka<br/>postgres<br/>lakefs<br/>oms<br/>otel-collector |
| `oms` | ${OMS_PORT_HOST:-8000}:8000<br/>8000:8000 | db-migrations<br/>terminusdb<br/>postgres<br/>redis<br/>elasticsearch<br/>minio<br/>otel-collector |
| `ontology-worker` | - | terminusdb<br/>kafka<br/>redis<br/>message-relay<br/>postgres<br/>otel-collector |
| `otel-collector` | 4317:4317<br/>4318:4318<br/>8889:8889 | jaeger |
| `pipeline-scheduler` | - | kafka<br/>postgres<br/>lakefs<br/>otel-collector |
| `pipeline-worker` | - | kafka<br/>postgres<br/>minio<br/>lakefs<br/>otel-collector |
| `postgres` | ${POSTGRES_PORT_HOST:-5433}:5432 | - |
| `projection-worker` | - | kafka<br/>elasticsearch<br/>redis<br/>message-relay<br/>postgres<br/>otel-collector |
| `prometheus` | 19090:9090 | otel-collector<br/>alertmanager |
| `redis` | ${REDIS_PORT_HOST:-6379}:6379 | - |
| `registry-cleaner` | - | bff<br/>postgres |
| `search-projection-worker` | - | elasticsearch<br/>kafka<br/>otel-collector |
| `terminusdb` | 6363:6363 | - |
| `writeback-materializer-worker` | - | minio<br/>lakefs<br/>otel-collector |
| `zookeeper` | ${ZOOKEEPER_PORT_HOST:-2181}:2181 | - |
<!-- END AUTO-GENERATED ARCH: COMPOSE_INVENTORY -->

### Compose Dependency Graph (docker-compose.full.yml)

<!-- BEGIN AUTO-GENERATED ARCH: COMPOSE_GRAPH -->
```{mermaid}
graph TD
  svc_action_outbox_worker[action-outbox-worker]
  svc_action_worker[action-worker]
  svc_agent[agent]
  svc_alertmanager[alertmanager]
  svc_bff[bff]
  svc_connector_sync_worker[connector-sync-worker]
  svc_connector_trigger_service[connector-trigger-service]
  svc_db_migrations[db-migrations]
  svc_elasticsearch[elasticsearch]
  svc_funnel[funnel]
  svc_grafana[grafana]
  svc_ingest_reconciler_worker[ingest-reconciler-worker]
  svc_instance_worker[instance-worker]
  svc_jaeger[jaeger]
  svc_kafka[kafka]
  svc_kafka_ui[kafka-ui]
  svc_lakefs[lakefs]
  svc_lakefs_init[lakefs-init]
  svc_message_relay[message-relay]
  svc_minio[minio]
  svc_minio_init[minio-init]
  svc_objectify_worker[objectify-worker]
  svc_oms[oms]
  svc_ontology_worker[ontology-worker]
  svc_otel_collector[otel-collector]
  svc_pipeline_scheduler[pipeline-scheduler]
  svc_pipeline_worker[pipeline-worker]
  svc_postgres[postgres]
  svc_projection_worker[projection-worker]
  svc_prometheus[prometheus]
  svc_redis[redis]
  svc_registry_cleaner[registry-cleaner]
  svc_search_projection_worker[search-projection-worker]
  svc_terminusdb[terminusdb]
  svc_writeback_materializer_worker[writeback-materializer-worker]
  svc_zookeeper[zookeeper]
  svc_action_outbox_worker --> svc_kafka
  svc_action_outbox_worker --> svc_postgres
  svc_action_outbox_worker --> svc_minio
  svc_action_outbox_worker --> svc_lakefs
  svc_action_outbox_worker --> svc_otel_collector
  svc_action_worker --> svc_terminusdb
  svc_action_worker --> svc_kafka
  svc_action_worker --> svc_postgres
  svc_action_worker --> svc_minio
  svc_action_worker --> svc_lakefs
  svc_action_worker --> svc_message_relay
  svc_action_worker --> svc_otel_collector
  svc_agent --> svc_bff
  svc_agent --> svc_postgres
  svc_agent --> svc_minio
  svc_agent --> svc_otel_collector
  svc_bff --> svc_db_migrations
  svc_bff --> svc_oms
  svc_bff --> svc_postgres
  svc_bff --> svc_redis
  svc_bff --> svc_elasticsearch
  svc_bff --> svc_kafka
  svc_bff --> svc_minio
  svc_bff --> svc_lakefs
  svc_bff --> svc_funnel
  svc_bff --> svc_otel_collector
  svc_connector_sync_worker --> svc_bff
  svc_connector_sync_worker --> svc_kafka
  svc_connector_sync_worker --> svc_postgres
  svc_connector_sync_worker --> svc_otel_collector
  svc_connector_trigger_service --> svc_kafka
  svc_connector_trigger_service --> svc_postgres
  svc_connector_trigger_service --> svc_otel_collector
  svc_db_migrations --> svc_postgres
  svc_funnel --> svc_otel_collector
  svc_grafana --> svc_prometheus
  svc_ingest_reconciler_worker --> svc_postgres
  svc_ingest_reconciler_worker --> svc_otel_collector
  svc_instance_worker --> svc_terminusdb
  svc_instance_worker --> svc_kafka
  svc_instance_worker --> svc_elasticsearch
  svc_instance_worker --> svc_minio
  svc_instance_worker --> svc_message_relay
  svc_instance_worker --> svc_postgres
  svc_instance_worker --> svc_otel_collector
  svc_kafka --> svc_zookeeper
  svc_kafka_ui --> svc_kafka
  svc_kafka_ui --> svc_zookeeper
  svc_lakefs --> svc_postgres
  svc_lakefs --> svc_minio
  svc_lakefs_init --> svc_lakefs
  svc_lakefs_init --> svc_minio_init
  svc_message_relay --> svc_kafka
  svc_message_relay --> svc_postgres
  svc_message_relay --> svc_minio
  svc_message_relay --> svc_otel_collector
  svc_minio_init --> svc_minio
  svc_objectify_worker --> svc_kafka
  svc_objectify_worker --> svc_postgres
  svc_objectify_worker --> svc_lakefs
  svc_objectify_worker --> svc_oms
  svc_objectify_worker --> svc_otel_collector
  svc_oms --> svc_db_migrations
  svc_oms --> svc_terminusdb
  svc_oms --> svc_postgres
  svc_oms --> svc_redis
  svc_oms --> svc_elasticsearch
  svc_oms --> svc_minio
  svc_oms --> svc_otel_collector
  svc_ontology_worker --> svc_terminusdb
  svc_ontology_worker --> svc_kafka
  svc_ontology_worker --> svc_redis
  svc_ontology_worker --> svc_message_relay
  svc_ontology_worker --> svc_postgres
  svc_ontology_worker --> svc_otel_collector
  svc_otel_collector --> svc_jaeger
  svc_pipeline_scheduler --> svc_kafka
  svc_pipeline_scheduler --> svc_postgres
  svc_pipeline_scheduler --> svc_lakefs
  svc_pipeline_scheduler --> svc_otel_collector
  svc_pipeline_worker --> svc_kafka
  svc_pipeline_worker --> svc_postgres
  svc_pipeline_worker --> svc_minio
  svc_pipeline_worker --> svc_lakefs
  svc_pipeline_worker --> svc_otel_collector
  svc_projection_worker --> svc_kafka
  svc_projection_worker --> svc_elasticsearch
  svc_projection_worker --> svc_redis
  svc_projection_worker --> svc_message_relay
  svc_projection_worker --> svc_postgres
  svc_projection_worker --> svc_otel_collector
  svc_prometheus --> svc_otel_collector
  svc_prometheus --> svc_alertmanager
  svc_registry_cleaner --> svc_bff
  svc_registry_cleaner --> svc_postgres
  svc_search_projection_worker --> svc_elasticsearch
  svc_search_projection_worker --> svc_kafka
  svc_search_projection_worker --> svc_otel_collector
  svc_writeback_materializer_worker --> svc_minio
  svc_writeback_materializer_worker --> svc_lakefs
  svc_writeback_materializer_worker --> svc_otel_collector
```
<!-- END AUTO-GENERATED ARCH: COMPOSE_GRAPH -->

### Service Entry Points (backend/*/main.py)

<!-- BEGIN AUTO-GENERATED ARCH: ENTRYPOINTS -->
- `backend/action_outbox_worker/main.py`
- `backend/action_worker/main.py`
- `backend/agent/main.py`
- `backend/bff/main.py`
- `backend/connector_sync_worker/main.py`
- `backend/connector_trigger_service/main.py`
- `backend/funnel/main.py`
- `backend/ingest_reconciler_worker/main.py`
- `backend/instance_worker/main.py`
- `backend/message_relay/main.py`
- `backend/objectify_worker/main.py`
- `backend/oms/main.py`
- `backend/ontology_worker/main.py`
- `backend/pipeline_scheduler/main.py`
- `backend/pipeline_worker/main.py`
- `backend/projection_worker/main.py`
- `backend/writeback_materializer_worker/main.py`
<!-- END AUTO-GENERATED ARCH: ENTRYPOINTS -->

### Router Inventory (BFF)

<!-- BEGIN AUTO-GENERATED ARCH: BFF_ROUTERS -->
| Router | Prefix | Tags |
| --- | --- | --- |
| `actions.router` | `/api/v1` | - |
| `admin.router` | `/api/v1` | - |
| `agent_proxy.router` | `/api/v1` | - |
| `ai.router` | `/api/v1` | - |
| `audit.router` | `/api/v1` | - |
| `ci_webhooks.router` | `/api/v1` | - |
| `command_status.router` | `/api/v1` | - |
| `config_monitoring.router` | `/api/v1/config` | - |
| `context7.router` | `/api/v1` | - |
| `context_tools.router` | `/api/v1` | - |
| `data_connector.router` | `/api/v1` | - |
| `database.router` | `/api/v1` | - |
| `document_bundles.router` | `/api/v1` | - |
| `governance.router` | `/api/v1` | - |
| `graph.router` | `router-defined` | - |
| `health.router` | `/api/v1` | - |
| `instance_async.router` | `/api/v1` | - |
| `instances.router` | `/api/v1` | - |
| `lineage.router` | `/api/v1` | - |
| `link_types.router` | `/api/v1` | - |
| `mapping.router` | `/api/v1` | - |
| `merge_conflict.router` | `/api/v1` | - |
| `monitoring.router` | `/api/v1/monitoring` | - |
| `object_types.router` | `/api/v1` | - |
| `objectify.router` | `/api/v1` | - |
| `ontology.router` | `/api/v1` | - |
| `ontology_agent.router` | `/api/v1` | - |
| `ontology_extensions.router` | `/api/v1` | - |
| `ops.router` | `/api/v1` | - |
| `pipeline.router` | `/api/v1` | - |
| `pipeline_plans.router` | `/api/v1` | - |
| `query.router` | `/api/v1` | - |
| `schema_changes.router` | `/api/v1` | - |
| `summary.router` | `/api/v1` | - |
| `tasks.router` | `/api/v1` | - |
| `websocket.router` | `/api/v1` | - |
<!-- END AUTO-GENERATED ARCH: BFF_ROUTERS -->

### Router Inventory (OMS)

<!-- BEGIN AUTO-GENERATED ARCH: OMS_ROUTERS -->
| Router | Prefix | Tags |
| --- | --- | --- |
| `action_async.router` | `/api/v1` | async-actions |
| `branch.router` | `/api/v1` | branch |
| `command_status.router` | `/api/v1` | command-status |
| `config_monitoring.router` | `/api/v1/config` | config-monitoring |
| `database.router` | `/api/v1` | database |
| `instance.router` | `/api/v1` | instance |
| `instance_async.router` | `/api/v1` | async-instance |
| `monitoring.router` | `/api/v1/monitoring` | monitoring |
| `ontology.router` | `/api/v1` | ontology |
| `ontology_extensions.router` | `/api/v1` | ontology |
| `pull_request.router` | `/api/v1` | pull-requests |
| `query.router` | `/api/v1` | query |
| `version.router` | `/api/v1` | version |
<!-- END AUTO-GENERATED ARCH: OMS_ROUTERS -->

### Router Inventory (Funnel)

<!-- BEGIN AUTO-GENERATED ARCH: FUNNEL_ROUTERS -->
| Router | Prefix | Tags |
| --- | --- | --- |
| `type_inference_router` | `/api/v1` | - |
<!-- END AUTO-GENERATED ARCH: FUNNEL_ROUTERS -->

<!-- DOC_SYNC: 2026-02-13 Foundry pipeline parity + runtime consistency sweep -->
