# DevOps 운영 리스크/코스트 최소화 보고서 (MSA) — SPICE-Harvester

> 작성일: 2025-12-18  
> 관점: Senior DevOps / SRE  
> 범위: Backend MSA 전체(OMS/BFF/Funnel/Workers) + Infra(Postgres/Redis/Kafka/MinIO/Elasticsearch/TerminusDB)  
> 전제: 현재 레포의 `docker-compose.*`는 “단일 노드 개발/스테이징형” 구성으로 보이며, 프로덕션은 별도 HA 구성이 필요합니다.

---

## 0) Executive Summary (요약)

SPICE-Harvester는 **Event Sourcing(SSoT: S3/MinIO) + CQRS(Projection to ES) + Graph(TerminusDB)** 구조를 갖습니다.  
운영 리스크와 코스트를 낮추기 위해서는 “기능 개발”보다 아래 4가지가 우선입니다.

1) **단일 장애점(SPOF) 제거**: Kafka/ES/Postgres/MinIO/Terminus 단일 노드 → HA 또는 Managed로 전환  
2) **관측(Observability) 강화**: 장애가 “발견/원인분리/복구” 가능한 시스템으로 만들기(지표+로그+트레이싱+알람)  
3) **데이터 수명/보관 정책(ILM/retention) 도입**: ES/MinIO/Kafka/Logs/Traces의 폭증을 비용/장애로 번지기 전에 제어  
4) **운영 표준화(배포/롤백/런북/DR)**: 배포 실패, 데이터 복구, 재처리(rebuild/replay)를 절차화

최근 추가된 **Lineage/Audit(1급 시민)** 은 “운영 도구”의 핵심 기반입니다.  
다만 이 기반 위에 **백필(backfill) 잡의 상시 운영**, **지표/알람**, **권한/테넌트 스코핑**이 붙어야 “장애 대응 비용”이 내려갑니다.

---

## 1) 시스템 구성 요약 (MSA + Dataflow)

### 서비스/인프라 구성 (docker-compose 기준)

- Core API
  - **BFF**: 프론트 진입점 (HTTP)
  - **OMS**: Ontology/Command/Event 처리 (HTTP)
  - **Funnel**: 구조 분석/타입 추론 (HTTP)
- Async Workers
  - **message-relay**: Event Store → Kafka publish(추정)
  - **ontology-worker**: Ontology 명령 처리 → Terminus write → domain event append
  - **instance-worker**: Instance 명령 처리 → Terminus/ES/S3 side-effect → domain event append
  - **projection-worker**: domain event → Elasticsearch materialize
- Infra
  - **Postgres**: idempotency/sequence allocator + lineage/audit store 등
  - **Redis**: 캐시/command status (선택적)
  - **Kafka/Zookeeper**: 비동기 이벤트 버스
  - **MinIO(S3)**: Event Store(SSoT) + instance snapshot/tombstone
  - **Elasticsearch**: 검색/프로젝션 저장소(핵심 read model)
  - **TerminusDB**: 그래프 관계 저장소

### 핵심 데이터 흐름 (요약)

1) 사용자 요청(BFF) → OMS(명령 등록/상태)  
2) command event(Envelope) → Kafka  
3) worker(ontology/instance)가 command 소비 → side-effect 수행(Trinus/ES/S3) → domain event 생성  
4) domain event는 Event Store(SSoT)에 append → message-relay가 Kafka publish  
5) projection-worker가 domain event를 소비 → ES read model 생성/업데이트  
6) Lineage/Audit가 각 단계의 “원인/결과/운영 기록”을 Postgres에 저장

---

## 2) 운영 리스크 Top 12 (리스크/비용 관점)

> 각 항목은 “발생 확률 × 영향도 × 복구 비용” 관점으로 정리했습니다.

### R1. 단일 장애점(SPOF) 다수 (가장 큰 리스크)

- Kafka(단일 broker), Postgres(단일), ES(단일), MinIO(단일), TerminusDB(단일)  
- 장애 시 영향: 전체 write/read 경로 중단, 복구 시 재처리 필요, 인력 투입 급증

**권장**
- 프로덕션은 최소:
  - Postgres: primary/replica(+자동 failover) or Managed(RDS/CloudSQL)
  - Kafka: 3 brokers(+KRaft 또는 ZK) 또는 Managed(MSK/Confluent)
  - ES: 최소 3 nodes(또는 OpenSearch Managed)
  - MinIO: distributed(EC) 또는 S3 Managed
  - TerminusDB: 백업 + standby/restore 전략(실사용/로드 고려해 대체 검토)

### R2. “저장은 됐는데 파생 데이터 누락” (eventual consistency) 관리 비용

- EventStore(SSoT)는 성공했지만, projection/graph/index가 지연/누락될 수 있음  
→ 고객은 “데이터가 들어갔는데 안 보여요” 형태로 인지

**권장**
- “재구축이 정답”인 구조이므로:
  - Projection rebuild/replay를 **정식 운영 기능**으로 제공(잡/런북/알람 포함)
  - consumer lag, DLQ, 처리율(thruput) 지표 필수

### R3. 재시도(At-least-once)로 인한 중복/오염

- Kafka/worker는 재시도/중복처리를 피할 수 없음
- 해결책은 “멱등성”을 전 레이어에서 강제해야 함

**현 상태(강점)**
- `processed_event_registry`로 idempotency/ordering 가드가 존재
- lineage_edges는 DB unique로 중복 오염 방지

**남은 권장**
- 모든 side-effect(ES/Terminus/S3)의 멱등성 계약을 문서화 + 테스트(chaos/idempotency) 확장

### R4. ES 비용 폭증(저장/인덱싱/refresh) + 장애 유발

- ES는 가장 흔한 “코스트+장애” 원인
- 위험 신호:
  - 모든 write에서 `refresh=True`를 사용하면 IO/CPU 비용이 증가하고 성능이 떨어질 수 있음
  - 대량 인입 시 bulk/refresh 전략 부재는 “인덱스/노드 불안정”으로 이어짐

**권장**
- 운영 기준:
  - ingestion 경로는 `refresh=false`(기본) + batch/interval refresh
  - bulk indexing 표준화
  - ILM(보관/rollover/삭제) 필수

### R5. Event Store(MinIO/S3) 용량/보관 정책 부재

- event log는 구조상 계속 커짐
- versioning/retention이 없으면 비용과 복구 시간이 증가

**권장**
- 버킷 단위 수명주기 정책:
  - 오래된 raw event는 cold tier로 이동(또는 압축)
  - 인덱스 오브젝트(derived) 재생성 가능하면 짧게 가져가기

### R6. TerminusDB 운영 난이도/백업/복구 리스크

- Graph DB는 백업/복구가 까다롭고, CPU/IO 패턴이 ES/Postgres와 다름
- 장애 시 “특정 쿼리/데이터 패턴”이 성능을 망가뜨릴 수 있음

**권장**
- Terminus는 “경량 그래프” 원칙 유지(이미 코드가 지향)
- 백업 전략을 문서/자동화(스냅샷 + 복구 연습)
- 장기적으로 대체(Managed graph / RDF store / Postgres graph extension 등) 검토

### R7. 보안/권한: Lineage/Audit/Graph는 “연결 그래프”라 유출 임팩트가 큼

- lineage는 모든 아티팩트를 연결하므로, 권한이 약하면 “데이터 유출 그래프”가 됨

**권장**
- 최소:
  - 모든 lineage/audit 조회는 `db_name/tenant_id` 스코핑 + 접근권한 검사 필요
  - PII는 metadata에 넣지 않고 ID/해시/참조만
  - secrets/env 관리(Secrets Manager + rotation)

### R8. 배포/마이그레이션 표준 부재 → 인력/장애 비용 증가

- 스키마가 코드에서 `ensure_schema()`로 늘어나는 패턴은 초기엔 빠르지만,
  - 팀이 커지면 drift/권한/롤백이 어려워지고 운영 사고가 늘어남

**권장**
- 운영 환경은 “명시적 마이그레이션” 도구 도입:
  - SQL migration tool(Alembic/Flyway 등) + CI에서 migration 검증

### R9. 백업/DR(재해복구) 체계 미흡

- “복구 가능한가?”는 기술이 아니라 절차/연습 문제

**권장**
- DR은 문서+자동화+리허설:
  - RPO/RTO 정의
  - 월 1회 restore drill(표준 체크리스트)

### R10. 관측(Observability) 불완전 → 장애 때 원인 분리 비용 급증

- 모니터링 없이 장애가 오면 “로그 grep + 추측”이 되어 인력/시간 비용이 폭증

**권장**
- 서비스별 Golden Signals + Kafka lag + DB pool + ES health + Terminus latency + EventStore errors
- 알람은 “사용자 영향” 기준으로 제한(노이즈 감소)

### R11. 설정/환경 파편화(환경변수 드리프트)

- MSA는 config drift가 곧 장애

**권장**
- 환경변수 표준화 + config schema + startup validation + config diff

### R12. 비용이 “눈덩이처럼” 커지는 지점이 명확

- ES(저장/인덱싱), Kafka retention, MinIO event log, 로그/트레이스 저장이 대표

**권장**
- Retention/ILM/샘플링을 “기본값”으로 운영에 포함

---

## 3) 우선순위 로드맵 (Risk ↓ / Cost ↓ 중심)

> P0=지금 바로(1~2주), P1=다음 스프린트(2~6주), P2=중장기(6주+)

### P0 (즉시) — “장애 대응 비용”을 먼저 낮추기

1. **대시보드/알람 최소 세트**
   - Kafka consumer lag (topic별)
   - EventStore append 실패율
   - Projection 처리율/실패율/DLQ 적재량
   - Postgres connection pool/lock/wait
   - ES cluster health + indexing latency
   - Terminus API latency/5xx
2. **데이터 수명정책(최소)**
   - ES: ILM/rollover/삭제 정책
   - Kafka: retention.hours 설정
   - MinIO: lifecycle(인덱스/파생 오브젝트 중심)
3. **릴리즈 가드**
   - 배포 시 smoke test(핵심 API+Kafka consume+ES index)
   - feature flag로 비용 큰 기능(대형 import/preview)을 제한
4. **lineage/audit 운영화**
   - `lineage_backfill_queue` 소비 잡을 정식 운영(cron/job)
   - `/api/v1/lineage/metrics` 알람 연결(누락률/lag)

### P1 (단기) — “HA + 표준 운영”으로 안정화

1. **Infra HA/Managed 전환**
   - Postgres, Kafka, ES, Object store(S3)
2. **명시적 마이그레이션 체계**
   - schema drift 방지, 롤백/승인 프로세스 포함
3. **권한/테넌트 모델**
   - tenant_id(또는 db_name 기반) 권한 체크를 BFF에 강제
4. **성능/비용 최적화**
   - ES write path에서 refresh 정책/벌크 표준화
   - 큰 payload는 S3에 두고 ES는 검색/필요 필드만 인덱싱(“hot/cold” 분리)

### P2 (중장기) — “운영 자동화 + DR 고도화”

1. **Rebuild/Replay 자동화**
   - “이벤트 범위 → projection rebuild” 자동 실행기 + 안전장치
2. **멀티 리전 DR**
   - S3 cross-region replication + Postgres replica
3. **그래프 저장소 전략 재검토**
   - Terminus 운영비(인력/장애) vs 대체(Managed/단순화) 평가

---

## 4) “필수” 운영 지표/알람 체크리스트 (초안)

### API (BFF/OMS/Funnel)
- `p50/p95/p99 latency`, `5xx rate`, `RPS`
- `request timeout` 발생률
- rate-limit hit/reject

### Kafka
- consumer lag(토픽/파티션별)
- DLQ 누적량
- rebalancing 빈도(consumer group churn)

### Workers
- 처리율(events/sec), 실패율, 재시도율
- `processed_event_registry` lease 실패/스테일 처리량

### Postgres
- connections in-use/max
- lock wait, slow query, disk usage
- WAL/replication lag(HA 시)

### Elasticsearch
- cluster health(yellow/red), disk watermark
- indexing latency, rejection rate

### MinIO/S3 (EventStore)
- put/get error rate
- bucket size growth rate

### TerminusDB
- latency, 5xx, storage growth

### Lineage/Audit (운영 핵심)
- `GET /api/v1/lineage/metrics` 결과:
  - `lineage_lag_seconds` (알람: 예 10분 이상)
  - `missing_lineage_ratio_estimate` (알람: 예 1% 이상 지속)

---

## 5) 권장 “운영 런북” 최소 세트

1. Kafka 장애/지연 시
   - 어떤 토픽이 막혔나 → consumer lag 확인 → DLQ 유무 → 재처리 절차
2. ES 장애 시
   - cluster health 확인 → write 정지(백프레셔) → projection rebuild 절차
3. Postgres 장애 시
   - failover/restore → processed_event_registry 일관성 확인
4. MinIO/S3 장애 시
   - event append 실패율 확인 → 임시 degraded 모드 전환 → 복구 후 replay
5. Terminus 장애 시
   - write side-effect 실패가 event pipeline을 멈추지 않게(fail-open) → 복구 후 재물질화

---

## 6) 레포 기준 “추가 개발이 필요한 것”(정리)

> 아래는 “운영 리스크/코스트 감소”에 직접 연결되는 개발 항목들입니다.

### 반드시 필요한 개발(P0/P1)
- **KPI/알람 연동**: Kafka lag + lineage_lag/missing_ratio + projection failure + DLQ
- **Projection rebuild 도구**: 범위/조건 기반 재구축(운영 승인/안전장치 포함)
- **ES write 최적화**: bulk/refresh/ILM 기본값 확정
- **권한/RBAC**: BFF에서 db/tenant 스코프 접근 제어
- **DB migrations 정식화**: schema 변경의 안전한 배포/롤백

### 있으면 운영비가 크게 내려가는 개발(P2)
- 자동 incident triage(알람 → 원인 후보/최근 배포/라인리지 영향)
- 운영 UI(읽기 전용)로 lineage/audit/command status를 한 화면에
- 멀티리전 DR 자동화

---

## 7) “발행/운영”을 위한 실제 artefacts (현재 레포 기준)

- 운영 매뉴얼: `docs/OPERATIONS.md`
- Observability 문서: `backend/docs/OPENTELEMETRY_OBSERVABILITY.md`
- Data Lineage 문서: `docs/DATA_LINEAGE.md`
- Audit Logs 문서: `docs/AUDIT_LOGS.md`
- Lineage backfill 스크립트: `backend/scripts/backfill_lineage.py`
- Lineage 지표 API(BFF): `GET /api/v1/lineage/metrics`

