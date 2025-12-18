# Data Lineage (Provenance) — First-Class Graph

SPICE-Harvester는 provenance(출처/계보)를 “로그 한 줄”이 아니라 **그래프로 탐색 가능한 1급 데이터**로 취급합니다.  
목표는 다음 3가지를 동시에 만족하는 것입니다.

1. **디버깅/영향 분석(impact analysis)**: “이 결과(ES 문서/Terminus 노드/S3 오브젝트)는 어떤 이벤트에서 왔나?”
2. **운영(rollback plan)**: “이 이벤트/트랜잭션을 되돌리려면 어떤 하류 아티팩트를 조치해야 하나?”
3. **엔티티 탐색 연결**: Ontology/Instance 같은 엔티티 변경이 **어떤 저장소 아티팩트에 반영되었는지** 추적

---

## 1) 저장 구조 (Postgres)

Lineage는 Postgres에 저장됩니다.

- 스키마: `spice_lineage`
- 테이블:
  - `lineage_nodes`
  - `lineage_edges`
  - `lineage_backfill_queue` (누락 복구용)

구현: `backend/shared/services/lineage_store.py`

---

## 2) 그래프 모델

### Node ID 규칙 (안정적인 문자열 ID)

- `event:<event_id>`
- `agg:<aggregate_type>:<aggregate_id>`
- `artifact:<kind>:<...>`

`artifact` 노드는 실제 저장/검색/프로젝션 결과물(ES 문서, S3 오브젝트, Terminus 문서 등)을 표현합니다.

### 대표 Edge Type

아래 edge type들은 “도메인(업무) 특화”가 아니라 “플랫폼/파이프라인” 관점의 중립적인 의미를 가집니다.

- `aggregate_emitted_event`: aggregate → event
- `command_caused_domain_event`: command event → domain event
- `event_stored_in_object_store`: domain event → S3/MinIO event-store object
- `event_wrote_s3_object`: event → S3 object (snapshot/tombstone 등)
- `event_wrote_terminus_document`: event → Terminus artifact (문서/스키마)
- `event_deleted_terminus_document`: event → Terminus artifact (삭제)
- `event_wrote_es_document`: event → ES document (write-side 직접 인덱싱)
- `event_materialized_es_document`: event → ES document (projection materialization)
- `event_deleted_es_document`: event → ES document (삭제)

---

## 3) API (BFF)

프론트는 BFF만 호출하도록, lineage 조회 API는 BFF에 노출됩니다.

- `GET /api/v1/lineage/graph`
  - Query: `root`, `direction`, `max_depth`, `max_nodes`, `max_edges`
  - 반환: nodes/edges/warnings 포함한 그래프
- `GET /api/v1/lineage/impact`
  - Query: `root`, `direction`, `max_depth`, `artifact_kind` …
  - 반환: 하류 `artifact` 노드 목록(운영 도구/롤백 플래너용)
- `GET /api/v1/lineage/metrics`
  - Query: `db_name`, `window_minutes`
  - 반환: `lineage_lag_seconds`, `missing_lineage_ratio_estimate` 등 운영 지표

구현: `backend/bff/routers/lineage.py`

---

## 4) 운영 사용 예시 (Impact / Rollback Plan)

1) 어떤 이벤트(또는 command_id)를 root로 `GET /lineage/impact` 호출  
2) 결과의 `artifacts[]`를 대상으로:
- ES 문서: reindex/delete 계획 수립
- Terminus 문서: reset/reapply 계획 수립
- S3 오브젝트: 원본 재생성/검증 계획 수립

“실제 롤백 실행기(Executor)”는 아티팩트 종류별로 동작이 달라 별도 모듈로 분리하는 것이 안전합니다.  
현재는 **롤백 계획(식별)** 을 lineage graph로부터 안정적으로 뽑아낼 수 있도록 설계되어 있습니다.

---

## 5) 원자성(Atomicity) & 복구(Backfill)

Lineage는 `fail-open`(서비스는 계속 동작)일 수 있습니다.  
대신 “나중에 반드시 복구된다”를 보장하기 위해 다음 2가지 경로를 제공합니다.

1) **Queue 기반 복구**
- EventStore가 lineage 기록에 실패하면 `lineage_backfill_queue`에 enqueue(가능한 경우)합니다.
- 별도 잡/스크립트가 큐를 소비하여 재기록합니다.

2) **Event Store 스캔 기반 복구 (최종 안전망)**
- Event Store(S3/MinIO)를 기간 단위로 스캔하여 lineage를 재생성할 수 있습니다.
- 스크립트: `backend/scripts/backfill_lineage.py`

---

## 6) 멱등성(Idempotency)

현실에서 이벤트 처리는 항상 재시도됩니다.  
그래서 `lineage_edges`는 DB 레벨에서 유니크를 보장합니다.

- 유니크 키: `(from_node_id, to_node_id, edge_type, projection_name)`
- 중복 기록은 `ON CONFLICT ... DO NOTHING`로 무시되어 그래프가 오염되지 않습니다.

---

## 7) 시간/버전 메타데이터

운영 질문(언제/어떤 버전) 답변을 위해 최소 메타를 남깁니다.

- `occurred_at`: “이 이벤트(원인)가 발생한 시간”
- `recorded_at`: “라인리지를 기록한 시간(지연/백필 시점 포함)”
- `run_id`, `code_sha`, `schema_version`: 파이프라인/코드/스키마 컨텍스트(가능한 경우)

---

## 5) 런타임 옵션

- `ENABLE_LINEAGE=true|false`  
  - 기본값 `true`
  - 비활성화 시, 이벤트 처리/저장은 계속되지만 lineage 기록은 생략됩니다(Fail-open).
