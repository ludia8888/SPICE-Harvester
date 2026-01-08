# Audit Logs — First-Class, Tamper-Evident

SPICE-Harvester의 Audit은 단순 “애플리케이션 로그 출력”이 아니라,
**구조화된 레코드(스키마) + 조회 API + 무결성(해시 체인)** 을 갖는 1급 기능입니다.

Audit의 목적:

- **감사(Audit)**: 누가/무엇을/언제/어떤 리소스에/성공·실패했는지
- **운영 디버깅**: 장애 시 “어느 단계에서 실패했는지”를 저장소 단위로 재구성
- **보장(guarantees)**: 레코드가 사후에 조작되었는지(tamper) 탐지 가능

---

## 1) 저장 구조 (Postgres)

- 스키마: `spice_audit`
- 테이블:
  - `audit_logs`: 실제 audit 레코드
  - `audit_chain_heads`: partition별 해시 체인 헤드(정합성/원자성 유지)

구현: `backend/shared/services/audit_log_store.py`

---

## 2) 스키마 개요

### audit_logs (핵심 필드)

- `audit_id` (UUID, PK)
- `partition_key` (TEXT): 체인을 나누는 키 (예: `db:<db_name>`)
- `occurred_at` (TIMESTAMPTZ)
- `actor` (TEXT): 수행 주체(서비스/사용자)
- `action` (TEXT): 수행 동작명 (예: `EVENT_APPENDED`, `PROJECTION_ES_INDEX` …)
- `status` (TEXT): `success` | `failure`
- `resource_type`, `resource_id`
- `event_id`, `command_id`, `trace_id`, `correlation_id`
- `metadata` (JSONB): 상세 컨텍스트 (skip_reason, seq, index/doc_id 등)
- `error` (TEXT): 실패 원인(문자열)
- `prev_hash`, `entry_hash`: tamper-evident 해시 체인

### audit_chain_heads

- `partition_key` (PK)
- `head_audit_id`, `head_hash`, `updated_at`

`append()`는 `SELECT ... FOR UPDATE`로 `partition_key`별 헤드를 잠그고,
체인의 `prev_hash → entry_hash`를 원자적으로 갱신합니다.

---

## 3) 보장(Guarantees)

### ✅ Tamper-evident (조작 탐지 가능)

각 레코드는 `(prev_hash, payload)`로 `entry_hash`를 계산하고,
partition의 head hash를 갱신합니다.  
중간 레코드가 바뀌면 이후 해시가 연쇄적으로 깨지므로 조작을 탐지할 수 있습니다.

### ⚠️ Not tamper-proof (완전 방지 아님)

DB 권한이 있는 공격자는 데이터를 수정할 수 있습니다.  
다만 **수정 흔적을 숨기기 어렵게** 만드는 것이 목적입니다.
필요하면 `entry_hash`를 외부 WORM 저장소로 주기적으로 앵커링하는 방식으로 강화할 수 있습니다.

---

## 4) API (BFF)

프론트는 BFF만 호출하도록 Audit 조회 API는 BFF에 노출됩니다.

- `GET /api/v1/audit/logs`
  - Query: `partition_key`, `action`, `status`, `resource_type`, `resource_id`, `event_id`, `command_id`, `actor`, `since`, `until`, `limit`, `offset`
- `GET /api/v1/audit/chain-head`
  - Query: `partition_key`

구현: `backend/bff/routers/audit.py`

---

## 5) 런타임 옵션

- `ENABLE_AUDIT_LOGS=true|false`
  - 기본값 `true`
  - 비활성화 시, 서비스 동작은 계속되지만 audit 기록은 생략됩니다(Fail-open).
  - 참고: 과거 문서/설정에는 `ENABLE_AUDIT_LOGGING`이 등장할 수 있으나, 실제 코드는 `ENABLE_AUDIT_LOGS`를 읽습니다.

---

## 6) EVENT_APPENDED vs 중복 재시도

EventStore는 멱등성을 지원하므로, 같은 이벤트를 재시도해서 보내도 “새로 저장”되지 않을 수 있습니다.

- `EVENT_APPENDED`: 실제로 Event Store(S3/MinIO)에 **신규 저장**된 경우
- `EVENT_APPEND_SKIPPED_DUPLICATE`: 이미 존재해서 **멱등(no-op)** 으로 처리된 경우
