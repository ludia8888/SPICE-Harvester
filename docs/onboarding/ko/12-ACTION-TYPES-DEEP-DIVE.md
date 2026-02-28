# Action Type 완전 해부 - 데이터를 "읽기만" 하던 플랫폼이 "행동"하게 만드는 방법

> 지금까지 배운 기능들은 대부분 **데이터를 읽고 검색하는** 것이었어요. 하지만 실무에서는 "이 티켓의 상태를 `승인`으로 바꿔줘", "이 주문을 취소하고 환불 처리해줘" 같은 **쓰기 작업**이 반드시 필요합니다. 이걸 책임지는 게 바로 **Action Type**이에요.

---

## Action Type이 뭔가요?

한 마디로: **"데이터를 안전하게 변경하는 사전 정의된 명령서"**

보통 데이터베이스에서 데이터를 바꾸려면 SQL `UPDATE`문을 직접 날리죠. 하지만 이건 위험해요:
- 누가 바꿨는지 모름
- 잘못 바꾸면 롤백이 안 됨
- 어떤 필드를 바꿀 수 있는지 통제 불가

Action Type은 이 문제를 해결합니다:

| 기존 방식 (raw SQL) | Action Type 방식 |
|---|---|
| `UPDATE tickets SET status='approved'` | `approve_ticket` 액션 실행 |
| 누가 바꿨는지 모름 | 실행자(submitted_by) 자동 기록 |
| 실수해도 되돌릴 수 없음 | 모든 변경이 이벤트로 영구 저장 |
| 아무 필드나 수정 가능 | input_schema로 허용 필드만 통과 |
| 권한 체크 없음 | permission_policy로 RBAC 적용 |

---

## 실제 ActionType 정의를 뜯어보자

다음은 우리 코드에서 실제로 동작하는 ActionType 정의예요. "티켓 승인"이라는 업무를 예시로 들게요.

```json
{
  "id": "approve_ticket",
  "label": { "en": "Approve Ticket", "ko": "티켓 승인" },
  "description": { "en": "Approve a support ticket and assign handler" },
  "spec": {
    "input_schema": {
      "fields": [
        { "name": "ticket", "type": "object_ref", "object_type": "SupportTicket", "required": true },
        { "name": "approval_note", "type": "string", "max_length": 500 },
        { "name": "priority", "type": "string", "enum": ["low", "medium", "high", "critical"] },
        { "name": "is_urgent", "type": "boolean" }
      ]
    },
    "permission_policy": {
      "allow": [{ "role": "editor" }],
      "default": "deny"
    },
    "writeback_target": {
      "repo": "spice_action_writeback",
      "branch": "actions_mydb_approve_ticket"
    },
    "conflict_policy": "WRITEBACK_WINS",
    "implementation": {
      "type": "template_v2",
      "targets": [{
        "target": { "from": "input.ticket" },
        "changes": {
          "set": {
            "status": { "$ref": "input.priority" },
            "approved_at": { "$now": true },
            "handler": {
              "$if": {
                "cond": { "$ref": "input.is_urgent" },
                "then": { "$ref": "user.principal_id" },
                "else": { "$call": { "fn": "coalesce", "args": [{ "$ref": "target.handler" }, { "$ref": "user.principal_id" }] } }
              }
            },
            "approval_note": { "$ref": "input.approval_note" }
          }
        }
      }]
    },
    "audit_policy": {
      "input_mode": "FULL",
      "result_mode": "FULL",
      "redact_keys": ["password", "token"]
    }
  }
}
```

무섭게 생겼지만 하나씩 뜯어보면 전혀 어렵지 않아요.

### 한 줄씩 해부하기

| 블록 | 역할 | 비유 |
|------|------|------|
| `id` | 액션의 고유 이름 | 회사 내 업무 코드 (예: `WF-001`) |
| `label` | UI에 표시되는 이름 (다국어 지원) | "티켓 승인" 버튼 텍스트 |
| `input_schema` | 어떤 파라미터를 받는지 정의 | 결재 양식의 입력 칸들 |
| `permission_policy` | 누가 실행할 수 있는지 | "팀장 이상만 결재 가능" |
| `writeback_target` | 변경사항을 어디에 저장하는지 | 결재 문서함 위치 |
| `conflict_policy` | 동시 수정 시 어떻게 처리하는지 | "나중에 결재한 사람이 이김" |
| `implementation` | 실제로 어떤 필드를 어떻게 바꾸는지 | 결재 도장 찍으면 자동으로 바뀌는 것들 |
| `audit_policy` | 감사 로그를 어떻게 남기는지 | 결재 이력 보관 규칙 |

---

## input_schema: 결재 양식 설계하기

`input_schema`는 "이 액션을 실행하려면 어떤 정보를 입력해야 하는지" 정의해요.

### 지원하는 필드 타입 7가지

| 타입 | 설명 | 예시 |
|------|------|------|
| `string` | 문자열 | 메모, 이름, 코멘트 |
| `integer` | 정수 | 수량, 순서 번호 |
| `number` | 소수 포함 숫자 | 금액, 비율 |
| `boolean` | true/false | 긴급 여부, 동의 체크 |
| `object_ref` | 다른 객체 참조 (`class_id` + `instance_id`) | 대상 티켓, 관련 주문 |
| `list` | 배열 | 태그 목록, 첨부파일 ID 리스트 |
| `object` | 중첩 객체 | 주소 정보 (시, 구, 동) |

### 유효성 검사 옵션

```json
{
  "name": "approval_note",
  "type": "string",
  "required": true,
  "min_length": 10,
  "max_length": 500,
  "enum": null
}
```

- **required**: `true`면 필수 입력, 없으면 거부
- **min_length / max_length**: 문자열 길이 범위
- **min / max**: 숫자 범위 (integer, number 타입)
- **enum**: 허용된 값 목록 (예: `["low", "medium", "high"]`)
- **object_type**: object_ref 타입에서 참조할 수 있는 클래스 제한

### 보안 장치

우리 코드의 `action_input_schema.py`를 보면 이런 보안 장치가 기본 내장되어 있어요:

```python
_RESERVED_INTERNAL_KEYS = {
    "base_token", "observed_base", "applied_changes",
    "conflict", "patchset_commit_id", "action_applied_seq",
}
```

- **예약어 차단**: 시스템 내부용 키를 사용자가 입력하면 자동 거부
- **페이로드 크기 제한**: 기본 200KB 초과 시 거부
- **알 수 없는 필드 거부**: `allow_extra_fields: true`를 명시하지 않으면 스키마에 없는 필드는 자동 차단
- **SQL 인젝션/XSS 검사**: `input_sanitizer`가 모든 문자열 값을 검사

---

## implementation: 실제로 뭘 바꾸는지 정의하기

이 블록이 Action Type의 심장이에요. "파라미터를 받으면 → 대상 객체의 어떤 필드를 어떻게 바꿀 것인가"를 선언합니다.

### 구현 타입 3가지

| 타입 | 상태 | 설명 |
|------|------|------|
| `template_v1` | ✅ 지원 | 단순한 `$ref`/`$now` 참조만 가능 |
| `template_v2` | ✅ 지원 | `$if`, `$switch`, `$call` 등 조건/함수 지원 |
| `function_v1` | 🔒 예약됨 | 코드 기반 변환 (P0 단계에서는 아직 비실행) |

### $ref: 값 참조하기

```json
{ "$ref": "input.approval_note" }
```

3가지 출발점(root)을 쓸 수 있어요:

| Root | 의미 | 예시 |
|------|------|------|
| `input.*` | 사용자가 입력한 파라미터 | `input.priority` → 사용자가 선택한 우선순위 |
| `user.*` | 액션을 실행한 사용자 정보 | `user.principal_id` → 실행자 ID |
| `target.*` | 변경 대상 객체의 현재 값 | `target.handler` → 현재 담당자 |

### $now: 현재 시간 찍기

```json
{ "$now": true }
```

ISO 8601 형식의 현재 UTC 시간을 자동으로 삽입해요. `"2026-03-01T09:30:00+00:00"` 같은 값이 들어갑니다.

### $if: 조건부 분기

```json
{
  "$if": {
    "cond": { "$ref": "input.is_urgent" },
    "then": { "$ref": "user.principal_id" },
    "else": { "$ref": "target.handler" }
  }
}
```

"긴급이면 → 실행자 본인이 담당, 아니면 → 기존 담당자 유지"

### $switch: 다중 조건 분기

```json
{
  "$switch": {
    "cases": [
      { "when": { "$eq": [{ "$ref": "input.priority" }, "critical"] }, "then": "red" },
      { "when": { "$eq": [{ "$ref": "input.priority" }, "high"] }, "then": "orange" }
    ],
    "default": "green"
  }
}
```

### $call: 내장 함수 호출

현재 지원하는 함수들:

| 함수 | 설명 | 예시 |
|------|------|------|
| `concat` | 문자열 연결 | `concat("TICKET-", input.id)` → `"TICKET-42"` |
| `coalesce` | 첫 번째 null 아닌 값 반환 | `coalesce(target.handler, user.id)` |
| `upper` / `lower` | 대/소문자 변환 | `upper("hello")` → `"HELLO"` |
| `trim` | 양쪽 공백 제거 | `trim("  hi  ")` → `"hi"` |
| `add` / `sub` / `mul` / `div` | 사칙연산 | `add(target.count, 1)` → 현재값 + 1 |

### 변경 연산 5가지

| 연산 | 설명 | 예시 |
|------|------|------|
| `set` | 필드 값 설정 | `"status": "approved"` |
| `unset` | 필드 삭제 | `["temp_note", "draft_flag"]` |
| `link_add` | 관계 추가 | 티켓에 관련 담당자 연결 |
| `link_remove` | 관계 제거 | 이전 담당자 연결 해제 |
| `delete` | 객체 자체 삭제 | `true` (다른 연산과 혼용 불가) |

---

## 액션이 실행되는 전체 과정

"티켓 승인" 버튼을 누르면 내부에서 어떤 일이 벌어지는지 전체 흐름을 따라가 볼게요.

### 1단계: 클라이언트 → BFF

사용자가 UI에서 "승인" 버튼을 클릭하면 이 API가 호출됩니다:

```
POST /api/v2/ontologies/{ontologyRid}/actions/approve_ticket/apply
```

```json
{
  "parameters": {
    "ticket": { "class_id": "SupportTicket", "instance_id": "TICKET-42" },
    "approval_note": "검토 완료, 승인합니다.",
    "priority": "high",
    "is_urgent": false
  }
}
```

### 2단계: BFF → OMS

BFF(프론트 전용 서버)가 요청을 받아서:
1. 사용자 인증 확인
2. 데이터베이스 이름(ontologyRid) 해석
3. OMS(오케스트레이션 서버)로 전달

### 3단계: OMS에서 제출 처리

OMS의 `action_submit_service.py`가 핵심이에요. 여기서 벌어지는 일:

```
a. ActionTypeDefinition 로드 (온톨로지 리소스에서)
b. writeback_target {repo, branch} 확인
c. input_schema 기반 파라미터 유효성 검사
d. submission_criteria 서버측 게이팅 표현식 평가
e. permission_policy 기반 권한 확인
f. Postgres에 ActionLogRecord 생성 (status: PENDING)
g. ActionCommand를 EventStore에 기록
h. Kafka 토픽으로 커맨드 발행
i. 클라이언트에 action_log_id 반환
```

### 4단계: Action Worker가 실행

Kafka에서 `ActionCommand`를 소비하는 별도 워커가 있어요:

```
a. EventEnvelope에서 ActionCommand 꺼내기
b. ActionTypeDefinition + 구현체 로드
c. lakeFS base 브랜치에서 대상 객체의 현재 상태 읽기
d. 템플릿 엔진으로 patchset(변경사항 묶음) 계산
e. 충돌 감지 (base_token vs observed_base)
f. conflict_policy에 따라 해결
g. lakeFS overlay 브랜치에 patchset 커밋
h. Postgres ActionLog 상태: COMMIT_WRITTEN
i. ActionAppliedEvent를 EventStore에 발행
j. Postgres ActionLog 상태: EVENT_EMITTED → SUCCEEDED
```

### 아키텍처 흐름 한눈에 보기

```
사용자 (UI)
  │
  ▼
┌────────────────────┐
│  BFF (FastAPI)     │ ← POST /actions/approve_ticket/apply
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  OMS               │ ← 유효성 검사, 권한 확인, ActionLog 생성
│  (Orchestration)   │
└────────┬───────────┘
         │ ActionCommand
         ▼
┌────────────────────┐
│  Kafka             │ ← spice-action-commands 토픽
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  Action Worker     │ ← 템플릿 컴파일, patchset 생성
└────────┬───────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌──────────────┐
│ lakeFS │ │ EventStore   │
│(S3/    │ │(Kafka)       │
│MinIO)  │ │              │
│        │ │→ ACTION_     │
│patchset│ │  APPLIED     │
│commit  │ │              │
└────────┘ └──────┬───────┘
                  │
                  ▼
           ┌────────────┐
           │Elasticsearch│ ← overlay 인덱스 업데이트
           └────────────┘
```

---

## ActionLog: 실행 기록이 남는 과정

모든 액션 실행은 Postgres의 `ontology_action_logs` 테이블에 기록돼요.

### 상태 머신

```
PENDING ──→ COMMIT_WRITTEN ──→ EVENT_EMITTED ──→ SUCCEEDED
   │
   └──→ FAILED (커밋 생성 실패 시 즉시 종결)
```

| 상태 | 의미 | 비유 |
|------|------|------|
| `PENDING` | 커맨드 접수됨, 아직 처리 안 함 | 결재 접수함에 올라감 |
| `COMMIT_WRITTEN` | lakeFS에 patchset 커밋 완료 | 결재 도장은 찍었지만 아직 공문 발송 안 함 |
| `EVENT_EMITTED` | ActionAppliedEvent 발행 완료 | 공문 발송 완료, 관련 부서에 통보됨 |
| `SUCCEEDED` | 모든 처리 완료 | 결재 완전 종결 |
| `FAILED` | 처리 중 오류 발생 | 결재 반려 |

### ActionLog에 저장되는 정보

```sql
-- 실제 테이블 구조 (backend/shared/services/registries/action_log_registry.py)
action_log_id UUID          -- 실행 고유 ID
db_name TEXT                -- 어떤 데이터베이스
action_type_id TEXT         -- 어떤 액션 (예: approve_ticket)
input JSONB                 -- 입력 파라미터 (감사 로그)
status TEXT                 -- 현재 상태 (PENDING → SUCCEEDED)
result JSONB                -- 실행 결과 (적용된 변경사항, 충돌 정보 등)
correlation_id TEXT         -- 추적용 ID
submitted_by TEXT           -- 누가 실행했는지
submitted_at TIMESTAMPTZ    -- 언제 제출했는지
finished_at TIMESTAMPTZ     -- 언제 완료됐는지
writeback_commit_id TEXT    -- lakeFS 커밋 ID
action_applied_event_id TEXT -- EventStore 이벤트 ID
```

> 💡 **핵심 포인트**: 모든 액션은 "누가, 언제, 무엇을, 어떻게" 바꿨는지 완벽하게 추적 가능합니다. 금융 감사(audit)나 컴플라이언스에서 이 수준의 기록이 필수예요.

---

## 충돌 감지와 해결

두 사람이 동시에 같은 티켓을 수정하면 어떻게 될까요?

### 충돌 감지 원리

```
1. 액션 제출 시: 대상 객체의 현재 상태를 스냅샷 (base_token)
2. 워커 실행 시: 현재 상태를 다시 읽어서 base_token과 비교
3. 달라졌다면 → 충돌 발생!
```

우리 코드의 `writeback_conflicts.py`에서 실제 감지 로직:

```python
# 필드 수준 충돌 감지
def detect_overlap_fields(*, observed_base, current_base):
    """observed_base 스냅샷과 현재 값이 다른 필드 이름 목록 반환"""

# 관계(link) 수준 충돌 감지
def detect_overlap_links(*, observed_base, current_base, changes):
    """base에서 삭제됐는데 patch가 추가하려는 경우 등 감지"""
```

### 4가지 충돌 정책

| 정책 | 동작 | 사용 시나리오 |
|------|------|---------------|
| `WRITEBACK_WINS` | 액션의 변경이 우선 (기본값) | 상태 변경처럼 "마지막 결정"이 중요한 경우 |
| `BASE_WINS` | 현재 상태가 우선 (액션 무시) | 참고용 업데이트, 충돌 시 무시해도 되는 경우 |
| `FAIL` | 충돌 시 액션 거부 | 금전 거래처럼 정확성이 최우선인 경우 |
| `MANUAL_REVIEW` | 충돌 마커 생성, 사람이 판단 | 복잡한 비즈니스 로직이 필요한 경우 |

---

## 배치(Batch) 실행과 의존성 DAG

한 번에 여러 액션을 실행할 수도 있어요.

### 단건 vs 배치

```
# 단건 실행
POST /actions/approve_ticket/apply
→ 1개 액션 실행, 1개 결과

# 배치 실행
POST /actions/approve_ticket/applyBatch
→ 최대 20개(BFF) / 500개(OMS) 액션 동시 실행
```

### 의존성 체인

배치 안에서 "A가 성공하면 → B를 실행" 같은 순서를 정할 수 있어요:

```json
{
  "items": [
    { "request_id": "step1", "input": { "ticket": "..." } },
    {
      "request_id": "step2",
      "input": { "ticket": "..." },
      "depends_on": ["step1"],
      "trigger_on": "SUCCEEDED"
    }
  ]
}
```

의존성 관계는 `ontology_action_dependencies` 테이블에 저장됩니다:

| 필드 | 설명 |
|------|------|
| `child_action_log_id` | 실행 대기 중인 액션 |
| `parent_action_log_id` | 선행 조건 액션 |
| `trigger_on` | `SUCCEEDED` / `FAILED` / `COMPLETED` |

> `COMPLETED` = 성공이든 실패든 상관없이, 부모가 끝나면 자식 실행

---

## 시뮬레이션 모드

실제로 변경하지 않고 "이 액션을 실행하면 어떻게 될까?" 미리 확인할 수 있어요.

```json
{
  "options": { "mode": "VALIDATE_ONLY" },
  "parameters": {
    "ticket": { "class_id": "SupportTicket", "instance_id": "TICKET-42" }
  }
}
```

또는 전용 시뮬레이션 엔드포인트:

```
POST /api/v2/ontologies/{ontology}/actions/{action}/simulate
```

응답:
```json
{
  "validation": { "valid": true },
  "effects": {
    "changes": [
      {
        "class_id": "SupportTicket",
        "instance_id": "TICKET-42",
        "set": { "status": "approved", "handler": "user-123" }
      }
    ],
    "conflicts": []
  }
}
```

> 💡 **실무 팁**: 중요한 배치 작업 전에 꼭 시뮬레이션으로 변경 범위를 확인하세요. 특히 `delete: true`가 포함된 액션은 반드시!

---

## 권한과 보안

### 두 가지 권한 모델

| 모델 | 설명 | 적합한 경우 |
|------|------|-------------|
| `ontology_roles` (기본) | 액션 정의에 RBAC 정책 직접 포함 | 일반적인 업무 액션 |
| `datasource_derived` | 데이터셋 접근 권한에서 자동 파생 | 데이터셋 기반 쓰기 제어가 필요한 경우 |

### RBAC 정책 예시

```json
{
  "permission_policy": {
    "allow": [
      { "role": "editor" },
      { "principal": { "type": "user", "id": "admin-001" }, "role": "owner" }
    ],
    "deny": [
      { "role": "viewer" }
    ],
    "default": "deny"
  }
}
```

### 감사(Audit) 정책

민감한 데이터를 다루는 액션은 감사 정책을 세밀하게 설정할 수 있어요:

```json
{
  "audit_policy": {
    "input_mode": "FULL",
    "result_mode": "FULL",
    "max_input_bytes": 20000,
    "max_result_bytes": 200000,
    "redact_keys": ["password", "ssn", "credit_card"],
    "redact_value": "REDACTED"
  }
}
```

- **FULL**: 모든 입출력을 로그에 저장
- **NONE**: 저장하지 않음
- **redact_keys**: 지정한 키의 값은 `"REDACTED"`로 마스킹

---

## 이벤트 소싱과의 연결

액션 시스템은 우리 플랫폼의 이벤트 소싱 아키텍처와 깊이 연결되어 있어요.

### Command와 Event

| 구분 | 클래스 | 설명 |
|------|--------|------|
| **Command** (의도) | `ActionCommand` | "이 액션을 실행해 주세요" |
| **Event** (사실) | `ActionAppliedEvent` | "이 액션이 실행되었습니다" |

우리 코드에서의 정의:

```python
# commands.py - 의도(Intent)
class CommandType(str, Enum):
    EXECUTE_ACTION = "EXECUTE_ACTION"  # ← 액션 전용 커맨드 타입

# events.py - 사실(Fact)
class EventType(str, Enum):
    ACTION_APPLIED = "ACTION_APPLIED"  # ← 액션 전용 이벤트 타입
```

### 왜 Command/Event를 분리할까?

```
"티켓 승인해줘" (Command)  →  처리  →  "티켓이 승인되었다" (Event)
     의도(Intent)                           사실(Fact)
```

- **Command는 거부될 수 있어요**: 권한 없음, 유효성 실패 등
- **Event는 이미 일어난 사실이에요**: 삭제/수정 불가, 영구 보존
- 이 분리 덕분에 감사 추적, 시간여행 쿼리, 재생(replay)이 가능합니다

---

## 🔮 아직 구현 안 됐지만, 이런 것들이 가능해질 거예요

> ⚠️ **주의**: 이 섹션의 내용은 **현재 코드에 구현되어 있지 않습니다**. 아키텍처 설계상 확장 가능한 방향을 설명합니다. 실제 구현 시 설계가 변경될 수 있어요.

현재 Action Type은 **플랫폼 내부에서** 데이터를 변경하는 데 집중하고 있어요. 하지만 이 아키텍처가 외부 서비스와 연동된다면 훨씬 강력한 것들이 가능해집니다.

### 외부 연동이 가능해지는 확장 포인트들

현재 코드에 이미 확장을 위한 씨앗이 심어져 있어요:

```python
# ontology_resources.py - ActionTypeDefinition
side_effects: List[str] = Field(default_factory=list)  # ← 아직 사용 안 됨
implementation: Dict[str, Any] = Field(...)             # ← type: "function_v1"은 예약만 되어 있음
```

- `side_effects` 필드: `["send_email", "notify_webhook"]` 같은 값을 선언할 수는 있지만, 현재 런타임에서 실행하지는 않음
- `function_v1` 구현 타입: 정의와 검증은 되지만 실제 실행은 아직 비활성 (코드에 `"implementation.type=function_v1 is not executable in P0"`라고 명시됨)

### Webhook 연동이 된다면

**시나리오: 주문 승인 → 자동 배송 요청**

```
1. 사용자가 "주문 승인" 액션 실행
2. 플랫폼이 주문 상태를 "approved"로 변경 (현재 구현됨 ✅)
3. ActionAppliedEvent 발행 (현재 구현됨 ✅)
4. Webhook이 Event를 수신하여 외부 배송사 API 호출 (미구현 ❌)
5. 배송사 응답을 Action Writeback으로 플랫폼에 기록 (미구현 ❌)
```

가능해지는 것들:

| 비즈니스 시나리오 | 액션 → 외부 호출 | 외부 → Writeback |
|---|---|---|
| 주문 승인 → 배송 요청 | `approve_order` → 배송사 API | 운송장 번호 자동 기록 |
| 인보이스 발행 → 결제 요청 | `issue_invoice` → PG사 API | 결제 결과 자동 업데이트 |
| 직원 퇴사 → 계정 비활성화 | `deactivate_employee` → AD/LDAP | 비활성화 확인 기록 |
| 재고 부족 감지 → 자동 발주 | `reorder_stock` → ERP API | 발주 번호/예정일 기록 |

### MCP(Model Context Protocol) 연동이 된다면

**시나리오: AI 에이전트가 데이터를 분석하고 액션을 제안**

```
1. AI 에이전트가 MCP Tool로 "이상 거래" 검색
2. 검색 결과를 분석하여 "이 3건은 정지해야 합니다" 판단
3. MCP Tool로 suspend_account 액션 시뮬레이션 (simulate)
4. 사람이 리뷰 후 승인
5. MCP Tool로 실제 액션 실행 (apply)
6. ActionAppliedEvent로 감사 추적
```

MCP 연동의 핵심 가치:
- **AI가 읽기뿐 아니라 쓰기까지 수행** 가능 (당연히 사람의 승인 하에)
- 현재 시뮬레이션 모드(`VALIDATE_ONLY`)가 AI-in-the-loop에 완벽히 적합
- 모든 실행이 ActionLog에 기록되므로 "AI가 뭘 했는지" 완전 추적 가능

### Polling 기반 연동이 된다면

**시나리오: 외부 시스템의 변경사항을 주기적으로 반영**

```
1. Polling 워커가 외부 CRM API를 5분마다 체크
2. 변경된 고객 정보 발견
3. update_customer 액션을 배치로 제출 (applyBatch, 최대 500건)
4. 충돌 감지 → conflict_policy에 따라 해결
5. 변경사항이 lakeFS + EventStore에 영구 기록
```

### 왜 이 확장들이 "자연스러운" 걸까?

현재 아키텍처가 이미 이 확장들을 위한 기반을 갖추고 있기 때문이에요:

| 필요한 것 | 이미 있는 것 |
|---|---|
| 외부 호출 후 결과 기록 | Writeback + ActionLog |
| 실행 전 사전 검증 | VALIDATE_ONLY 시뮬레이션 모드 |
| AI 에이전트 통합 | 잘 정의된 REST API + input_schema |
| 대량 처리 | applyBatch (최대 500건) + 의존성 DAG |
| 감사 추적 | EventStore + audit_policy |
| 동시 수정 보호 | base_token + conflict_policy |

> 💡 **신규 개발자에게**: 외부 연동을 구현하게 된다면, 기존 `ActionCommand → Kafka → Action Worker` 파이프라인을 그대로 활용하되 Worker 내부에서 외부 API 호출을 추가하는 방향이 자연스러워요. `side_effects` 필드와 `function_v1` 구현 타입이 이 확장의 진입점이 될 가능성이 높습니다.

---

## 실무에서 확인해 볼 것들

### 관련 코드 파일

| 파일 | 역할 |
|------|------|
| `backend/shared/models/ontology_resources.py` | ActionTypeDefinition 모델 |
| `backend/shared/utils/action_input_schema.py` | 입력 유효성 검사 |
| `backend/shared/utils/action_template_engine.py` | 템플릿 엔진 ($ref, $if, $call) |
| `backend/shared/utils/writeback_conflicts.py` | 충돌 감지 & 해결 |
| `backend/shared/utils/action_audit_policy.py` | 감사 정책 |
| `backend/shared/utils/action_permission_profile.py` | 권한 모델 |
| `backend/shared/services/registries/action_log_registry.py` | ActionLog 저장소 |
| `backend/shared/models/commands.py` | ActionCommand 모델 |
| `backend/shared/models/events.py` | ActionAppliedEvent 모델 |
| `backend/bff/routers/foundry_ontology_v2.py` | BFF API 엔드포인트 |
| `backend/oms/routers/action_async.py` | OMS API 엔드포인트 |
| `backend/action_worker/main.py` | 액션 실행 워커 |

### 관련 테스트

```bash
# 입력 스키마 유효성 검사 테스트
pytest backend/tests/unit/utils/test_action_input_schema.py -v

# 감사 정책 테스트
pytest backend/tests/unit/utils/test_action_audit_policy.py -v

# 권한 모델 테스트
pytest backend/tests/unit/utils/test_action_permission_profile.py -v

# 시뮬레이션 테스트
pytest backend/tests/unit/services/test_action_simulation* -v

# E2E 스모크 테스트 (전체 스택 필요)
RUN_LIVE_ACTION_WRITEBACK_SMOKE=true pytest backend/tests/test_action_writeback_e2e_smoke.py -v
```

---

## 한눈에 정리

```
ActionType = 입력 스키마(input_schema)
           + 권한 정책(permission_policy)
           + 변경 템플릿(implementation)
           + 충돌 정책(conflict_policy)
           + 감사 규칙(audit_policy)
           + 저장 위치(writeback_target)

실행 흐름 = 클라이언트 → BFF → OMS → Kafka → Action Worker → lakeFS + EventStore

상태 추적 = PENDING → COMMIT_WRITTEN → EVENT_EMITTED → SUCCEEDED (or FAILED)

보안 3중 장치 = input_schema (무엇을) + permission_policy (누가) + audit_policy (기록)
```

---

## 다음 문서

- [아키텍처 이해하기](05-ARCHITECTURE-EXPLAINED.md) — 전체 서비스 구조
- [데이터 흐름 추적](06-DATA-FLOW-WALKTHROUGH.md) — 요청이 시스템을 관통하는 과정
- [실전 시나리오](11-REAL-WORLD-SCENARIOS.md) — 멀티홉 쿼리, 검색 연산자 등 다른 핵심 기능들
