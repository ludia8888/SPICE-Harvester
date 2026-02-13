# Action Writeback Design (Code-Aligned)

> Updated: 2026-01-08  
> Status: design/target spec (aligned to current code patterns)

## Table of contents
- Decisions (locked)
- Scope and non-goals
- Alignment with current code
- Philosophy alignment gaps (current backend)
- Implementation gaps and build plan
- Migration plan (phased)
- API contract: base_branch / overlay_branch
- Data model
- Write path (Action execution)
- Decision simulation (dry-run)
- Read path (ES overlay)
- Conflict policy (first-class)
- Permissions and submission criteria
- Idempotency and retry
- Observability and audit
- Implementation map (files)
- Configuration

## Decisions (locked)
- Actions are the only supported operational writeback path (including A/Rewrite flows).
- All Action effects are durably stored as lakeFS writeback datasets (commit-addressed).
- Read-your-write is guaranteed through ES projection/overlay, not by mutating base objectify outputs.
- Authoritative writeback state is the ledger/snapshot + queue; ES overlay is a derived cache only.
- Action 1 run = 1 logical apply unit; commit/log/event emission are coordinated via a state machine/outbox.
- Effect application may skip some targets only when conflict_policy explicitly allows it, and every skip must be recorded.
- Permission = can_modify(target_objects) AND can_modify(action_log).
- ActionLog is a first-class **Ontology object** (not “just a DB row”): it is the durable record of decisions, conflicts,
  and outcomes, and is explicitly designed to be queryable for governance and future agent meta-cognition.
- Action definitions are versioned with ontology branches/commits; only deployed commits are executable.
- Conflicts between datasource updates and writeback edits are first-class and policy-driven.
- Clients submit only intent (action_type + input). Patchsets are computed server-side.
- Base datasets (ingestion/objectify outputs) are immutable facts; writeback accumulates decisions in a separate layer.

## P0 Fix List (출시 전 필수)
- Aggregate boundary를 문서로 고정: `base_token` 정의역(객체/row/materialized view)과 생성 함수(결정론)를 고정.
- conflict policy를 field-level로 세분화 또는 `non-overlap auto-merge + overlap FAIL` 정책 도입.
- `lifecycle_id`(epoch) 도입으로 delete/recreate의 물리적 보장.
- Action log 선생성(PENDING) + “시도한 변경”과 “적용된 변경(정책 반영 후)”을 분리 기록.
- snapshot manifest에 재현성 필드(기반 버전/커밋/하이워터마크/정의 해시) 강제.
- ES overlay 문서에 `(instance_id, lifecycle_id, base_token, patchset_commit_id)` 포함.
- `overlay_status=DEGRADED`에서 항상 명시적 상태/오류 반환(절대 silent fallback 금지).
- writeback dataset ACL alignment 검증 실패 시 hard reject.
- Side-effects는 상태와 분리된 이벤트/로그로 처리(상태 변경은 patchset에만).
- 성능: server merge fallback을 위한 per-object edits index/queue + action worker SSoT replay는 스냅샷/캐시 전략을 P0로 포함.

## Scope and non-goals
Scope:
- Define a code-aligned Action execution flow that fits current Event Sourcing + workers architecture.
- Introduce an Action patchset format and writeback storage layout in lakeFS.
- Define ES overlay behavior for read-your-write.
- Define conflict detection + policy resolution as a first-class contract.

Non-goals:
- Full workflow/orchestration engine (multi-step, long-running saga). Actions remain single atomic patchsets.
- Mutating base objectify outputs or backing datasets.
- Rewriting existing ingestion/objectify flows.

## Alignment with current code
This design reuses existing primitives and paths:
- Event Store (S3/MinIO): `backend/shared/services/event_store.py`
- Event envelope: `backend/shared/models/event_envelope.py`
- Idempotency registry: `backend/shared/services/processed_event_registry.py`
- ES projection: `backend/projection_worker/main.py` (overlay indices by branch)
- Branch overlay indices: `backend/shared/config/search_config.py`
- Ontology deploy registry (executable commits): `backend/oms/services/ontology_deployment_registry_v2.py`
- Ontology resources (action_type definitions): `backend/oms/services/ontology_resources.py`
- Resource validation: `backend/oms/services/ontology_resource_validator.py`
- lakeFS client/storage: `backend/shared/services/lakefs_client.py`, `backend/shared/services/lakefs_storage_service.py`
- Writeback branch config: `backend/shared/config/app_config.py`

## Philosophy alignment status (current backend)
The following philosophy-alignment gaps were addressed for P0 Action writeback:

- **Direct CRUD guard (ingest-only)** for writeback-enabled object types when `WRITEBACK_ENFORCE=true` (OMS + instance-worker backstop).
- **Read path overlay integration**: writeback-enabled types avoid silent Terminus-only fallback when overlay is required; degraded behavior is explicit.
- **Branch split**: `base_branch` (Terminus) is separated from `overlay_branch` (ES overlay).
- **Permissions + submission criteria enforcement**: Action worker gates `permission_policy` + `submission_criteria` + `data_access` before any writeback commit.
- **Conflict policy** is implemented with default `FAIL`; conflicts are recorded in ActionLog and surfaced via overlay metadata.

## Implementation gaps and build plan
These are required to realize the design while preserving the philosophy above.

- **Action type schema + validation:** Extend ActionTypeDefinition with required `writeback_target` and
  recommended fields (`submission_criteria`, `conflict_policy`, `validation_rules`, `audit_policy`,
  `implementation`). Enforce required fields and valid policies in the validator.
- **Action submission + events:** Add ActionCommand/ActionApplied models + topics, and a BFF/OMS action
  submission API that resolves the deployed ontology commit and accepts intent-only payloads.
- **Action worker:** Consume ActionCommand, evaluate permissions/criteria, compute patchset, write a
  lakeFS patchset commit, write the action log, emit ActionApplied, append per-object queue entries
  (keyed by `action_applied_seq`), and apply conflict policy with default FAIL.
- **Action log registry + table:** Implement `ontology_action_logs` and a registry service to store
  status, conflicts, and writeback_commit_id (audit SSoT).
- **Writeback datasets + snapshot job:** Implement `writeback_patchsets`, `writeback_edits_queue`, and
  `writeback_merged_snapshot`, plus a materialization job for snapshotting and queue compaction.
- **Read path overlay integration:** Projection worker must merge patchsets into overlay indices, and
  BFF/OMS/graph federation must use overlay branches for writeback-enabled types with explicit degraded
  responses when overlay is unavailable.
- **Governance + ACL alignment:** Enforce writeback datasets in the same project with aligned ACLs;
  reject actions if alignment cannot be verified.

## Migration plan (phased)
단계별로 안전하게 전환하고, 각 단계마다 롤백 가능한 상태를 유지한다.

Phase 0: 준비/가시화
- writeback-enabled object type 식별(메타데이터 플래그) 및 인벤토리 작성.
- 기존 CRUD 경로/엔드포인트/워커 사용 현황 로깅과 대시보드 추가.
- 기능 플래그 도입(`WRITEBACK_ENFORCE`, `WRITEBACK_READ_OVERLAY` 등).

Phase 1: 읽기 경로 사전 배선 (비차단)
- BFF/OMS/Graph API에 `base_branch` + `overlay_branch` 파라미터 추가(아래 계약 준수).
- writeback-enabled 타입에서 overlay 읽기 경로를 **옵트인**으로 제공하고,
  응답에 `overlay_status`를 포함(예: ACTIVE/DEGRADED/DISABLED).
- 이 단계에서는 기존 fallback을 유지하되, overlay 미사용/오류를 명시적으로 노출.

Phase 2: Action 스키마/로그/제출 경로 정비
- ActionType 스키마 확장 및 validator 강화(`writeback_target` 필수).
- `ontology_action_logs` 테이블 + 레지스트리 구현.
- Action submission API 공개(의도만 접수) + dry-run/검증 경로 제공.

Phase 3: Writeback 데이터 플레인 구현 (카나리)
- Action worker + lakeFS writeback datasets + overlay projection 구현.
- conflict_policy 적용(기본 FAIL) 및 conflict 기록.
- side-effects는 post-commit, at-least-once, idempotent로 처리.
- 일부 object type에만 카나리 적용 후 품질/성능 검증.

Phase 4: 강제 전환 (정책 고정)
- writeback-enabled 타입에 대해 CRUD 명령 **차단**(ingest/system만 예외).
- ES 장애 시 Terminus-only fallback 금지, degraded 응답으로 전환.
- 액션 권한/데이터 접근/criteria 결합 평가를 **필수 게이트**로 고정.

Phase 5: 정리/폐기
- legacy CRUD 경로 deprecate 및 제거 계획 수립.
- `branch` 단일 파라미터의 의미를 `base_branch`로 고정하고, overlay는 별도 파라미터로만 유지.

## API contract: base_branch / overlay_branch
읽기 경로에서 **원천(branch)과 overlay(branch)를 명확히 분리**한다.

Request parameters (read APIs):
- `base_branch`: Terminus/SSoT 조회에 사용되는 브랜치. 기본값 `main`.
- `overlay_branch`: ES overlay 조회에 사용되는 브랜치. 기본값은 다음 규칙으로 서버가 결정:
  - writeback-enabled 타입이면 `writeback-{db_name}` (또는 action_type.writeback_target.branch).
  - writeback 비활성 타입이면 `null`(overlay 미사용).
- `branch` (deprecated): 하위호환을 위해 `base_branch`의 별칭으로만 사용한다.

Response fields (read APIs):
- `base_branch`, `overlay_branch`를 명시적으로 반환.
- `overlay_status`: `ACTIVE | DISABLED | DEGRADED`
  - `DEGRADED`는 overlay/merge 불가 또는 ES 장애를 의미하며,
    writeback-enabled 타입에서는 **성공 응답으로 대체하지 않는다**.
- `writeback_enabled`: writeback 대상 타입 여부.
- `writeback_edits_present`: writeback ledger/snapshot 기준으로 편집 존재 여부(가능할 때만 정확).

Behavior:
- ES 조회는 `overlay_branch` 인덱스를 우선한다.
- overlay에 문서가 없고 tombstone도 없으면 base 인덱스로 fallback 가능(정상 상태).
- overlay가 **사용 불가**하더라도 서버 merge 경로가 가능하면 authoritative view를 반환하고
  `overlay_status=DEGRADED`로 표기한다.
- overlay와 서버 merge 경로가 모두 불가할 때만 503 + `overlay_status=DEGRADED`로 응답하며,
  Terminus-only fallback은 금지한다.
- `DEGRADED` 응답에서는 최소한 `writeback_enabled`와 `writeback_edits_present` 메타를 반환해
  "writeback 대상이며 최신 상태 뷰는 제공 불가"를 명시한다.

Graph federation:
- Terminus 쿼리는 `base_branch`를 사용한다.
- ES enrichment는 `overlay_branch`를 사용한다.
- `overlay_branch=writeback-{db_name}`는 Terminus 브랜치로 사용하지 않는다.

## Data model

### Action type (Terminus resource)
Stored as ontology resource `action_type` via `OntologyResourceService`. The `spec` payload is the contract.

Required fields (P0):
- `input_schema` (existing required field)
- `permission_policy` (existing required field)
- `writeback_target` (new required field)
- `implementation` (new required field; defines how intent becomes patchset changes)

`writeback_target.branch` is a **lakeFS branch id** and MUST be lakeFS-compatible (no `/`; only letters/digits/`_`/`-`).
We intentionally use `writeback-{db_name}` (not `writeback/{db_name}`) so the same string can be used for both:
- lakeFS refs (patchsets + queue)
- ES overlay branch identity

Recommended fields:
- `submission_criteria` (boolean expression over user + inputs + target object state)
- `validation_rules` (pre-apply validation, server-side)
- `side_effects` (non-transactional post-commit hooks)
- `conflict_policy` (see below)
- `audit_policy` (log payload size, redaction rules)
- `write_targets` (optional target selectors for preflight/permission planning; execution still derives from implementation)

### ActionType execution contract (P0)

This section fixes the minimum **executable** ActionType contract so that:
- clients submit only intent (inputs),
- servers compute patchsets deterministically and safely,
- audit data stays structured and learnable (ActionLog as ontology object),
- governance/permission gates can run without ambiguity.

This contract is inspired by well-known ActionType mental models used in enterprise ontology platforms
(rules/functions + submission criteria + action logs),
but is strictly constrained to the capabilities of this codebase (lakeFS patchset + ES overlay).

#### 1) `input_schema` (type system + validation)

Canonical schema form (P0):
```json
{
  "fields": [
    { "name": "ticket", "type": "object_ref", "required": true, "object_type": "Ticket" },
    { "name": "comment", "type": "string", "required": false, "max_length": 2000 }
  ],
  "allow_extra_fields": false
}
```

Supported field types (P0):
- `string` (optional `min_length`, `max_length`, `enum`)
- `integer` (optional `min`, `max`)
- `number` (optional `min`, `max`)
- `boolean`
- `object_ref` (a reference to an existing instance)
- `list` (requires `items` schema)
- `object` (requires `properties` schema; discouraged in P0 unless necessary)

`object_ref` value shape (P0):
```json
{ "class_id": "Ticket", "instance_id": "ticket-123" }
```

Validation rules (P0):
- Unknown fields are rejected unless `allow_extra_fields=true`.
- `object_ref.class_id` and `object_ref.instance_id` use the same sanitizers as instance APIs.
- This is input validation only; it does not grant permissions or guarantee base state existence.

Reserved/internal keys (P0):
- Clients MUST NOT send patchset-internal fields (they are server-owned): `base_token`, `observed_base`,
  `applied_changes`, `conflict`, `patchset_commit_id`, `action_applied_seq`.

#### 2) `implementation` (intent → patchset compilation)

P0 implementation kinds:
- `template_v1` (required; declarative patchset template)
- `function_v1` (reserved; not executable until Function runtime exists)

`template_v1` schema (P0):
```json
{
  "type": "template_v1",
  "targets": [
    {
      "target": { "from": "input.ticket" },
      "changes": {
        "set": { "status": "APPROVED", "approved_by": { "$ref": "user.id" } },
        "unset": ["rejected_reason"],
        "link_add": [],
        "link_remove": [],
        "delete": false
      }
    }
  ]
}
```

Target selection (P0):
- `target.from` is a dotted path starting with `input.`.
- The resolved value must be either:
  - a single `object_ref`, or
  - a list of `object_ref` values (bulk).

Value references (P0):
- Any JSON literal is allowed as a value.
- A reference object `{ "$ref": "<path>" }` copies a value from:
  - `user.<field>` (e.g., `user.id`, `user.role`)
  - `input.<field>` (validated input payload)
  - `target.<field>` (current base document for that target)
- `{ "$now": true }` yields an ISO8601 UTC timestamp string captured at execution time.

Composition/merge rules (P0):
- If multiple template entries resolve to the same `(class_id, instance_id)`, they are merged in order:
  - later `set` wins per key
  - `unset` is unioned; `set` overrides `unset` for the same key
  - link ops are concatenated and then de-duplicated at apply time
- Delete is a tombstone operation:
  - If `delete=true` for a target, that target MUST NOT include other field/link edits.
  - If any template entry requests `delete=true` for a target and another requests edits for the same target,
    the action is rejected as invalid.

Out-of-scope (P0):
- Creating brand-new instances via Actions is not supported yet (writeback is edits/tombstones only).

#### 3) `validation_rules` (server-side asserts)

Schema (P0):
```json
[
  { "type": "assert", "scope": "each_target", "expr": "target.status == 'OPEN'", "message": "Ticket must be OPEN" }
]
```

Rules:
- `type=assert` only (P0).
- `scope`:
  - `action`: evaluate once with `targets` list available
  - `each_target`: evaluate for each target with `target` bound to the current base doc
- If a rule evaluates to `false` or cannot be evaluated, the action is rejected before any writeback commit.

#### 4) `submission_criteria` (gating expression language)

Language (P0) is the same safe boolean evaluator used by the worker (`backend/shared/utils/safe_bool_expression.py`):
- Allowed: `and/or/not`, comparisons (`==`, `!=`, `<`, `<=`, `>`, `>=`), `in`, `not in`, `is`, `is not`,
  dict attribute access (`user.id`), constant subscripts (`input['x']`, `targets[0]`), literals, lists/dicts.
- Disallowed: function calls, arithmetic, comprehensions, private identifiers (`_foo`), dynamic subscripts.

Variables available (P0):
- `user`: `{ id, role, is_system }`
- `input`: validated input payload
- `targets`: list of base documents (current state)
- `target`: base document when exactly one target, else `null`
- `db_name`, `base_branch`

Failure policy (P0):
- Syntax error / unsafe expression / evaluation error ⇒ reject (never “best-effort allow”).

#### 5) `audit_policy` (PII redaction + size contracts)

Goal: keep ActionLog queryable + learnable, without leaking sensitive payloads or storing unbounded blobs.

Schema (P0, minimal):
```json
{
  "input_mode": "FULL",
  "result_mode": "FULL",
  "max_input_bytes": 20000,
  "max_result_bytes": 200000,
  "redact_keys": ["password", "ssn", "token"],
  "redact_value": "REDACTED",
  "max_changes": 200
}
```

Rules (P0):
- Redaction is recursive by key name match.
- If a payload exceeds max bytes, store a structured stub: `{ "__truncated__": true, "sha256": "...", "bytes": N }`.
- If `max_changes` is exceeded, store summaries + digests instead of full arrays.

#### 6) ActionLog object schema + linking

P0 exposure:
- ActionLog is a first-class ontology object (backed by Postgres) and is queryable via BFF instance routes.
- It stores both:
  - intent (`input`, `submitted_by`, `submitted_at`, `ontology_commit_id`)
  - decision artifacts (`attempted_changes`, `applied_changes`, `conflicts`, `writeback_commit_id`)
- Principal identity is carried as `(metadata.user_type, submitted_by)`; policy evaluators use `{principal_type}:{principal_id}` + `role:{role}` tags (e.g. `user:alice`, `service:svc-1`, `role:DomainModeler`).

Linking (P0):
- ActionLog includes a stable list of target object refs in `result.attempted_changes[*]` and `result.applied_changes[*]`
  (each entry includes `resource_rid`, `instance_id`, `lifecycle_id`).
- P1 may materialize these as explicit graph edges; P0 keeps them as structured refs to avoid coupling.

#### 7) Permission + governance gates (P0)

Execution requires all of:
- `action_type.permission_policy` allows the actor
- actor can read the target rows (data_access row filters) and is allowed to write (dataset ACL)
- writeback dataset ACL is aligned with the backing dataset ACL (and verifiable)
- `can_modify(action_log)` holds (P0: same principal context as the action execution; no anonymous submissions)

Dataset ACL policy shape (P0, minimal):
```json
{ "effect": "ALLOW", "principals": ["role:DomainModeler", "user:alice"] }
```

Example (payload merged into `spec` by validator):
```json
{
  "id": "approve_ticket",
  "label": "Approve Ticket",
  "spec": {
    "input_schema": { "fields": [{ "name": "ticket", "type": "object_ref" }] },
    "permission_policy": { "effect": "ALLOW", "principals": ["role:DomainModeler"] },
    "writeback_target": { "repo": "ontology-writeback", "branch": "writeback-{db_name}" },
    "submission_criteria": "user.id != ticket.requester",
    "conflict_policy": "WRITEBACK_WINS",
    "implementation": {
      "type": "template_v1",
      "targets": [
        {
          "target": { "from": "input.ticket" },
          "changes": { "set": { "status": "APPROVED", "approved_by": { "$ref": "user.id" } } }
        }
      ]
    }
  }
}
```

### Action patchset (lakeFS)
Each Action execution produces one patchset, stored in lakeFS and addressed by commit id.

Patchset shape (JSON file):
```json
{
  "action_log_id": "uuid",
  "action_type_rid": "action_type:approve_ticket@3",
  "ontology_commit_id": "c-ontology",
  "targets": [
    {
      "resource_rid": "object_type:Ticket@5",
      "instance_id": "ticket-123",
      "lifecycle_id": "lc-001",
      "base_token": {
        "base_dataset_version_id": "ds-2026-01-08T12:00:00Z",
        "object_type_version_id": "object_type:Ticket@5",
        "instance_id": "ticket-123",
        "lifecycle_id": "lc-001",
        "base_state_hash": "sha256:..."
      },
      "observed_base": {
        "fields": { "status": "OPEN", "approved_by": null },
        "links": { "assignees": ["user:bob"] }
      },
      "changes": {
        "set": { "status": "APPROVED", "approved_by": "user:alice" },
        "unset": ["rejected_reason"],
        "link_add": [],
        "link_remove": [],
        "delete": false
      }
    }
  ],
  "metadata": {
    "submitted_by": "alice",
    "submitted_at": "2026-01-08T12:00:00Z"
  }
}
```

Notes:
- `base_token` is a structured, deterministic conflict token derived from the authoritative base doc:
  - `base_token = (base_dataset_version_id, object_type_version_id, instance_id, lifecycle_id, base_state_hash)`
  - `base_state_hash = sha256(canonical_json(authoritative_base_doc))` (canonical JSON must be fixed and versioned)
  - Aggregate boundary must be defined at P0; object state must be derivable from a single authoritative
    object-state source, or via an explicit composite token.
- `observed_base` captures only the fields/links touched by the patchset (from authoritative base state),
  enabling field-level conflict checks.
- ES may cache `base_token`/`__rev` for fast reads but is not authoritative for conflict checks.
- `changes` are field-level operations; projection merges them with the effective doc (overlay if exists, else base).
- Patchsets are append-only ledgers; they are not the only writeback dataset (see below).
- `delete=true` is a first-class tombstone operation (not a field edit).
- Patchsets are derived artifacts; they are never accepted from clients.

### Action log (Postgres)
Action logs are the audit SSoT for writeback decisions.

Governance-aligned exposure:
- ActionLog is also exposed as an Ontology object type (first-class resource) for read/ACL/governance.
- Postgres is the backing store for that object type (or dual-write with verification).
- `can_modify(action_log)` is evaluated against the object type permission model, not just service/table ACLs.

Why this matters (meta-cognition / agents):
- We treat ActionLog as a **domain object** that represents “what we decided and why”, not a transient audit row.
- By keeping ActionLog schema structured and stable (inputs, attempted/applied changes, conflicts, outcomes,
  principal context), future agents can learn decision patterns (“how did we resolve conflicts before?”) and build
  higher-order operational behavior (self-reflection / meta-cognition) on top of the system’s own history.

Safety note: Some enterprise ontology platforms document an **Action Log object type** (audit/decision records as
object data). We align with that documented philosophy, but we do **not** assume or claim any vendor’s internal
storage/implementation details beyond what’s publicly documented.

Proposed table (aligns with `docs/ontology_resource_design.md`):
```
ontology_action_logs(
  action_log_id uuid PK,
  action_type_rid text,
  resource_rid text,
  ontology_commit_id text,
  input jsonb,
  status text,
  result jsonb,
  correlation_id text,
  submitted_by text,
  submitted_at timestamptz,
  finished_at timestamptz,
  writeback_target jsonb,
  writeback_commit_id text,
  metadata jsonb
)
```

Result structure (P0):
- Separate "attempted changes" (computed patchset intent) from "applied changes" (post-policy).
- Record field-level conflicts and any skipped targets/fields with reasons.

Status is a state machine to tolerate partial failures:
- `PENDING` -> `COMMIT_WRITTEN` -> `EVENT_EMITTED` -> `SUCCEEDED`
- `FAILED` is terminal if commit creation fails.
- `COMMIT_WRITTEN` means the writeback commit exists but the ActionApplied event may not be emitted yet;
  a reconciler/outbox MUST emit it and advance the status.
- `EVENT_EMITTED` means ActionApplied is durable but per-object queue indexing may not be complete yet;
  a reconciler MUST append queue entries and advance to `SUCCEEDED`.

### Writeback datasets (lakeFS)
- Repository: `AppConfig.ONTOLOGY_WRITEBACK_REPO` (default `ontology-writeback`)
- Branch: `AppConfig.get_ontology_writeback_branch(db_name)` (default `writeback-{db_name}`)
- Writeback is both a decision ledger (patchsets/action logs) and a materialized latest-state view.
- Datasets / paths:
  - **writeback_edits_queue** (append-only)
    - `queue/by_object/{object_type}/{instance_id}/{lifecycle_id}/{action_applied_seq}_{action_log_id}.json`
      (per-object addressed queue/index)
    - payload includes `action_log_id`, `patchset_commit_id`, `action_applied_seq`, `resource_rid`,
      `instance_id`, `lifecycle_id`, `base_token`, `submitted_at`
    - `action_applied_seq` is the Event Store `sequence_number` for ActionApplied (queue offset / high-watermark).
    - Queue entries are only written for targets whose `applied_changes` are **non-noop**
      (including tombstones). If a policy results in a no-op (e.g. `BASE_WINS` skip), there is no queue entry.
  - **writeback_patchsets** (append-only action ledger)
    - `actions/{action_log_id}/patchset.json`
    - `actions/{action_log_id}/metadata.json`
  - **writeback_merged_snapshot** (materialized)
    - `snapshots/{snapshot_id}/manifest.json`
    - `snapshots/{snapshot_id}/objects/{object_type}/{instance_id}/{lifecycle_id}.json`

Each action run creates one lakeFS commit that contains the patchset payload.
Queue entries (one per target object) are appended using `action_applied_seq` after ActionApplied is
durably recorded, and can be rebuilt from the patchset ledger if needed.

The merged snapshot is built out-of-band by a scheduled materialization job.
It never mutates the backing dataset; it only produces writeback views.

## Write path (Action execution)

1) Submission (BFF)
   - Validate request schema (input_schema) and basic payload sanity.
   - Accept only action_type + input payload (no direct CRUD/patchset payloads).
   - Resolve **deployed** ontology commit from `ontology_deployments_v2`.
   - Allocate `action_log_id` and create ActionLog (`status=PENDING`) as an Ontology object type
     (backed by Postgres).
   - Enforce permissions: `can_modify(action_log)` at create-time (not just service/table ACLs).
   - Append ActionCommand to Event Store (`metadata.kind=command`), topic `ACTION_COMMANDS_TOPIC`
     (includes `action_log_id`).

2) Action worker (new worker)
   - Consume ActionCommand events (idempotent via `ProcessedEventRegistry`).
   - Load ActionLog (`status=PENDING`) and evaluate submission gating in the same principal context.
   - Load action definition from Terminus at the **deployed commit**.
   - Load target object state from SSoT (Event Store replay or S3 command log replay).
   - Derive `base_token` from the authoritative base doc (deterministic function).
   - ES can be used as a performance cache, but never as the conflict token source.
   - Enforce `can_modify(target_objects)` + `submission_criteria` before any writeback commit.
   - Evaluate validation_rules.
   - Compute patchset (rules/functions).

3) Atomic apply (logical apply unit)
   - Write patchset to lakeFS and create one commit.
   - Update action log to `COMMIT_WRITTEN` with `writeback_commit_id`.
   - Emit ActionApplied via outbox/reconciler; mark `EVENT_EMITTED`.
   - Append per-object queue entries keyed by `action_applied_seq` (for server merge/snapshotting).
   - Mark `SUCCEEDED` once ActionApplied + queue append are durably recorded.
   - If commit fails, action is `FAILED` and no writeback commit is recorded.
   - If emit fails, leave status at `COMMIT_WRITTEN` and retry from the outbox.

4) Side-effects (optional)
   - Executed after commit (never inside the atomic patchset).
   - Side-effects must tolerate at-least-once retries and be idempotent.

## Decision simulation (dry-run)

Goal:
- Preview **applied diffs** + predicted downstream artifacts (**lakeFS patchset/queue keys, ES overlay docs**) without mutating
  lakeFS / ES / Event Store.
- Support scenario comparisons (e.g. conflict policies) and persist versioned results for a "Decision Simulation app" UI.

Endpoints (code-aligned):
- **BFF**: `POST /api/v1/databases/{db_name}/actions/{action_type_id}/simulate`
- **OMS**: `POST /api/v1/actions/{db_name}/async/{action_type_id}/simulate`
- **BFF (versioning/read)**:
  - `GET /api/v1/databases/{db_name}/actions/simulations`
  - `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}`
  - `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions`
  - `GET /api/v1/databases/{db_name}/actions/simulations/{simulation_id}/versions/{version}`

Behavior (P0):
- Runs the same preflight gates as the Action worker:
  - `permission_policy` + database role gate
  - writeback governance (dataset ACL alignment) when enabled
  - `data_access` policy gate
  - `submission_criteria` evaluation (reject on false)
  - `validation_rules` evaluation (reject on failure)
- Computes conflict resolution per scenario:
  - request may include `scenarios[]` with `conflict_policy` overrides
  - per-scenario outcome is `ACCEPTED` or `REJECTED` with full conflict diagnostics
- Produces predicted downstream artifacts (no writes):
  - `patchset` (includes both `changes` and `applied_changes`)
  - `es_overlay.documents[].overlay_document` (predicted overlay doc to index)
  - `lakefs.patchset_key` and per-object `queue_entry_key_template`

Determinism + semantics:
- `patchset_id` is a deterministic digest: `sha256_canonical_json_prefixed(patchset)` and is **not** a lakeFS commit id.
- Queue entry keys include an `<seq>` placeholder because `action_applied_seq` is only assigned on real apply.
- Simulation uses a synthetic `preview_action_log_id` to reuse the same storage key conventions as real runs.

Versioning store (control-plane):
- Stored in Postgres schema `spice_action_simulations`:
  - `action_simulations` (simulation header)
  - `action_simulation_versions` (immutable per-run versions, including errors)
- Simulation failures are persisted:
  - `status=REJECTED` for policy gate failures (e.g. criteria/permission/conflict FAIL)
  - `status=FAILED` for infrastructure/HTTP failures

## Read path (ES overlay)

Projection worker processes ActionApplied events:
1) Load patchset from lakeFS commit.
2) For each target instance:
   - Resolve effective doc:
     - If an overlay doc exists, use it.
     - Else fetch base doc from main index (`{db}_instances`) as a cache of SSoT state.
     - If backing data has advanced, rebuild from `writeback_merged_snapshot` + queue tail first.
   - Merge `changes` onto the effective doc to produce a full document.
   - Index into **overlay index** (branch index) with external version guard.
3) Read-your-write uses overlay index first, then falls back to base.

Authority model (P0):
- ES overlay is a presentation cache; it must never be treated as the source of truth.
- The authoritative effective state is derived from writeback ledger/snapshot + queue.
- If overlay is stale or unavailable, serve the authoritative merged view (with `overlay_status=DEGRADED`),
  not a Terminus-only view.
 - If an Action resolves to a no-op (`applied_changes` empty; e.g. `BASE_WINS` skip), projection does not
   write an overlay document. Reads naturally fall back to base state; the ActionLog remains the source for
   decision/conflict metadata.

Overlay branch key:
- Reuse writeback branch string for ES overlay indices: `writeback-{db_name}`.
- This aligns with `get_instances_index_name(db_name, branch=...)`.

Query path alignment:
- Graph federation already supports branch overlay (`graph_federation_service_woql.py`).
- BFF instance queries should accept a `branch` parameter and pass it to `get_instances_index_name`.

Revision field in ES:
- Projection writes `event_sequence` (alias `__rev`) from Event Store `sequence_number`.
- ES stores this as a cached revision for fast reads; conflict checks still use SSoT.
- Overlay docs MUST include debug fields: `instance_id`, `lifecycle_id`, `base_token`, `patchset_commit_id`.
- `delete=true` produces a tombstone document in the overlay index for `(instance_id, lifecycle_id)`.

Reprojection / rebuild:
- Use the latest `writeback_merged_snapshot` as the base.
- Replay only the per-object tail of `writeback_edits_queue` after the snapshot high-watermark.
- This avoids O(N) patchset replays for full reindex/rebuild.

ES outage fallback:
- When ES is unavailable, reads must still merge writeback edits.
- Fallback path serves a server-side merged view:
  1) Load base state from SSoT (Event Store replay or S3 command log replay).
  2) Apply latest `writeback_merged_snapshot`.
  3) Apply per-object queue tail after the snapshot high-watermark.
- If this merge path is not available, return an explicit degraded response
  (no silent fallback to Terminus-only state for writeback-enabled types).
- 운영 정책상 서버 merge fallback을 제공하지 않기로 한 경우, 항상 503 + `overlay_status=DEGRADED`
  로 통일하며 Terminus-only fallback은 금지한다.

## Writeback materialization (merged snapshot)

Goal: bound queue growth and provide a durable merged dataset for rebuilds.
`writeback_merged_snapshot` is an official reproducible artifact for downstream reads/rebuilds, not a best-effort cache.

Trigger conditions (governance-aligned):
- New datasource transaction detected (fresh base data version).
- Edits detected since last snapshot (queue not empty).
- Periodic schedule (default: every 6 hours).

Materialization flow (pipeline worker):
1) Load base dataset version + latest `writeback_merged_snapshot`.
2) Apply queued edits up to `queue_high_watermark` (`action_applied_seq`).
3) Write new snapshot and manifest:
   - `snapshot_id`, `snapshot_revision`, `queue_high_watermark`, `created_at`.
   - `base_dataset_version_id` (or backing datasource tx id)
   - `ontology_commit_id`
   - `materializer_definition_hash` (code/definition hash)
   - `inputs_digest` (merkle/hash over applied patchset commits)
4) Record snapshot pointer in Postgres metadata (optional) and publish a snapshot event.

Queue compaction policy:
- `writeback_edits_queue` is append-only; entries up to the snapshot high-watermark
  can be marked compacted for faster scans.
- `writeback_patchsets` are retained as the audit ledger (no deletion).

## Conflict policy (first-class)

Conflict token (P0):
- Use `base_token` as the authoritative conflict token.
- `base_token` MUST be deterministic and structured:
  - `base_token = (base_dataset_version_id, object_type_version_id, instance_id, lifecycle_id, base_state_hash)`
  - `base_state_hash = sha256(canonical_json(authoritative_base_doc))`
  - Canonical JSON normalization rules MUST be fixed (e.g., RFC8785/JCS) and versioned.
- Token generation MUST live in a shared library and be unit-tested; changes must bump
  `materializer_definition_hash`.

Conflict detection (P0):
- Conflicts are detected at field/link granularity using `observed_base` + current base state.
- System default is `non-overlap auto-merge + overlap FAIL`:
  - If only unrelated fields changed, apply automatically (even when `base_token` differs).
  - If any touched field/link overlaps, treat as conflict and apply `conflict_policy` (default `FAIL`).

Field/link overlap rules (P0):
- `set`/`unset`: conflict if the same field’s current base value differs from `observed_base.fields[field]`.
- `link_add`/`link_remove`: treat links as sets; conflict if the same link element has an opposing concurrent change
  (base added vs patch removes, or base removed vs patch adds).

Atomicity clarification:
- Patchset commit + action log form a logical apply unit; partial failures are expected and recovered via
  the action log state machine/outbox.
- Conflict resolution may ignore some target changes without failing the commit.
  This is policy-driven and explicitly recorded.
- If no explicit conflict_policy is set on the action_type or object_type, the system default MUST be FAIL
  (on overlap conflicts; non-overlap auto-merge applies before policy evaluation).

Policy sources (priority):
1) `action_type.spec.conflict_policy`
2) `object_type.spec.conflict_policy`
3) System default (must be FAIL; no override unless explicitly documented and approved)

Policy options (P0):
- `WRITEBACK_WINS`: apply patchset on top of current base.
- `BASE_WINS`: skip conflicting target; mark conflict in action log.
- `FAIL`: reject entire action (no writeback commit).
- `MANUAL_REVIEW`: apply nothing; log conflict for human resolution.

System default `FAIL` is an intentional safety policy (a deliberate divergence from edits-win defaults).

User-facing actions guidance (P0):
- 기본값은 `FAIL`을 유지하되, 사용자-facing 액션은 대체로 `WRITEBACK_WINS` 또는
  필드 단위 머지 정책을 사용한다.
- 충돌 정의는 object 전체가 아니라 field 단위로 평가하는 것을 기본으로 한다.
  (예: non-overlapping이면 자동 머지, overlapping이면 FAIL)

Conflict metadata is stored in:
- `ontology_action_logs.result.conflicts`
- ES overlay documents include `conflict_status` for downstream UI **when an overlay doc is written**
  (non-noop applied_changes / tombstone). For skip/no-op outcomes, conflict/decision metadata is read from ActionLog.

## Delete / recreate semantics

Delete/recreate rules:
- Delete is not an edit; it is a tombstone operation.
- If an object is deleted and later re-created, prior writeback edits are not inherited.

Operational rules:
- Use `(instance_id, lifecycle_id)` as the identity for writeback lineage.
- `delete=true` writes a tombstone for `(instance_id, lifecycle_id)` and marks it deleted in
  `writeback_merged_snapshot`.
- Re-create issues a new `lifecycle_id` for the same `instance_id`, starting a new lineage.
- Patchset targets MUST include `lifecycle_id` to prevent legacy edits from being re-applied.

## Permissions and submission criteria

Permission checks are conjunctive:
- `can_modify(target_objects)`
- `can_modify(action_log)`

Alignment with current code:
- `database_access` roles (`shared/security/database_access.py`) are the base guard.
- `permission_policy` in `action_type` evaluates principals (user/group/role).
- `submission_criteria` is evaluated server-side using input + target object state.

If either check fails, the Action is rejected before any writeback.

Permission evaluator (governance-aligned):
- `can_modify(target_objects)` is evaluated as the AND of:
  - `action_type.permission_policy`
  - dataset ACL for the backing dataset (read/write)
  - writeback dataset ACL (write), aligned to the backing dataset ACL
  - restricted view / row-level policy (data_access)
- `can_modify(action_log)` must be satisfied in the same principal context and enforced through the
  ActionLog object type permission model.

Writeback dataset governance:
- Writeback datasets must live in the same ontology project and inherit the same ACL
  as the backing dataset (no cross-project writeback).
- If ACL alignment cannot be verified, the action is rejected.

## Idempotency and retry
- Idempotency key: `event_id` in EventEnvelope.
- Action worker uses `ProcessedEventRegistry` to prevent duplicate patchset commits.
- Event Store rejects event_id reuse with different payloads (see `IDEMPOTENCY_CONTRACT.md`).
- Projection worker treats ActionApplied as idempotent (event_id gated).
- ProcessedEventRegistry alone does not prevent commit/event gaps; the action log state machine + outbox
  provides recovery for partial failures.

## Observability and audit
- Action logs are the audit SSoT for operational edits.
- Event Store + AuditLogStore record append/side-effect metadata.
- `ontology_action_logs` is queryable for governance, debugging, and lineage.

## Implementation map (files)

Add / extend:
- `backend/shared/models/ontology_resources.py` (ActionTypeDefinition spec fields)
- `backend/oms/services/ontology_resource_validator.py` (action_type validation)
- `backend/shared/models/commands.py` (ActionCommand)
- `backend/shared/models/events.py` (ActionApplied event type)
- `backend/oms/routers/action_async.py` (new action submission API)
- `backend/bff/routers/actions.py` (BFF action API + permission gate)
- `backend/action_worker/main.py` (new worker)
- `backend/action_outbox_worker/main.py` (outbox reconciler; fills commit/event/queue gaps)
- `backend/projection_worker/main.py` (ActionApplied handler + overlay merge)
- `backend/writeback_materializer_worker/main.py` (merged snapshot builder)
- `backend/shared/services/action_log_registry.py` (new Postgres registry)
- `backend/shared/config/app_config.py` (ACTION_* topics)
- `backend/shared/config/search_config.py` (reuse overlay branch token)
- `backend/tests/test_action_writeback_e2e_smoke.py` (live-stack E2E smoke + verification suite)

## Configuration
- `ONTOLOGY_WRITEBACK_REPO` (default `ontology-writeback`)
- `ONTOLOGY_WRITEBACK_BRANCH_PREFIX` (default `writeback`)
- `ONTOLOGY_WRITEBACK_DATASET_ID` (optional)
- `WRITEBACK_ENFORCE` / `WRITEBACK_READ_OVERLAY` / `WRITEBACK_ENABLED_OBJECT_TYPES` (feature flags)
- `WRITEBACK_ENFORCE_GOVERNANCE` (when true, require writeback/backing dataset ACL alignment; unverifiable => reject)
- `WRITEBACK_DATASET_ACL_SCOPE` (default `dataset_acl`)
- `ACTION_COMMANDS_TOPIC` / `ACTION_EVENTS_TOPIC` (new)
- `ACTION_OUTBOX_POLL_SECONDS` / `ACTION_OUTBOX_BATCH_SIZE` (outbox reconciler tuning)
- `WRITEBACK_MATERIALIZER_DB_NAMES` / `WRITEBACK_MATERIALIZER_INTERVAL_SECONDS` / `WRITEBACK_MATERIALIZER_BASE_BRANCH` / `WRITEBACK_MATERIALIZER_RUN_ONCE` (snapshot materializer tuning)
- `ENABLE_PROCESSED_EVENT_REGISTRY` (idempotency)
- `EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE` (error|warn)

<!-- DOC_SYNC: 2026-02-13 Foundry pipeline parity + runtime consistency sweep -->
