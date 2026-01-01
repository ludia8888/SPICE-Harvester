# Ontology Resource Design (P0)

Foundry-style split: TerminusDB holds **definitions + compiled snapshot**, Postgres holds **control-plane state** (proposal/approve/deploy/health/outbox). This doc fixes P0 schemas for Terminus.

## Decision framework (Foundry docs vs implementation choices)
두 층으로 판단한다:
1) Foundry 공식 문서에서 “명시적으로 말하는 철학/행동” (반드시 맞아야 함)
2) Foundry가 내부 구현을 공개하지 않는 영역 (정렬 가능한 구현 선택이면 OK)

### 1) 공식 문서와 강하게 정렬되는 부분 (must align)
A. Object type은 backing datasource로 property 값을 생성한다
- 문서 요지: object properties는 backing datasource로부터 생성/표시된다. backing datasource 위치는 permissions에도 영향을 준다.
- ✅ 정렬 평가: `backing_source(kind/ref/schema_hash)`를 1급 계약으로 고정했고 drift를 차단한다는 점이 철학과 정렬됨.

B. Primary key는 unique + deterministic이어야 하며, 바뀌면 edits/links가 깨질 수 있다
- 문서 요지: non-deterministic PK는 edits 손실/links 소실 위험이 크다.
- ✅ 정렬 평가: `pk_spec` 필수 + `PK_DRIFT`를 Health ERROR로 차단하는 방향은 1:1 정렬.

C. Action type은 “한 번에 수행하는 변경 단위 + side effects”이며 Ontology의 기본 수정 경로다
- 문서 요지: end users는 Actions로 변경한다. Action 밖에서 edit 실행해도 object data는 수정되지 않는다.
- ✅ 정렬 평가: “Action execution이 default mutation path” + side effects 고정은 문서와 강하게 정렬됨.

D. Action Log(감사/의사결정 기록)를 객체 데이터로 남긴다
- 문서 요지: Action Log object type은 Action type과 1:1이며, 제출 자체가 객체로 기록된다.
- ✅ 정렬 평가: `ontology_action_logs`를 P0에 포함하고 writeback commit을 남기는 방향은 문서 철학과 정렬됨.

E. Roles/permissions는 Ontology의 핵심 개념이며, 리소스별 권한이 요구된다
- 문서 요지: roles 중심의 권한 모델이며, links/actions 편집 권한은 관련 리소스 권한과 결합된다.
- ✅ 정렬 평가: role 기반 READ/WRITE/EXECUTE + action permission_policy에 role principal 포함은 정렬되는 방향.

F. Protected branch + proposal workflow는 운영의 기본 모델이다
- 문서 요지: Require proposals가 켜진 protected branch는 직접 변경 불가, proposal/approval/merge 필수.
- ✅ 정렬 평가: branch policies(require_proposals/health gate) + proposal→approval→merge via BFF는 정렬됨.

### 2) 문서에 “구현”이 명시되지 않은 부분 (aligned choice)
- TerminusDB에 definitions/compiled snapshot 저장
- Postgres에 control-plane state 저장
- `rid(kind:id@rev)` 포맷
- compiled snapshot idempotency hash
- deploy outbox/retry/dlq 설계
✅ 판정: 공식 문서 근거는 없으므로 “Foundry도 이렇게 한다”는 주장만 피한다. 대신 거버넌스/재현성/운영 안정성이라는 Foundry 가치에 정렬되는 구현이면 충분히 타당하다.

### 의사결정 기준 (trade-offs)
- Governance: protected branch + proposal/approval gate를 깨지 않는가?
- Determinism/Reproducibility: identity/commit 기준이 결정적이고 재현 가능한가?
- Auditability: 액션/배포/승인/헬스의 근거가 SSoT로 남는가?
- Operational durability: outbox/retry/DLQ/repair로 유실 없이 복구 가능한가?
- Forward compatibility: project-based permissions(Compass 등)과의 정렬 여지가 남는가?

### 주의 지점 (문서 기반 리스크)
A. Roles를 온톨로지 리소스로 두는 건 가능하나, Foundry는 project-based permissions 방향을 병행한다
- 권장: ontology_role를 유지하되 “장기적으로 project-based permissioning과 정렬” 문장을 명시.

B. Writeback dataset 권한/노출 정책을 control-plane에 고정해야 한다
- 권한 모델(읽기/쓰기/실행)과 action permission을 writeback 대상 리소스까지 연결해야 안전하다.

## Common conventions
- `rid`: immutable reference id for every resource (all references use rid only).
  - P0 decision: `rid` uses `kind:id@rev` (e.g., `value_type:Money@3`). Revisions are new docs; commit remains the SSoT.
- `kind` is a resource type token (not a collection name).
  - fixed enum: `object_type`, `link_type`, `shared_property`, `value_type`, `interface`, `group`, `role`, `function`, `action_type`, `compiled_snapshot`.
  - Terminus collections may use longer names (e.g., `ontology_object_type`).
  - `id` regex: `^[A-Za-z][A-Za-z0-9_-]*$` (letters start, then alnum/underscore/hyphen).
  - `rev` is a positive integer.
- `label`, `description`, `spec`, `metadata` (owners/tags/internal), `created_at`, `updated_at`.
- No per-doc “version” counters; SSoT for versioning is the Terminus commit. Compatibility chains reference prior rids.
- Protected branches are governed by branch policies (require_proposals, require_health_gate). Direct writes are rejected; changes must go through proposal -> approval -> merge via BFF.

## Definition collections (Terminus)
All below share the common fields and a type-specific `spec`.

- `ontology_object_type`
  - `spec`: `{ pk_spec, backing_source, properties: [...], shared_properties: [rid], interfaces: [rid], groups: [rid], functions: [rid], actions: [rid] }` (actions use `action_type` rid)
  - `pk_spec`: deterministic identity (single/compound), required.
  - `backing_source`: structured reference for build-time checks:
    - `{ kind, ref, artifact_output_name?, schema_hash }`
    - `kind` enum: `ARTIFACT_OUTPUT|DATASET_VERSION|CONNECTOR_STREAM`
    - `schema_hash` required (prevents drift).
    - `schema_hash` is the canonicalized output schema hash (column name + logical type, ordered by name; nullable/cardinality excluded).
    - `ref` formats by kind:
      - `artifact:{artifact_id}` + `artifact_output_name` required
      - `dataset_version:{dataset_version_id}`
      - `connector:{connector_id}` or `stream:{name}`
  - `properties` item schema (propertyKey is stable and interface-referenced):
    - `{ key, typeRef, required, nullable, cardinality?, source? }`
    - example: `{ "key": "createdAt", "typeRef": "value_type:Timestamp@1", "required": true, "nullable": false }`
    - property key regex: `^[A-Za-z][A-Za-z0-9_]*$`
    - `typeRef` uses rid kind tokens only: `value_type:*@rev` or `primitive:*` (compiler normalizes primitives to value_type rids).
    - `typeRef` for `object_type`/`value_type` must include full rid with `@rev` (e.g., `object_type:Facility@1`, `value_type:Timestamp@1`).
    - `key` is case-sensitive; style lint warns on non-camelCase, compiler does not normalize.

- `ontology_link_type`
  - `spec`: `{ from: rid, to: rid, predicate, cardinality, constraints, id_stability }`
  - `from/to` must be `object_type:*@rev` rid (validator-enforced).
  - Uniqueness rule: `(from, predicate, to)` must be unique within a commit (Health code: `DUPLICATE_PREDICATE`, severity ERROR).
  - `DUPLICATE_PREDICATE` is always ERROR and blocks deploy under default gate_policy.
  - `predicate` regex: `^[a-z][a-z0-9_]*$` (lower snake; validator enforces).

- `ontology_shared_property`
  - `spec`: `{ value_type_ref: rid, cardinality, nullable, default, constraints, display, category }`
  - Label/description cover display names; refs use rid only.

- `ontology_value_type`
  - `spec`: `{ base_type, constraints, api_name, serialization: { format, canonical }, coercion_policy: "STRICT|SAFE", compatibility: { previous: [rid], change_class: "NON_BREAKING|BREAKING" } }`
  - `compatibility.previous` must share the same `kind:id` (rev can differ); otherwise 409.

- `ontology_interface`
  - `spec`: contracts only as objects (no inline/strings):
    - `required_properties`: `[ { propertyKey: "createdAt", typeRef: rid, required: true } ]` (propertyKey is the object_type-local key; typeRef is `value_type:*@rev`)
    - `required_links`: `[ { predicate, targetTypeRef: rid, cardinality } ]`
    - `allowed_actions`: `[ rid ]`
  - `propertyKey` follows object_type property key rules (case-sensitive; lint warns on non-camelCase).

- `ontology_group`
  - `spec`: `{ slug, visibility_rules, tags }` (label/description remain common fields)
  - `slug` regex: `^[a-z][a-z0-9_-]*$` and unique per commit.
  - `slug` is for display/search; references use group rid only.

- `ontology_role`
  - `spec`: `{ permissions: [ { resource_rid, actions, effect } ] }`
  - `actions` enum: `READ|WRITE|EXECUTE`
  - `effect` enum: `ALLOW|DENY`
  - `resource_rid` scope (P0): definition resources only (`object_type`, `link_type`, `action_type`, `shared_property`, `value_type`, `interface`, `group`, `function`); instance/field-level scopes are P1.
  - Roles are referenced in permission_policy; bindings to users/groups live in control-plane.

- `ontology_function`
  - `spec`: `{ signature: { inputs, outputs }, runtime: { kind, version }, execution: { mode: "ON_READ|MATERIALIZED", ttl_seconds }, dependencies: { reads: [...], writes: [...] }, deterministic: bool, side_effects }`
  - `signature` item schema: `{ name, typeRef, cardinality }` where `typeRef` is `object_type:*@rev` or `value_type:*@rev`.
  - `primitive:*` is allowed only in definitions; compiler normalizes to a value_type rid.

- `ontology_action_type`
  - `spec`: `{ input_schema, permission_policy, validation_rules, side_effects, writeback_target, write_targets?, audit_policy }`
  - `permission_policy` minimum: `{ effect: "ALLOW|DENY", principals: [string], conditions?: {} }` (principals may include `role:<rid>`).
  - `principals` must be namespaced: `user:<id>` | `group:<id>` | `role:<rid>` (validator rejects unprefixed values).
  - `writeback_target` is required (writeback dataset for latest state); action submissions must be logged for audit.
  - P0 writeback target is fixed: lakeFS writeback dataset_version (each action produces a new writeback commit recorded in action logs).
  - Default naming is deterministic but config-injectable:
    - repo: `ontology_writeback`, branch prefix: `writeback/{db_name}`
    - overrides: `ONTOLOGY_WRITEBACK_REPO`, `ONTOLOGY_WRITEBACK_BRANCH_PREFIX`, `ONTOLOGY_WRITEBACK_DATASET_ID` (optional)

## Compiled snapshot (Terminus)
Single doc per deploy to keep snapshot atomic.

- `ontology_compiled_snapshot`
  - `snapshot_rid`: `compiled_snapshot:{ontology_commit_id}@1` (rid rule compliant; rev is always 1)
  - `ontology_commit_id`, `compiled_at`
  - `compiled`: `{ object_types: [...], link_types: [...], value_types: [...], interfaces: [...], shared_properties: [...], functions: [...], actions: [...] }` (resolved props/links/groups/interfaces/value_types/functions/actions)
  - `compiled.object_types[]` minimum: `{ rid, label, pk_spec, backing_source, properties: [...], links: [...], groups: [...], interfaces: [...] }`
  - `compiled.object_types[].properties[]` minimum: `{ key, typeRef, required, nullable, cardinality }`
  - `compiled.object_types[].links[]` minimum: `{ predicate, targetTypeRef, cardinality }`
  - `report`: `{ errors: [...], warnings: [...], contracts_checked: [...] }`
  - `report.errors[]`/`report.warnings[]` reuse Health issue schema: `{ code, severity, resource_rid, message, details, suggested_fix, source }`.
  - Snapshot write is immutable or idempotent (if exists, validate identical content and reuse).
  - Idempotency check: `compiled_hash = sha256(canonical_json(compiled + report))`. If snapshot_rid exists and compiled_hash differs → ERROR.
  - Optional alias: `metadata.snapshot_key = "__compiled_ontology:{ontology_commit_id}"`.

## Compile pipeline (Deploy gate)
1) Load definitions at deploy ontology_commit_id (approved commit), not just branch head.
2) Normalize refs to rid.
3) Resolve: shared_property → object props; interface contracts; value_type refs; function/action refs.
4) Validate: ref existence, PKSpec completeness/backing source, link from/to, action permission/input schema, value_type coercion rules, PK/backing_source drift vs last deployed snapshot (Health code: `PK_DRIFT`, severity ERROR).
5) Emit Health issues (existing issues catalog).
6) Gate: errors block deploy (gate_policy persisted with deployment; WARN handling is policy-driven).
7) Write `ontology_compiled_snapshot`; link snapshot_rid in Postgres deploy record.

## Control plane (Postgres) – next step
- branch policies (protected branches, require_proposals, require_health_gate)
- proposal store: reuse `pull_requests` (source_commit_id capture + merge_commit_id)
- deployments v2 + outbox v2 (SSoT for deployed ontology state)
- role bindings (users/groups -> role rid)
- health results cache (optional)
- outbox/retry/dlq for deploy processing
- action logs (audit SSoT for ontology mutations)
- execution logs (later with functions)

---

# Control Plane (Postgres) – P0 schema proposal

## P0 closure invariants
1) RID rule fixed: `kind:id@rev`.
2) Proposal store reuses `pull_requests`; `source_commit_id` is captured at proposal creation.
3) Approve performs merge with head/Health gate checks; deploy performs validate+record (role separation).
4) Compiled snapshot write is atomic with deployment update (no partial success).
5) Deploy-time Health run is persisted to health_runs + health_issues.
6) Gate policy is persisted in deployments and reusable for re-run.
7) Protected branches enforce proposal-only writes (require_proposals) via BFF.
8) Approval applies Health gate on the proposed commit; ERROR blocks merge by default.
9) Action execution is the default mutation path; writeback_target + action logs are mandatory.
10) Role-based permissions govern read/write/execute for ontology resources and actions.

## Tables

- `ontology_branch_policies`
  - `policy_id (uuid PK)`, `db_name`, `branch`, `protected bool`, `require_proposals bool`, `require_health_gate bool`, `gate_policy`, `updated_by`, `updated_at`, `metadata jsonb`
  - `require_health_gate` means approval must run Health and block merge on ERROR; deploy reuses the approval run or re-runs if policy/commit changed.

- `pull_requests` (proposal store)
  - `id (uuid PK)`, `db_name`, `source_branch`, `target_branch`, `title`, `description`, `author`
  - `status (open|merged|closed|rejected)`, `merge_commit_id`, `source_commit_id`
  - `diff_cache`, `conflicts`, `version`, `created_at`, `updated_at`, `merged_at`
  - `source_commit_id` is captured at proposal creation; approve must verify head still matches or require re-propose.
  - `pull_request_reviews` may be used later for explicit approval records (optional).

- `ontology_deployments_v2`
  - `deployment_id (uuid PK)`, `db_name`, `target_branch`, `ontology_commit_id`, `snapshot_rid` (Terminus compiled snapshot rid), `proposal_id` (pull_requests.id), `status (pending|running|succeeded|failed)`, `gate_policy`, `health_summary`, `deployed_by`, `deployed_at`, `error`, `metadata jsonb`
  - `health_summary` schema: `{ "ERROR": int, "WARN": int, "INFO": int }`
  - Status transitions: `pending -> running -> succeeded|failed` (failed may be retried).

- `ontology_deploy_outbox_v2`
  - `outbox_id (uuid PK)`, `deployment_id` FK, `payload jsonb`, `status (pending|publishing|failed|published|dead)`, `retry_count`, `next_attempt_at`, `claimed_by`, `claimed_at`, `last_error`, `created_at`, `updated_at`
  - Purpose: post-deploy follow-up events (cache invalidate, projection refresh, downstream triggers). Optional if no downstream tasks.
  - Payload minimum: `{ deployment_id, ontology_commit_id, snapshot_rid, event_type, created_at }`.

- `ontology_role_bindings`
  - `binding_id (uuid PK)`, `db_name`, `role_rid` (`role:*@rev`), `principal_type (USER|GROUP|SERVICE)`, `principal_id`, `created_by`, `created_at`, `metadata jsonb`

- `ontology_action_logs`
  - `action_log_id (uuid PK)`, `action_type_rid`, `resource_rid`, `ontology_commit_id`, `input`, `status (SUBMITTED|APPROVED|REJECTED|EXECUTING|SUCCEEDED|FAILED)`, `result`, `correlation_id`, `submitted_by`, `submitted_at`, `finished_at`, `writeback_target`, `writeback_commit_id`, `metadata`

- `ontology_health_runs`
  - `run_id (uuid PK)`, `db_name`, `branch`, `ontology_commit_id`, `snapshot_rid` (if applicable), `status (PENDING|RUNNING|SUCCEEDED|FAILED)`, `started_at`, `finished_at`, `error`, `summary jsonb`, `catalog_version`, `schema_version`, `dedupe_key (unique)`, `metadata jsonb`

- `ontology_health_issues`
  - `issue_id (uuid PK)`, `run_id` FK, `code`, `severity (ERROR|WARN|INFO)`, `resource_rid`, `message`, `details jsonb`, `suggested_fix`, `source`, `created_at`
  - Global issues use `resource_rid = "ontology:{db_name}@{ontology_commit_id}"`.

- (later) `ontology_function_exec_logs`
  - `exec_id (uuid PK)`, `resource_rid`, `function`, `input`, `result`, `status`, `started_at`, `finished_at`, `actor`, `metadata`

## Control-plane flow
1) Branch policy enforces protected/proposal-only writes; proposal created via `pull_requests`.
2) Approval runs Health gate on the proposed commit when required; ERROR blocks merge.
   - Deploy reuses the approval Health run when commit/gate_policy matches; otherwise re-runs.
3) Approval merges source branch; deploy validates + records state.
4) Deploy requested: create `ontology_deployments_v2` with `snapshot_rid` (compiled rid) and enqueue `ontology_deploy_outbox_v2`.
   - If no downstream tasks, outbox enqueue may be skipped.
5) Worker publishes/executes deploy; outbox handles retry/backoff/DLQ, updates deployment status.
6) Health run after deploy writes `ontology_health_runs` + `ontology_health_issues`. Errors can gate future deploys or surface in Ops API.

## Compile/validator next steps
1) Fix CompileContext input/output types.
   - `CompileContext`: `{ db_name, ontology_commit_id, target_branch, gate_policy, catalog_version }`
2) Resolve rules: shared_property conflict priority; interface required property type mismatch → ERROR.
3) Health issue code mapping per rule.
4) Gate policy application logic (ERROR-only vs WARN-blocking).

## P0' operational refinements
- Gate policy schema example: `{ "block_on": ["ERROR"], "warn_threshold": 0, "allow_internal_only": true }`
- Health run idempotency: `dedupe_key = "{db}:{ontology_commit_id}:{gate_policy_hash}"` (reuse existing run if present).
- `gate_policy_hash` uses canonical JSON (sorted keys) to avoid ordering mismatches.

## Ops/API (aligned with ingest/objectify)
- `/api/v1/ops/status` can be extended with:
  - `ontology_deploy_outbox` backlog/oldest/next_attempt
  - latest `ontology_health_runs` summary and counts by severity
  - counts of deployments by status
