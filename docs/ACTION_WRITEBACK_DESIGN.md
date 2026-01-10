# Action Writeback Design (Code-Aligned)

> Updated: 2026-01-08  
> Status: design/target spec (aligned to current code patterns)

## Table of contents
- Decisions (locked)
- Scope and non-goals
- Alignment with current code
- Data model
- Write path (Action execution)
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
- Action 1 run = 1 atomic apply unit (patchset). No partial success.
- Permission = can_modify(target_objects) AND can_modify(action_log).
- Action definitions are versioned with ontology branches/commits; only deployed commits are executable.
- Conflicts between datasource updates and writeback edits are first-class and policy-driven.

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

## Data model

### Action type (Terminus resource)
Stored as ontology resource `action_type` via `OntologyResourceService`. The `spec` payload is the contract.

Required fields (P0):
- `input_schema` (existing required field)
- `permission_policy` (existing required field)
- `writeback_target` (new required field)

Recommended fields:
- `submission_criteria` (boolean expression over user + inputs + target object state)
- `validation_rules` (pre-apply validation, server-side)
- `side_effects` (non-transactional post-commit hooks)
- `conflict_policy` (see below)
- `audit_policy` (log payload size, redaction rules)
- `implementation` (rule-based or function-backed)

Example (payload merged into `spec` by validator):
```json
{
  "id": "approve_ticket",
  "label": "Approve Ticket",
  "spec": {
    "input_schema": { "fields": [{ "name": "ticket", "type": "object_ref" }] },
    "permission_policy": { "effect": "ALLOW", "principals": ["role:DomainModeler"] },
    "writeback_target": { "repo": "ontology_writeback", "branch": "writeback/{db_name}" },
    "submission_criteria": "user.id != ticket.requester",
    "conflict_policy": "WRITEBACK_WINS",
    "implementation": { "type": "function", "ref": "function:approve_ticket@3" }
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
      "base_version": 42,
      "changes": {
        "set": { "status": "APPROVED", "approved_by": "user:alice" },
        "unset": ["rejected_reason"],
        "link_add": [],
        "link_remove": []
      }
    }
  ],
  "metadata": {
    "submitted_by": "user:alice",
    "submitted_at": "2026-01-08T12:00:00Z"
  }
}
```

Notes:
- `base_version` is the ES version (or base snapshot hash) at compute time.
- `changes` are field-level operations; projection merges them with base docs.

### Action log (Postgres)
Action logs are the audit SSoT for writeback decisions.

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

### Writeback dataset layout (lakeFS)
- Repository: `AppConfig.ONTOLOGY_WRITEBACK_REPO` (default `ontology_writeback`)
- Branch: `AppConfig.get_ontology_writeback_branch(db_name)` (default `writeback/{db_name}`)
- Object keys:
  - `actions/{action_log_id}/patchset.json`
  - `actions/{action_log_id}/metadata.json`

Each action run creates one lakeFS commit, which is recorded in the action log.

## Write path (Action execution)

1) Submission (BFF)
   - Validate request schema (input_schema) and basic payload sanity.
   - Resolve **deployed** ontology commit from `ontology_deployments_v2`.
   - Enforce permissions: `can_modify(target_objects)` AND `can_modify(action_log)`.
   - Append ActionCommand to Event Store (`metadata.kind=command`), topic `ACTION_COMMANDS_TOPIC`.

2) Action worker (new worker)
   - Consume ActionCommand events (idempotent via `ProcessedEventRegistry`).
   - Load action definition from Terminus at the **deployed commit**.
   - Load current object states from ES base index (not from lakeFS writeback).
   - Evaluate submission_criteria and validation_rules.
   - Compute patchset (rules/functions).

3) Atomic apply
   - Write patchset to lakeFS and create one commit.
   - Record action log with `status=SUCCEEDED` and `writeback_commit_id`.
   - Append one ActionApplied domain event pointing to the lakeFS commit.
   - If any step fails before commit, action is `FAILED` and no writeback commit is recorded.

4) Side-effects (optional)
   - Executed after commit (never inside the atomic patchset).

## Read path (ES overlay)

Projection worker processes ActionApplied events:
1) Load patchset from lakeFS commit.
2) For each target instance:
   - Fetch base doc from main index (`{db}_instances`).
   - Merge `changes` onto base doc to produce a full document.
   - Index into **overlay index** (branch index) with external version guard.
3) Read-your-write uses overlay index first, then falls back to base.

Overlay branch key:
- Reuse writeback branch string for ES overlay indices: `writeback/{db_name}`.
- This aligns with `get_instances_index_name(db_name, branch=...)`.

Query path alignment:
- Graph federation already supports branch overlay (`graph_federation_service_woql.py`).
- BFF instance queries should accept a `branch` parameter and pass it to `get_instances_index_name`.

## Conflict policy (first-class)

Conflicts are detected when `base_version` in patchset does not match current base doc version.

Policy sources (priority):
1) `action_type.spec.conflict_policy`
2) `object_type.spec.conflict_policy`
3) System default (env/config)

Policy options (P0):
- `WRITEBACK_WINS`: apply patchset on top of current base.
- `BASE_WINS`: skip conflicting target; mark conflict in action log.
- `FAIL`: reject entire action (no writeback commit).
- `MANUAL_REVIEW`: apply nothing; log conflict for human resolution.

Conflict metadata is stored in:
- `ontology_action_logs.result.conflicts`
- ES overlay documents include `conflict_status` for downstream UI.

## Permissions and submission criteria

Permission checks are conjunctive:
- `can_modify(target_objects)`
- `can_modify(action_log)`

Alignment with current code:
- `database_access` roles (`shared/security/database_access.py`) are the base guard.
- `permission_policy` in `action_type` evaluates principals (user/group/role).
- `submission_criteria` is evaluated server-side using input + target object state.

If either check fails, the Action is rejected before any writeback.

## Idempotency and retry
- Idempotency key: `event_id` in EventEnvelope.
- Action worker uses `ProcessedEventRegistry` to prevent duplicate patchset commits.
- Event Store rejects event_id reuse with different payloads (see `IDEMPOTENCY_CONTRACT.md`).
- Projection worker treats ActionApplied as idempotent (event_id gated).

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
- `backend/projection_worker/main.py` (ActionApplied handler + overlay merge)
- `backend/shared/services/action_log_registry.py` (new Postgres registry)
- `backend/shared/config/app_config.py` (ACTION_* topics)
- `backend/shared/config/search_config.py` (reuse overlay branch token)

## Configuration
- `ONTOLOGY_WRITEBACK_REPO` (default `ontology_writeback`)
- `ONTOLOGY_WRITEBACK_BRANCH_PREFIX` (default `writeback`)
- `ONTOLOGY_WRITEBACK_DATASET_ID` (optional)
- `ACTION_COMMANDS_TOPIC` / `ACTION_EVENTS_TOPIC` (new)
- `ENABLE_PROCESSED_EVENT_REGISTRY` (idempotency)
- `EVENT_STORE_IDEMPOTENCY_MISMATCH_MODE` (error|warn)
