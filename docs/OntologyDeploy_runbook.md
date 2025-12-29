# Ontology Deploy Runbook (P0)

## Rollback options

### Option A: Re-promote a previous deployment (preferred)
1. Look up the last known-good deployment in `ontology_deployments`.
2. Create a new proposal from a branch at the approved commit (or reuse an existing branch).
3. Approve/merge the proposal.
4. Deploy using `/api/v1/database/{db}/ontology/deploy` with:
   - `proposal_id`
   - `ontology_commit_id` = approved commit id
5. Confirm a new row is written to `ontology_deployments` and the outbox is published.

### Option B: Reset main to a previous commit (only if policy allows)
1. Enable rollback: `ENABLE_OMS_ROLLBACK=true`.
2. Call `/api/v1/version/{db}/rollback` with `target_commit` and `reason`.
3. Re-run `/api/v1/database/{db}/ontology/deploy` for the restored commit to record SSoT.

## Operational checks
- Ensure `ontology_deploy_outbox` rows move from `pending` → `published`.
- Verify `ontology_events` receives `ONTOLOGY_DEPLOYED` events after deploy.
- Confirm downstream consumers (search/projection/cache) refresh after deploy.
