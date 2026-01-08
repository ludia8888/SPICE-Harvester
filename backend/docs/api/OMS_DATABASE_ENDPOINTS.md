# OMS API Endpoints (Code-Backed)

> Updated: 2026-01-08  \
> Base URL (local): `http://localhost:8000/api/v1` (debug ports) / `http://oms:8000/api/v1` (docker network)

## Auth

- OMS auth is required by default.
- Token headers: `X-Admin-Token` or `Authorization: Bearer <token>`.
- Config: `OMS_REQUIRE_AUTH`, `OMS_ADMIN_TOKEN`/`OMS_WRITE_TOKEN`.

## Database

- `POST /api/v1/database/create`
- `GET /api/v1/database/list`
- `GET /api/v1/database/exists/{db_name}`
- `DELETE /api/v1/database/{db_name}`

## Branch

- `GET /api/v1/branch/{db_name}/list`
- `POST /api/v1/branch/{db_name}/create`
- `DELETE /api/v1/branch/{db_name}/branch/{branch_name:path}`
- `POST /api/v1/branch/{db_name}/checkout`
- `GET /api/v1/branch/{db_name}/branch/{branch_name:path}/info`
- `POST /api/v1/branch/{db_name}/commit`

## Version

- `GET /api/v1/version/{db_name}/head`
- `GET /api/v1/version/{db_name}/history`
- `GET /api/v1/version/{db_name}/diff`
- `POST /api/v1/version/{db_name}/commit`
- `POST /api/v1/version/{db_name}/merge`
- `POST /api/v1/version/{db_name}/rollback`
- `POST /api/v1/version/{db_name}/rebase`
- `GET /api/v1/version/{db_name}/common-ancestor`

## Ontology (classes + validation)

- `POST /api/v1/database/{db_name}/ontology`
- `GET /api/v1/database/{db_name}/ontology`
- `GET /api/v1/database/{db_name}/ontology/{class_id}`
- `PUT /api/v1/database/{db_name}/ontology/{class_id}`
- `DELETE /api/v1/database/{db_name}/ontology/{class_id}`
- `POST /api/v1/database/{db_name}/ontology/validate`
- `POST /api/v1/database/{db_name}/ontology/{class_id}/validate`
- `POST /api/v1/database/{db_name}/ontology/query`
- `POST /api/v1/database/{db_name}/ontology/create-advanced`
- `POST /api/v1/database/{db_name}/ontology/validate-relationships`
- `POST /api/v1/database/{db_name}/ontology/detect-circular-references`
- `GET /api/v1/database/{db_name}/ontology/analyze-network`
- `GET /api/v1/database/{db_name}/ontology/relationship-paths/{start_entity}`
- `GET /api/v1/database/{db_name}/ontology/reachable-entities/{start_entity}`

## Ontology Extensions

- `GET /api/v1/database/{db_name}/ontology/branches`
- `POST /api/v1/database/{db_name}/ontology/branches`
- `GET /api/v1/database/{db_name}/ontology/proposals`
- `POST /api/v1/database/{db_name}/ontology/proposals`
- `POST /api/v1/database/{db_name}/ontology/proposals/{proposal_id}/approve`
- `POST /api/v1/database/{db_name}/ontology/deploy`
- `GET /api/v1/database/{db_name}/ontology/health`
- `GET /api/v1/database/{db_name}/ontology/resources/{resource_type}`
- `POST /api/v1/database/{db_name}/ontology/resources/{resource_type}`
- `GET /api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}`
- `PUT /api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}`
- `DELETE /api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}`

## Instances (read + sparql)

- `GET /api/v1/instance/{db_name}/class/{class_id}/instances`
- `GET /api/v1/instance/{db_name}/instance/{instance_id}`
- `GET /api/v1/instance/{db_name}/class/{class_id}/count`
- `POST /api/v1/instance/{db_name}/sparql`

## Instances (async writes)

- `POST /api/v1/instances/{db_name}/async/{class_id}/create`
- `PUT /api/v1/instances/{db_name}/async/{class_id}/{instance_id}/update`
- `DELETE /api/v1/instances/{db_name}/async/{class_id}/{instance_id}/delete`
- `POST /api/v1/instances/{db_name}/async/{class_id}/bulk-create`
- `POST /api/v1/instances/{db_name}/async/{class_id}/bulk-update`
- `POST /api/v1/instances/{db_name}/async/{class_id}/bulk-create-tracked`
- `GET /api/v1/instances/{db_name}/async/command/{command_id}/status`

## Query

- `POST /api/v1/query/{db_name}`
- `POST /api/v1/query/{db_name}/woql`
- `GET /api/v1/instances/{db_name}/{class_id}`

## Command Status

- `GET /api/v1/commands/{command_id}/status`

## Internal Tasks

- `GET /api/v1/tasks/internal/status/{task_id}`
- `GET /api/v1/tasks/internal/active`
- `POST /api/v1/tasks/internal/cleanup`
- `GET /api/v1/tasks/internal/health`

## Pull Requests (ENABLE_PULL_REQUESTS)

- `POST /api/v1/database/{db_name}/pull-requests`
- `GET /api/v1/database/{db_name}/pull-requests`
- `GET /api/v1/database/{db_name}/pull-requests/{pr_id}`
- `POST /api/v1/database/{db_name}/pull-requests/{pr_id}/merge`
- `POST /api/v1/database/{db_name}/pull-requests/{pr_id}/close`
- `GET /api/v1/database/{db_name}/pull-requests/{pr_id}/diff`
