# Enterprise Error Spec

> Updated: 2026-01-08

This repository uses structured enterprise error codes for consistent
classification across services and workers. The source of truth is
`backend/shared/errors/enterprise_catalog.py`.

## Format

`SHV-{SUBSYS}-{DOMAIN}-{CLASS}-{NNNN}`

- `SUBSYS`: service/worker subsystem code
- `DOMAIN`: business or technical domain
- `CLASS`: error class
- `NNNN`: stable numeric identifier

## Subsystem Codes

- `BFF` - API gateway (Backend-for-Frontend)
- `OMS` - Ontology Management Service
- `ACT` - Action worker (writeback executor)
- `OBJ` - Objectify worker
- `PIP` - Pipeline worker
- `PRJ` - Projection workers
- `CON` - Connector workers
- `SHR` - Shared services
- `GEN` - Generic/unknown

## Domain Codes

- `INP` input
- `ACC` access
- `RES` resource
- `CNF` conflict
- `RAT` rate_limit
- `UPS` upstream
- `DB` database
- `SYS` system
- `DAT` data
- `MAP` mapping
- `PIP` pipeline
- `ONT` ontology
- `OBJ` objectify

## Class Codes

- `VAL` validation
- `SEC` security
- `AUT` auth
- `PER` permission
- `NOT` not_found
- `CON` conflict
- `LIM` limit
- `TMO` timeout
- `UNA` unavailable
- `INT` internal
- `STA` state
- `INTG` integration

## Error Payload Shape

```json
{
  "status": "error",
  "message": "Request validation failed",
  "detail": "Request validation failed",
  "code": "REQUEST_VALIDATION_FAILED",
  "category": "input",
  "http_status": 422,
  "retryable": false,
  "enterprise": {
    "schema": "1.1",
    "catalog_ref": "4566eb7",
    "catalog_fingerprint": "sha256:…",
    "code": "SHV-BFF-INP-VAL-0001",
    "domain": "input",
    "class": "validation",
    "subsystem": "BFF",
    "severity": "error",
    "title": "Request validation failed",
    "http_status": 422,
    "http_status_hint": 422,
    "retryable": false,
    "default_retry_policy": "none",
    "max_attempts": 1,
    "base_delay_ms": 0,
    "max_delay_ms": 0,
    "jitter_strategy": "none",
    "retry_after_header_respect": false,
    "human_required": true,
    "runbook_ref": "REQUEST_VALIDATION_FAILED",
    "safe_next_actions": ["request_human"],
    "action": "fix_input",
    "owner": "user",
    "legacy_code": "REQUEST_VALIDATION_FAILED",
    "legacy_category": "input"
  }
}
```

## Mapping Rules

- API errors map from `ErrorCode` and `ErrorCategory` in
  `backend/shared/errors/enterprise_catalog.py`.
- Objectify worker hard-gate errors map from error strings like
  `dataset_not_found` or `validation_failed` in the same file.
- `enterprise.http_status` / `enterprise.retryable` follow taxonomy defaults (not only raw HTTP status).
- `enterprise.http_status_hint` is a *hint* for standard HTTP semantics when the actual HTTP status is absent/unknown:
  actual HTTP status (response) > `enterprise.http_status_hint` > fallback.
- Retry automation should key off the enterprise retry fields (`retryable`, `default_retry_policy`, `max_attempts`,
  `base_delay_ms`, `max_delay_ms`, `jitter_strategy`, `retry_after_header_respect`) instead of ad-hoc string matching.

## Examples

- `SHV-BFF-INP-VAL-0001` Request validation failed
- `SHV-OMS-RES-NOT-0001` Resource not found
- `SHV-OBJ-DAT-VAL-0001` Data validation failed (objectify)

## Full Taxonomy

The full legacy-to-enterprise mapping is listed in `docs/ERROR_TAXONOMY.md`.
