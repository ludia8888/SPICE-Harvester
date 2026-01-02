# Enterprise Error Spec

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
- `OBJ` - Objectify worker
- `PIP` - Pipeline worker
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
    "schema": "1.0",
    "code": "SHV-BFF-INP-VAL-0001",
    "domain": "input",
    "class": "validation",
    "subsystem": "BFF",
    "severity": "error",
    "title": "Request validation failed",
    "http_status": 422,
    "retryable": false,
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

## Examples

- `SHV-BFF-INP-VAL-0001` Request validation failed
- `SHV-OMS-RES-NOT-0001` Resource not found
- `SHV-OBJ-DAT-VAL-0001` Data validation failed (objectify)

## Full Taxonomy

The full legacy-to-enterprise mapping is listed in `docs/ERROR_TAXONOMY.md`.
