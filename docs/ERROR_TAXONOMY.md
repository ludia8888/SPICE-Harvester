# Enterprise Error Taxonomy

> Updated: 2026-01-12

This file is auto-generated from `backend/shared/errors/enterprise_catalog.py`.
Run: `python scripts/generate_error_taxonomy.py`.

`SUBSYS` values: `BFF`, `OMS`, `OBJ`, `PIP`, `PRJ`, `CON`, `SHR`, `GEN`.

## Core API Errors (ErrorCode)

| Legacy code | Enterprise code | Domain | Class | Severity | Title | Retryable | Default retry policy | Human required | Runbook ref | Safe next actions | HTTP status hint |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| AUTH_EXPIRED | SHV-{SUBSYS}-ACC-AUT-0003 | access | auth | error | Authentication expired | false | after_refresh | false | AUTH_EXPIRED | after_refresh | 401 |
| AUTH_INVALID | SHV-{SUBSYS}-ACC-AUT-0002 | access | auth | error | Invalid authentication | false | after_refresh | false | AUTH_INVALID | after_refresh | 401 |
| AUTH_REQUIRED | SHV-{SUBSYS}-ACC-AUT-0001 | access | auth | error | Authentication required | false | after_refresh | false | AUTH_REQUIRED | after_refresh | 401 |
| CONFLICT | SHV-{SUBSYS}-CNF-CON-0001 | conflict | conflict | error | Conflict detected | false | none | true | CONFLICT | request_human | 409 |
| DB_CONSTRAINT_VIOLATION | SHV-{SUBSYS}-DB-CON-0001 | database | conflict | error | Database constraint violation | false | none | true | DB_CONSTRAINT_VIOLATION | request_human | 409 |
| DB_ERROR | SHV-{SUBSYS}-DB-INT-0001 | database | internal | critical | Database error | false | none | true | DB_ERROR | request_human | 500 |
| DB_TIMEOUT | SHV-{SUBSYS}-DB-TMO-0001 | database | timeout | critical | Database timeout | true | backoff | false | DB_TIMEOUT | retry_backoff | 504 |
| DB_UNAVAILABLE | SHV-{SUBSYS}-DB-UNA-0001 | database | unavailable | critical | Database unavailable | true | backoff | false | DB_UNAVAILABLE | retry_backoff | 503 |
| HTTP_ERROR | SHV-{SUBSYS}-SYS-INT-0002 | system | internal | error | HTTP error | false | none | true | HTTP_ERROR | request_human | 400 |
| INPUT_SANITIZATION_FAILED | SHV-{SUBSYS}-INP-SEC-0001 | input | security | error | Input rejected by security policy | false | none | true | INPUT_SANITIZATION_FAILED | request_human | 400 |
| INTERNAL_ERROR | SHV-{SUBSYS}-SYS-INT-0001 | system | internal | critical | Internal server error | false | none | true | INTERNAL_ERROR | request_human | 500 |
| JSON_DECODE_ERROR | SHV-{SUBSYS}-INP-VAL-0002 | input | validation | error | Invalid JSON payload | false | none | true | JSON_DECODE_ERROR | request_human | 400 |
| OMS_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0003 | upstream | unavailable | error | Upstream service unavailable | true | backoff | false | OMS_UNAVAILABLE | retry_backoff | 503 |
| PAYLOAD_TOO_LARGE | SHV-{SUBSYS}-INP-LIM-0001 | input | limit | error | Payload too large | false | none | true | PAYLOAD_TOO_LARGE | request_human | 413 |
| PERMISSION_DENIED | SHV-{SUBSYS}-ACC-PER-0001 | access | permission | error | Permission denied | false | none | true | PERMISSION_DENIED | request_human | 403 |
| RATE_LIMITED | SHV-{SUBSYS}-RAT-LIM-0001 | rate_limit | limit | error | Rate limit exceeded | true | backoff | false | RATE_LIMITED | retry_backoff | 429 |
| REQUEST_VALIDATION_FAILED | SHV-{SUBSYS}-INP-VAL-0001 | input | validation | error | Request validation failed | false | none | true | REQUEST_VALIDATION_FAILED | request_human | 422 |
| RESOURCE_ALREADY_EXISTS | SHV-{SUBSYS}-RES-CON-0001 | resource | conflict | error | Resource already exists | false | none | true | RESOURCE_ALREADY_EXISTS | request_human | 409 |
| RESOURCE_NOT_FOUND | SHV-{SUBSYS}-RES-NOT-0001 | resource | not_found | error | Resource not found | false | none | true | RESOURCE_NOT_FOUND | request_human | 404 |
| TERMINUS_CONFLICT | SHV-{SUBSYS}-UPS-CON-0001 | upstream | conflict | error | Upstream conflict | false | none | true | TERMINUS_CONFLICT | request_human | 409 |
| TERMINUS_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0002 | upstream | unavailable | error | Upstream service unavailable | true | backoff | false | TERMINUS_UNAVAILABLE | retry_backoff | 503 |
| UPSTREAM_ERROR | SHV-{SUBSYS}-UPS-INTG-0001 | upstream | integration | error | Upstream service error | false | none | true | UPSTREAM_ERROR | request_human | 502 |
| UPSTREAM_TIMEOUT | SHV-{SUBSYS}-UPS-TMO-0001 | upstream | timeout | error | Upstream request timed out | true | backoff | false | UPSTREAM_TIMEOUT | retry_backoff | 504 |
| UPSTREAM_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0001 | upstream | unavailable | error | Upstream service unavailable | true | backoff | false | UPSTREAM_UNAVAILABLE | retry_backoff | 503 |

## Objectify Job Errors

| Legacy code | Enterprise code | Domain | Class | Severity | Title | Retryable | Default retry policy | Human required | Runbook ref | Safe next actions | HTTP status hint |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| artifact_key_mismatch | SHV-OBJ-PIP-CON-0001 | pipeline | conflict | error | Artifact key mismatch | false | none | true | artifact_key_mismatch | request_human | 409 |
| artifact_key_missing | SHV-OBJ-PIP-VAL-0001 | pipeline | validation | error | Artifact key missing | false | none | true | artifact_key_missing | request_human | 400 |
| artifact_not_build | SHV-OBJ-PIP-STA-0002 | pipeline | state | error | Artifact not a build output | false | none | true | artifact_not_build | request_human | 409 |
| artifact_not_found | SHV-OBJ-PIP-NOT-0001 | pipeline | not_found | error | Artifact not found | false | none | true | artifact_not_found | request_human | 404 |
| artifact_not_success | SHV-OBJ-PIP-STA-0001 | pipeline | state | error | Artifact not successful | false | none | true | artifact_not_success | request_human | 409 |
| artifact_output_ambiguous | SHV-OBJ-PIP-CON-0002 | pipeline | conflict | error | Artifact output ambiguous | false | none | true | artifact_output_ambiguous | request_human | 409 |
| artifact_output_name_missing | SHV-OBJ-PIP-VAL-0003 | pipeline | validation | error | Artifact output name required | false | none | true | artifact_output_name_missing | request_human | 400 |
| artifact_output_name_required | SHV-OBJ-PIP-VAL-0003 | pipeline | validation | error | Artifact output name required | false | none | true | artifact_output_name_required | request_human | 400 |
| artifact_output_not_found | SHV-OBJ-PIP-NOT-0002 | pipeline | not_found | error | Artifact output not found | false | none | true | artifact_output_not_found | request_human | 404 |
| artifact_outputs_missing | SHV-OBJ-PIP-VAL-0002 | pipeline | validation | error | Artifact outputs missing | false | none | true | artifact_outputs_missing | request_human | 400 |
| dataset_db_name_mismatch | SHV-OBJ-DAT-CON-0001 | data | conflict | error | Dataset database mismatch | false | none | true | dataset_db_name_mismatch | request_human | 409 |
| dataset_not_found | SHV-OBJ-DAT-NOT-0001 | data | not_found | error | Dataset not found | false | none | true | dataset_not_found | request_human | 404 |
| dataset_version_mismatch | SHV-OBJ-DAT-CON-0002 | data | conflict | error | Dataset version mismatch | false | none | true | dataset_version_mismatch | request_human | 409 |
| mapping_spec_dataset_mismatch | SHV-OBJ-MAP-CON-0001 | mapping | conflict | error | Mapping spec dataset mismatch | false | none | true | mapping_spec_dataset_mismatch | request_human | 409 |
| mapping_spec_not_found | SHV-OBJ-MAP-NOT-0001 | mapping | not_found | error | Mapping spec not found | false | none | true | mapping_spec_not_found | request_human | 404 |
| mapping_spec_version_mismatch | SHV-OBJ-MAP-CON-0002 | mapping | conflict | error | Mapping spec version mismatch | false | none | true | mapping_spec_version_mismatch | request_human | 409 |
| no_rows_loaded | SHV-OBJ-DAT-VAL-0002 | data | validation | error | No rows loaded | false | none | true | no_rows_loaded | request_human | 422 |
| no_valid_instances | SHV-OBJ-DAT-VAL-0003 | data | validation | error | No valid instances | false | none | true | no_valid_instances | request_human | 422 |
| objectify_input_conflict | SHV-OBJ-OBJ-CON-0001 | objectify | conflict | error | Objectify input conflict | false | none | true | objectify_input_conflict | request_human | 409 |
| objectify_input_missing | SHV-OBJ-OBJ-VAL-0001 | objectify | validation | error | Objectify input missing | false | none | true | objectify_input_missing | request_human | 400 |
| validation_failed | SHV-OBJ-DAT-VAL-0001 | data | validation | error | Data validation failed | false | none | true | validation_failed | request_human | 422 |

## External Codes

| Legacy code | Enterprise code | Domain | Class | Severity | Title | Retryable | Default retry policy | Human required | Runbook ref | Safe next actions | HTTP status hint |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BUILD_NOT_SUCCESS | SHV-{SUBSYS}-PIP-STA-1001 | pipeline | state | error | Build not successful | false | none | true | BUILD_NOT_SUCCESS | request_human | 409 |
| CARDINALITY_RECOMMENDATION | SHV-{SUBSYS}-ONT-VAL-1107 | ontology | validation | info | Cardinality recommendation | false | none | true | CARDINALITY_RECOMMENDATION | request_human | 400 |
| DUPLICATE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1118 | ontology | validation | error | Duplicate predicate | false | none | true | DUPLICATE_PREDICATE | request_human | 400 |
| DUPLICATE_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1117 | ontology | validation | error | Duplicate relationship | false | none | true | DUPLICATE_RELATIONSHIP | request_human | 400 |
| DUPLICATE_ROW_KEY | SHV-{SUBSYS}-DAT-CON-3002 | data | conflict | error | Duplicate row key | false | none | true | DUPLICATE_ROW_KEY | request_human | 409 |
| EMPTY_LABEL | SHV-{SUBSYS}-ONT-VAL-1112 | ontology | validation | warning | Relationship label empty | false | none | true | EMPTY_LABEL | request_human | 400 |
| EXTERNAL_CLASS_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1120 | ontology | validation | warning | External class reference | false | none | true | EXTERNAL_CLASS_REFERENCE | request_human | 400 |
| FULL_SYNC_FAILED | SHV-{SUBSYS}-OBJ-INT-3001 | objectify | internal | error | Full sync failed | false | none | true | FULL_SYNC_FAILED | request_human | 500 |
| GLOBAL_PREDICATE_CONFLICT | SHV-{SUBSYS}-ONT-VAL-1121 | ontology | validation | warning | Global predicate conflict | false | none | true | GLOBAL_PREDICATE_CONFLICT | request_human | 400 |
| IFACE_MISSING_PROPERTY | SHV-{SUBSYS}-ONT-VAL-1001 | ontology | validation | error | Interface missing property | false | none | true | IFACE_MISSING_PROPERTY | request_human | 400 |
| IFACE_MISSING_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1003 | ontology | validation | error | Interface missing relationship | false | none | true | IFACE_MISSING_RELATIONSHIP | request_human | 400 |
| IFACE_NOT_FOUND | SHV-{SUBSYS}-ONT-NOT-1002 | ontology | not_found | error | Interface not found | false | none | true | IFACE_NOT_FOUND | request_human | 404 |
| IFACE_PROPERTY_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1002 | ontology | validation | error | Interface property type mismatch | false | none | true | IFACE_PROPERTY_TYPE_MISMATCH | request_human | 400 |
| IFACE_REL_TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1004 | ontology | validation | error | Interface relationship target mismatch | false | none | true | IFACE_REL_TARGET_MISMATCH | request_human | 400 |
| INCOMPATIBLE_CARDINALITIES | SHV-{SUBSYS}-ONT-VAL-1113 | ontology | validation | error | Incompatible cardinalities | false | none | true | INCOMPATIBLE_CARDINALITIES | request_human | 400 |
| INVALID_BUILD_REF | SHV-{SUBSYS}-PIP-VAL-1003 | pipeline | validation | error | Invalid build reference | false | none | true | INVALID_BUILD_REF | request_human | 400 |
| INVALID_BUILD_REPOSITORY | SHV-{SUBSYS}-PIP-VAL-1002 | pipeline | validation | error | Invalid build repository | false | none | true | INVALID_BUILD_REPOSITORY | request_human | 400 |
| INVALID_CARDINALITY | SHV-{SUBSYS}-ONT-VAL-1106 | ontology | validation | error | Invalid cardinality | false | none | true | INVALID_CARDINALITY | request_human | 400 |
| INVALID_PREDICATE_FORMAT | SHV-{SUBSYS}-ONT-VAL-1104 | ontology | validation | error | Predicate format invalid | false | none | true | INVALID_PREDICATE_FORMAT | request_human | 400 |
| INVALID_TARGET_FORMAT | SHV-{SUBSYS}-ONT-VAL-1108 | ontology | validation | error | Target class format invalid | false | none | true | INVALID_TARGET_FORMAT | request_human | 400 |
| ISOLATED_CLASS | SHV-{SUBSYS}-ONT-VAL-1119 | ontology | validation | info | Isolated class | false | none | true | ISOLATED_CLASS | request_human | 400 |
| KEY_SPEC_MISMATCH | SHV-{SUBSYS}-PIP-CON-3003 | pipeline | conflict | error | Key spec mismatch | false | none | true | KEY_SPEC_MISMATCH | request_human | 409 |
| KEY_SPEC_MISSING | SHV-{SUBSYS}-PIP-CON-3002 | pipeline | conflict | error | Key spec missing | false | none | true | KEY_SPEC_MISSING | request_human | 409 |
| KEY_SPEC_PRIMARY_KEY_MISSING | SHV-{SUBSYS}-PIP-VAL-3001 | pipeline | validation | error | Key spec primary key missing | false | none | true | KEY_SPEC_PRIMARY_KEY_MISSING | request_human | 400 |
| KEY_SPEC_PRIMARY_KEY_TARGET_MISMATCH | SHV-{SUBSYS}-PIP-CON-3004 | pipeline | conflict | error | Key spec primary key target mismatch | false | none | true | KEY_SPEC_PRIMARY_KEY_TARGET_MISMATCH | request_human | 409 |
| LAKEFS_MERGE_CONFLICT | SHV-{SUBSYS}-PIP-CON-1007 | pipeline | conflict | error | LakeFS merge conflict | false | none | true | LAKEFS_MERGE_CONFLICT | request_human | 409 |
| LAKEFS_MERGE_FAILED | SHV-{SUBSYS}-PIP-INTG-1001 | pipeline | integration | error | LakeFS merge failed | false | none | true | LAKEFS_MERGE_FAILED | request_human | 502 |
| LINK_EDITS_DISABLED | SHV-{SUBSYS}-ONT-STA-3005 | ontology | state | error | Link edits disabled | false | none | true | LINK_EDITS_DISABLED | request_human | 409 |
| LINK_EDIT_FETCH_FAILED | SHV-{SUBSYS}-SYS-INTG-3001 | system | integration | error | Link edit fetch failed | false | none | true | LINK_EDIT_FETCH_FAILED | request_human | 502 |
| LINK_EDIT_PREDICATE_UNKNOWN | SHV-{SUBSYS}-ONT-VAL-3018 | ontology | validation | error | Link edit predicate unknown | false | none | true | LINK_EDIT_PREDICATE_UNKNOWN | request_human | 400 |
| LINK_EDIT_TARGET_INVALID | SHV-{SUBSYS}-DAT-VAL-3010 | data | validation | error | Link edit target invalid | false | none | true | LINK_EDIT_TARGET_INVALID | request_human | 400 |
| LINK_EDIT_TARGET_MISSING | SHV-{SUBSYS}-DAT-NOT-3002 | data | not_found | error | Link edit target missing | false | none | true | LINK_EDIT_TARGET_MISSING | request_human | 404 |
| LINK_EDIT_TYPE_INVALID | SHV-{SUBSYS}-INP-VAL-3005 | input | validation | error | Link edit type invalid | false | none | true | LINK_EDIT_TYPE_INVALID | request_human | 400 |
| LINK_TYPE_PREDICATE_MISSING | SHV-{SUBSYS}-ONT-VAL-3017 | ontology | validation | error | Link type predicate missing | false | none | true | LINK_TYPE_PREDICATE_MISSING | request_human | 400 |
| MAPPING_SPEC_BACKING_SCHEMA_MISMATCH | SHV-{SUBSYS}-MAP-CON-3001 | mapping | conflict | error | Mapping spec backing schema mismatch | false | none | true | MAPPING_SPEC_BACKING_SCHEMA_MISMATCH | request_human | 409 |
| MAPPING_SPEC_DATASET_MISMATCH | SHV-{SUBSYS}-MAP-CON-3002 | mapping | conflict | error | Mapping spec dataset mismatch | false | none | true | MAPPING_SPEC_DATASET_MISMATCH | request_human | 409 |
| MAPPING_SPEC_DATASET_PK_MISSING | SHV-{SUBSYS}-MAP-VAL-3009 | mapping | validation | error | Mapping spec dataset primary key missing | false | none | true | MAPPING_SPEC_DATASET_PK_MISSING | request_human | 400 |
| MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH | SHV-{SUBSYS}-MAP-VAL-3010 | mapping | validation | error | Mapping spec dataset primary key target mismatch | false | none | true | MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH | request_human | 400 |
| MAPPING_SPEC_MISMATCH | SHV-{SUBSYS}-MAP-CON-1001 | mapping | conflict | error | Mapping spec mismatch | false | none | true | MAPPING_SPEC_MISMATCH | request_human | 409 |
| MAPPING_SPEC_NOT_FOUND | SHV-{SUBSYS}-MAP-NOT-1001 | mapping | not_found | error | Mapping spec not found | false | none | true | MAPPING_SPEC_NOT_FOUND | request_human | 404 |
| MAPPING_SPEC_PRIMARY_KEY_MISMATCH | SHV-{SUBSYS}-MAP-VAL-3008 | mapping | validation | error | Mapping spec primary key mismatch | false | none | true | MAPPING_SPEC_PRIMARY_KEY_MISMATCH | request_human | 400 |
| MAPPING_SPEC_PRIMARY_KEY_MISSING | SHV-{SUBSYS}-MAP-VAL-3018 | mapping | validation | error | Mapping spec primary key missing | false | none | true | MAPPING_SPEC_PRIMARY_KEY_MISSING | request_human | 400 |
| MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED | SHV-{SUBSYS}-MAP-VAL-3014 | mapping | validation | error | Mapping spec relationship cardinality unsupported | false | none | true | MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED | request_human | 400 |
| MAPPING_SPEC_RELATIONSHIP_REQUIRED | SHV-{SUBSYS}-MAP-VAL-3013 | mapping | validation | error | Mapping spec relationship required | false | none | true | MAPPING_SPEC_RELATIONSHIP_REQUIRED | request_human | 400 |
| MAPPING_SPEC_REQUIRED_MISSING | SHV-{SUBSYS}-MAP-VAL-3005 | mapping | validation | error | Mapping spec required targets missing | false | none | true | MAPPING_SPEC_REQUIRED_MISSING | request_human | 400 |
| MAPPING_SPEC_REQUIRED_SOURCE_MISSING | SHV-{SUBSYS}-MAP-VAL-3004 | mapping | validation | error | Mapping spec required source missing | false | none | true | MAPPING_SPEC_REQUIRED_SOURCE_MISSING | request_human | 400 |
| MAPPING_SPEC_SOURCE_MISSING | SHV-{SUBSYS}-MAP-VAL-3003 | mapping | validation | error | Mapping spec source missing | false | none | true | MAPPING_SPEC_SOURCE_MISSING | request_human | 400 |
| MAPPING_SPEC_SOURCE_TYPE_UNKNOWN | SHV-{SUBSYS}-MAP-VAL-3011 | mapping | validation | error | Mapping spec source type unknown | false | none | true | MAPPING_SPEC_SOURCE_TYPE_UNKNOWN | request_human | 400 |
| MAPPING_SPEC_SOURCE_TYPE_UNSUPPORTED | SHV-{SUBSYS}-MAP-VAL-3012 | mapping | validation | error | Mapping spec source type unsupported | false | none | true | MAPPING_SPEC_SOURCE_TYPE_UNSUPPORTED | request_human | 400 |
| MAPPING_SPEC_TARGET_TYPE_MISMATCH | SHV-{SUBSYS}-MAP-VAL-3002 | mapping | validation | error | Mapping spec target type mismatch | false | none | true | MAPPING_SPEC_TARGET_TYPE_MISMATCH | request_human | 400 |
| MAPPING_SPEC_TARGET_UNKNOWN | SHV-{SUBSYS}-MAP-VAL-3001 | mapping | validation | error | Mapping spec target unknown | false | none | true | MAPPING_SPEC_TARGET_UNKNOWN | request_human | 400 |
| MAPPING_SPEC_TITLE_KEY_MISSING | SHV-{SUBSYS}-MAP-VAL-3006 | mapping | validation | error | Mapping spec title key missing | false | none | true | MAPPING_SPEC_TITLE_KEY_MISSING | request_human | 400 |
| MAPPING_SPEC_TYPE_INCOMPATIBLE | SHV-{SUBSYS}-MAP-VAL-3015 | mapping | validation | error | Mapping spec type incompatible | false | none | true | MAPPING_SPEC_TYPE_INCOMPATIBLE | request_human | 400 |
| MAPPING_SPEC_UNIQUE_KEY_MISSING | SHV-{SUBSYS}-MAP-VAL-3007 | mapping | validation | error | Mapping spec unique key missing | false | none | true | MAPPING_SPEC_UNIQUE_KEY_MISSING | request_human | 400 |
| MAPPING_SPEC_UNSUPPORTED_TYPE | SHV-{SUBSYS}-MAP-VAL-3016 | mapping | validation | error | Mapping spec unsupported type | false | none | true | MAPPING_SPEC_UNSUPPORTED_TYPE | request_human | 400 |
| MAPPING_SPEC_UNSUPPORTED_TYPES | SHV-{SUBSYS}-MAP-VAL-3017 | mapping | validation | error | Mapping spec unsupported types | false | none | true | MAPPING_SPEC_UNSUPPORTED_TYPES | request_human | 400 |
| MERGE_CONFLICT | SHV-{SUBSYS}-PIP-CON-1002 | pipeline | conflict | error | Merge conflict | false | none | true | MERGE_CONFLICT | request_human | 409 |
| MERGE_NOT_SUPPORTED | SHV-{SUBSYS}-PIP-VAL-1001 | pipeline | validation | error | Merge not supported | false | none | true | MERGE_NOT_SUPPORTED | request_human | 400 |
| MISMATCHED_INVERSE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1115 | ontology | validation | error | Inverse predicate mismatch | false | none | true | MISMATCHED_INVERSE_PREDICATE | request_human | 400 |
| MISSING_LABEL | SHV-{SUBSYS}-ONT-VAL-1103 | ontology | validation | warning | Relationship label missing | false | none | true | MISSING_LABEL | request_human | 400 |
| MISSING_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1101 | ontology | validation | error | Relationship predicate missing | false | none | true | MISSING_PREDICATE | request_human | 400 |
| MISSING_TARGET | SHV-{SUBSYS}-ONT-VAL-1102 | ontology | validation | error | Relationship target missing | false | none | true | MISSING_TARGET | request_human | 400 |
| NO_RELATIONSHIPS_INDEXED | SHV-{SUBSYS}-OBJ-STA-3001 | objectify | state | error | No relationships indexed | false | none | true | NO_RELATIONSHIPS_INDEXED | request_human | 409 |
| OBJECT_TYPE_AUTO_MAPPING_EMPTY | SHV-{SUBSYS}-ONT-VAL-3014 | ontology | validation | error | Object type auto mapping empty | false | none | true | OBJECT_TYPE_AUTO_MAPPING_EMPTY | request_human | 400 |
| OBJECT_TYPE_BACKING_ARTIFACT_REQUIRED | SHV-{SUBSYS}-ONT-VAL-3015 | ontology | validation | error | Object type backing artifact required | false | none | true | OBJECT_TYPE_BACKING_ARTIFACT_REQUIRED | request_human | 400 |
| OBJECT_TYPE_BACKING_DATASET_MISMATCH | SHV-{SUBSYS}-ONT-CON-3005 | ontology | conflict | error | Object type backing dataset mismatch | false | none | true | OBJECT_TYPE_BACKING_DATASET_MISMATCH | request_human | 409 |
| OBJECT_TYPE_BACKING_MISMATCH | SHV-{SUBSYS}-ONT-CON-3004 | ontology | conflict | error | Object type backing source mismatch | false | none | true | OBJECT_TYPE_BACKING_MISMATCH | request_human | 409 |
| OBJECT_TYPE_BACKING_VERSION_REQUIRED | SHV-{SUBSYS}-ONT-VAL-3016 | ontology | validation | error | Object type backing version required | false | none | true | OBJECT_TYPE_BACKING_VERSION_REQUIRED | request_human | 400 |
| OBJECT_TYPE_CONTRACT_MISSING | SHV-{SUBSYS}-ONT-NOT-3004 | ontology | not_found | error | Object type contract missing | false | none | true | OBJECT_TYPE_CONTRACT_MISSING | request_human | 404 |
| OBJECT_TYPE_EDIT_RESET_REQUIRED | SHV-{SUBSYS}-ONT-STA-3003 | ontology | state | error | Object type edit reset required | false | none | true | OBJECT_TYPE_EDIT_RESET_REQUIRED | request_human | 409 |
| OBJECT_TYPE_INACTIVE | SHV-{SUBSYS}-ONT-STA-3002 | ontology | state | error | Object type inactive | false | none | true | OBJECT_TYPE_INACTIVE | request_human | 409 |
| OBJECT_TYPE_KEY_FIELDS_MISSING | SHV-{SUBSYS}-ONT-VAL-3012 | ontology | validation | error | Object type key fields missing | false | none | true | OBJECT_TYPE_KEY_FIELDS_MISSING | request_human | 400 |
| OBJECT_TYPE_MAPPING_SPEC_REQUIRED | SHV-{SUBSYS}-ONT-VAL-3013 | ontology | validation | error | Object type mapping spec required | false | none | true | OBJECT_TYPE_MAPPING_SPEC_REQUIRED | request_human | 400 |
| OBJECT_TYPE_MIGRATION_REQUIRED | SHV-{SUBSYS}-ONT-STA-3004 | ontology | state | error | Object type migration required | false | none | true | OBJECT_TYPE_MIGRATION_REQUIRED | request_human | 409 |
| OBJECT_TYPE_PRIMARY_KEY_MISMATCH | SHV-{SUBSYS}-ONT-CON-3002 | ontology | conflict | error | Object type primary key mismatch | false | none | true | OBJECT_TYPE_PRIMARY_KEY_MISMATCH | request_human | 409 |
| OBJECT_TYPE_PRIMARY_KEY_MISSING | SHV-{SUBSYS}-ONT-VAL-3010 | ontology | validation | error | Object type primary key missing | false | none | true | OBJECT_TYPE_PRIMARY_KEY_MISSING | request_human | 400 |
| OBJECT_TYPE_SCHEMA_HASH_MISMATCH | SHV-{SUBSYS}-ONT-CON-3003 | ontology | conflict | error | Object type schema hash mismatch | false | none | true | OBJECT_TYPE_SCHEMA_HASH_MISMATCH | request_human | 409 |
| OBJECT_TYPE_TITLE_KEY_MISSING | SHV-{SUBSYS}-ONT-VAL-3011 | ontology | validation | error | Object type title key missing | false | none | true | OBJECT_TYPE_TITLE_KEY_MISSING | request_human | 400 |
| ONTOLOGY_GATE_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-1001 | upstream | unavailable | error | Ontology gate unavailable | true | backoff | false | ONTOLOGY_GATE_UNAVAILABLE | retry_backoff | 503 |
| ONTOLOGY_SCHEMA_MISSING | SHV-{SUBSYS}-ONT-NOT-3001 | ontology | not_found | error | Ontology schema missing | false | none | true | ONTOLOGY_SCHEMA_MISSING | request_human | 404 |
| ONTOLOGY_VERSION_MISMATCH | SHV-{SUBSYS}-ONT-CON-1002 | ontology | conflict | error | Ontology version mismatch | false | none | true | ONTOLOGY_VERSION_MISMATCH | request_human | 409 |
| ONTOLOGY_VERSION_UNKNOWN | SHV-{SUBSYS}-ONT-NOT-1001 | ontology | not_found | error | Ontology version unknown | false | none | true | ONTOLOGY_VERSION_UNKNOWN | request_human | 404 |
| PIPELINE_ALREADY_EXISTS | SHV-{SUBSYS}-PIP-CON-1001 | pipeline | conflict | error | Pipeline already exists | false | none | true | PIPELINE_ALREADY_EXISTS | request_human | 409 |
| PIPELINE_DEFINITION_INVALID | SHV-{SUBSYS}-PIP-VAL-2002 | pipeline | validation | error | Pipeline definition invalid | false | none | true | PIPELINE_DEFINITION_INVALID | request_human | 422 |
| PIPELINE_EXECUTION_FAILED | SHV-{SUBSYS}-PIP-INT-2001 | pipeline | internal | error | Pipeline execution failed | false | none | true | PIPELINE_EXECUTION_FAILED | request_human | 500 |
| PIPELINE_EXPECTATIONS_FAILED | SHV-{SUBSYS}-DAT-VAL-2003 | data | validation | error | Pipeline expectations failed | false | none | true | PIPELINE_EXPECTATIONS_FAILED | request_human | 422 |
| PIPELINE_PREFLIGHT_FAILED | SHV-{SUBSYS}-PIP-CON-3001 | pipeline | conflict | error | Pipeline preflight failed | false | none | true | PIPELINE_PREFLIGHT_FAILED | request_human | 409 |
| PIPELINE_REQUEST_INVALID | SHV-{SUBSYS}-PIP-VAL-2001 | pipeline | validation | error | Pipeline request invalid | false | none | true | PIPELINE_REQUEST_INVALID | request_human | 422 |
| PIPELINE_SCHEDULE_INVALID | SHV-{SUBSYS}-PIP-VAL-2003 | pipeline | validation | error | Pipeline schedule configuration invalid | false | none | true | PIPELINE_SCHEDULE_INVALID | request_human | 422 |
| PIPELINE_SCHEMA_CHECK_FAILED | SHV-{SUBSYS}-DAT-VAL-2001 | data | validation | error | Pipeline schema checks failed | false | none | true | PIPELINE_SCHEMA_CHECK_FAILED | request_human | 422 |
| PIPELINE_SCHEMA_CONTRACT_FAILED | SHV-{SUBSYS}-DAT-VAL-2002 | data | validation | error | Pipeline schema contract failed | false | none | true | PIPELINE_SCHEMA_CONTRACT_FAILED | request_human | 422 |
| PREDICATE_NAMING_CONVENTION | SHV-{SUBSYS}-ONT-VAL-1105 | ontology | validation | info | Predicate naming convention | false | none | true | PREDICATE_NAMING_CONVENTION | request_human | 400 |
| PRIMARY_KEY_DUPLICATE | SHV-{SUBSYS}-DAT-CON-3001 | data | conflict | error | Primary key duplicate | false | none | true | PRIMARY_KEY_DUPLICATE | request_human | 409 |
| PRIMARY_KEY_MAPPING_INVALID | SHV-{SUBSYS}-MAP-VAL-3020 | mapping | validation | error | Primary key mapping invalid | false | none | true | PRIMARY_KEY_MAPPING_INVALID | request_human | 400 |
| PRIMARY_KEY_MISSING | SHV-{SUBSYS}-DAT-VAL-3001 | data | validation | error | Primary key missing | false | none | true | PRIMARY_KEY_MISSING | request_human | 400 |
| PROPOSAL_ARTIFACT_MISMATCH | SHV-{SUBSYS}-PIP-CON-1004 | pipeline | conflict | error | Proposal artifact mismatch | false | none | true | PROPOSAL_ARTIFACT_MISMATCH | request_human | 409 |
| PROPOSAL_BUILD_MISMATCH | SHV-{SUBSYS}-PIP-CON-1003 | pipeline | conflict | error | Proposal build mismatch | false | none | true | PROPOSAL_BUILD_MISMATCH | request_human | 409 |
| PROPOSAL_DEFINITION_MISMATCH | SHV-{SUBSYS}-PIP-CON-1005 | pipeline | conflict | error | Proposal definition mismatch | false | none | true | PROPOSAL_DEFINITION_MISMATCH | request_human | 409 |
| PROPOSAL_LAKEFS_MISMATCH | SHV-{SUBSYS}-PIP-CON-1006 | pipeline | conflict | error | Proposal LakeFS mismatch | false | none | true | PROPOSAL_LAKEFS_MISMATCH | request_human | 409 |
| PROPOSAL_ONTOLOGY_MISMATCH | SHV-{SUBSYS}-ONT-CON-1001 | ontology | conflict | error | Proposal ontology mismatch | false | none | true | PROPOSAL_ONTOLOGY_MISMATCH | request_human | 409 |
| RELATIONSHIP_CARDINALITY_VIOLATION | SHV-{SUBSYS}-DAT-CON-3005 | data | conflict | error | Relationship cardinality violation | false | none | true | RELATIONSHIP_CARDINALITY_VIOLATION | request_human | 409 |
| RELATIONSHIP_DUPLICATE | SHV-{SUBSYS}-DAT-CON-3004 | data | conflict | error | Duplicate relationship | false | none | true | RELATIONSHIP_DUPLICATE | request_human | 409 |
| RELATIONSHIP_FK_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3006 | ontology | conflict | error | Relationship FK type mismatch | false | none | true | RELATIONSHIP_FK_TYPE_MISMATCH | request_human | 409 |
| RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3008 | ontology | conflict | error | Relationship join source type mismatch | false | none | true | RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH | request_human | 409 |
| RELATIONSHIP_JOIN_TARGET_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3009 | ontology | conflict | error | Relationship join target type mismatch | false | none | true | RELATIONSHIP_JOIN_TARGET_TYPE_MISMATCH | request_human | 409 |
| RELATIONSHIP_MAPPING_MISSING | SHV-{SUBSYS}-MAP-VAL-3021 | mapping | validation | error | Relationship mapping missing | false | none | true | RELATIONSHIP_MAPPING_MISSING | request_human | 400 |
| RELATIONSHIP_REF_INVALID | SHV-{SUBSYS}-DAT-VAL-3007 | data | validation | error | Relationship reference invalid | false | none | true | RELATIONSHIP_REF_INVALID | request_human | 400 |
| RELATIONSHIP_SOURCE_PK_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3007 | ontology | conflict | error | Relationship source PK type mismatch | false | none | true | RELATIONSHIP_SOURCE_PK_TYPE_MISMATCH | request_human | 409 |
| RELATIONSHIP_TARGET_LOOKUP_FAILED | SHV-{SUBSYS}-UPS-INTG-3001 | upstream | integration | error | Relationship target lookup failed | false | none | true | RELATIONSHIP_TARGET_LOOKUP_FAILED | request_human | 502 |
| RELATIONSHIP_TARGET_MISSING | SHV-{SUBSYS}-DAT-NOT-3001 | data | not_found | error | Relationship target missing | false | none | true | RELATIONSHIP_TARGET_MISSING | request_human | 404 |
| RELATIONSHIP_VALUE_INVALID | SHV-{SUBSYS}-DAT-VAL-3009 | data | validation | error | Relationship value invalid | false | none | true | RELATIONSHIP_VALUE_INVALID | request_human | 400 |
| RELATIONSHIP_VALUE_MISSING | SHV-{SUBSYS}-DAT-VAL-3008 | data | validation | error | Relationship value missing | false | none | true | RELATIONSHIP_VALUE_MISSING | request_human | 400 |
| REL_CARDINALITY_RECOMMENDATION | SHV-{SUBSYS}-ONT-VAL-1107 | ontology | validation | info | Cardinality recommendation | false | none | true | REL_CARDINALITY_RECOMMENDATION | request_human | 400 |
| REL_DUPLICATE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1118 | ontology | validation | error | Duplicate predicate | false | none | true | REL_DUPLICATE_PREDICATE | request_human | 400 |
| REL_DUPLICATE_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1117 | ontology | validation | error | Duplicate relationship | false | none | true | REL_DUPLICATE_RELATIONSHIP | request_human | 400 |
| REL_EMPTY_LABEL | SHV-{SUBSYS}-ONT-VAL-1112 | ontology | validation | warning | Relationship label empty | false | none | true | REL_EMPTY_LABEL | request_human | 400 |
| REL_EXTERNAL_CLASS_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1120 | ontology | validation | warning | External class reference | false | none | true | REL_EXTERNAL_CLASS_REFERENCE | request_human | 400 |
| REL_GLOBAL_PREDICATE_CONFLICT | SHV-{SUBSYS}-ONT-VAL-1121 | ontology | validation | warning | Global predicate conflict | false | none | true | REL_GLOBAL_PREDICATE_CONFLICT | request_human | 400 |
| REL_INCOMPATIBLE_CARDINALITIES | SHV-{SUBSYS}-ONT-VAL-1113 | ontology | validation | error | Incompatible cardinalities | false | none | true | REL_INCOMPATIBLE_CARDINALITIES | request_human | 400 |
| REL_INVALID_CARDINALITY | SHV-{SUBSYS}-ONT-VAL-1106 | ontology | validation | error | Invalid cardinality | false | none | true | REL_INVALID_CARDINALITY | request_human | 400 |
| REL_INVALID_PREDICATE_FORMAT | SHV-{SUBSYS}-ONT-VAL-1104 | ontology | validation | error | Predicate format invalid | false | none | true | REL_INVALID_PREDICATE_FORMAT | request_human | 400 |
| REL_INVALID_TARGET_FORMAT | SHV-{SUBSYS}-ONT-VAL-1108 | ontology | validation | error | Target class format invalid | false | none | true | REL_INVALID_TARGET_FORMAT | request_human | 400 |
| REL_ISOLATED_CLASS | SHV-{SUBSYS}-ONT-VAL-1119 | ontology | validation | info | Isolated class | false | none | true | REL_ISOLATED_CLASS | request_human | 400 |
| REL_MISMATCHED_INVERSE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1115 | ontology | validation | error | Inverse predicate mismatch | false | none | true | REL_MISMATCHED_INVERSE_PREDICATE | request_human | 400 |
| REL_MISSING_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1101 | ontology | validation | error | Relationship predicate missing | false | none | true | REL_MISSING_PREDICATE | request_human | 400 |
| REL_MISSING_TARGET | SHV-{SUBSYS}-ONT-VAL-1102 | ontology | validation | error | Relationship target missing | false | none | true | REL_MISSING_TARGET | request_human | 400 |
| REL_PREDICATE_NAMING_CONVENTION | SHV-{SUBSYS}-ONT-VAL-1105 | ontology | validation | info | Predicate naming convention | false | none | true | REL_PREDICATE_NAMING_CONVENTION | request_human | 400 |
| REL_SELF_REFERENCE_DETECTED | SHV-{SUBSYS}-ONT-VAL-1111 | ontology | validation | info | Self reference detected | false | none | true | REL_SELF_REFERENCE_DETECTED | request_human | 400 |
| REL_SELF_REFERENCE_ONE_TO_ONE | SHV-{SUBSYS}-ONT-VAL-1110 | ontology | validation | warning | Self reference one-to-one | false | none | true | REL_SELF_REFERENCE_ONE_TO_ONE | request_human | 400 |
| REL_TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1116 | ontology | validation | error | Relationship target mismatch | false | none | true | REL_TARGET_MISMATCH | request_human | 400 |
| REL_UNKNOWN_TARGET_CLASS | SHV-{SUBSYS}-ONT-VAL-1109 | ontology | validation | warning | Unknown target class | false | none | true | REL_UNKNOWN_TARGET_CLASS | request_human | 400 |
| REL_UNUSUAL_CARDINALITY_PAIR | SHV-{SUBSYS}-ONT-VAL-1114 | ontology | validation | warning | Unusual cardinality pair | false | none | true | REL_UNUSUAL_CARDINALITY_PAIR | request_human | 400 |
| REPLAY_REQUIRED | SHV-{SUBSYS}-PIP-STA-1002 | pipeline | state | error | Pipeline replay required | false | none | true | REPLAY_REQUIRED | request_human | 409 |
| RES001 | SHV-{SUBSYS}-ONT-VAL-1007 | ontology | validation | error | Resource required fields missing | false | none | true | RES001 | request_human | 400 |
| RES002 | SHV-{SUBSYS}-ONT-VAL-1008 | ontology | validation | error | Resource missing references | false | none | true | RES002 | request_human | 400 |
| RESOURCE_MISSING_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1008 | ontology | validation | error | Resource missing references | false | none | true | RESOURCE_MISSING_REFERENCE | request_human | 400 |
| RESOURCE_OBJECT_TYPE_CONTRACT_MISSING | SHV-{SUBSYS}-ONT-VAL-1006 | ontology | validation | error | Object type contract missing | false | none | true | RESOURCE_OBJECT_TYPE_CONTRACT_MISSING | request_human | 400 |
| RESOURCE_SPEC_INVALID | SHV-{SUBSYS}-ONT-VAL-1005 | ontology | validation | error | Resource spec invalid | false | none | true | RESOURCE_SPEC_INVALID | request_human | 400 |
| RESOURCE_UNUSED | SHV-{SUBSYS}-ONT-VAL-1009 | ontology | validation | warning | Resource unused | false | none | true | RESOURCE_UNUSED | request_human | 400 |
| ROW_KEY_MISSING | SHV-{SUBSYS}-DAT-VAL-3002 | data | validation | error | Row key missing | false | none | true | ROW_KEY_MISSING | request_human | 400 |
| SELF_REFERENCE_DETECTED | SHV-{SUBSYS}-ONT-VAL-1111 | ontology | validation | info | Self reference detected | false | none | true | SELF_REFERENCE_DETECTED | request_human | 400 |
| SELF_REFERENCE_ONE_TO_ONE | SHV-{SUBSYS}-ONT-VAL-1110 | ontology | validation | warning | Self reference one-to-one | false | none | true | SELF_REFERENCE_ONE_TO_ONE | request_human | 400 |
| SHEET_NOT_ACCESSIBLE | SHV-{SUBSYS}-UPS-UNA-2001 | upstream | unavailable | error | Google Sheet not accessible | true | backoff | false | SHEET_NOT_ACCESSIBLE | retry_backoff | 503 |
| SOURCE_FIELD_MISSING | SHV-{SUBSYS}-DAT-VAL-3004 | data | validation | error | Source field missing | false | none | true | SOURCE_FIELD_MISSING | request_human | 400 |
| SOURCE_FIELD_UNKNOWN | SHV-{SUBSYS}-DAT-VAL-3005 | data | validation | error | Source field unknown | false | none | true | SOURCE_FIELD_UNKNOWN | request_human | 400 |
| TARGET_FIELD_UNKNOWN | SHV-{SUBSYS}-DAT-VAL-3006 | data | validation | error | Target field unknown | false | none | true | TARGET_FIELD_UNKNOWN | request_human | 400 |
| TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1116 | ontology | validation | error | Relationship target mismatch | false | none | true | TARGET_MISMATCH | request_human | 400 |
| UNIQUE_KEY_DUPLICATE | SHV-{SUBSYS}-DAT-CON-3003 | data | conflict | error | Unique key duplicate | false | none | true | UNIQUE_KEY_DUPLICATE | request_human | 409 |
| UNKNOWN_TARGET_CLASS | SHV-{SUBSYS}-ONT-VAL-1109 | ontology | validation | warning | Unknown target class | false | none | true | UNKNOWN_TARGET_CLASS | request_human | 400 |
| UNUSUAL_CARDINALITY_PAIR | SHV-{SUBSYS}-ONT-VAL-1114 | ontology | validation | warning | Unusual cardinality pair | false | none | true | UNUSUAL_CARDINALITY_PAIR | request_human | 400 |
| VALUE_CONSTRAINT_FAILED | SHV-{SUBSYS}-DAT-VAL-3003 | data | validation | error | Value constraint failed | false | none | true | VALUE_CONSTRAINT_FAILED | request_human | 400 |
| VALUE_TYPE_BASE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3001 | ontology | conflict | error | Value type base mismatch | false | none | true | VALUE_TYPE_BASE_MISMATCH | request_human | 409 |
| VALUE_TYPE_NOT_FOUND | SHV-{SUBSYS}-ONT-NOT-3002 | ontology | not_found | error | Value type not found | false | none | true | VALUE_TYPE_NOT_FOUND | request_human | 404 |
| action_implementation_compile_error | SHV-{SUBSYS}-ONT-VAL-3004 | ontology | validation | error | Action implementation compile error | false | none | true | action_implementation_compile_error | request_human | 400 |
| action_implementation_invalid | SHV-{SUBSYS}-ONT-VAL-3003 | ontology | validation | error | Action implementation invalid | false | none | true | action_implementation_invalid | request_human | 400 |
| action_input_invalid | SHV-{SUBSYS}-INP-VAL-3001 | input | validation | error | Action input invalid | false | none | true | action_input_invalid | request_human | 400 |
| action_no_targets | SHV-{SUBSYS}-ONT-VAL-3005 | ontology | validation | error | Action has no targets | false | none | true | action_no_targets | request_human | 400 |
| action_type_implementation_invalid | SHV-{SUBSYS}-ONT-VAL-3003 | ontology | validation | error | Action implementation invalid | false | none | true | action_type_implementation_invalid | request_human | 400 |
| action_type_input_schema_invalid | SHV-{SUBSYS}-ONT-VAL-3002 | ontology | validation | error | Action type input schema invalid | false | none | true | action_type_input_schema_invalid | request_human | 400 |
| action_type_missing_writeback_target | SHV-{SUBSYS}-ONT-VAL-3001 | ontology | validation | error | Action type writeback target missing | false | none | true | action_type_missing_writeback_target | request_human | 400 |
| action_type_not_found | SHV-{SUBSYS}-ONT-NOT-3003 | ontology | not_found | error | Action type not found | false | none | true | action_type_not_found | request_human | 404 |
| conflict_detected | SHV-{SUBSYS}-CNF-CON-3001 | conflict | conflict | error | Conflict detected | false | none | true | conflict_detected | request_human | 409 |
| context7_unavailable | SHV-{SUBSYS}-UPS-UNA-3001 | upstream | unavailable | error | Context7 unavailable | true | backoff | false | context7_unavailable | retry_backoff | 503 |
| data_access_denied | SHV-{SUBSYS}-ACC-PER-3001 | access | permission | error | Data access denied | false | none | true | data_access_denied | request_human | 403 |
| no_deployed_ontology | SHV-{SUBSYS}-ONT-STA-3001 | ontology | state | error | No deployed ontology | false | none | true | no_deployed_ontology | request_human | 409 |
| optimistic_concurrency_conflict | SHV-{SUBSYS}-CNF-CON-2001 | conflict | conflict | error | Optimistic concurrency conflict | false | none | true | optimistic_concurrency_conflict | request_human | 409 |
| overlay_degraded | SHV-{SUBSYS}-SYS-UNA-3001 | system | unavailable | error | Overlay degraded | false | none | true | overlay_degraded | safe_mode,request_human | 503 |
| rate_limiter_unavailable | SHV-{SUBSYS}-RAT-UNA-2001 | rate_limit | unavailable | error | Rate limiter unavailable | true | backoff | false | rate_limiter_unavailable | retry_backoff | 503 |
| reconciler_timeout | SHV-{SUBSYS}-SYS-TMO-2002 | system | timeout | error | Reconciler timed out | true | backoff | false | reconciler_timeout | retry_backoff | 504 |
| submission_criteria_error | SHV-{SUBSYS}-ONT-VAL-3007 | ontology | validation | error | Submission criteria invalid | false | none | true | submission_criteria_error | request_human | 400 |
| submission_criteria_failed | SHV-{SUBSYS}-ACC-PER-3004 | access | permission | error | Submission criteria failed | false | none | true | submission_criteria_failed | request_human | 403 |
| submission_criteria_missing_user | SHV-{SUBSYS}-INP-VAL-3003 | input | validation | error | Submission criteria missing user | false | none | true | submission_criteria_missing_user | request_human | 400 |
| submitted_by_required | SHV-{SUBSYS}-INP-VAL-3002 | input | validation | error | submitted_by required | false | none | true | submitted_by_required | request_human | 400 |
| timeout | SHV-{SUBSYS}-SYS-TMO-2001 | system | timeout | error | Operation timed out | true | backoff | false | timeout | retry_backoff | 504 |
| unknown_label_keys | SHV-{SUBSYS}-INP-VAL-2001 | input | validation | error | Unknown label keys | false | none | true | unknown_label_keys | request_human | 400 |
| validation_rule_error | SHV-{SUBSYS}-ONT-VAL-3008 | ontology | validation | error | Validation rule evaluation error | false | none | true | validation_rule_error | request_human | 400 |
| validation_rule_failed | SHV-{SUBSYS}-INP-VAL-3004 | input | validation | error | Validation rule failed | false | none | true | validation_rule_failed | request_human | 400 |
| validation_rule_invalid | SHV-{SUBSYS}-ONT-VAL-3006 | ontology | validation | error | Validation rules invalid | false | none | true | validation_rule_invalid | request_human | 400 |
| validation_rules_invalid | SHV-{SUBSYS}-ONT-VAL-3006 | ontology | validation | error | Validation rules invalid | false | none | true | validation_rules_invalid | request_human | 400 |
| writeback_acl_denied | SHV-{SUBSYS}-ACC-PER-3002 | access | permission | error | Writeback ACL denied | false | none | true | writeback_acl_denied | request_human | 403 |
| writeback_acl_misaligned | SHV-{SUBSYS}-ACC-PER-3003 | access | permission | error | Writeback ACL misaligned | false | none | true | writeback_acl_misaligned | request_human | 403 |
| writeback_acl_unverifiable | SHV-{SUBSYS}-ACC-UNA-3001 | access | unavailable | error | Writeback ACL unverifiable | true | backoff | false | writeback_acl_unverifiable | retry_backoff | 503 |
| writeback_enforced | SHV-{SUBSYS}-ACC-PER-3005 | access | permission | error | Writeback enforced | false | none | true | writeback_enforced | request_human | 403 |
| writeback_governance_unavailable | SHV-{SUBSYS}-ACC-UNA-3002 | access | unavailable | error | Writeback governance unavailable | true | backoff | false | writeback_governance_unavailable | retry_backoff | 503 |
