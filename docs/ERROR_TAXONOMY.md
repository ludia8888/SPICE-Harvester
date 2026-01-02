# Enterprise Error Taxonomy

This file lists all structured error codes currently mapped in
`backend/shared/errors/enterprise_catalog.py`.

`SUBSYS` values: `BFF`, `OMS`, `OBJ`, `PIP`, `SHR`, `GEN`.

## Core API Errors (ErrorCode)

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| REQUEST_VALIDATION_FAILED | SHV-{SUBSYS}-INP-VAL-0001 | input | validation | error | Request validation failed |
| JSON_DECODE_ERROR | SHV-{SUBSYS}-INP-VAL-0002 | input | validation | error | Invalid JSON payload |
| INPUT_SANITIZATION_FAILED | SHV-{SUBSYS}-INP-SEC-0001 | input | security | error | Input rejected by security policy |
| PAYLOAD_TOO_LARGE | SHV-{SUBSYS}-INP-LIM-0001 | input | limit | error | Payload too large |
| AUTH_REQUIRED | SHV-{SUBSYS}-ACC-AUT-0001 | access | auth | error | Authentication required |
| AUTH_INVALID | SHV-{SUBSYS}-ACC-AUT-0002 | access | auth | error | Invalid authentication |
| AUTH_EXPIRED | SHV-{SUBSYS}-ACC-AUT-0003 | access | auth | error | Authentication expired |
| PERMISSION_DENIED | SHV-{SUBSYS}-ACC-PER-0001 | access | permission | error | Permission denied |
| RESOURCE_NOT_FOUND | SHV-{SUBSYS}-RES-NOT-0001 | resource | not_found | error | Resource not found |
| RESOURCE_ALREADY_EXISTS | SHV-{SUBSYS}-RES-CON-0001 | resource | conflict | error | Resource already exists |
| CONFLICT | SHV-{SUBSYS}-CNF-CON-0001 | conflict | conflict | error | Conflict detected |
| RATE_LIMITED | SHV-{SUBSYS}-RAT-LIM-0001 | rate_limit | limit | error | Rate limit exceeded |
| UPSTREAM_ERROR | SHV-{SUBSYS}-UPS-INTG-0001 | upstream | integration | error | Upstream service error |
| UPSTREAM_TIMEOUT | SHV-{SUBSYS}-UPS-TMO-0001 | upstream | timeout | error | Upstream request timed out |
| UPSTREAM_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0001 | upstream | unavailable | error | Upstream service unavailable |
| TERMINUS_CONFLICT | SHV-{SUBSYS}-UPS-CON-0001 | upstream | conflict | error | Upstream conflict |
| TERMINUS_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0002 | upstream | unavailable | error | Upstream service unavailable |
| OMS_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0003 | upstream | unavailable | error | Upstream service unavailable |
| DB_ERROR | SHV-{SUBSYS}-DB-INT-0001 | database | internal | critical | Database error |
| DB_UNAVAILABLE | SHV-{SUBSYS}-DB-UNA-0001 | database | unavailable | critical | Database unavailable |
| DB_TIMEOUT | SHV-{SUBSYS}-DB-TMO-0001 | database | timeout | critical | Database timeout |
| DB_CONSTRAINT_VIOLATION | SHV-{SUBSYS}-DB-CON-0001 | database | conflict | error | Database constraint violation |
| HTTP_ERROR | SHV-{SUBSYS}-SYS-INT-0002 | system | internal | error | HTTP error |
| INTERNAL_ERROR | SHV-{SUBSYS}-SYS-INT-0001 | system | internal | critical | Internal server error |

## Objectify Job Errors

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| dataset_not_found | SHV-OBJ-DAT-NOT-0001 | data | not_found | error | Dataset not found |
| dataset_db_name_mismatch | SHV-OBJ-DAT-CON-0001 | data | conflict | error | Dataset database mismatch |
| dataset_version_mismatch | SHV-OBJ-DAT-CON-0002 | data | conflict | error | Dataset version mismatch |
| objectify_input_conflict | SHV-OBJ-OBJ-CON-0001 | objectify | conflict | error | Objectify input conflict |
| objectify_input_missing | SHV-OBJ-OBJ-VAL-0001 | objectify | validation | error | Objectify input missing |
| artifact_key_missing | SHV-OBJ-PIP-VAL-0001 | pipeline | validation | error | Artifact key missing |
| artifact_key_mismatch | SHV-OBJ-PIP-CON-0001 | pipeline | conflict | error | Artifact key mismatch |
| mapping_spec_not_found | SHV-OBJ-MAP-NOT-0001 | mapping | not_found | error | Mapping spec not found |
| mapping_spec_dataset_mismatch | SHV-OBJ-MAP-CON-0001 | mapping | conflict | error | Mapping spec dataset mismatch |
| mapping_spec_version_mismatch | SHV-OBJ-MAP-CON-0002 | mapping | conflict | error | Mapping spec version mismatch |
| artifact_not_found | SHV-OBJ-PIP-NOT-0001 | pipeline | not_found | error | Artifact not found |
| artifact_not_success | SHV-OBJ-PIP-STA-0001 | pipeline | state | error | Artifact not successful |
| artifact_not_build | SHV-OBJ-PIP-STA-0002 | pipeline | state | error | Artifact not a build output |
| artifact_outputs_missing | SHV-OBJ-PIP-VAL-0002 | pipeline | validation | error | Artifact outputs missing |
| artifact_output_name_required | SHV-OBJ-PIP-VAL-0003 | pipeline | validation | error | Artifact output name required |
| artifact_output_name_missing | SHV-OBJ-PIP-VAL-0003 | pipeline | validation | error | Artifact output name required |
| artifact_output_not_found | SHV-OBJ-PIP-NOT-0002 | pipeline | not_found | error | Artifact output not found |
| artifact_output_ambiguous | SHV-OBJ-PIP-CON-0002 | pipeline | conflict | error | Artifact output ambiguous |
| validation_failed | SHV-OBJ-DAT-VAL-0001 | data | validation | error | Data validation failed |
| no_rows_loaded | SHV-OBJ-DAT-VAL-0002 | data | validation | error | No rows loaded |
| no_valid_instances | SHV-OBJ-DAT-VAL-0003 | data | validation | error | No valid instances |

## Pipeline / Build / Mapping External Codes

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| MAPPING_SPEC_NOT_FOUND | SHV-{SUBSYS}-MAP-NOT-1001 | mapping | not_found | error | Mapping spec not found |
| MAPPING_SPEC_MISMATCH | SHV-{SUBSYS}-MAP-CON-1001 | mapping | conflict | error | Mapping spec mismatch |
| PIPELINE_ALREADY_EXISTS | SHV-{SUBSYS}-PIP-CON-1001 | pipeline | conflict | error | Pipeline already exists |
| MERGE_NOT_SUPPORTED | SHV-{SUBSYS}-PIP-VAL-1001 | pipeline | validation | error | Merge not supported |
| MERGE_CONFLICT | SHV-{SUBSYS}-PIP-CON-1002 | pipeline | conflict | error | Merge conflict |
| BUILD_NOT_SUCCESS | SHV-{SUBSYS}-PIP-STA-1001 | pipeline | state | error | Build not successful |
| INVALID_BUILD_REPOSITORY | SHV-{SUBSYS}-PIP-VAL-1002 | pipeline | validation | error | Invalid build repository |
| INVALID_BUILD_REF | SHV-{SUBSYS}-PIP-VAL-1003 | pipeline | validation | error | Invalid build reference |
| REPLAY_REQUIRED | SHV-{SUBSYS}-PIP-STA-1002 | pipeline | state | error | Pipeline replay required |
| LAKEFS_MERGE_CONFLICT | SHV-{SUBSYS}-PIP-CON-1007 | pipeline | conflict | error | LakeFS merge conflict |
| LAKEFS_MERGE_FAILED | SHV-{SUBSYS}-PIP-INTG-1001 | pipeline | integration | error | LakeFS merge failed |
| PROPOSAL_BUILD_MISMATCH | SHV-{SUBSYS}-PIP-CON-1003 | pipeline | conflict | error | Proposal build mismatch |
| PROPOSAL_ARTIFACT_MISMATCH | SHV-{SUBSYS}-PIP-CON-1004 | pipeline | conflict | error | Proposal artifact mismatch |
| PROPOSAL_DEFINITION_MISMATCH | SHV-{SUBSYS}-PIP-CON-1005 | pipeline | conflict | error | Proposal definition mismatch |
| PROPOSAL_LAKEFS_MISMATCH | SHV-{SUBSYS}-PIP-CON-1006 | pipeline | conflict | error | Proposal LakeFS mismatch |
| PROPOSAL_ONTOLOGY_MISMATCH | SHV-{SUBSYS}-ONT-CON-1001 | ontology | conflict | error | Proposal ontology mismatch |
| ONTOLOGY_VERSION_UNKNOWN | SHV-{SUBSYS}-ONT-NOT-1001 | ontology | not_found | error | Ontology version unknown |
| ONTOLOGY_VERSION_MISMATCH | SHV-{SUBSYS}-ONT-CON-1002 | ontology | conflict | error | Ontology version mismatch |
| ONTOLOGY_GATE_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-1001 | upstream | unavailable | error | Ontology gate unavailable |

## Ontology Interface / Resource Codes

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| IFACE_NOT_FOUND | SHV-{SUBSYS}-ONT-NOT-1002 | ontology | not_found | error | Interface not found |
| IFACE_MISSING_PROPERTY | SHV-{SUBSYS}-ONT-VAL-1001 | ontology | validation | error | Interface missing property |
| IFACE_PROPERTY_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1002 | ontology | validation | error | Interface property type mismatch |
| IFACE_MISSING_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1003 | ontology | validation | error | Interface missing relationship |
| IFACE_REL_TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1004 | ontology | validation | error | Interface relationship target mismatch |
| RESOURCE_SPEC_INVALID | SHV-{SUBSYS}-ONT-VAL-1005 | ontology | validation | error | Resource spec invalid |
| RESOURCE_OBJECT_TYPE_CONTRACT_MISSING | SHV-{SUBSYS}-ONT-VAL-1006 | ontology | validation | error | Object type contract missing |
| RES001 | SHV-{SUBSYS}-ONT-VAL-1007 | ontology | validation | error | Resource required fields missing |
| RES002 | SHV-{SUBSYS}-ONT-VAL-1008 | ontology | validation | error | Resource missing references |
| RESOURCE_MISSING_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1008 | ontology | validation | error | Resource missing references |
| RESOURCE_UNUSED | SHV-{SUBSYS}-ONT-VAL-1009 | ontology | validation | warning | Resource unused |

## Ontology Relationship Validation Codes

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| MISSING_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1101 | ontology | validation | error | Relationship predicate missing |
| REL_MISSING_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1101 | ontology | validation | error | Relationship predicate missing |
| MISSING_TARGET | SHV-{SUBSYS}-ONT-VAL-1102 | ontology | validation | error | Relationship target missing |
| REL_MISSING_TARGET | SHV-{SUBSYS}-ONT-VAL-1102 | ontology | validation | error | Relationship target missing |
| MISSING_LABEL | SHV-{SUBSYS}-ONT-VAL-1103 | ontology | validation | warning | Relationship label missing |
| EMPTY_LABEL | SHV-{SUBSYS}-ONT-VAL-1112 | ontology | validation | warning | Relationship label empty |
| REL_EMPTY_LABEL | SHV-{SUBSYS}-ONT-VAL-1112 | ontology | validation | warning | Relationship label empty |
| INVALID_PREDICATE_FORMAT | SHV-{SUBSYS}-ONT-VAL-1104 | ontology | validation | error | Predicate format invalid |
| REL_INVALID_PREDICATE_FORMAT | SHV-{SUBSYS}-ONT-VAL-1104 | ontology | validation | error | Predicate format invalid |
| PREDICATE_NAMING_CONVENTION | SHV-{SUBSYS}-ONT-VAL-1105 | ontology | validation | info | Predicate naming convention |
| REL_PREDICATE_NAMING_CONVENTION | SHV-{SUBSYS}-ONT-VAL-1105 | ontology | validation | info | Predicate naming convention |
| INVALID_CARDINALITY | SHV-{SUBSYS}-ONT-VAL-1106 | ontology | validation | error | Invalid cardinality |
| REL_INVALID_CARDINALITY | SHV-{SUBSYS}-ONT-VAL-1106 | ontology | validation | error | Invalid cardinality |
| CARDINALITY_RECOMMENDATION | SHV-{SUBSYS}-ONT-VAL-1107 | ontology | validation | info | Cardinality recommendation |
| REL_CARDINALITY_RECOMMENDATION | SHV-{SUBSYS}-ONT-VAL-1107 | ontology | validation | info | Cardinality recommendation |
| INVALID_TARGET_FORMAT | SHV-{SUBSYS}-ONT-VAL-1108 | ontology | validation | error | Target class format invalid |
| REL_INVALID_TARGET_FORMAT | SHV-{SUBSYS}-ONT-VAL-1108 | ontology | validation | error | Target class format invalid |
| UNKNOWN_TARGET_CLASS | SHV-{SUBSYS}-ONT-VAL-1109 | ontology | validation | warning | Unknown target class |
| REL_UNKNOWN_TARGET_CLASS | SHV-{SUBSYS}-ONT-VAL-1109 | ontology | validation | warning | Unknown target class |
| SELF_REFERENCE_ONE_TO_ONE | SHV-{SUBSYS}-ONT-VAL-1110 | ontology | validation | warning | Self reference one-to-one |
| REL_SELF_REFERENCE_ONE_TO_ONE | SHV-{SUBSYS}-ONT-VAL-1110 | ontology | validation | warning | Self reference one-to-one |
| SELF_REFERENCE_DETECTED | SHV-{SUBSYS}-ONT-VAL-1111 | ontology | validation | info | Self reference detected |
| REL_SELF_REFERENCE_DETECTED | SHV-{SUBSYS}-ONT-VAL-1111 | ontology | validation | info | Self reference detected |
| INCOMPATIBLE_CARDINALITIES | SHV-{SUBSYS}-ONT-VAL-1113 | ontology | validation | error | Incompatible cardinalities |
| REL_INCOMPATIBLE_CARDINALITIES | SHV-{SUBSYS}-ONT-VAL-1113 | ontology | validation | error | Incompatible cardinalities |
| UNUSUAL_CARDINALITY_PAIR | SHV-{SUBSYS}-ONT-VAL-1114 | ontology | validation | warning | Unusual cardinality pair |
| REL_UNUSUAL_CARDINALITY_PAIR | SHV-{SUBSYS}-ONT-VAL-1114 | ontology | validation | warning | Unusual cardinality pair |
| MISMATCHED_INVERSE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1115 | ontology | validation | error | Inverse predicate mismatch |
| REL_MISMATCHED_INVERSE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1115 | ontology | validation | error | Inverse predicate mismatch |
| TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1116 | ontology | validation | error | Relationship target mismatch |
| REL_TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1116 | ontology | validation | error | Relationship target mismatch |
| DUPLICATE_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1117 | ontology | validation | error | Duplicate relationship |
| REL_DUPLICATE_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1117 | ontology | validation | error | Duplicate relationship |
| DUPLICATE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1118 | ontology | validation | error | Duplicate predicate |
| REL_DUPLICATE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1118 | ontology | validation | error | Duplicate predicate |
| ISOLATED_CLASS | SHV-{SUBSYS}-ONT-VAL-1119 | ontology | validation | info | Isolated class |
| REL_ISOLATED_CLASS | SHV-{SUBSYS}-ONT-VAL-1119 | ontology | validation | info | Isolated class |
| EXTERNAL_CLASS_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1120 | ontology | validation | warning | External class reference |
| REL_EXTERNAL_CLASS_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1120 | ontology | validation | warning | External class reference |
| GLOBAL_PREDICATE_CONFLICT | SHV-{SUBSYS}-ONT-VAL-1121 | ontology | validation | warning | Global predicate conflict |
| REL_GLOBAL_PREDICATE_CONFLICT | SHV-{SUBSYS}-ONT-VAL-1121 | ontology | validation | warning | Global predicate conflict |

## Operational / Integration Codes

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| unknown_label_keys | SHV-{SUBSYS}-INP-VAL-2001 | input | validation | error | Unknown label keys |
| optimistic_concurrency_conflict | SHV-{SUBSYS}-CNF-CON-2001 | conflict | conflict | error | Optimistic concurrency conflict |
| rate_limiter_unavailable | SHV-{SUBSYS}-RAT-UNA-2001 | rate_limit | unavailable | error | Rate limiter unavailable |
| timeout | SHV-{SUBSYS}-SYS-TMO-2001 | system | timeout | error | Operation timed out |
| reconciler_timeout | SHV-{SUBSYS}-SYS-TMO-2002 | system | timeout | error | Reconciler timed out |
| SHEET_NOT_ACCESSIBLE | SHV-{SUBSYS}-UPS-UNA-2001 | upstream | unavailable | error | Google Sheet not accessible |
