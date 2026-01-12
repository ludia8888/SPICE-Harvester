# Enterprise Error Taxonomy

> Updated: 2026-01-12

This file is auto-generated from `backend/shared/errors/enterprise_catalog.py`.
Run: `python scripts/generate_error_taxonomy.py`.

`SUBSYS` values: `BFF`, `OMS`, `OBJ`, `PIP`, `PRJ`, `CON`, `SHR`, `GEN`.

## Core API Errors (ErrorCode)

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| AUTH_EXPIRED | SHV-{SUBSYS}-ACC-AUT-0003 | access | auth | error | Authentication expired |
| AUTH_INVALID | SHV-{SUBSYS}-ACC-AUT-0002 | access | auth | error | Invalid authentication |
| AUTH_REQUIRED | SHV-{SUBSYS}-ACC-AUT-0001 | access | auth | error | Authentication required |
| CONFLICT | SHV-{SUBSYS}-CNF-CON-0001 | conflict | conflict | error | Conflict detected |
| DB_CONSTRAINT_VIOLATION | SHV-{SUBSYS}-DB-CON-0001 | database | conflict | error | Database constraint violation |
| DB_ERROR | SHV-{SUBSYS}-DB-INT-0001 | database | internal | critical | Database error |
| DB_TIMEOUT | SHV-{SUBSYS}-DB-TMO-0001 | database | timeout | critical | Database timeout |
| DB_UNAVAILABLE | SHV-{SUBSYS}-DB-UNA-0001 | database | unavailable | critical | Database unavailable |
| HTTP_ERROR | SHV-{SUBSYS}-SYS-INT-0002 | system | internal | error | HTTP error |
| INPUT_SANITIZATION_FAILED | SHV-{SUBSYS}-INP-SEC-0001 | input | security | error | Input rejected by security policy |
| INTERNAL_ERROR | SHV-{SUBSYS}-SYS-INT-0001 | system | internal | critical | Internal server error |
| JSON_DECODE_ERROR | SHV-{SUBSYS}-INP-VAL-0002 | input | validation | error | Invalid JSON payload |
| OMS_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0003 | upstream | unavailable | error | Upstream service unavailable |
| PAYLOAD_TOO_LARGE | SHV-{SUBSYS}-INP-LIM-0001 | input | limit | error | Payload too large |
| PERMISSION_DENIED | SHV-{SUBSYS}-ACC-PER-0001 | access | permission | error | Permission denied |
| RATE_LIMITED | SHV-{SUBSYS}-RAT-LIM-0001 | rate_limit | limit | error | Rate limit exceeded |
| REQUEST_VALIDATION_FAILED | SHV-{SUBSYS}-INP-VAL-0001 | input | validation | error | Request validation failed |
| RESOURCE_ALREADY_EXISTS | SHV-{SUBSYS}-RES-CON-0001 | resource | conflict | error | Resource already exists |
| RESOURCE_NOT_FOUND | SHV-{SUBSYS}-RES-NOT-0001 | resource | not_found | error | Resource not found |
| TERMINUS_CONFLICT | SHV-{SUBSYS}-UPS-CON-0001 | upstream | conflict | error | Upstream conflict |
| TERMINUS_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0002 | upstream | unavailable | error | Upstream service unavailable |
| UPSTREAM_ERROR | SHV-{SUBSYS}-UPS-INTG-0001 | upstream | integration | error | Upstream service error |
| UPSTREAM_TIMEOUT | SHV-{SUBSYS}-UPS-TMO-0001 | upstream | timeout | error | Upstream request timed out |
| UPSTREAM_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-0001 | upstream | unavailable | error | Upstream service unavailable |

## Objectify Job Errors

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| artifact_key_mismatch | SHV-OBJ-PIP-CON-0001 | pipeline | conflict | error | Artifact key mismatch |
| artifact_key_missing | SHV-OBJ-PIP-VAL-0001 | pipeline | validation | error | Artifact key missing |
| artifact_not_build | SHV-OBJ-PIP-STA-0002 | pipeline | state | error | Artifact not a build output |
| artifact_not_found | SHV-OBJ-PIP-NOT-0001 | pipeline | not_found | error | Artifact not found |
| artifact_not_success | SHV-OBJ-PIP-STA-0001 | pipeline | state | error | Artifact not successful |
| artifact_output_ambiguous | SHV-OBJ-PIP-CON-0002 | pipeline | conflict | error | Artifact output ambiguous |
| artifact_output_name_missing | SHV-OBJ-PIP-VAL-0003 | pipeline | validation | error | Artifact output name required |
| artifact_output_name_required | SHV-OBJ-PIP-VAL-0003 | pipeline | validation | error | Artifact output name required |
| artifact_output_not_found | SHV-OBJ-PIP-NOT-0002 | pipeline | not_found | error | Artifact output not found |
| artifact_outputs_missing | SHV-OBJ-PIP-VAL-0002 | pipeline | validation | error | Artifact outputs missing |
| dataset_db_name_mismatch | SHV-OBJ-DAT-CON-0001 | data | conflict | error | Dataset database mismatch |
| dataset_not_found | SHV-OBJ-DAT-NOT-0001 | data | not_found | error | Dataset not found |
| dataset_version_mismatch | SHV-OBJ-DAT-CON-0002 | data | conflict | error | Dataset version mismatch |
| mapping_spec_dataset_mismatch | SHV-OBJ-MAP-CON-0001 | mapping | conflict | error | Mapping spec dataset mismatch |
| mapping_spec_not_found | SHV-OBJ-MAP-NOT-0001 | mapping | not_found | error | Mapping spec not found |
| mapping_spec_version_mismatch | SHV-OBJ-MAP-CON-0002 | mapping | conflict | error | Mapping spec version mismatch |
| no_rows_loaded | SHV-OBJ-DAT-VAL-0002 | data | validation | error | No rows loaded |
| no_valid_instances | SHV-OBJ-DAT-VAL-0003 | data | validation | error | No valid instances |
| objectify_input_conflict | SHV-OBJ-OBJ-CON-0001 | objectify | conflict | error | Objectify input conflict |
| objectify_input_missing | SHV-OBJ-OBJ-VAL-0001 | objectify | validation | error | Objectify input missing |
| validation_failed | SHV-OBJ-DAT-VAL-0001 | data | validation | error | Data validation failed |

## External Codes

| Legacy code | Enterprise code | Domain | Class | Severity | Title |
| --- | --- | --- | --- | --- | --- |
| BUILD_NOT_SUCCESS | SHV-{SUBSYS}-PIP-STA-1001 | pipeline | state | error | Build not successful |
| CARDINALITY_RECOMMENDATION | SHV-{SUBSYS}-ONT-VAL-1107 | ontology | validation | info | Cardinality recommendation |
| DUPLICATE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1118 | ontology | validation | error | Duplicate predicate |
| DUPLICATE_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1117 | ontology | validation | error | Duplicate relationship |
| DUPLICATE_ROW_KEY | SHV-{SUBSYS}-DAT-CON-3002 | data | conflict | error | Duplicate row key |
| EMPTY_LABEL | SHV-{SUBSYS}-ONT-VAL-1112 | ontology | validation | warning | Relationship label empty |
| EXTERNAL_CLASS_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1120 | ontology | validation | warning | External class reference |
| FULL_SYNC_FAILED | SHV-{SUBSYS}-OBJ-INT-3001 | objectify | internal | error | Full sync failed |
| GLOBAL_PREDICATE_CONFLICT | SHV-{SUBSYS}-ONT-VAL-1121 | ontology | validation | warning | Global predicate conflict |
| IFACE_MISSING_PROPERTY | SHV-{SUBSYS}-ONT-VAL-1001 | ontology | validation | error | Interface missing property |
| IFACE_MISSING_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1003 | ontology | validation | error | Interface missing relationship |
| IFACE_NOT_FOUND | SHV-{SUBSYS}-ONT-NOT-1002 | ontology | not_found | error | Interface not found |
| IFACE_PROPERTY_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1002 | ontology | validation | error | Interface property type mismatch |
| IFACE_REL_TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1004 | ontology | validation | error | Interface relationship target mismatch |
| INCOMPATIBLE_CARDINALITIES | SHV-{SUBSYS}-ONT-VAL-1113 | ontology | validation | error | Incompatible cardinalities |
| INVALID_BUILD_REF | SHV-{SUBSYS}-PIP-VAL-1003 | pipeline | validation | error | Invalid build reference |
| INVALID_BUILD_REPOSITORY | SHV-{SUBSYS}-PIP-VAL-1002 | pipeline | validation | error | Invalid build repository |
| INVALID_CARDINALITY | SHV-{SUBSYS}-ONT-VAL-1106 | ontology | validation | error | Invalid cardinality |
| INVALID_PREDICATE_FORMAT | SHV-{SUBSYS}-ONT-VAL-1104 | ontology | validation | error | Predicate format invalid |
| INVALID_TARGET_FORMAT | SHV-{SUBSYS}-ONT-VAL-1108 | ontology | validation | error | Target class format invalid |
| ISOLATED_CLASS | SHV-{SUBSYS}-ONT-VAL-1119 | ontology | validation | info | Isolated class |
| KEY_SPEC_MISMATCH | SHV-{SUBSYS}-PIP-CON-3003 | pipeline | conflict | error | Key spec mismatch |
| KEY_SPEC_MISSING | SHV-{SUBSYS}-PIP-CON-3002 | pipeline | conflict | error | Key spec missing |
| KEY_SPEC_PRIMARY_KEY_MISSING | SHV-{SUBSYS}-PIP-VAL-3001 | pipeline | validation | error | Key spec primary key missing |
| KEY_SPEC_PRIMARY_KEY_TARGET_MISMATCH | SHV-{SUBSYS}-PIP-CON-3004 | pipeline | conflict | error | Key spec primary key target mismatch |
| LAKEFS_MERGE_CONFLICT | SHV-{SUBSYS}-PIP-CON-1007 | pipeline | conflict | error | LakeFS merge conflict |
| LAKEFS_MERGE_FAILED | SHV-{SUBSYS}-PIP-INTG-1001 | pipeline | integration | error | LakeFS merge failed |
| LINK_EDITS_DISABLED | SHV-{SUBSYS}-ONT-STA-3005 | ontology | state | error | Link edits disabled |
| LINK_EDIT_FETCH_FAILED | SHV-{SUBSYS}-SYS-INTG-3001 | system | integration | error | Link edit fetch failed |
| LINK_EDIT_PREDICATE_UNKNOWN | SHV-{SUBSYS}-ONT-VAL-3018 | ontology | validation | error | Link edit predicate unknown |
| LINK_EDIT_TARGET_INVALID | SHV-{SUBSYS}-DAT-VAL-3010 | data | validation | error | Link edit target invalid |
| LINK_EDIT_TARGET_MISSING | SHV-{SUBSYS}-DAT-NOT-3002 | data | not_found | error | Link edit target missing |
| LINK_EDIT_TYPE_INVALID | SHV-{SUBSYS}-INP-VAL-3005 | input | validation | error | Link edit type invalid |
| LINK_TYPE_PREDICATE_MISSING | SHV-{SUBSYS}-ONT-VAL-3017 | ontology | validation | error | Link type predicate missing |
| MAPPING_SPEC_BACKING_SCHEMA_MISMATCH | SHV-{SUBSYS}-MAP-CON-3001 | mapping | conflict | error | Mapping spec backing schema mismatch |
| MAPPING_SPEC_DATASET_MISMATCH | SHV-{SUBSYS}-MAP-CON-3002 | mapping | conflict | error | Mapping spec dataset mismatch |
| MAPPING_SPEC_DATASET_PK_MISSING | SHV-{SUBSYS}-MAP-VAL-3009 | mapping | validation | error | Mapping spec dataset primary key missing |
| MAPPING_SPEC_DATASET_PK_TARGET_MISMATCH | SHV-{SUBSYS}-MAP-VAL-3010 | mapping | validation | error | Mapping spec dataset primary key target mismatch |
| MAPPING_SPEC_MISMATCH | SHV-{SUBSYS}-MAP-CON-1001 | mapping | conflict | error | Mapping spec mismatch |
| MAPPING_SPEC_NOT_FOUND | SHV-{SUBSYS}-MAP-NOT-1001 | mapping | not_found | error | Mapping spec not found |
| MAPPING_SPEC_PRIMARY_KEY_MISMATCH | SHV-{SUBSYS}-MAP-VAL-3008 | mapping | validation | error | Mapping spec primary key mismatch |
| MAPPING_SPEC_PRIMARY_KEY_MISSING | SHV-{SUBSYS}-MAP-VAL-3018 | mapping | validation | error | Mapping spec primary key missing |
| MAPPING_SPEC_RELATIONSHIP_CARDINALITY_UNSUPPORTED | SHV-{SUBSYS}-MAP-VAL-3014 | mapping | validation | error | Mapping spec relationship cardinality unsupported |
| MAPPING_SPEC_RELATIONSHIP_REQUIRED | SHV-{SUBSYS}-MAP-VAL-3013 | mapping | validation | error | Mapping spec relationship required |
| MAPPING_SPEC_REQUIRED_MISSING | SHV-{SUBSYS}-MAP-VAL-3005 | mapping | validation | error | Mapping spec required targets missing |
| MAPPING_SPEC_REQUIRED_SOURCE_MISSING | SHV-{SUBSYS}-MAP-VAL-3004 | mapping | validation | error | Mapping spec required source missing |
| MAPPING_SPEC_SOURCE_MISSING | SHV-{SUBSYS}-MAP-VAL-3003 | mapping | validation | error | Mapping spec source missing |
| MAPPING_SPEC_SOURCE_TYPE_UNKNOWN | SHV-{SUBSYS}-MAP-VAL-3011 | mapping | validation | error | Mapping spec source type unknown |
| MAPPING_SPEC_SOURCE_TYPE_UNSUPPORTED | SHV-{SUBSYS}-MAP-VAL-3012 | mapping | validation | error | Mapping spec source type unsupported |
| MAPPING_SPEC_TARGET_TYPE_MISMATCH | SHV-{SUBSYS}-MAP-VAL-3002 | mapping | validation | error | Mapping spec target type mismatch |
| MAPPING_SPEC_TARGET_UNKNOWN | SHV-{SUBSYS}-MAP-VAL-3001 | mapping | validation | error | Mapping spec target unknown |
| MAPPING_SPEC_TITLE_KEY_MISSING | SHV-{SUBSYS}-MAP-VAL-3006 | mapping | validation | error | Mapping spec title key missing |
| MAPPING_SPEC_TYPE_INCOMPATIBLE | SHV-{SUBSYS}-MAP-VAL-3015 | mapping | validation | error | Mapping spec type incompatible |
| MAPPING_SPEC_UNIQUE_KEY_MISSING | SHV-{SUBSYS}-MAP-VAL-3007 | mapping | validation | error | Mapping spec unique key missing |
| MAPPING_SPEC_UNSUPPORTED_TYPE | SHV-{SUBSYS}-MAP-VAL-3016 | mapping | validation | error | Mapping spec unsupported type |
| MAPPING_SPEC_UNSUPPORTED_TYPES | SHV-{SUBSYS}-MAP-VAL-3017 | mapping | validation | error | Mapping spec unsupported types |
| MERGE_CONFLICT | SHV-{SUBSYS}-PIP-CON-1002 | pipeline | conflict | error | Merge conflict |
| MERGE_NOT_SUPPORTED | SHV-{SUBSYS}-PIP-VAL-1001 | pipeline | validation | error | Merge not supported |
| MISMATCHED_INVERSE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1115 | ontology | validation | error | Inverse predicate mismatch |
| MISSING_LABEL | SHV-{SUBSYS}-ONT-VAL-1103 | ontology | validation | warning | Relationship label missing |
| MISSING_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1101 | ontology | validation | error | Relationship predicate missing |
| MISSING_TARGET | SHV-{SUBSYS}-ONT-VAL-1102 | ontology | validation | error | Relationship target missing |
| NO_RELATIONSHIPS_INDEXED | SHV-{SUBSYS}-OBJ-STA-3001 | objectify | state | error | No relationships indexed |
| OBJECT_TYPE_AUTO_MAPPING_EMPTY | SHV-{SUBSYS}-ONT-VAL-3014 | ontology | validation | error | Object type auto mapping empty |
| OBJECT_TYPE_BACKING_ARTIFACT_REQUIRED | SHV-{SUBSYS}-ONT-VAL-3015 | ontology | validation | error | Object type backing artifact required |
| OBJECT_TYPE_BACKING_DATASET_MISMATCH | SHV-{SUBSYS}-ONT-CON-3005 | ontology | conflict | error | Object type backing dataset mismatch |
| OBJECT_TYPE_BACKING_MISMATCH | SHV-{SUBSYS}-ONT-CON-3004 | ontology | conflict | error | Object type backing source mismatch |
| OBJECT_TYPE_BACKING_VERSION_REQUIRED | SHV-{SUBSYS}-ONT-VAL-3016 | ontology | validation | error | Object type backing version required |
| OBJECT_TYPE_CONTRACT_MISSING | SHV-{SUBSYS}-ONT-NOT-3004 | ontology | not_found | error | Object type contract missing |
| OBJECT_TYPE_EDIT_RESET_REQUIRED | SHV-{SUBSYS}-ONT-STA-3003 | ontology | state | error | Object type edit reset required |
| OBJECT_TYPE_INACTIVE | SHV-{SUBSYS}-ONT-STA-3002 | ontology | state | error | Object type inactive |
| OBJECT_TYPE_KEY_FIELDS_MISSING | SHV-{SUBSYS}-ONT-VAL-3012 | ontology | validation | error | Object type key fields missing |
| OBJECT_TYPE_MAPPING_SPEC_REQUIRED | SHV-{SUBSYS}-ONT-VAL-3013 | ontology | validation | error | Object type mapping spec required |
| OBJECT_TYPE_MIGRATION_REQUIRED | SHV-{SUBSYS}-ONT-STA-3004 | ontology | state | error | Object type migration required |
| OBJECT_TYPE_PRIMARY_KEY_MISMATCH | SHV-{SUBSYS}-ONT-CON-3002 | ontology | conflict | error | Object type primary key mismatch |
| OBJECT_TYPE_PRIMARY_KEY_MISSING | SHV-{SUBSYS}-ONT-VAL-3010 | ontology | validation | error | Object type primary key missing |
| OBJECT_TYPE_SCHEMA_HASH_MISMATCH | SHV-{SUBSYS}-ONT-CON-3003 | ontology | conflict | error | Object type schema hash mismatch |
| OBJECT_TYPE_TITLE_KEY_MISSING | SHV-{SUBSYS}-ONT-VAL-3011 | ontology | validation | error | Object type title key missing |
| ONTOLOGY_GATE_UNAVAILABLE | SHV-{SUBSYS}-UPS-UNA-1001 | upstream | unavailable | error | Ontology gate unavailable |
| ONTOLOGY_SCHEMA_MISSING | SHV-{SUBSYS}-ONT-NOT-3001 | ontology | not_found | error | Ontology schema missing |
| ONTOLOGY_VERSION_MISMATCH | SHV-{SUBSYS}-ONT-CON-1002 | ontology | conflict | error | Ontology version mismatch |
| ONTOLOGY_VERSION_UNKNOWN | SHV-{SUBSYS}-ONT-NOT-1001 | ontology | not_found | error | Ontology version unknown |
| PIPELINE_ALREADY_EXISTS | SHV-{SUBSYS}-PIP-CON-1001 | pipeline | conflict | error | Pipeline already exists |
| PIPELINE_DEFINITION_INVALID | SHV-{SUBSYS}-PIP-VAL-2002 | pipeline | validation | error | Pipeline definition invalid |
| PIPELINE_EXECUTION_FAILED | SHV-{SUBSYS}-PIP-INT-2001 | pipeline | internal | error | Pipeline execution failed |
| PIPELINE_EXPECTATIONS_FAILED | SHV-{SUBSYS}-DAT-VAL-2003 | data | validation | error | Pipeline expectations failed |
| PIPELINE_PREFLIGHT_FAILED | SHV-{SUBSYS}-PIP-CON-3001 | pipeline | conflict | error | Pipeline preflight failed |
| PIPELINE_REQUEST_INVALID | SHV-{SUBSYS}-PIP-VAL-2001 | pipeline | validation | error | Pipeline request invalid |
| PIPELINE_SCHEDULE_INVALID | SHV-{SUBSYS}-PIP-VAL-2003 | pipeline | validation | error | Pipeline schedule configuration invalid |
| PIPELINE_SCHEMA_CHECK_FAILED | SHV-{SUBSYS}-DAT-VAL-2001 | data | validation | error | Pipeline schema checks failed |
| PIPELINE_SCHEMA_CONTRACT_FAILED | SHV-{SUBSYS}-DAT-VAL-2002 | data | validation | error | Pipeline schema contract failed |
| PREDICATE_NAMING_CONVENTION | SHV-{SUBSYS}-ONT-VAL-1105 | ontology | validation | info | Predicate naming convention |
| PRIMARY_KEY_DUPLICATE | SHV-{SUBSYS}-DAT-CON-3001 | data | conflict | error | Primary key duplicate |
| PRIMARY_KEY_MAPPING_INVALID | SHV-{SUBSYS}-MAP-VAL-3020 | mapping | validation | error | Primary key mapping invalid |
| PRIMARY_KEY_MISSING | SHV-{SUBSYS}-DAT-VAL-3001 | data | validation | error | Primary key missing |
| PROPOSAL_ARTIFACT_MISMATCH | SHV-{SUBSYS}-PIP-CON-1004 | pipeline | conflict | error | Proposal artifact mismatch |
| PROPOSAL_BUILD_MISMATCH | SHV-{SUBSYS}-PIP-CON-1003 | pipeline | conflict | error | Proposal build mismatch |
| PROPOSAL_DEFINITION_MISMATCH | SHV-{SUBSYS}-PIP-CON-1005 | pipeline | conflict | error | Proposal definition mismatch |
| PROPOSAL_LAKEFS_MISMATCH | SHV-{SUBSYS}-PIP-CON-1006 | pipeline | conflict | error | Proposal LakeFS mismatch |
| PROPOSAL_ONTOLOGY_MISMATCH | SHV-{SUBSYS}-ONT-CON-1001 | ontology | conflict | error | Proposal ontology mismatch |
| RELATIONSHIP_CARDINALITY_VIOLATION | SHV-{SUBSYS}-DAT-CON-3005 | data | conflict | error | Relationship cardinality violation |
| RELATIONSHIP_DUPLICATE | SHV-{SUBSYS}-DAT-CON-3004 | data | conflict | error | Duplicate relationship |
| RELATIONSHIP_FK_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3006 | ontology | conflict | error | Relationship FK type mismatch |
| RELATIONSHIP_JOIN_SOURCE_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3008 | ontology | conflict | error | Relationship join source type mismatch |
| RELATIONSHIP_JOIN_TARGET_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3009 | ontology | conflict | error | Relationship join target type mismatch |
| RELATIONSHIP_MAPPING_MISSING | SHV-{SUBSYS}-MAP-VAL-3021 | mapping | validation | error | Relationship mapping missing |
| RELATIONSHIP_REF_INVALID | SHV-{SUBSYS}-DAT-VAL-3007 | data | validation | error | Relationship reference invalid |
| RELATIONSHIP_SOURCE_PK_TYPE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3007 | ontology | conflict | error | Relationship source PK type mismatch |
| RELATIONSHIP_TARGET_LOOKUP_FAILED | SHV-{SUBSYS}-UPS-INTG-3001 | upstream | integration | error | Relationship target lookup failed |
| RELATIONSHIP_TARGET_MISSING | SHV-{SUBSYS}-DAT-NOT-3001 | data | not_found | error | Relationship target missing |
| RELATIONSHIP_VALUE_INVALID | SHV-{SUBSYS}-DAT-VAL-3009 | data | validation | error | Relationship value invalid |
| RELATIONSHIP_VALUE_MISSING | SHV-{SUBSYS}-DAT-VAL-3008 | data | validation | error | Relationship value missing |
| REL_CARDINALITY_RECOMMENDATION | SHV-{SUBSYS}-ONT-VAL-1107 | ontology | validation | info | Cardinality recommendation |
| REL_DUPLICATE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1118 | ontology | validation | error | Duplicate predicate |
| REL_DUPLICATE_RELATIONSHIP | SHV-{SUBSYS}-ONT-VAL-1117 | ontology | validation | error | Duplicate relationship |
| REL_EMPTY_LABEL | SHV-{SUBSYS}-ONT-VAL-1112 | ontology | validation | warning | Relationship label empty |
| REL_EXTERNAL_CLASS_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1120 | ontology | validation | warning | External class reference |
| REL_GLOBAL_PREDICATE_CONFLICT | SHV-{SUBSYS}-ONT-VAL-1121 | ontology | validation | warning | Global predicate conflict |
| REL_INCOMPATIBLE_CARDINALITIES | SHV-{SUBSYS}-ONT-VAL-1113 | ontology | validation | error | Incompatible cardinalities |
| REL_INVALID_CARDINALITY | SHV-{SUBSYS}-ONT-VAL-1106 | ontology | validation | error | Invalid cardinality |
| REL_INVALID_PREDICATE_FORMAT | SHV-{SUBSYS}-ONT-VAL-1104 | ontology | validation | error | Predicate format invalid |
| REL_INVALID_TARGET_FORMAT | SHV-{SUBSYS}-ONT-VAL-1108 | ontology | validation | error | Target class format invalid |
| REL_ISOLATED_CLASS | SHV-{SUBSYS}-ONT-VAL-1119 | ontology | validation | info | Isolated class |
| REL_MISMATCHED_INVERSE_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1115 | ontology | validation | error | Inverse predicate mismatch |
| REL_MISSING_PREDICATE | SHV-{SUBSYS}-ONT-VAL-1101 | ontology | validation | error | Relationship predicate missing |
| REL_MISSING_TARGET | SHV-{SUBSYS}-ONT-VAL-1102 | ontology | validation | error | Relationship target missing |
| REL_PREDICATE_NAMING_CONVENTION | SHV-{SUBSYS}-ONT-VAL-1105 | ontology | validation | info | Predicate naming convention |
| REL_SELF_REFERENCE_DETECTED | SHV-{SUBSYS}-ONT-VAL-1111 | ontology | validation | info | Self reference detected |
| REL_SELF_REFERENCE_ONE_TO_ONE | SHV-{SUBSYS}-ONT-VAL-1110 | ontology | validation | warning | Self reference one-to-one |
| REL_TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1116 | ontology | validation | error | Relationship target mismatch |
| REL_UNKNOWN_TARGET_CLASS | SHV-{SUBSYS}-ONT-VAL-1109 | ontology | validation | warning | Unknown target class |
| REL_UNUSUAL_CARDINALITY_PAIR | SHV-{SUBSYS}-ONT-VAL-1114 | ontology | validation | warning | Unusual cardinality pair |
| REPLAY_REQUIRED | SHV-{SUBSYS}-PIP-STA-1002 | pipeline | state | error | Pipeline replay required |
| RES001 | SHV-{SUBSYS}-ONT-VAL-1007 | ontology | validation | error | Resource required fields missing |
| RES002 | SHV-{SUBSYS}-ONT-VAL-1008 | ontology | validation | error | Resource missing references |
| RESOURCE_MISSING_REFERENCE | SHV-{SUBSYS}-ONT-VAL-1008 | ontology | validation | error | Resource missing references |
| RESOURCE_OBJECT_TYPE_CONTRACT_MISSING | SHV-{SUBSYS}-ONT-VAL-1006 | ontology | validation | error | Object type contract missing |
| RESOURCE_SPEC_INVALID | SHV-{SUBSYS}-ONT-VAL-1005 | ontology | validation | error | Resource spec invalid |
| RESOURCE_UNUSED | SHV-{SUBSYS}-ONT-VAL-1009 | ontology | validation | warning | Resource unused |
| ROW_KEY_MISSING | SHV-{SUBSYS}-DAT-VAL-3002 | data | validation | error | Row key missing |
| SELF_REFERENCE_DETECTED | SHV-{SUBSYS}-ONT-VAL-1111 | ontology | validation | info | Self reference detected |
| SELF_REFERENCE_ONE_TO_ONE | SHV-{SUBSYS}-ONT-VAL-1110 | ontology | validation | warning | Self reference one-to-one |
| SHEET_NOT_ACCESSIBLE | SHV-{SUBSYS}-UPS-UNA-2001 | upstream | unavailable | error | Google Sheet not accessible |
| SOURCE_FIELD_MISSING | SHV-{SUBSYS}-DAT-VAL-3004 | data | validation | error | Source field missing |
| SOURCE_FIELD_UNKNOWN | SHV-{SUBSYS}-DAT-VAL-3005 | data | validation | error | Source field unknown |
| TARGET_FIELD_UNKNOWN | SHV-{SUBSYS}-DAT-VAL-3006 | data | validation | error | Target field unknown |
| TARGET_MISMATCH | SHV-{SUBSYS}-ONT-VAL-1116 | ontology | validation | error | Relationship target mismatch |
| UNIQUE_KEY_DUPLICATE | SHV-{SUBSYS}-DAT-CON-3003 | data | conflict | error | Unique key duplicate |
| UNKNOWN_TARGET_CLASS | SHV-{SUBSYS}-ONT-VAL-1109 | ontology | validation | warning | Unknown target class |
| UNUSUAL_CARDINALITY_PAIR | SHV-{SUBSYS}-ONT-VAL-1114 | ontology | validation | warning | Unusual cardinality pair |
| VALUE_CONSTRAINT_FAILED | SHV-{SUBSYS}-DAT-VAL-3003 | data | validation | error | Value constraint failed |
| VALUE_TYPE_BASE_MISMATCH | SHV-{SUBSYS}-ONT-CON-3001 | ontology | conflict | error | Value type base mismatch |
| VALUE_TYPE_NOT_FOUND | SHV-{SUBSYS}-ONT-NOT-3002 | ontology | not_found | error | Value type not found |
| action_implementation_compile_error | SHV-{SUBSYS}-ONT-VAL-3004 | ontology | validation | error | Action implementation compile error |
| action_implementation_invalid | SHV-{SUBSYS}-ONT-VAL-3003 | ontology | validation | error | Action implementation invalid |
| action_input_invalid | SHV-{SUBSYS}-INP-VAL-3001 | input | validation | error | Action input invalid |
| action_no_targets | SHV-{SUBSYS}-ONT-VAL-3005 | ontology | validation | error | Action has no targets |
| action_type_implementation_invalid | SHV-{SUBSYS}-ONT-VAL-3003 | ontology | validation | error | Action implementation invalid |
| action_type_input_schema_invalid | SHV-{SUBSYS}-ONT-VAL-3002 | ontology | validation | error | Action type input schema invalid |
| action_type_missing_writeback_target | SHV-{SUBSYS}-ONT-VAL-3001 | ontology | validation | error | Action type writeback target missing |
| action_type_not_found | SHV-{SUBSYS}-ONT-NOT-3003 | ontology | not_found | error | Action type not found |
| conflict_detected | SHV-{SUBSYS}-CNF-CON-3001 | conflict | conflict | error | Conflict detected |
| context7_unavailable | SHV-{SUBSYS}-UPS-UNA-3001 | upstream | unavailable | error | Context7 unavailable |
| data_access_denied | SHV-{SUBSYS}-ACC-PER-3001 | access | permission | error | Data access denied |
| no_deployed_ontology | SHV-{SUBSYS}-ONT-STA-3001 | ontology | state | error | No deployed ontology |
| optimistic_concurrency_conflict | SHV-{SUBSYS}-CNF-CON-2001 | conflict | conflict | error | Optimistic concurrency conflict |
| overlay_degraded | SHV-{SUBSYS}-SYS-UNA-3001 | system | unavailable | error | Overlay degraded |
| rate_limiter_unavailable | SHV-{SUBSYS}-RAT-UNA-2001 | rate_limit | unavailable | error | Rate limiter unavailable |
| reconciler_timeout | SHV-{SUBSYS}-SYS-TMO-2002 | system | timeout | error | Reconciler timed out |
| submission_criteria_error | SHV-{SUBSYS}-ONT-VAL-3007 | ontology | validation | error | Submission criteria invalid |
| submission_criteria_failed | SHV-{SUBSYS}-ACC-PER-3004 | access | permission | error | Submission criteria failed |
| submission_criteria_missing_user | SHV-{SUBSYS}-INP-VAL-3003 | input | validation | error | Submission criteria missing user |
| submitted_by_required | SHV-{SUBSYS}-INP-VAL-3002 | input | validation | error | submitted_by required |
| timeout | SHV-{SUBSYS}-SYS-TMO-2001 | system | timeout | error | Operation timed out |
| unknown_label_keys | SHV-{SUBSYS}-INP-VAL-2001 | input | validation | error | Unknown label keys |
| validation_rule_error | SHV-{SUBSYS}-ONT-VAL-3008 | ontology | validation | error | Validation rule evaluation error |
| validation_rule_failed | SHV-{SUBSYS}-INP-VAL-3004 | input | validation | error | Validation rule failed |
| validation_rule_invalid | SHV-{SUBSYS}-ONT-VAL-3006 | ontology | validation | error | Validation rules invalid |
| validation_rules_invalid | SHV-{SUBSYS}-ONT-VAL-3006 | ontology | validation | error | Validation rules invalid |
| writeback_acl_denied | SHV-{SUBSYS}-ACC-PER-3002 | access | permission | error | Writeback ACL denied |
| writeback_acl_misaligned | SHV-{SUBSYS}-ACC-PER-3003 | access | permission | error | Writeback ACL misaligned |
| writeback_acl_unverifiable | SHV-{SUBSYS}-ACC-UNA-3001 | access | unavailable | error | Writeback ACL unverifiable |
| writeback_enforced | SHV-{SUBSYS}-ACC-PER-3005 | access | permission | error | Writeback enforced |
| writeback_governance_unavailable | SHV-{SUBSYS}-ACC-UNA-3002 | access | unavailable | error | Writeback governance unavailable |
