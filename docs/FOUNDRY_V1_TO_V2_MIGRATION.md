# Foundry v1 -> v2 Migration Guide

## Scope
This guide covers read/query routes and action execution routes that now have Foundry-style v2 successors.
It also documents the strict-compat baseline used to harden v2 wire/behavior parity.

## Cross-Check Baseline (2026-02-16)
- Official docs baseline follows `docs/FOUNDRY_ALIGNMENT_CHECKLIST.md` reference URLs.
- API v2 overview/index: `https://www.palantir.com/docs/foundry/api/v2`
- `Get Ontology Full Metadata` canonical URL: `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontologies/get-ontology-full-metadata`
- `Search Json Query` canonical reference: `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/search-objects` (request body contract)
- `Outgoing Link Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/list-outgoing-link-types`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/object-types/get-outgoing-link-type`
- `Object Sets` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-multiple-object-types`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/load-object-set-objects-or-interfaces`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontology-object-sets/aggregate-object-set`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-object-sets/create-temporary-object-set/`
- `Connectivity Table Imports` canonical reference: `https://www.palantir.com/docs/foundry/api/v2/connectivity-v2-resources/table-imports/create-table-import`
- `Dataset Schema` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/datasets-v2-resources/datasets/get-dataset-schema`
  - `https://www.palantir.com/docs/foundry/api/datasets-v2-resources/datasets/put-dataset-schema`
- `Action Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/list-action-types`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type-by-rid`
- `Actions` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/actions/apply-action-batch`
- `Query Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/list-query-types`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/get-query-type`
- Supplementary (non-authoritative) validation sources:
  - Foundry platform Python SDK: `https://github.com/palantir/foundry-platform-python`
  - Foundry SDK object-set preview contract (`load_links`): `https://github.com/palantir/foundry-platform-python/blob/develop/foundry_sdk/v2/ontologies/ontology_object_set.py`
  - Palantir Developer Community: `https://community.palantir.com/`

## Deprecation Policy
- v2 successor가 있는 legacy read/query compat 엔드포인트는 코드에서 완전 제거되었습니다.
- 제거된 operation은 OpenAPI에서 노출되지 않으며, 런타임에서도 더 이상 제공되지 않습니다.
- 동일 path에 다른 method가 남아 있는 경우(`object-types`의 `POST/PUT`), 제거된 method 호출은 `405`로 종료될 수 있습니다.

### Removed v1 compatibility routes (code deleted)
These routes are fully deleted from runtime handlers and OpenAPI:
- `GET /api/v1/databases/{db_name}/ontology/object-types`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{class_id}`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types`
- `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}`
- `POST /api/v1/databases/{db_name}/query`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/simulate`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit`
- `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch`
- `POST /api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo`
- `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit`
- `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit-batch`
- `POST /api/v1/actions/{db_name}/async/{action_type_id}/simulate`
- `POST /api/v1/actions/{db_name}/async/logs/{action_log_id}/undo`
- `GET /api/v1/databases/{db_name}/ontology/link-types`
- `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}`
- `GET /api/v1/databases/{db_name}/ontology/action-types`
- `GET /api/v1/databases/{db_name}/ontology/action-types/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/interfaces`
- `GET /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/shared-properties`
- `GET /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}`
- `GET /api/v1/databases/{db_name}/ontology/value-types`
- `GET /api/v1/databases/{db_name}/ontology/value-types/{resource_id}`
- `GET /api/v1/databases/{db_name}/classes`
- `GET /api/v1/databases/{db_name}/classes/{class_id}`
- `GET /api/v1/databases/{db_name}/class/{class_id}/instances`
- `GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}`

## Endpoint Mapping
| v1 | v2 successor |
|---|---|
| `GET /api/v1/databases/{db_name}/ontology/object-types` | `GET /api/v2/ontologies/{ontology}/objectTypes` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{class_id}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}` |
| `POST /api/v1/databases/{db_name}/query` | `POST /api/v2/ontologies/{ontology}/objects/{objectType}/search` |
| `POST /api/v1/databases/{db_name}/actions/{action_type_id}/simulate` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` (`options.mode=VALIDATE_ONLY`) |
| `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` |
| `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` |
| `POST /api/v1/actions/{db_name}/async/{action_type_id}/submit-batch` | `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch` |
| `POST /api/v1/actions/{db_name}/async/{action_type_id}/simulate` | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` (`options.mode=VALIDATE_ONLY`) |
| `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch` | `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch` |
| `POST /api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo` | `POST /api/v2/ontologies/{ontology}/actions/logs/{actionLogId}/undo` |
| `GET /api/v1/databases/{db_name}/ontology/link-types` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes` |
| `GET /api/v1/databases/{db_name}/ontology/link-types/{link_type_id}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}` |
| `GET /api/v1/databases/{db_name}/ontology/action-types` | `GET /api/v2/ontologies/{ontology}/actionTypes` |
| `GET /api/v1/databases/{db_name}/ontology/action-types/{resource_id}` | `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` |
| `GET /api/v1/databases/{db_name}/ontology/interfaces` | `GET /api/v2/ontologies/{ontology}/interfaceTypes` |
| `GET /api/v1/databases/{db_name}/ontology/interfaces/{resource_id}` | `GET /api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}` |
| `GET /api/v1/databases/{db_name}/ontology/shared-properties` | `GET /api/v2/ontologies/{ontology}/sharedPropertyTypes` |
| `GET /api/v1/databases/{db_name}/ontology/shared-properties/{resource_id}` | `GET /api/v2/ontologies/{ontology}/sharedPropertyTypes/{sharedPropertyType}` |
| `GET /api/v1/databases/{db_name}/ontology/value-types` | `GET /api/v2/ontologies/{ontology}/valueTypes` |
| `GET /api/v1/databases/{db_name}/ontology/value-types/{resource_id}` | `GET /api/v2/ontologies/{ontology}/valueTypes/{valueType}` |
| `GET /api/v1/databases/{db_name}/classes` | `GET /api/v2/ontologies/{ontology}/objectTypes` |
| `GET /api/v1/databases/{db_name}/classes/{class_id}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}` |
| `GET /api/v1/databases/{db_name}/class/{class_id}/instances` | `GET /api/v2/ontologies/{ontology}/objects/{objectType}` |
| `GET /api/v1/databases/{db_name}/class/{class_id}/instance/{instance_id}` | `GET /api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/actions/{action}/apply` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch` |
| (new in v2-like actions) | `POST /api/v2/ontologies/{ontology}/actions/logs/{actionLogId}/undo` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/fullMetadata` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/queryTypes` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/interfaceTypes` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/valueTypes` |
| (new in v2, preview param supported) | `GET /api/v2/ontologies/{ontology}/valueTypes/{valueType}` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/objectSets/loadObjects` |
| (new in v2, preview param supported) | `POST /api/v2/ontologies/{ontology}/objectSets/loadLinks` |
| (new in v2, preview param supported) | `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes` |
| (new in v2, preview param supported) | `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/objectSets/aggregate` |
| (new in v2) | `POST /api/v2/ontologies/{ontology}/objectSets/createTemporary` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/objectSets/{objectSetRid}` |

`{ontology}` is the ontology API name (usually same value as `db_name` in current deployments).

## Contract Changes
### Foundry Surface Boundary
- Foundry 공식 문서 기준으로 tabular type inference / sheet structure analysis는 public REST contract가 아닙니다.
- tabular type inference runtime은 내부(in-process) 컴포넌트로 고정되며, external Funnel HTTP transport mode는 제거되었습니다.
- 기본 gateway/nginx 구성에서는 Funnel 전용 프록시 경로(`/api/funnel/*`, `/health/funnel`)를 노출하지 않습니다.
- compose/deploy/test 기본 경로에서도 별도 Funnel 서비스 부팅 단계가 존재하지 않습니다.
- Action: 외부 통합 계약은 `/api/v2/ontologies/*` 중심으로 유지하고, Funnel 계열 경로를 신규 공개 계약으로 추가하지 않습니다.

### Pagination
- v1: 일부 경로에서 base64 offset 토큰 사용
- v2: opaque `pageToken` 사용 (scope-bound + TTL)
- v2 token 재사용 시 `pageSize`/요청 파라미터가 동일해야 함
- Action: page token을 직접 생성하지 말고, 이전 응답의 `nextPageToken`만 전달

### Errors
- v1: 서비스별 에러 포맷 혼재
- v2: Foundry envelope 고정
  - `{errorCode, errorName, errorInstanceId, parameters}`
- 권한 실패는 `403 + PERMISSION_DENIED`, 입력 오류는 `400 + INVALID_ARGUMENT`
- Action: `errorName` 기반 분기 추가 (`OntologyNotFound`, `ObjectTypeNotFound`, `LinkTypeNotFound`, `ObjectNotFound`, `LinkedObjectNotFound`)

### Query DSL
- v2는 `SearchJsonQueryV2` 중심
- 텍스트 연산자는 `containsAnyTerm`, `containsAllTerms`, `containsAllTermsInOrder`, `containsAllTermsInOrderPrefixLastTerm` 사용
- `startsWith`는 Foundry 문서 기준 deprecated alias로 여전히 허용되지만 신규 코드는 비권장
- Action: 신규/마이그레이션 코드는 `containsAllTermsInOrderPrefixLastTerm`를 우선 사용

### Parameters
- v2 object read/search routes는 `branch` 외 `sdkPackageRid`, `sdkVersion`를 허용
- Action: SDK 기반 호출은 해당 파라미터를 전달 가능하도록 클라이언트 스키마 업데이트
- Foundry 문서의 preview 엔드포인트(`fullMetadata`, `interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`, `objectTypes/*/fullMetadata`)는 warning 기준 `preview=true` 호출을 전제로 함
- 현재 구현은 strict compat 고정 동작으로 preview 엔드포인트에서 `preview=true`를 항상 강제
- Foundry/Postgres 런타임에서는 legacy branch-management API(`/api/v1/databases/{db_name}/branches*`, `/api/v1/databases/{db_name}/ontology/branches`)에 의존하지 않음
- Action: 제안/배포/OCC 호출은 branch API 조회 대신 `branch:<name>` 토큰 기준으로 처리
- `GET /api/v2/ontologies/{ontology}/actionTypes`는 `pageSize`, `pageToken`, `branch`를 사용하고 `preview`/`sdk*` 파라미터는 사용하지 않음
- `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` 및 `/actionTypes/byRid/{actionTypeRid}`는 `branch`만 사용 (pagination/preview/sdks 미사용)
- `GET /api/v2/ontologies/{ontology}/queryTypes`는 `pageSize/pageToken`만 사용 (branch 미사용)
- `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}`는 `version`, `sdkPackageRid`, `sdkVersion`를 허용
- `GET /api/v2/ontologies/{ontology}/valueTypes`는 pagination 파라미터를 받지 않음
- `POST /api/v2/ontologies/{ontology}/actions/{action}/apply`는 query로 `branch`, `sdkPackageRid`, `sdkVersion`, `transactionId`를 사용하고, 실행 모드는 body `options.mode`로 제어
- `POST /api/v2/ontologies/{ontology}/actions/{action}/applyBatch`는 query로 `branch`, `sdkPackageRid`, `sdkVersion`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadObjects`는 query로 `branch`, `transactionId`, `sdkPackageRid`, `sdkVersion`를 사용하고 body로 `select/selectV2`, `pageSize/pageToken`, `orderBy`, `excludeRid`, `snapshot`, `includeComputeUsage`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadLinks`는 query로 `branch`, `preview`, `sdkPackageRid`, `sdkVersion`를 사용하고 body로 `objectSet`, `links`, `pageToken`, `includeComputeUsage`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsMultipleObjectTypes`는 query로 `branch`, `preview`, `transactionId`, `sdkPackageRid`, `sdkVersion`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/loadObjectsOrInterfaces`는 query로 `branch`, `preview`, `sdkPackageRid`, `sdkVersion`를 사용
- `POST /api/v2/ontologies/{ontology}/objectSets/aggregate`는 query로 `branch`, `transactionId`, `sdkPackageRid`, `sdkVersion`를 사용하고 body로 `aggregation`, `groupBy`, `accuracy`, `includeComputeUsage`를 사용
- Action: preview 엔드포인트 호출 클라이언트는 `preview=true`를 기본값으로 고정

### Full Metadata Shape
- `GET /api/v2/ontologies/{ontology}/fullMetadata` 응답은 top-level `ontology` 객체를 포함
- `branch` 필드는 `{"rid": "<branch>"}`를 사용
- `queryTypes` map key는 `VersionedQueryTypeApiName` (`{apiName}:{version}`) 형식 사용

### Strict Compat Baseline (P0 hardening)
- Foundry v2 strict wire/행동 계약은 런타임 기본이 아니라 고정 동작입니다(옵트아웃/완화 게이트 제거).
- 고정된 핵심 계약:
  - v2 object/link 응답 필수 필드 자동 보정
  - unresolved outgoing link type 처리 엄격화 (list에서 제외, get은 `404 LinkTypeNotFound`)
  - preview 엔드포인트(`fullMetadata`, `interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`, `objectTypes/*/fullMetadata`)는 `preview=true` 미지정 시 `400 INVALID_ARGUMENT` + `errorName=ApiFeaturePreviewUsageOnly`
  - ontology extension resource OCC 엄격화 (`expected_head_commit` 공백 `400 INVALID_ARGUMENT`, 불일치 `409 CONFLICT`)

### Action Execution (Foundry-style)
- Batch apply: `POST /v2/ontologies/{ontology}/actions/{action}/applyBatch`는 v2 공식 경로입니다.
- Dependency trigger: batch item은 `dependencies`(`on`, `trigger_on`)로 선행 액션 완료 조건을 정의
- Undo contract: `POST /actions/logs/{actionLogId}/undo`는 OSv2 revert 계약으로 비동기 undo 액션을 생성
- Runtime note: BFF -> OMS 내부 프록시도 `/api/v2/ontologies/{ontology}/actions/*` 경로를 사용하며, legacy `/api/v1/actions/{db_name}/async/*` 경로 의존은 제거되었습니다.
- Runtime note: live action writeback E2E suites도 OMS v1 async submit-batch 경로를 더 이상 호출하지 않으며, v2 `apply` 호출 + action log 조회 방식으로 동작합니다.
- 제약:
  - `trigger_on`은 `SUCCEEDED|FAILED|COMPLETED`만 허용
  - undo는 원본 ActionLog가 `SUCCEEDED`이고 patchset이 존재해야 함
  - delete 기반 액션은 undo 대상에서 제외됨

## Recommended Cutover Steps
1. v2 라우트로 읽기/검색 요청을 먼저 전환
2. v1 page token 생성 로직 삭제
3. v2 에러 스키마(`errorName`) 기반 처리로 교체
4. 모니터링에서 v1 호출량을 0으로 수렴
5. legacy v1 read/query 의존성 제거 완료
