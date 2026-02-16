# Foundry v1 -> v2 Migration Guide

## Scope
This guide covers read/query routes and action execution routes that now have Foundry-style v2 successors.
It also documents the strict-compat rollout mode used to harden v2 wire/behavior parity without breaking legacy clients by default.

## Cross-Check Baseline (2026-02-16)
- Official docs baseline follows `docs/FOUNDRY_ALIGNMENT_CHECKLIST.md` reference URLs.
- `Get Ontology Full Metadata` canonical URL: `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/ontologies/get-ontology-full-metadata`
- `Search Json Query` canonical reference: `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/ontology-objects/search-objects` (request body contract)
- `Action Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/list-action-types`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type`
  - `https://www.palantir.com/docs/foundry/api/ontologies-v2-resources/action-types/get-action-type-by-rid`
- `Query Types` canonical references:
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/list-query-types`
  - `https://www.palantir.com/docs/foundry/api/v2/ontologies-v2-resources/query-types/get-query-type`
- Supplementary (non-authoritative) validation sources:
  - Foundry platform Python SDK: `https://github.com/palantir/foundry-platform-python`
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

## Endpoint Mapping
| v1 | v2 successor |
|---|---|
| `GET /api/v1/databases/{db_name}/ontology/object-types` | `GET /api/v2/ontologies/{ontology}/objectTypes` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{class_id}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes` |
| `GET /api/v1/databases/{db_name}/ontology/object-types/{object_type_api_name}/outgoing-link-types/{link_type_api_name}` | `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/outgoingLinkTypes/{linkType}` |
| `POST /api/v1/databases/{db_name}/query` | `POST /api/v2/ontologies/{ontology}/objects/{objectType}/search` |
| `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit` | `POST /api/v1/databases/{db_name}/actions/{action_type_id}/submit-batch` (single item allowed) |
| (new in v2-like actions) | `POST /api/v1/databases/{db_name}/actions/logs/{action_log_id}/undo` |
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

`{ontology}` is the ontology API name (usually same value as `db_name` in current deployments).

## Contract Changes
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
- Foundry 문서의 preview 엔드포인트(`interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`, `objectTypes/*/fullMetadata`)는 `preview=true` 호출을 전제로 함
- `GET /api/v2/ontologies/{ontology}/fullMetadata`는 `preview` 파라미터를 사용하지 않음 (official SDK contract 기준)
- 현재 구현은 레거시 호환을 위해 strict compat off에서는 preview 누락을 허용하고, strict compat on에서는 위 preview 엔드포인트에만 `preview=true`를 강제
- Foundry/Postgres 런타임에서는 legacy branch-management API(`/api/v1/databases/{db_name}/branches*`, `/api/v1/databases/{db_name}/ontology/branches`)에 의존하지 않음
- Action: 제안/배포/OCC 호출은 branch API 조회 대신 `branch:<name>` 토큰 기준으로 처리
- `GET /api/v2/ontologies/{ontology}/actionTypes`는 `pageSize`, `pageToken`, `branch`를 사용하고 `preview`/`sdk*` 파라미터는 사용하지 않음
- `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` 및 `/actionTypes/byRid/{actionTypeRid}`는 `branch`만 사용 (pagination/preview/sdks 미사용)
- `GET /api/v2/ontologies/{ontology}/queryTypes`는 `pageSize/pageToken`만 사용 (branch 미사용)
- `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}`는 `version`, `sdkPackageRid`, `sdkVersion`를 허용
- `GET /api/v2/ontologies/{ontology}/valueTypes`는 pagination 파라미터를 받지 않음
- Action: preview 엔드포인트 호출 클라이언트는 `preview=true`를 기본값으로 고정

### Full Metadata Shape
- `GET /api/v2/ontologies/{ontology}/fullMetadata` 응답은 top-level `ontology` 객체를 포함
- `branch` 필드는 strict compat 기본값 기준 `{"rid": "<branch>"}`를 사용
- 명시적 호환 모드(`ENABLE_FOUNDRY_V2_STRICT_COMPAT=false` + allowlist 미적용)에서만 `{"name": "<branch>"}` legacy shape가 허용됨
- `queryTypes` map key는 `VersionedQueryTypeApiName` (`{apiName}:{version}`) 형식 사용

### Strict Compat Rollout (P0 hardening)
- 목적: Foundry v2 wire/행동 계약을 기본값으로 강제하고, 필요 시에만 호환 모드로 완화
- 게이트:
  - `ENABLE_FOUNDRY_V2_STRICT_COMPAT` (global)
  - `FOUNDRY_V2_STRICT_COMPAT_DB_ALLOWLIST` (db allowlist, comma-separated)
  - 활성 조건: `global=true OR db_name in allowlist`
- strict mode에서 강화되는 핵심:
  - v2 object/link 응답 필수 필드 자동 보정
  - unresolved outgoing link type 처리 엄격화 (list에서 제외, get은 `404 LinkTypeNotFound`)
  - preview 엔드포인트(`interfaceTypes*`, `sharedPropertyTypes*`, `valueTypes*`, `objectTypes/*/fullMetadata`)는 `preview=true` 미지정 시 `400 INVALID_ARGUMENT` + `errorName=ApiFeaturePreviewUsageOnly`
  - ontology extension resource OCC 엄격화 (`expected_head_commit` 공백 `400 INVALID_ARGUMENT`, 불일치 `409 CONFLICT`)
- 기본값:
  - `ENABLE_FOUNDRY_V2_STRICT_COMPAT=true` (strict on)
  - 긴급 완화가 필요할 때만 `ENABLE_FOUNDRY_V2_STRICT_COMPAT=false` + allowlist 비움

### Action Execution (Foundry-style)
- Batch apply: `POST /actions/{actionType}/submit-batch`는 한 요청에 다수 액션을 제출하고 item별 결과를 반환
- Dependency trigger: batch item은 `dependencies`(`on`, `trigger_on`)로 선행 액션 완료 조건을 정의
- Undo contract: `POST /actions/logs/{actionLogId}/undo`는 OSv2 revert 계약으로 비동기 undo 액션을 생성
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
