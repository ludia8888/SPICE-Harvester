# Foundry v1 -> v2 Migration Guide

## Scope
This guide covers read/query routes and action execution routes that now have Foundry-style v2 successors.

## Deprecation Policy
- `Deprecation: true` header is returned on v1 compatibility routes.
- `Sunset: Sun, 28 Feb 2027 23:59:59 GMT` is returned with the same responses.
- `Link` header includes:
  - `rel="successor-version"`: v2 replacement route
  - `rel="deprecation"`: this guide

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
| (new in v2) | `GET /api/v2/ontologies/{ontology}/fullMetadata?preview=true` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/actionTypes/byRid/{actionTypeRid}` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/queryTypes` |
| (new in v2) | `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}` |
| (new in v2, preview) | `GET /api/v2/ontologies/{ontology}/interfaceTypes?preview=true` |
| (new in v2, preview) | `GET /api/v2/ontologies/{ontology}/interfaceTypes/{interfaceType}?preview=true` |
| (new in v2, preview) | `GET /api/v2/ontologies/{ontology}/valueTypes?preview=true` |
| (new in v2, preview) | `GET /api/v2/ontologies/{ontology}/valueTypes/{valueType}?preview=true` |

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
- `startsWith`(deprecated alias)는 런타임 허용 목록에서 제거됨
- Action: 기존 `startsWith` 사용 코드는 `containsAllTermsInOrderPrefixLastTerm`로 치환

### Parameters
- v2 object read/search routes는 `branch` 외 `sdkPackageRid`, `sdkVersion`를 허용
- Action: SDK 기반 호출은 해당 파라미터를 전달 가능하도록 클라이언트 스키마 업데이트
- `GET /api/v2/ontologies/{ontology}/fullMetadata`는 `preview=true`가 필수
- `GET /api/v2/ontologies/{ontology}/interfaceTypes*`, `GET /api/v2/ontologies/{ontology}/valueTypes*`는 `preview=true`가 필수
- `GET /api/v2/ontologies/{ontology}/queryTypes`는 `pageSize/pageToken`만 사용 (branch 미사용)
- `GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}`는 `version`, `sdkPackageRid`, `sdkVersion`를 허용
- `GET /api/v2/ontologies/{ontology}/valueTypes`는 pagination 파라미터 없이 `preview=true`만 사용
- Action: full metadata 호출 클라이언트는 preview 파라미터를 항상 명시

### Full Metadata Shape
- `GET /api/v2/ontologies/{ontology}/fullMetadata?preview=true` 응답은 top-level `ontology` 객체를 포함
- `queryTypes` map key는 `VersionedQueryTypeApiName` (`{apiName}:{version}`) 형식 사용

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
5. Sunset 이전에 v1 경로 의존성 제거 완료
