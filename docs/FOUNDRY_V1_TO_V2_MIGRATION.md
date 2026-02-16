# Foundry v1 -> v2 Migration Guide

## Scope
This guide covers read/query routes that now have Foundry-style v2 successors.

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

`{ontology}` is the ontology API name (usually same value as `db_name` in current deployments).

## Contract Changes
### Pagination
- v1: 일부 경로에서 base64 offset 토큰 사용
- v2: opaque `pageToken` 사용 (scope-bound + TTL)
- Action: page token을 직접 생성하지 말고, 이전 응답의 `nextPageToken`만 전달

### Errors
- v1: 서비스별 에러 포맷 혼재
- v2: Foundry envelope 고정
  - `{errorCode, errorName, errorInstanceId, parameters}`
- Action: `errorName` 기반 분기 추가 (`OntologyNotFound`, `ObjectTypeNotFound`, `LinkTypeNotFound`, `ObjectNotFound`, `LinkedObjectNotFound`)

### Query DSL
- v2는 `SearchJsonQueryV2` 중심
- `startsWith`는 deprecated alias이므로 신규 클라이언트는
  `containsAllTermsInOrderPrefixLastTerm` 계열 사용 권장

### Parameters
- v2 object read/search routes는 `branch` 외 `sdkPackageRid`, `sdkVersion`를 허용
- Action: SDK 기반 호출은 해당 파라미터를 전달 가능하도록 클라이언트 스키마 업데이트

## Recommended Cutover Steps
1. v2 라우트로 읽기/검색 요청을 먼저 전환
2. v1 page token 생성 로직 삭제
3. v2 에러 스키마(`errorName`) 기반 처리로 교체
4. 모니터링에서 v1 호출량을 0으로 수렴
5. Sunset 이전에 v1 경로 의존성 제거 완료
