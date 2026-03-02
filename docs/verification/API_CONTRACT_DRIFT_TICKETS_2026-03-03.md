# API Contract Drift Tickets (P0/P1) — 2026-03-03

> Source: `/Users/isihyeon/Desktop/SPICE-Harvester/docs/reference/_generated/API_DUPLICATE_LEGACY_INVENTORY.json` (Phase2)
> Context: high-risk runtime checks, HTTP-only validation

## Drift Snapshot (current)

- Phase2 summary: `12/12 checks`, `server_errors=0`, `contract_mismatch=5`, `fallback_4xx=5`
- Affected operations:
  - `GET /api/v1/commands/{command_id}/status` (BFF, OMS)
  - `POST /api/v2/ontologies/attachments/upload` (BFF, OMS)
  - `GET /api/v1/config/config/drift-analysis` (OMS)

## Ticket Matrix

| Ticket | Priority | Type | Scope | Status |
| --- | --- | --- | --- |
| SHV-API-DRIFT-001 | P0 | Handler fix | BFF command-status error passthrough | ✅ Done |
| SHV-API-DRIFT-002 | P0 | OpenAPI doc fix | BFF v2 attachments upload responses | ✅ Done |
| SHV-API-DRIFT-003 | P0 | OpenAPI doc fix | OMS v2 attachments upload responses | ✅ Done |
| SHV-API-DRIFT-004 | P1 | OpenAPI doc fix | OMS/BFF command-status documented responses | ✅ Done |
| SHV-API-DRIFT-005 | P1 | OpenAPI doc fix | OMS config drift-analysis documented responses | ✅ Done |

---

## SHV-API-DRIFT-001 (P0)

- **Type**: 실제 핸들러 보정
- **Title**: BFF command-status proxy가 upstream 404를 `UPSTREAM_ERROR`로 변환하는 문제 수정
- **Endpoint**: `GET /api/v1/commands/{command_id}/status` (BFF)
- **Evidence**
  - Runtime: HTTP `404` (정상)이나 body `code=UPSTREAM_ERROR`로 노출
  - OMS 원본은 동일 상황에서 `code=RESOURCE_NOT_FOUND`
  - 코드 위치: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/command_status.py:46`
- **판정**: 상태코드 드리프트는 아니지만, BFF가 의미코드를 손실해 계약 의미가 바뀜 (proxy_mirror 품질 저하)
- **Fix scope**
  - `httpx.HTTPStatusError` 처리에서 upstream error envelope를 보존(가능하면 `code`, `detail`, `category` 전달)
  - 최소한 404는 `RESOURCE_NOT_FOUND`로 매핑
  - OpenAPI responses도 400/404/503 명시
- **Acceptance**
  - 동일 UUID 미존재 요청 시 BFF 응답 `status=404`, `code=RESOURCE_NOT_FOUND`
  - `python scripts/api_audit -m phase2 --risk high --strict --strict-contract`에서 해당 엔드포인트 계약 미스매치 0
  - 관련 단위 테스트 추가/수정: BFF command status router test

## SHV-API-DRIFT-002 (P0)

- **Type**: OpenAPI 문서 보정
- **Title**: BFF v2 attachment upload가 실제 400(`InvalidArgument`)를 반환하지만 OpenAPI에는 422만 문서화됨
- **Endpoint**: `POST /api/v2/ontologies/attachments/upload` (BFF)
- **Evidence**
  - Runtime: missing `filename` 쿼리로 HTTP `400`, `errorCode=INVALID_ARGUMENT`
  - OpenAPI: responses `200,422`만 존재
  - 코드 위치: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/foundry_ontology_v2.py:7552`
- **판정**: Foundry v2 public surface 계약 문서 불일치 (P0)
- **Fix scope**
  - 라우트 데코레이터에 `responses` 명시: `400 InvalidArgument`, `401`, `403`, `500` (실제 동작 기준)
  - 가능하면 v2 공통 오류 스키마(`errorCode/errorName/errorInstanceId`) 참조 반영
- **Acceptance**
  - OpenAPI responses에 `400` 포함
  - Phase2 strict-contract에서 해당 endpoint mismatch 제거

## SHV-API-DRIFT-003 (P0)

- **Type**: OpenAPI 문서 보정
- **Title**: OMS v2 attachment upload가 실제 400(`InvalidArgument`)를 반환하지만 OpenAPI에는 422만 문서화됨
- **Endpoint**: `POST /api/v2/ontologies/attachments/upload` (OMS)
- **Evidence**
  - Runtime: missing `filename` 쿼리로 HTTP `400`, `errorCode=INVALID_ARGUMENT`
  - OpenAPI: responses `200,422`만 존재
  - 코드 위치: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/oms/routers/attachments.py:216`
- **판정**: Foundry v2 public surface 계약 문서 불일치 (P0)
- **Fix scope**
  - 라우트 데코레이터 `responses` 명시: `400 InvalidArgument`, `403`, `404`, `500`
  - 업로드 empty body 처리(`400 EmptyAttachment`)도 문서화
- **Acceptance**
  - OpenAPI responses에 `400` 포함
  - Phase2 strict-contract에서 해당 endpoint mismatch 제거

## SHV-API-DRIFT-004 (P1)

- **Type**: OpenAPI 문서 보정
- **Title**: Command status endpoint들의 404/503가 문서화되지 않음
- **Endpoints**
  - `GET /api/v1/commands/{command_id}/status` (OMS, BFF)
- **Evidence**
  - Runtime: 미존재 command는 `404`
  - OMS는 Redis/registry unavailable 시 `503` 경로 존재
  - OpenAPI는 `200,422`만 존재
  - 코드 위치:
    - OMS: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/oms/routers/command_status.py:73`
    - BFF: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/bff/routers/command_status.py:26`
- **판정**: v1 운영/관측 경로 문서 정확성 이슈
- **Fix scope**
  - 양쪽 라우트에 `responses`로 `400`, `404`, `503` 명시
  - BFF는 `502/503` upstream 실패 계약도 정리
- **Acceptance**
  - OpenAPI 문서에 404/503 노출
  - API audit strict-contract에서 command status mismatch 제거

## SHV-API-DRIFT-005 (P1)

- **Type**: OpenAPI 문서 보정
- **Title**: OMS config drift-analysis의 필수 query 누락 시 실제 400이지만 OpenAPI는 422로 고정
- **Endpoint**: `GET /api/v1/config/config/drift-analysis` (OMS)
- **Evidence**
  - Runtime: `compare_environment` 누락 시 HTTP `400` + `REQUEST_VALIDATION_FAILED`
  - OpenAPI: responses `200,422`
  - 코드 위치: `/Users/isihyeon/Desktop/SPICE-Harvester/backend/shared/routers/config_monitoring.py:336`
- **판정**: internal config API 문서 드리프트
- **Fix scope**
  - `responses`에 `400` 명시
  - 이 라우터를 공유하는 BFF/OMS 모두 문서 일치 확인
- **Acceptance**
  - OpenAPI responses에 `400` 포함
  - API audit strict-contract에서 해당 mismatch 제거

---

## Rollout Order (권장)

1. `SHV-API-DRIFT-001` (P0 handler)
2. `SHV-API-DRIFT-002`, `SHV-API-DRIFT-003` (P0 v2 문서)
3. `SHV-API-DRIFT-004`, `SHV-API-DRIFT-005` (P1 v1/internal 문서)

## Completion Snapshot

- `python scripts/api_audit -m phase2 --risk high --strict --strict-contract` 결과:
  - `checks=12/12`, `failed=0`, `server_errors=0`, `contract_mismatch=0`, `fallback_4xx=0`

## Verification Commands

```bash
python /Users/isihyeon/Desktop/SPICE-Harvester/scripts/api_audit -m phase1 --services bff,oms,agent --include-hidden --strict
python /Users/isihyeon/Desktop/SPICE-Harvester/scripts/api_audit -m phase2 --input /Users/isihyeon/Desktop/SPICE-Harvester/docs/reference/_generated/API_DUPLICATE_LEGACY_INVENTORY.json --risk high --strict --strict-contract
```
