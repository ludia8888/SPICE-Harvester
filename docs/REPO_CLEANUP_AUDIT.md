# 레포 정리 감사(Audit) (레거시 / 중복)

> 날짜: 2025-12-18  
> 범위: SPICE-Harvester 모노레포 (`backend/`, `docs/`, `frontend/`, 루트 스크립트)

## 요약 (TL;DR)
- 레포에 잘못 커밋된 **가상환경 폴더 + 레거시 백업 소스**를 제거했습니다.
- 운영 사고를 유발할 수 있는 **“가짜 성공” CSV 임포트 Placeholder 엔드포인트**를 제거했습니다.
- Google Sheets 커넥터 라우팅을 **`/data-connectors/...`로 표준화**하고, 기존 `/connectors/...`는 **deprecated 별칭**으로 유지했습니다.
- 문서 드리프트: `docs/API_REFERENCE.md`의 깨진 ToC 항목(“Legacy Direct API”)을 제거했습니다.

## 이번 패스에서 실제 적용된 정리

### 1) 커밋된 virtualenv 폴더 (레포 오염)
- 제거: `backend/test_env/`, `backend/test_venv/`
- 재유입 방지: `backend/.gitignore`에 ignore 규칙 추가

왜 중요한가:
- OS/파이썬 버전에 따라 달라지는 바이너리/스텁이 섞여 들어오며, 코드 리뷰 노이즈/불필요한 충돌/보안 우려를 만듭니다.

### 2) 레거시 백업 소스 파일 (중복 코드 + 혼란스러운 엔트리포인트)
- 제거:
  - `backend/bff/main_legacy_backup.py`
  - `backend/bff/dependencies_legacy_backup.py`
  - `backend/instance_worker/_legacy_backup_20250813/`

왜 중요한가:
- “main 엔트리포인트/DI wiring”의 복사본이 남아 있으면, 잘못된 import/배포/문서 참조로 이어지기 쉽습니다.

### 3) 커넥터 라우트 prefix 중복 (“connectors” vs “data-connectors”)
- 수정: `backend/bff/routers/data_connector.py`
  - 표준 경로: `/data-connectors/google-sheets/...`
  - 레거시 별칭: `/connectors/google-sheets/...` (OpenAPI에서 `deprecated=True`)

왜 중요한가:
- 하나의 표준 계약(contract)을 유지해야 BFF↔Funnel↔프론트 간 “경로/스키마 불일치”가 줄어듭니다.

### 4) Placeholder CSV 임포트 엔드포인트 제거 (가짜 성공 응답)
- 제거 대상 (기존 위치: `backend/bff/routers/ontology.py`):
  - `POST /api/v1/import-csv-data`
  - `GET /api/v1/import-progress/{job_id}`

왜 중요한가:
- 실제 write 파이프라인(Event Store → Kafka → workers)을 타지 않는데도 “완료”처럼 응답하는 엔드포인트는 운영에서 매우 위험합니다.
- 사용자/운영자는 데이터가 들어간 줄 알지만 실제로는 Terminus/ES에 아무것도 반영되지 않는 “침묵 실패”가 됩니다.

### 5) 문서 드리프트 정리
- 수정: `docs/API_REFERENCE.md`에서 존재하지 않는 “Legacy Direct API” ToC 링크 제거

## 남아있는 레거시/중복 후보 (아직 미적용)

### A) 루트 레벨 임시 스크립트 (`debug_*.py`, `test_*.py`, `verify_*.py`)
예:
- `debug_terminusdb_api.py`, `debug_terminusdb_endpoints.py`
- `test_improved_structure.py`, `test_version_management.py`

리스크/판단:
- 서비스 런타임에 import되지 않는 경우가 대부분이라 **실행 리스크는 낮지만**, 레포 신뢰도/가독성 측면에서 **노이즈가 큼**.
- 권장: `scripts/devtools/`(또는 `docs/devtools/`)로 이동하고 “수동 실행/비운영”임을 명확히 표기.

### B) `backend/` 내부 임시 스크립트 (debug/fix runner가 제품 코드와 혼재)
예:
- `backend/debug_*.py` 다수
- `backend/fix_*.py`, `backend/final_*.py`, `backend/quick_*`

리스크/판단:
- 대부분 안전하지만 “어떤 스크립트가 공식인가?” 혼란을 유발.
- 권장: `backend/scripts/`로 재정리하고 하위 폴더를 분리:
  - `backend/scripts/devtools/`
  - `backend/scripts/migrations/`
  - `backend/scripts/ops/`
  그리고 문서에는 소수의 “공식 엔트리포인트”만 남기기.

### C) Docker compose 기준 불일치 (루트 vs `backend/docker-compose.yml`)
현 상태:
- README/주요 문서 기준: 루트 `docker-compose.full.yml`
- 일부 백엔드 문서/툴: `backend/docker-compose.yml`을 여전히 참조

리스크/판단:
- **중간**: 운영자가 다른 compose를 띄워서 포트/환경 변수가 어긋날 수 있습니다.
- 권장: compose 전략을 하나로 고정:
  - 추천: 루트 `docker-compose.full.yml` + 부분 파일(`docker-compose.databases.yml` 등)
  - `backend/docker-compose.yml`은 deprecated 처리 후 문서/툴 참조 제거

### D) “레거시 호환” 함수(삭제 가능성 있음) — 확인 후 정리
예(정의는 있으나 호출부 확인이 필요):
- `backend/shared/services/storage_service.py:create_storage_service_legacy`
- `backend/shared/services/redis_service.py:create_redis_service_legacy`
- `backend/shared/services/elasticsearch_service.py:create_elasticsearch_service_legacy`

리스크/판단:
- **중간~높음(운영 히스토리에 의존)**: S3에 옛 이벤트 포맷이 남아 있으면 replay/backfill에서 필요할 수 있음.
- 삭제 전 체크리스트:
  - 코드 내 호출부 0개 확인
  - 운영 데이터가 레거시 파싱 경로를 요구하지 않는지 확인
  - 필요하다면 “명시적 마이그레이션/백필 도구”로 대체

## 다음 정리 스프린트 권장 순서 (안전 우선)
1) 루트 `debug_*.py`/`test_*.py`를 `scripts/devtools/`로 이동 (기능 변경 없음).
2) `backend/` 임시 스크립트를 `backend/scripts/`로 재배치하고 문서 참조를 정리.
3) docker compose 기준을 하나로 고정하고, 레거시 참조를 deprecated/삭제.
4) `*_legacy` 팩토리/호환 경로는 “운영 데이터 의존성” 확인 후 단계적으로 제거.
