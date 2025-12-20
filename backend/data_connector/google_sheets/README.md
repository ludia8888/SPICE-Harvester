# Google Sheets Connector for SPICE HARVESTER

Google Sheets를 SPICE HARVESTER 온톨로지 시스템과 연동하는 커넥터입니다.

## 🚀 주요 기능

- **URL 기반 연결**: Google Sheets 공유 URL만으로 간편하게 연결
- **데이터 미리보기**: 컬럼 헤더와 샘플 데이터 추출
- **동기화 런타임 분리**: Trigger(변경 감지) + Sync Worker(반영)로 확장 가능하게 분리
- **다국어 지원**: 한국어, 영어, 일본어, 중국어 시트 이름 처리
- **보안**: 공개 시트만 접근 (OAuth2는 향후 지원 예정)

## 📋 API 엔드포인트

> 기준: **BFF** 서비스(`http://localhost:8002`)에 노출된 Google Sheets 커넥터 API입니다.

### 1) 시트 미리보기 (URL 기반; Funnel/프론트 공용)

```bash
POST /api/v1/data-connectors/google-sheets/preview
```

요청 예시:
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit#gid=0",
  "worksheet_name": "Sheet1",
  "api_key": null
}
```

### (Internal) Funnel 연동: grid + merged_cells 추출

구조 분석(멀티테이블/전치/병합셀/키-값)을 위해 “표준 grid + 병합셀 좌표”가 필요할 때 사용합니다.

```bash
POST /api/v1/data-connectors/google-sheets/grid
```

요청 예시:
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit#gid=0",
  "worksheet_name": "Sheet1",
  "max_rows": 5000,
  "max_cols": 200,
  "trim_trailing_empty": true
}
```

응답은 `grid`(2차원)와 `merged_cells`(0-based inclusive range)를 포함합니다.

응답:
```json
{
  "sheet_id": "YOUR_SHEET_ID",
  "sheet_title": "상품 데이터",
  "worksheet_title": "Sheet1",
  "columns": ["상품명", "카테고리", "가격", "재고"],
  "sample_rows": [
    ["티셔츠", "상의", "19000", "45"],
    ["바지", "하의", "29000", "21"]
  ],
  "total_rows": 100,
  "total_columns": 4
}
```

### 2) 시트 등록 (Sync 대상 등록)

```bash
POST /api/v1/data-connectors/google-sheets/register
```

요청:
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit",
  "worksheet_name": "Sheet1",
  "polling_interval": 300,
  "database_name": "demo_db",
  "branch": "main",
  "class_label": "Customer",
  "auto_import": true,
  "max_import_rows": 2000
}
```

응답은 `ApiResponse` 래퍼(`success/message/data`) 형태로 반환됩니다.

#### 등록 정보 저장 방식 (중요)

- 등록 정보/상태는 **Postgres**에 영속 저장됩니다. (Foundry-style)
  - `connector_sources`: source_type/source_id + config_json + enabled
  - `connector_mappings`: Source → Ontology 매핑(초안/확정) + enabled
  - `connector_sync_state`: last cursor/hash, last_success/failure, 재시도 등 운영 상태

#### auto_import (중요)

- `auto_import`는 커넥터가 아니라 **공용 Sync Worker**가 수행합니다.
- **매핑이 확정된 경우에만**(`database_name` + `class_label` 등) Sync Worker가 데이터를 fetch/normalize한 뒤 BFF의 bulk-create 커맨드를 제출합니다.
- 변경 감지는 Trigger 서비스가 수행하고, 이벤트는 Kafka `connector-updates`(EventEnvelope)로 통일됩니다.

### 3) 등록된 시트 목록

```bash
GET /api/v1/data-connectors/google-sheets/registered
```

### 4) 등록된 시트 미리보기 (sheet_id 기반)

```bash
GET /api/v1/data-connectors/google-sheets/{sheet_id}/preview
```

### 5) 시트 등록 해제

```bash
DELETE /api/v1/data-connectors/google-sheets/{sheet_id}
```

## 🔧 설정

### 환경 변수 (선택사항)
```bash
# Google API Key (공개 시트는 없어도 작동)
export GOOGLE_API_KEY="your_api_key_here"
```

### Google Sheets 공유 설정

1. Google Sheets 열기
2. 우측 상단 "공유" 버튼 클릭
3. "링크 가져오기" 클릭
4. "제한됨" → "링크가 있는 모든 사용자" 변경
5. "뷰어" 권한 설정
6. "링크 복사" 클릭

## 💡 사용 예시

### Python 예시
```python
import httpx
import asyncio

async def preview_google_sheet():
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8002/api/v1/data-connectors/google-sheets/preview",
            json={
                "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"시트 제목: {data['sheet_title']}")
            print(f"컬럼: {data['columns']}")
            print(f"샘플 데이터: {data['sample_rows'][0]}")

asyncio.run(preview_google_sheet())
```

### cURL 예시
```bash
# 시트 미리보기
curl -X POST http://localhost:8002/api/v1/data-connectors/google-sheets/preview \
  -H "Content-Type: application/json" \
  -d '{
    "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
  }'

# 시트 등록
curl -X POST http://localhost:8002/api/v1/data-connectors/google-sheets/register \
  -H "Content-Type: application/json" \
  -d '{
    "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit",
    "polling_interval": 300
  }'
```

## 🔄 온톨로지 연동 워크플로우

1. **Google Sheet 준비**
   - 첫 번째 행에 컬럼 이름 작성
   - 데이터를 정리된 표 형식으로 구성
   - 공개 공유 설정

2. **데이터 미리보기**
   - `POST /api/v1/data-connectors/google-sheets/preview`로 데이터 구조 확인
   - 컬럼과 데이터 타입 파악

3. **ObjectType 생성**
   - 미리보기 결과를 바탕으로 온톨로지 스키마 설계
   - 각 컬럼을 속성으로 매핑

4. **주기적 동기화**
   - `POST /api/v1/data-connectors/google-sheets/register`로 시트 등록
   - `connector-trigger-service`가 변경 감지(폴링/웹훅) → `connector-updates` 발행
   - `connector-sync-worker`가 매핑이 확정된 소스만 반영(큐잉/백오프/DLQ 기본)

## 🚧 제한사항

- 현재는 공개 시트만 지원 (OAuth2는 개발 중)
- 미리보기는 기본 `limit=10` (필요 시 쿼리 파라미터로 조정)
- `polling_interval`은 seconds 단위이며 너무 짧으면 외부 API/비용 이슈가 생길 수 있음
- `auto_import`는 “변경 감지 → bulk-create 제출”까지의 최소 파이프라인입니다. 업서트/삭제/PK 전략은 `connector_mappings`로 제품화(Foundry-style)하는 것이 안전합니다.
- `auto_import`에서 중복 생성을 피하려면, 시트에 안정적인 식별자 컬럼(예: `customer_id`)을 포함시키는 것을 권장합니다.

## 🔮 향후 계획

- [ ] OAuth2 인증으로 개인 시트 접근
- [ ] Webhook 기반 실시간 업데이트
- [ ] 자동 ObjectType 매핑 제안
- [ ] CSV, Excel 파일 지원
- [ ] 대용량 데이터 스트리밍

## 🧪 테스트

Google Sheets는 외부 서비스 의존성이 있어, 로컬에서는 아래처럼 엔드포인트 스모크로 확인하는 방식이 가장 안전합니다.

```bash
curl -fsS http://localhost:8002/api/v1/health | jq .

curl -fsS -X POST http://localhost:8002/api/v1/data-connectors/google-sheets/preview \\
  -H 'Content-Type: application/json' \\
  -d '{\"sheet_url\":\"https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit\"}' | jq .
```

## 📝 문제 해결

### "Cannot access the Google Sheet" 오류
- Google Sheet가 "링크가 있는 모든 사용자"로 공유되었는지 확인
- URL이 올바른 형식인지 확인

### 403 Forbidden 오류
- API Key가 필요한 경우 환경 변수 설정
- Google Cloud Console에서 Sheets API 활성화 확인

### 포트 충돌 (8002)
```bash
# 사용 중인 프로세스 확인
lsof -i :8002

# 프로세스 종료
kill <PID>
```
