# Google Sheets Connector for SPICE HARVESTER

Google Sheets를 SPICE HARVESTER 온톨로지 시스템과 연동하는 커넥터입니다.

## 🚀 주요 기능

- **URL 기반 연결**: Google Sheets 공유 URL만으로 간편하게 연결
- **데이터 미리보기**: 컬럼 헤더와 샘플 데이터 추출
- **실시간 동기화**: 주기적인 폴링으로 데이터 변경 감지
- **다국어 지원**: 한국어, 영어, 일본어, 중국어 시트 이름 처리
- **보안**: 공개 시트만 접근 (OAuth2는 향후 지원 예정)

## 📋 API 엔드포인트

### 1. 헬스 체크
```bash
GET /api/v1/connectors/google/health
```

응답 예시:
```json
{
  "status": "healthy",
  "service": "google_sheets_connector",
  "api_key_configured": false,
  "message": "Google Sheets connector is operational"
}
```

### 2. 시트 미리보기
```bash
POST /api/v1/connectors/google/preview
```

요청:
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
}
```

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

### 3. 시트 등록 (폴링)
```bash
POST /api/v1/connectors/google/register
```

요청:
```json
{
  "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit",
  "worksheet_name": "Sheet1",
  "polling_interval": 300
}
```

응답:
```json
{
  "status": "registered",
  "sheet_id": "YOUR_SHEET_ID",
  "worksheet_name": "Sheet1",
  "polling_interval": 300,
  "registered_at": "2025-07-18T12:00:00Z"
}
```

### 4. 등록된 시트 목록
```bash
GET /api/v1/connectors/google/registered
```

### 5. 시트 등록 해제
```bash
DELETE /api/v1/connectors/google/register/{sheet_id}
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
            "http://localhost:8002/api/v1/connectors/google/preview",
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
curl -X POST http://localhost:8002/api/v1/connectors/google/preview \
  -H "Content-Type: application/json" \
  -d '{
    "sheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
  }'

# 시트 등록
curl -X POST http://localhost:8002/api/v1/connectors/google/register \
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
   - `/preview` API로 데이터 구조 확인
   - 컬럼과 데이터 타입 파악

3. **ObjectType 생성**
   - 미리보기 결과를 바탕으로 온톨로지 스키마 설계
   - 각 컬럼을 속성으로 매핑

4. **주기적 동기화**
   - `/register` API로 시트 등록
   - 설정된 간격으로 자동 업데이트

## 🚧 제한사항

- 현재는 공개 시트만 지원 (OAuth2는 개발 중)
- 최대 5개의 샘플 행만 미리보기에 표시
- 폴링 간격은 60초~3600초 사이

## 🔮 향후 계획

- [ ] OAuth2 인증으로 개인 시트 접근
- [ ] Webhook 기반 실시간 업데이트
- [ ] 자동 ObjectType 매핑 제안
- [ ] CSV, Excel 파일 지원
- [ ] 대용량 데이터 스트리밍

## 🧪 테스트

### 유틸리티 테스트
```bash
cd backend
python tests/connectors/test_google_sheets_simple.py
```

### API 통합 테스트
```bash
cd backend
python test_google_sheets_quick.py
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