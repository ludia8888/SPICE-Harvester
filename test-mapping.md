# SPICE HARVESTER 매핑 엔진 검증 가이드

## 테스트 준비

1. **테스트 데이터 파일**
   - `/test_data/customers.csv` - 고객 데이터 (10건)
   - `/test_data/products.csv` - 제품 데이터 (10건)

2. **백엔드 서비스 상태**
   - ✅ OMS (localhost:8000)
   - ✅ BFF (localhost:8002)
   - ✅ Funnel (localhost:8004)
   - ✅ TerminusDB (localhost:6363)

3. **프론트엔드**
   - localhost:5174

## 테스트 시나리오

### 시나리오 1: 고객 데이터 → Person 온톨로지 매핑

1. 프론트엔드에서 새 데이터베이스 생성
   - 이름: `customer_test_db`
   - 설명: `고객 데이터 매핑 테스트`

2. "Connect Data" 버튼 클릭

3. CSV 파일 업로드
   - `/test_data/customers.csv` 드래그 앤 드롭

4. 데이터 프로파일링 확인
   - 타입 추론 결과:
     - `customer_id` → string (ID 패턴)
     - `full_name` → string
     - `email_address` → string (이메일 패턴)
     - `phone_number` → string (전화번호 패턴)
     - `birth_date` → date
     - `registration_date` → dateTime
     - `city` → string
     - `country` → string
     - `total_purchases` → decimal
     - `vip_status` → boolean

5. "Proceed to Mapping" 클릭

6. 매핑 제안 확인 (기존 Person 온톨로지가 있는 경우)
   - 예상 매핑:
     - `customer_id` → `id` (exact match)
     - `full_name` → `name` (token match: name)
     - `email_address` → `email` (semantic match)
     - `phone_number` → `phone` (semantic match)
     - `birth_date` → `dateOfBirth` (token match: birth, date)
     - `vip_status` → `isVIP` (semantic match)
     - `total_purchases` → `totalSpent` (semantic match)

### 시나리오 2: 제품 데이터 → 새 온톨로지 생성

1. 새 데이터베이스 생성
   - 이름: `product_test_db`
   - 설명: `제품 데이터 테스트`

2. CSV 파일 업로드
   - `/test_data/products.csv`

3. 데이터 프로파일링 확인
   - 타입 추론 결과:
     - `productSKU` → string (ID 패턴)
     - `productName` → string
     - `categoryName` → string
     - `unitPrice` → decimal
     - `stockQuantity` → integer
     - `manufacturer` → string
     - `releaseDate` → date
     - `isActive` → boolean

4. 온톨로지 자동 생성 (매핑 대상이 없는 경우)

## 검증 항목

### 1. 타입 추론 정확도
- [x] 이메일 패턴 인식
- [x] 전화번호 패턴 인식 (한국식)
- [x] 날짜 포맷 인식
- [x] ID 패턴 인식
- [x] 불린 값 인식
- [x] 숫자형 구분 (integer vs decimal)

### 2. 매핑 제안 품질
- [x] 정확한 이름 매칭 (exact match)
- [x] 토큰 기반 매칭 (camelCase, snake_case 분해)
- [x] 의미론적 매칭 (email_address → email)
- [x] 타입 호환성 검사
- [x] 신뢰도 점수 계산

### 3. 값 분포 유사도 (타겟 샘플 데이터가 있는 경우)
- [x] 수치형 분포 비교 (KS-test)
- [x] 범주형 분포 비교 (Jaccard)
- [x] 문자열 패턴 비교 (n-gram)

### 4. UI/UX
- [x] 드래그 앤 드롭 파일 업로드
- [x] 실시간 프로파일링 결과
- [x] 신뢰도 시각화 (프로그레스 바)
- [x] 매핑 수락/거부 토글
- [x] 미매핑 필드 표시

## 개선된 기능

### P0 (완료)
- ✅ 값 분포 유사도 매칭
- ✅ 토큰 기반 이름 유사도
- ✅ NFKC 정규화
- ✅ 다양한 날짜/전화 포맷 지원
- ✅ 설정 가능한 임계값/가중치

### 설정 파일
`/backend/bff/config/mapping_config.json`에서 조정 가능:
- 신뢰도 임계값
- 매칭 타입별 가중치
- 스톱워드 목록
- 도메인별 동의어

## 문제 해결

1. **매핑이 제안되지 않는 경우**
   - 임계값 확인 (기본: 0.5)
   - 타입 호환성 확인
   - 샘플 데이터 충분성 확인

2. **잘못된 매핑 제안**
   - 가중치 조정
   - 스톱워드 추가
   - 도메인 사전 업데이트

3. **성능 이슈**
   - 샘플 크기 제한 (기본: 200)
   - 블로킹 전략 적용 (P1)