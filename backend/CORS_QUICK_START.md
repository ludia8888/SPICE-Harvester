# 🔥 CORS 설정 빠른 시작 가이드

## 🚀 즉시 테스트하기

### 1. 환경변수 설정 (선택사항)

```bash
# .env 파일 생성 (기본 설정으로도 동작함)
cp .env.example .env

# 또는 직접 설정
export ENVIRONMENT=development
export CORS_ENABLED=true
```

### 2. 서비스 시작

```bash
# 모든 서비스 시작
python start_services.py

# 또는 개별 시작
python -m bff.main &
python -m oms.main &
python -m funnel.main &
```

### 3. CORS 설정 확인

```bash
# 각 서비스의 CORS 설정 확인
curl http://localhost:8002/debug/cors | jq  # BFF
curl http://localhost:8000/debug/cors | jq  # OMS
curl http://localhost:8003/debug/cors | jq  # Funnel
```

### 4. 자동 테스트 실행

```bash
# 의존성 설치 (필요시)
pip install aiohttp colorama

# CORS 테스트 실행
python test_cors_configuration.py
```

## 🎯 주요 변경사항

### ✅ 구현 완료

1. **ServiceConfig에 CORS 메서드 추가**
   - `get_cors_origins()` - 환경별 origin 설정
   - `get_cors_config()` - 완전한 CORS 설정
   - `get_cors_debug_info()` - 디버그 정보

2. **모든 서비스 업데이트**
   - BFF (8002), OMS (8000), Funnel (8003)
   - 환경변수 기반 동적 CORS 설정
   - 개발 환경에서 `/debug/cors` 엔드포인트 추가

3. **환경변수 설정**
   - `.env.example` 업데이트
   - 개발/프로덕션 환경별 설정 구분

4. **테스트 및 문서**
   - 자동 테스트 스크립트
   - 완전한 구성 가이드

### 🔧 기본 동작

- **개발 환경**: 자동으로 일반적인 프론트엔드 포트들 허용
- **프로덕션 환경**: 명시적으로 허용된 도메인만 허용
- **환경변수 없음**: 안전한 기본값 사용

### 💡 사용 예시

```bash
# 프론트엔드 포트 3000에서 개발 시
npm start  # http://localhost:3000

# 자동으로 허용되는 포트들:
# - 3000, 3001, 3002 (React)
# - 5173, 5174 (Vite)
# - 4200, 4201 (Angular)
# - 8080, 8081, 8082 (Vue.js)
```

## 🚨 문제 해결

### CORS 에러 발생 시

1. **서비스 상태 확인**
   ```bash
   curl http://localhost:8002/health
   curl http://localhost:8000/health
   curl http://localhost:8003/health
   ```

2. **CORS 설정 확인**
   ```bash
   curl http://localhost:8002/debug/cors
   ```

3. **특정 포트 허용**
   ```bash
   export CORS_ORIGINS='["http://localhost:3000", "http://localhost:YOUR_PORT"]'
   ```

### 완전한 문서

- 📖 **완전한 가이드**: `docs/development/CORS_CONFIGURATION_GUIDE.md`
- 🧪 **테스트 스크립트**: `test_cors_configuration.py`
- ⚙️ **설정 파일**: `.env.example`

---

**🔥 THINK ULTRA!** - 이제 모든 프론트엔드 개발 포트에서 자유롭게 API를 호출할 수 있습니다!