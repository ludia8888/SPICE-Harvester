# 트러블슈팅 FAQ - 자주 겪는 문제들

> 이 문서는 SPICE-Harvester 개발 중 실제로 자주 발생하는 문제와 해결법을 정리합니다. 문제가 생기면 여기를 먼저 확인하세요.

---

## 인프라 문제

### 1. Elasticsearch가 `Exited (137)`로 종료됨

**증상:**
```bash
docker compose ps
# spice_elasticsearch  Exited (137)
```

**원인:** 메모리 부족 (OOM Kill). Docker에 할당된 메모리가 부족합니다.

**해결:**
1. Docker Desktop → Settings → Resources → Memory → **8GB 이상**으로 설정
2. 또는 ES 힙 크기를 줄입니다 (`.env`에 추가):
   ```
   ES_JAVA_OPTS=-Xms256m -Xmx256m
   ```
3. 최소한의 서비스만 실행:
   ```bash
   docker compose -f docker-compose.databases.yml up -d
   ```

---

### 2. Elasticsearch에서 `SIGILL` 에러 (Apple Silicon)

**증상:**
```
elasticsearch | SIGILL: illegal instruction
```

**원인:** Apple Silicon (M1/M2/M3)에서 **Colima** 사용 시 발생. Colima의 vz 가상화에서 ES JVM이 SIGILL을 트리거합니다.

**해결:**
1. **Colima를 중지**하고 **Docker Desktop**을 설치/사용하세요
2. Colima를 꼭 사용해야 한다면 `--arch aarch64` 옵션으로 시작해보세요 (보장되지 않음)

> **권장:** Apple Silicon에서는 **Docker Desktop이 유일하게 안정적인 옵션**입니다.

---

### 3. PostgreSQL 접속 시 "Connection refused"

**증상:**
```bash
psql -h localhost -p 5432 -U spiceadmin -d spicedb
# Connection refused
```

**원인:** 호스트 포트가 **5432가 아니라 5433**입니다.

**해결:**
```bash
# 올바른 포트
psql -h localhost -p 5433 -U spiceadmin -d spicedb

# 비밀번호: spicepass123 (기본값)
```

**배경:** Docker Compose에서 PostgreSQL 컨테이너의 내부 포트는 5432이지만, **호스트 포트는 5433으로 매핑**되어 있습니다. 이는 로컬에 이미 설치된 PostgreSQL과 포트 충돌을 피하기 위함입니다.

---

### 4. LakeFS에서 "NoSuchBucket" 에러

**증상:**
```
An error occurred (NoSuchBucket) when calling the PutObject operation
```

**원인:** LakeFS S3 Gateway에서 PutObject를 할 때, **대상 브랜치가 존재하지 않습니다.**

**해결:**
- 코드에서 `_ensure_lakefs_branch_exists()`를 PutObject **이전에** 호출해야 합니다
- 이 함수 위치: `backend/shared/services/storage/lakefs_client.py`

```python
# 올바른 순서
await lakefs_client._ensure_lakefs_branch_exists(repo, branch)
await lakefs_client.put_object(repo, branch, path, data)
```

**관련 정보:** LakeFS 리포지토리의 기본 브랜치는 `"main"`입니다 (`"master"` 아님).

---

### 5. 서비스가 계속 `Restarting` 상태

**증상:**
```bash
docker compose ps
# spice_bff  Restarting
```

**원인:** 의존 서비스가 아직 준비되지 않았습니다. 특히 첫 실행 시 PostgreSQL 마이그레이션과 Kafka 초기화에 시간이 걸립니다.

**해결:**
1. 먼저 **2~3분 기다립니다** (대부분 자동 복구)
2. 로그를 확인합니다:
   ```bash
   docker compose -f docker-compose.full.yml logs bff --tail 50
   ```
3. 의존 서비스 상태를 확인합니다:
   ```bash
   docker compose -f docker-compose.full.yml ps
   ```
4. 특정 서비스만 재시작:
   ```bash
   docker compose -f docker-compose.full.yml restart bff
   ```

---

### 6. 포트 충돌 (Address already in use)

**증상:**
```
Error: Bind for 0.0.0.0:8000 failed: port is already allocated
```

**해결:**
```bash
# 해당 포트를 사용 중인 프로세스 찾기
lsof -i :8000

# 프로세스 종료
kill -9 <PID>

# 또는 Docker 컨테이너 확인
docker ps | grep 8000
```

---

## 테스트 관련 문제

### 7. `@requires_infra` 테스트 실패

**증상:**
```
SKIPPED [1] conftest.py: Infrastructure not available
```

또는 실행은 되지만 실패:
```
test_pipeline_udf_versioning FAILED
```

**원인:**
- 인프라가 실행되지 않았거나
- 로컬 PostgreSQL에 **이전 테스트의 잔여 데이터**가 있습니다

**해결:**
1. 전체 스택이 실행 중인지 확인:
   ```bash
   docker compose -f docker-compose.full.yml ps
   ```
2. 클린 재시작 (볼륨 삭제):
   ```bash
   docker compose -f docker-compose.full.yml down -v
   docker compose -f docker-compose.full.yml up -d
   ```

---

### 8. Financial Investigation E2E 테스트가 오래 걸림/실패

**증상:**
```
test_financial_investigation_workflow_e2e FAILED
# 또는 4분 이상 소요
```

**원인:** 이 테스트는 **objectify_worker**와 **projection_worker**가 실행 중이어야 합니다. 전체 스택이 아닌 일부만 실행하면 실패합니다.

**해결:**
```bash
# 전체 스택 실행 필수
docker compose -f docker-compose.full.yml up -d

# 워커 상태 확인
docker compose -f docker-compose.full.yml ps | grep worker
```

> 이 테스트는 정상적으로도 **4분 이상** 소요됩니다. 타임아웃이 아니라 원래 느린 테스트입니다.

---

### 9. `pytest.ini`에서 `--timeout` 사용 불가

**증상:**
```
ERROR: unrecognized arguments: --timeout
```

**원인:** 현재 `pytest.ini` 설정에서는 `--timeout` 플래그를 지원하지 않습니다.

**해결:**
- CLI에서 `--timeout` 대신 코드 내에서 `asyncio.wait_for()`를 사용하세요
- 또는 `pytest-timeout` 패키지를 별도 설치해야 합니다

---

## 코드 관련 문제

### 10. DB 마이그레이션 에러 (fresh DB)

**증상:**
```
ERROR: relation "xxx" does not exist
```

**원인:** 새로운(fresh) 데이터베이스에서 마이그레이션이 테이블이 이미 존재한다고 가정합니다.

**해결:**
- 마이그레이션 코드는 반드시 `ALTER TABLE IF EXISTS` 패턴을 사용해야 합니다
- 예외 처리에 `EXCEPTION WHEN undefined_table` 블록을 포함해야 합니다

```sql
-- 올바른 마이그레이션 패턴
DO $$
BEGIN
    ALTER TABLE IF EXISTS my_table ADD COLUMN new_col TEXT;
EXCEPTION
    WHEN undefined_table THEN NULL;
END $$;
```

---

### 11. `os.getenv()` 직접 사용 금지

**증상:** 코드 리뷰에서 반려됨

**원인:** Spice OS는 환경 변수를 `Pydantic Settings`로 중앙 관리합니다.

**해결:**
```python
# ❌ 잘못된 방법
import os
db_host = os.getenv("POSTGRES_HOST", "localhost")

# ✅ 올바른 방법
from shared.config.settings import get_settings
settings = get_settings()
db_host = settings.postgres_host
```

---

### 12. import 에러: "ModuleNotFoundError"

**증상:**
```
ModuleNotFoundError: No module named 'shared'
```

**원인:** `PYTHONPATH`가 설정되지 않았습니다.

**해결:**
```bash
# 방법 1: PYTHONPATH 설정
export PYTHONPATH=backend

# 방법 2: make 명령어 사용 (자동 설정됨)
make backend-unit

# 방법 3: pytest 실행 시 직접 지정
PYTHONPATH=backend python3 -m pytest ...
```

---

## 네트워크/연결 문제

### 13. BFF에서 OMS 연결 실패

**증상:**
```
httpx.ConnectError: [Errno 111] Connection refused
```

**원인:** OMS가 아직 시작되지 않았거나, BFF의 OMS URL 설정이 잘못되었습니다.

**해결:**
```bash
# OMS 상태 확인
curl http://localhost:8000/health

# Docker 네트워크 내에서 OMS URL
# (컨테이너에서는 localhost가 아니라 서비스 이름 사용)
# OMS_BASE_URL=http://oms:8000
```

---

### 14. Kafka 연결 에러

**증상:**
```
KafkaError: NoBrokersAvailable
```

**원인:** Kafka가 아직 시작되지 않았거나 포트가 다릅니다.

**해결:**
```bash
# Kafka 상태 확인
docker compose -f docker-compose.full.yml ps | grep kafka

# Kafka UI에서 확인
# http://localhost:8080
```

**참고:** 호스트에서 Kafka에 직접 접속할 때 포트는 **39092**입니다 (9092 아님).

---

## 빠른 진단 체크리스트

문제가 생기면 이 순서로 확인하세요:

```bash
# 1. 전체 서비스 상태 확인
docker compose -f docker-compose.full.yml ps

# 2. 문제 서비스 로그 확인
docker compose -f docker-compose.full.yml logs <서비스> --tail 100

# 3. 핵심 서비스 헬스 체크
curl http://localhost:8000/health      # OMS
curl http://localhost:8002/api/v1/health  # BFF
curl http://localhost:9200             # Elasticsearch

# 4. 디스크/메모리 확인
docker system df                       # Docker 디스크 사용량
docker stats --no-stream               # 컨테이너별 메모리 사용량

# 5. 완전 초기화 (최후의 수단)
docker compose -f docker-compose.full.yml down -v
docker compose -f docker-compose.full.yml up -d
```

---

## 다음으로 읽을 문서

- [30일 학습 로드맵](LEARNING-ROADMAP.md) - 체계적인 학습 일정
- [로컬 환경 설정](03-LOCAL-SETUP.md) - 환경 설정 재확인
