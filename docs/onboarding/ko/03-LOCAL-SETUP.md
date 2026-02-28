# 로컬 환경 설정 - 내 컴퓨터에서 실행하기

> 이 문서를 따라하면 내 컴퓨터에서 Spice OS 전체 스택을 실행할 수 있어요. 모든 단계에 **예상 출력**을 포함했으니, 결과가 다르면 하단의 트러블슈팅을 참고하세요.

---

## 사전 준비 체크리스트

> 시작하기 전에 필요한 도구가 설치되어 있는지 확인해 볼까요.

아래 도구들이 설치되어 있는지 확인하세요:

```bash
# 하나씩 실행해서 버전을 확인합니다
docker --version          # Docker 24.0+ 필요
docker compose version    # Docker Compose 2.20+ 필요
python3 --version         # Python 3.11+ 필요
node --version            # Node.js 20.0+ 필요
git --version             # Git 2.40+ 필요
```

### Apple Silicon (M1/M2/M3/M4) 필수 확인사항

| 항목 | 요구사항 | 이유 |
|------|---------|------|
| Docker 런타임 | **Docker Desktop** 사용 | Colima에서 Elasticsearch가 SIGILL 에러 발생 |
| 메모리 할당 | Docker Desktop → Settings → Resources → **8GB 이상** | ES가 OOM (exit 137) 발생 |
| 권장 메모리 | 16GB 이상 | 전체 32개 서비스 + 관측성 스택 운영 시 |

> ⚠️ **Colima를 사용 중이라면** Docker Desktop으로 전환하세요. Elasticsearch 8.x가 Colima의 가상화 모드(vz)에서 JVM SIGILL 에러를 일으켜요.

---

## 1단계: 저장소 클론

```bash
git clone <repository-url>
cd SPICE-Harvester
```

**예상 출력:**
```
Cloning into 'SPICE-Harvester'...
remote: Enumerating objects: ...
Receiving objects: 100% ...
```

---

## 2단계: 환경 변수 설정

```bash
cp .env.example .env
```

기본값으로 로컬 개발이 가능하므로, `.env` 파일을 따로 수정할 필요는 없어요. 주요 기본값을 확인해 볼까요:

| 환경 변수 | 기본값 | 설명 |
|-----------|--------|------|
| `POSTGRES_USER` | `spiceadmin` | PostgreSQL 사용자명 |
| `POSTGRES_PASSWORD` | `spicepass123` | PostgreSQL 비밀번호 |
| `POSTGRES_PORT_HOST` | **`5433`** | PostgreSQL 호스트 포트 (**5432가 아닙니다!**) |
| `REDIS_PASSWORD` | `spicepass123` | Redis 비밀번호 |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO 관리자 |
| `MINIO_ROOT_PASSWORD` | `minioadmin123` | MinIO 비밀번호 |
| `ADMIN_TOKEN` | (설정 필요) | BFF/OMS 관리자 토큰 |

> ⚠️ **PostgreSQL 포트 주의:** 호스트에서 접속할 때 포트는 **5433**이에요 (5432가 아니에요!). Docker 컨테이너 내부에서만 5432를 사용해요.

---

## 3단계: DB만 먼저 실행하기 (선택)

> 💡 전체 스택을 한번에 띄우기 전에, DB 서비스만 먼저 띄워서 확인해 볼 수 있어요.

```bash
docker compose -f docker-compose.databases.yml up -d
```

**예상 출력:**
```
[+] Running 7/7
 ✔ Container spice_postgres       Started
 ✔ Container spice_redis          Started
 ✔ Container spice_elasticsearch  Started
 ✔ Container spice_zookeeper      Started
 ✔ Container spice_kafka          Started
 ✔ Container spice_minio          Started
 ✔ Container spice_lakefs         Started
```

건강 상태를 확인해 봐요:

```bash
docker compose -f docker-compose.databases.yml ps
```

모든 서비스가 `Up (healthy)`로 표시되어야 해요. Elasticsearch는 시작에 1~3분이 걸릴 수 있어요.

> ⚠️ **안 되나요?** Elasticsearch가 `Exited (137)`이면 메모리 부족이에요. Docker Desktop의 메모리를 8GB 이상으로 늘려주세요.

DB 확인 후 전체 스택으로 넘어갈 때는 먼저 종료해 주세요:
```bash
docker compose -f docker-compose.databases.yml down
```

---

## 4단계: 전체 스택 실행

> 이제 진짜로 전체 시스템을 띄워 볼까요!

```bash
docker compose -f docker-compose.full.yml up -d
```

**예상 출력 (일부):**
```
[+] Running 30+/30+
 ✔ Container spice_postgres         Started
 ✔ Container spice_redis            Started
 ✔ Container spice_elasticsearch    Started
 ✔ Container spice_minio            Started
 ✔ Container spice_db_migrations    Started
 ✔ Container spice_oms              Started
 ✔ Container spice_bff              Started
 ✔ Container spice_projection_worker Started
 ...
```

전체 서비스가 healthy 상태가 될 때까지 **2~5분** 정도 기다려 주세요.

```bash
# 전체 상태 확인
docker compose -f docker-compose.full.yml ps
```

> ⚠️ **안 되나요?** 특정 서비스가 `Restarting`이면 로그를 확인해 보세요:
> ```bash
> docker compose -f docker-compose.full.yml logs <서비스이름> --tail 50
> ```

---

## 5단계: 서비스 헬스 체크

> 각 핵심 서비스가 정상적으로 떠 있는지 하나씩 확인해 볼까요.

### OMS (포트 8000)
```bash
curl http://localhost:8000/health
```

**예상 응답:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "checks": {
    "database": "ok",
    "elasticsearch": "ok"
  }
}
```

### BFF (포트 8002)
```bash
curl http://localhost:8002/api/v1/health
```

**예상 응답:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "checks": {
    "database": "ok",
    "elasticsearch": "ok",
    "redis": "ok",
    "kafka": "ok"
  }
}
```

### Elasticsearch (포트 9200)
```bash
curl http://localhost:9200
```

**예상 응답:**
```json
{
  "name": "...",
  "cluster_name": "docker-cluster",
  "version": {
    "number": "8.12.2"
  },
  "tagline": "You Know, for Search"
}
```

### PostgreSQL (포트 5433)
```bash
docker compose -f docker-compose.full.yml exec postgres pg_isready
```

**예상 응답:**
```
/var/run/postgresql:5432 - accepting connections
```

> 💡 컨테이너 내부에서는 5432이지만, 호스트에서 직접 접속할 때는 5433이에요.

---

## 6단계: UI 서비스 접속

> 브라우저에서 직접 접속해서 확인해 봐요.

| 서비스 | URL | 접속 정보 |
|--------|-----|----------|
| **프론트엔드** | http://localhost:5173 | (프론트엔드 별도 실행 필요, 아래 참고) |
| **MinIO Console** | http://localhost:9001 | ID: `minioadmin` / PW: `minioadmin123` |
| **LakeFS** | http://localhost:48080 | ID: `spice-lakefs-admin` / PW: (`.env` 확인) |
| **Kafka UI** | http://localhost:8080 | 인증 없음 |
| **Grafana** | http://localhost:13000 | ID: `admin` / PW: `admin` |
| **Jaeger** | http://localhost:16686 | 인증 없음 |
| **Prometheus** | http://localhost:19090 | 인증 없음 |

### 프론트엔드 로컬 실행

프론트엔드는 Docker에 포함되어 있지 않아서 별도로 실행해야 해요:

```bash
cd frontend
npm install
npm run dev
```

**예상 출력:**
```
  VITE v5.x.x  ready in 1000ms

  ➜  Local:   http://localhost:5173/
  ➜  Network: use --host to expose
```

브라우저에서 http://localhost:5173 을 열면 Spice OS 대시보드가 표시돼요. 여기까지 왔으면 성공!

---

## 7단계: Python 개발 환경 설정 (백엔드 개발 시)

> 백엔드 코드를 수정하거나 테스트를 실행하려면 Python 가상환경이 필요해요.

```bash
# 가상환경 생성
python3 -m venv .venv

# 활성화
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows

# 의존성 설치
pip install -e "backend/shared[dev]"
```

테스트가 정상 동작하는지 확인해 봐요:
```bash
make backend-unit
```

**예상 출력:**
```
... passed, ... warnings in XX.XXs
```

1,556개 이상의 테스트가 통과하면 성공이에요! ✅

---

## 서비스 종료

```bash
# 전체 스택 종료
docker compose -f docker-compose.full.yml down

# 볼륨까지 삭제 (완전 초기화, 주의!)
docker compose -f docker-compose.full.yml down -v
```

> ⚠️ **`-v` 옵션 주의:** 이 옵션은 PostgreSQL, Elasticsearch 등의 데이터를 모두 삭제해요. 깨끗하게 다시 시작하고 싶을 때만 사용하세요!

---

## 트러블슈팅

### Elasticsearch가 `Exited (137)`로 종료됨

**원인:** 메모리 부족 (OOM Kill)이에요.

**해결 방법:**
1. Docker Desktop → Settings → Resources → Memory를 **8GB 이상**으로 설정하세요.
2. 또는 `.env`에서 ES 힙 크기를 조절하세요: `ES_JAVA_OPTS=-Xms512m -Xmx512m`

### Elasticsearch에서 `SIGILL` 에러 발생

**원인:** Apple Silicon에서 Colima를 사용하고 있기 때문이에요.

**해결 방법:** Docker Desktop으로 전환하세요. Colima의 vz 가상화에서 ES JVM이 SIGILL을 일으켜요.

### PostgreSQL 접속 시 "Connection refused"

**확인할 것:**
- 포트가 **5433**인지 확인하세요. 5432가 아니에요!
- `docker compose ps`에서 postgres가 `Up (healthy)`인지 확인하세요.

```bash
# 올바른 접속 방법
psql -h localhost -p 5433 -U spiceadmin -d spicedb
```

### LakeFS에서 "NoSuchBucket" 에러

**원인:** 브랜치가 존재하지 않는 상태에서 S3 Gateway로 PutObject를 시도했기 때문이에요.

**해결 방법:** 코드에서 `_ensure_lakefs_branch_exists()`를 먼저 호출해야 해요.

### 특정 서비스가 계속 `Restarting` 상태

```bash
# 해당 서비스 로그 확인
docker compose -f docker-compose.full.yml logs <서비스이름> --tail 100

# 예: BFF 로그 확인
docker compose -f docker-compose.full.yml logs bff --tail 100
```

대부분 의존 서비스(PostgreSQL, Kafka 등)가 아직 준비되지 않아서 발생해요. 2~3분 기다리면 자동으로 복구돼요.

### 포트 충돌 (Address already in use)

이미 해당 포트를 사용하는 프로세스가 있을 때 발생해요:

```bash
# 어떤 프로세스가 포트를 사용 중인지 확인
lsof -i :8000   # OMS 포트
lsof -i :8002   # BFF 포트
lsof -i :5433   # PostgreSQL 포트
```

---

## 다음으로 읽을 문서

- [첫 API 호출](04-FIRST-API-CALL.md) — 실행된 플랫폼에서 직접 API를 호출해 봐요
- [프론트엔드 둘러보기](07-FRONTEND-TOUR.md) — UI를 통해 기능을 체험해 봐요
