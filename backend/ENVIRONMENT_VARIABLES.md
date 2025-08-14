# 🔥 SPICE HARVESTER ENVIRONMENT VARIABLES

## Complete Production Environment Configuration

All environment variables used across the entire SPICE HARVESTER system.

## Core Services

### OMS (Ontology Management Service)
```bash
# Service Configuration
SERVICE_NAME=oms
SERVICE_PORT=8000
SERVICE_HOST=0.0.0.0

# Docker/Local Mode
DOCKER_CONTAINER=false  # CRITICAL: Set to false for local development

# API Keys
API_KEY=your-secure-api-key-here
```

### BFF (Backend for Frontend)
```bash
SERVICE_NAME=bff
SERVICE_PORT=8002
SERVICE_HOST=0.0.0.0

# Service Discovery
OMS_URL=http://localhost:8000
FUNNEL_URL=http://localhost:8003
```

### Funnel Service
```bash
SERVICE_NAME=funnel
SERVICE_PORT=8003
SERVICE_HOST=0.0.0.0
```

## Database Configuration

### PostgreSQL (Primary Database)
```bash
# Connection Details
POSTGRES_HOST=localhost
POSTGRES_PORT=5432  # NOT 5433! This was a critical bug
POSTGRES_DB=spicedb
POSTGRES_USER=spiceadmin
POSTGRES_PASSWORD=spicepass123

# Legacy variables (deprecated)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=spicedb
DB_USER=spiceadmin
DB_PASSWORD=spicepass123
```

### Redis (Cache & Command Status)
```bash
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=spicepass123
REDIS_DB=0
```

### TerminusDB (Graph Database)
```bash
TERMINUS_SERVER_URL=http://localhost:6363
TERMINUS_USER=admin
TERMINUS_KEY=spice123!  # NOT 'admin'! This was a critical bug
TERMINUS_ACCOUNT=admin
```

### Elasticsearch (Search & Analytics)
```bash
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9201
ELASTICSEARCH_USER=elastic
ELASTICSEARCH_PASSWORD=spice123!
ELASTICSEARCH_URL=http://elastic:spice123!@localhost:9201
```

## Event Sourcing Infrastructure

### MinIO/S3 (Event Store)
```bash
# MinIO Configuration
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_USE_SSL=false

# S3 Configuration (AWS)
S3_ENDPOINT_URL=https://s3.amazonaws.com
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_REGION=us-east-1
S3_USE_SSL=true

# Event Store Configuration
ENABLE_S3_EVENT_STORE=true
ENABLE_DUAL_WRITE=true
S3_EVENT_BUCKET=events
```

### Kafka (Message Broker)
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=spice-harvester-group
KAFKA_AUTO_OFFSET_RESET=earliest

# Topics
ONTOLOGY_COMMANDS_TOPIC=ontology_commands
DATABASE_COMMANDS_TOPIC=database_commands
INSTANCE_COMMANDS_TOPIC=instance_commands
GRAPH_EVENTS_TOPIC=graph_events
SYSTEM_EVENTS_TOPIC=system_events
```

## Worker Services

### Ontology Worker
```bash
WORKER_NAME=ontology-worker
WORKER_CONCURRENCY=10
WORKER_BATCH_SIZE=100
```

### Instance Worker
```bash
WORKER_NAME=instance-worker
WORKER_CONCURRENCY=20
WORKER_BATCH_SIZE=50
```

### Projection Worker
```bash
WORKER_NAME=projection-worker
WORKER_CONCURRENCY=5
PROJECTION_BATCH_SIZE=100
```

## Observability

### Logging
```bash
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT=json  # json or text
LOG_FILE=/tmp/spice_harvester.log
LOG_MAX_SIZE=100MB
LOG_BACKUP_COUNT=10
```

### Monitoring
```bash
ENABLE_METRICS=true
METRICS_PORT=9090
PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
```

### Tracing (OpenTelemetry)
```bash
ENABLE_TRACING=false  # Set to true for production
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_SERVICE_NAME=spice-harvester
OTEL_TRACES_EXPORTER=otlp
```

## Security

### Authentication
```bash
JWT_SECRET_KEY=your-super-secret-jwt-key
JWT_ALGORITHM=HS256
JWT_EXPIRATION_MINUTES=60
```

### CORS
```bash
CORS_ORIGINS=["http://localhost:3000", "http://localhost:8080"]
CORS_ALLOW_CREDENTIALS=true
CORS_ALLOW_METHODS=["GET", "POST", "PUT", "DELETE"]
CORS_ALLOW_HEADERS=["*"]
```

## Feature Flags

```bash
# Event Sourcing Features
ENABLE_EVENT_SOURCING=true
ENABLE_CQRS=true
ENABLE_OUTBOX_PATTERN=true

# Storage Features
ENABLE_S3_EVENT_STORE=true
ENABLE_DUAL_WRITE=true

# Advanced Features
ENABLE_GRAPH_FEDERATION=true
ENABLE_ML_SCHEMA_INFERENCE=true
ENABLE_COMPLEX_TYPES=true
ENABLE_RBAC=false  # Not yet implemented
```

## Development/Testing

```bash
# Environment
ENVIRONMENT=development  # development, staging, production
DEBUG=true

# Testing
TEST_MODE=false
TEST_DATABASE_URL=postgresql://test:test@localhost:5432/test_db
PYTEST_WORKERS=auto
```

## Docker Compose Specific

```bash
# Network
DOCKER_NETWORK=spice-network

# Volumes
DATA_VOLUME=/data
LOG_VOLUME=/logs

# Resource Limits
MEMORY_LIMIT=2G
CPU_LIMIT=2
```

## Critical Environment Variables That Were Bug Sources

1. **POSTGRES_PORT=5432** (NOT 5433!)
   - Documentation had wrong port
   - Caused connection failures

2. **TERMINUS_KEY=spice123!** (NOT 'admin'!)
   - Wrong password in multiple places
   - Caused authentication failures

3. **DOCKER_CONTAINER=false** (for local development)
   - Must be false when running locally
   - Affects hostname resolution

4. **ELASTICSEARCH_PASSWORD=spice123!**
   - Missing in many service configs
   - Caused 401 Unauthorized errors

## Example .env File

```bash
# Create a .env file in the backend directory with:

# Core
ENVIRONMENT=development
DEBUG=true
DOCKER_CONTAINER=false

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=spicedb
POSTGRES_USER=spiceadmin
POSTGRES_PASSWORD=spicepass123

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=spicepass123

# TerminusDB
TERMINUS_SERVER_URL=http://localhost:6363
TERMINUS_USER=admin
TERMINUS_KEY=spice123!

# Elasticsearch
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9201
ELASTICSEARCH_USER=elastic
ELASTICSEARCH_PASSWORD=spice123!

# MinIO
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Event Sourcing
ENABLE_S3_EVENT_STORE=true
ENABLE_DUAL_WRITE=true
```

## Verification Commands

```bash
# Check all environment variables are set
./scripts/check_env.sh

# Validate database connections
python scripts/validate_connections.py

# Test service endpoints
curl http://localhost:8000/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

## Notes

- All passwords shown here are for development only
- Use strong, unique passwords in production
- Store secrets in a secure vault (AWS Secrets Manager, HashiCorp Vault, etc.)
- Never commit .env files to version control
- Use environment-specific configuration files