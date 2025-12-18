# SPICE HARVESTER Operations Manual

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Installation and Setup](#installation-and-setup)
3. [Service Configuration](#service-configuration)
4. [Database Administration](#database-administration)
5. [Monitoring and Health Checks](#monitoring-and-health-checks)
6. [Backup and Recovery](#backup-and-recovery)
7. [Performance Tuning](#performance-tuning)
8. [Troubleshooting](#troubleshooting)
9. [Security Operations](#security-operations)
10. [Disaster Recovery](#disaster-recovery)
11. [Maintenance Procedures](#maintenance-procedures)
12. [Operational Checklists](#operational-checklists)

## System Requirements

### Hardware Requirements

#### Minimum Configuration

| Component | Development | Production |
|-----------|-------------|------------|
| CPU | 2 cores | 8 cores |
| RAM | 8 GB | 32 GB |
| Storage | 50 GB SSD | 500 GB SSD |
| Network | 100 Mbps | 1 Gbps |

#### Recommended Configuration

| Component | Development | Production |
|-----------|-------------|------------|
| CPU | 4 cores | 16 cores |
| RAM | 16 GB | 64 GB |
| Storage | 100 GB SSD | 1 TB NVMe SSD |
| Network | 1 Gbps | 10 Gbps |

### Software Requirements

#### Operating System
- Ubuntu 20.04 LTS or later
- CentOS 8 or later
- macOS 11 (Big Sur) or later
- Windows Server 2019 or later (with WSL2)

#### Runtime Dependencies
- Python 3.9 or later
- Docker 20.10 or later
- Docker Compose 2.0 or later
- Git 2.25 or later

#### Database Requirements
- TerminusDB v11.x
- PostgreSQL 13+ (**required**: `processed_events`/`aggregate_versions` idempotency+ordering registry, write-side seq allocator)
- Redis 6+ (optional but recommended: command status, caching; write-side should continue without Redis)

## Installation and Setup

### 1. System Preparation

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3-pip python3-venv git docker.io docker-compose

# Add user to docker group
sudo usermod -aG docker $USER

# Configure system limits
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Enable and start Docker
sudo systemctl enable docker
sudo systemctl start docker
```

### 2. Application Installation

```bash
# Clone repository
git clone https://github.com/your-org/spice-harvester.git
cd spice-harvester

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
cd backend
pip install -r requirements.txt

# Create necessary directories
mkdir -p logs
mkdir -p data/backups
mkdir -p data/uploads
```

### 3. Initial Configuration

```bash
# Copy configuration templates
cp .env.example .env
cp config/production.yml.example config/production.yml

# Generate secret keys
python -c "import secrets; print(f'SECRET_KEY={secrets.token_urlsafe(32)}')" >> .env

# Set appropriate permissions
chmod 600 .env
chmod 700 data/backups
```

### 4. Database Initialization

```bash
# Start TerminusDB
docker-compose up -d terminusdb

# Wait for initialization
sleep 30

# Create initial admin user
docker exec -it terminusdb terminusdb user create admin --password

# Initialize system database
python scripts/init_database.py
```

## Service Configuration

### Service Factory Pattern
All services now use a standardized factory pattern that eliminates 600+ lines of boilerplate code:

```python
from shared.services.service_factory import create_fastapi_service, ServiceInfo

service_info = ServiceInfo(
    name="OMS",
    version="1.0.0",
    description="Ontology Management Service",
    port=int(os.getenv("OMS_PORT", "8000"))
)

app = create_fastapi_service(service_info)

# Register service-specific routers
app.include_router(database.router)
app.include_router(ontology.router)
app.include_router(branch.router)
# ... other routers
```

### Environment Variables

#### Common Variables

```bash
# Application
APP_NAME=spice-harvester
APP_ENV=production
DEBUG=false
LOG_LEVEL=INFO

# Security
SECRET_KEY=your-secret-key-here
ALLOWED_HOSTS=api.example.com,localhost
CORS_ORIGINS=https://app.example.com

# Database
TERMINUS_SERVER_URL=http://localhost:6363
TERMINUS_USER=admin
TERMINUS_ACCOUNT=admin
TERMINUS_KEY=your-terminus-key

# Service URLs
OMS_BASE_URL=http://localhost:8000
BFF_BASE_URL=http://localhost:8002
FUNNEL_BASE_URL=http://localhost:8003

# Performance
WORKER_PROCESSES=4
WORKER_CONNECTIONS=1000
REQUEST_TIMEOUT=30
```

#### Service-Specific Configuration

##### OMS Configuration
```yaml
# config/oms.yml
server:
  host: 0.0.0.0
  port: 8000
  workers: 4
  
database:
  pool_size: 10
  max_overflow: 20
  pool_timeout: 30
  
validation:
  max_property_count: 100
  max_relationship_depth: 5
  
caching:
  enabled: true
  ttl: 3600
```

##### BFF Configuration
```yaml
# config/bff.yml
server:
  host: 0.0.0.0
  port: 8002
  workers: 4
  
rate_limiting:
  enabled: true
  default_limit: 1000
  window_seconds: 60
  
authentication:
  jwt_enabled: false
  api_key_enabled: true
```

### Service Startup

#### Development Mode
```bash
# Start all services
python backend/start_services.py --env development

# Start individual services
python -m bff.main
python -m oms.main
python -m funnel.main
```

#### Production Mode
```bash
# Using systemd services
sudo systemctl start spice-harvester-bff
sudo systemctl start spice-harvester-oms
sudo systemctl start spice-harvester-funnel

# Using Docker Compose
docker compose -f docker-compose.full.yml up -d
# or: cd backend && ./deploy.sh up
```

## Database Administration

### TerminusDB Management

#### Basic Operations

```bash
# Check database status
curl -u admin:${TERMINUS_KEY:-admin} http://localhost:6363/api/db/admin

# List all databases
curl -u admin:${TERMINUS_KEY:-admin} http://localhost:6363/api/db/admin/_meta

# Create new database
curl -u admin:${TERMINUS_KEY:-admin} -X POST http://localhost:6363/api/db/admin/mydb \
  -H "Content-Type: application/json" \
  -d '{"label": "My Database", "comment": "Production database"}'

# Delete database (CAUTION!)
curl -u admin:${TERMINUS_KEY:-admin} -X DELETE http://localhost:6363/api/db/admin/mydb
```

#### Performance Monitoring

```bash
# Check database size
docker exec spice_terminusdb du -sh /app/terminusdb/storage

# Monitor active connections
docker exec spice_terminusdb netstat -an | grep 6363

# View database logs
docker logs spice_terminusdb --tail 100 -f
```

### Data Management

#### Export Operations

```bash
# Export database schema
python scripts/export_schema.py --database mydb --output schema.json

# Export full database
docker exec spice_terminusdb terminusdb db dump admin/mydb > backup.dump

# Export specific ontologies
python scripts/export_ontology.py --database mydb --class Product
```

#### Import Operations

```bash
# Import database dump
docker exec -i spice_terminusdb terminusdb db restore admin/mydb < backup.dump

# Import schema only
python scripts/import_schema.py --database mydb --file schema.json

# Bulk import data
python scripts/bulk_import.py --database mydb --file data.csv --type Product
```

## Monitoring and Health Checks

### Health Check Endpoints

```bash
# BFF health check
curl http://localhost:8002/health

# OMS health check
curl http://localhost:8000/health

# Funnel health check
curl http://localhost:8003/health

# TerminusDB health check
curl -u admin:${TERMINUS_KEY:-admin} http://localhost:6363/api/status
```

### Monitoring Script

```python
#!/usr/bin/env python3
# scripts/health_monitor.py

import asyncio
import httpx
from datetime import datetime

SERVICES = {
    "BFF": "http://localhost:8002/health",
    "OMS": "http://localhost:8000/health",
    "Funnel": "http://localhost:8003/health",
}

async def check_service_health(name, url):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=5.0)
            status = "UP" if response.status_code == 200 else "DOWN"
            return f"{name}: {status}"
        except Exception as e:
            return f"{name}: ERROR - {str(e)}"

async def monitor_health():
    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{timestamp}] Health Check")
        
        tasks = [check_service_health(name, url) for name, url in SERVICES.items()]
        results = await asyncio.gather(*tasks)
        
        for result in results:
            print(f"  {result}")
        
        await asyncio.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    asyncio.run(monitor_health())
```

### Log Monitoring

```bash
# Tail all service logs
tail -f logs/*.log

# Monitor error logs
tail -f logs/*.log | grep -E "ERROR|CRITICAL"

# Count errors by service
grep ERROR logs/*.log | cut -d: -f1 | sort | uniq -c

# Search for specific patterns
grep -r "ValidationError" logs/ --include="*.log"
```

## Backup and Recovery

### Automated Backup Strategy

#### Daily Backup Script

```bash
#!/bin/bash
# scripts/daily_backup.sh

# Configuration
BACKUP_DIR="/data/backups"
RETENTION_DAYS=7
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p $BACKUP_DIR/$TIMESTAMP

# Backup TerminusDB
echo "Backing up TerminusDB..."
docker exec terminusdb terminusdb db dump admin/production \
  > $BACKUP_DIR/$TIMESTAMP/terminusdb.dump

# Backup configuration files
echo "Backing up configuration..."
tar -czf $BACKUP_DIR/$TIMESTAMP/config.tar.gz \
  .env config/ docker-compose.yml

# Backup application logs
echo "Backing up logs..."
tar -czf $BACKUP_DIR/$TIMESTAMP/logs.tar.gz logs/

# Create metadata file
cat > $BACKUP_DIR/$TIMESTAMP/metadata.json << EOF
{
  "timestamp": "$TIMESTAMP",
  "type": "daily",
  "services": {
    "terminusdb": "$(docker exec terminusdb terminusdb version)",
    "python": "$(python --version)"
  }
}
EOF

# Cleanup old backups
find $BACKUP_DIR -type d -mtime +$RETENTION_DAYS -exec rm -rf {} \;

echo "Backup completed: $BACKUP_DIR/$TIMESTAMP"
```

### Recovery Procedures

#### Full System Recovery

```bash
#!/bin/bash
# scripts/restore_backup.sh

if [ $# -eq 0 ]; then
    echo "Usage: $0 <backup_timestamp>"
    exit 1
fi

BACKUP_TIMESTAMP=$1
BACKUP_DIR="/data/backups/$BACKUP_TIMESTAMP"

# Verify backup exists
if [ ! -d "$BACKUP_DIR" ]; then
    echo "Backup not found: $BACKUP_DIR"
    exit 1
fi

# Stop services
docker-compose down

# Restore configuration
tar -xzf $BACKUP_DIR/config.tar.gz

# Restore TerminusDB
docker-compose up -d terminusdb
sleep 30
docker exec -i terminusdb terminusdb db restore admin/production \
  < $BACKUP_DIR/terminusdb.dump

# Start services
docker-compose up -d

echo "Recovery completed from backup: $BACKUP_TIMESTAMP"
```

### Version Control Operations

SPICE HARVESTER utilizes TerminusDB's Git-like version control features for data management and recovery.

#### Git-like Features
**Progress**: 7/7 features working (100%)

- ✅ Rollback (기능은 있음, **기본값 비활성화**)
- ✅ Branches (작동)
- ✅ Commits (작동)
- ✅ Push/Pull (작동)
- ✅ Conflicts (작동)
- ✅ Versioning (작동)
- ✅ Metadata Tracking (작동)

### Using Git-like Features for Data Recovery

```bash
# List available branches
curl -X GET http://localhost:8002/api/v1/branch/production/list \
  -H "X-API-Key: $API_KEY"

# Create a backup branch before risky operations
curl -X POST http://localhost:8002/api/v1/branch/production/create \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "branch_name": "backup-$(date +%Y%m%d)",
    "from_branch": "main",
    "description": "Backup before major update"
  }'

# Rollback to previous commit using Git-style references
#
# ⚠️ Rollback is DISABLED by default. Enable explicitly in non-prod only:
#   ENABLE_OMS_ROLLBACK=true
#
# OMS endpoint:
curl -X POST "http://localhost:8000/api/v1/version/production/rollback?branch=main" \
  -H "Content-Type: application/json" \
  -d '{"target": "HEAD~1"}'
```

#### Preferred Production Recovery: Versioning + Recompute (Projection rebuild)

Rollback is risky in production because it rewrites branch history/state. The recommended recovery path is:
1) Keep TerminusDB history (no rollback), and
2) Rebuild read models (Elasticsearch) by replaying immutable domain events.

**Safety contract**
- The recompute API is an admin endpoint and is **disabled by default** unless `BFF_ADMIN_TOKEN` is configured.
- Strict rate limit applies (10 requests / 60s per IP).
- Every recompute request is recorded into the audit trail (including `requested_by` / IP when provided).

**Example**
```bash
# Enable operator access (local only)
export BFF_ADMIN_TOKEN="change-me"

# Recompute instance projection on main branch
curl -X POST "http://localhost:8002/api/v1/admin/recompute-projection" \
  -H "X-Admin-Token: ${BFF_ADMIN_TOKEN}" \
  -H "X-Admin-Actor: ops@spiceharvester.local" \
  -H "Content-Type: application/json" \
  -d '{
    "db_name": "production",
    "projection": "instances",
    "branch": "main",
    "from_ts": "2025-01-01T00:00:00Z",
    "to_ts": null,
    "promote": false,
    "allow_delete_base_index": false
  }'
```

#### Monitoring Version Control Status

```python
# scripts/check_version_status.py
import asyncio
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

async def check_version_status(db_name):
    config = ConnectionConfig(
        server_url="http://localhost:6363",
        user="admin",
        account="admin",
        key="admin"
    )
    
    service = AsyncTerminusService(config)
    await service.connect()
    
    # Get current branch
    current_branch = await service.get_current_branch(db_name)
    print(f"Current branch: {current_branch}")
    
    # Get recent commits
    history = await service.get_commit_history(db_name, limit=5)
    print(f"Recent commits: {len(history)}")
    
    for commit in history:
        print(f"  - {commit['id'][:8]}: {commit['message']} by {commit['author']}")
    
    # List all branches
    branches = await service.list_branches(db_name)
    print(f"Available branches: {branches}")
    
    await service.disconnect()

# Run status check
asyncio.run(check_version_status("production"))
```

#### Best Practices for Version Control

1. **Regular Commits**: Enable implicit commits for all data modifications
2. **Branch Strategy**: Create branches for major updates or experiments
3. **Rollback Planning**: Test rollback procedures in non-production environments
4. **Commit Messages**: Use descriptive commit messages for better tracking

## Performance Tuning

### Recent Performance Optimizations (2025-07-26)
- **HTTP Connection Pooling**: 50 keep-alive connections, 100 max connections
- **Concurrency Control**: Semaphore(50) for rate limiting
- **Response Time**: Improved from 29.8s to <5s
- **Success Rate**: Increased from 70.3% to 95%+
- **Standardized Response Format**: All APIs now use consistent ApiResponse format

### Python Application Tuning

```python
# gunicorn_config.py
import multiprocessing

# Worker configuration
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000

# Timeout configuration
timeout = 300
graceful_timeout = 30
keepalive = 5

# Performance settings
max_requests = 1000
max_requests_jitter = 50

# Logging
accesslog = "logs/access.log"
errorlog = "logs/error.log"
loglevel = "info"
```

### TerminusDB Optimization

```yaml
# terminusdb_config.yml
performance:
  cache_size: 1GB
  query_timeout: 300s
  max_connections: 100
  
storage:
  compression: true
  auto_vacuum: true
  checkpoint_interval: 5m
```

### System Tuning

```bash
# /etc/sysctl.conf additions
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_syncookies = 1
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 30
vm.swappiness = 10

# Apply settings
sudo sysctl -p
```

## Troubleshooting

### Common Issues and Solutions

#### Service Won't Start

```bash
# Check port availability
sudo lsof -i :8000
sudo lsof -i :8002
sudo lsof -i :8003
sudo lsof -i :6363

# Check logs
tail -n 100 logs/oms.log
tail -n 100 logs/bff.log

# Verify environment variables
python scripts/check_config.py

# Test database connection
python scripts/test_terminus_connection.py
```

#### High Memory Usage

```bash
# Check memory usage by process
ps aux | grep python | sort -k 4 -r

# Monitor memory over time
vmstat 1 10

# Check for memory leaks
python -m memory_profiler scripts/memory_test.py
```

#### Slow Performance

```bash
# Profile application
python -m cProfile -o profile.stats scripts/performance_test.py

# Analyze database queries
python scripts/analyze_slow_queries.py

# Check network latency
ping -c 10 terminusdb
traceroute terminusdb
```

### Debug Mode Operations

```python
# scripts/debug_mode.py
import os
import logging

def enable_debug_mode():
    """Enable debug mode for troubleshooting"""
    os.environ["DEBUG"] = "true"
    os.environ["LOG_LEVEL"] = "DEBUG"
    
    # Configure detailed logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/debug.log'),
            logging.StreamHandler()
        ]
    )
    
    # Enable query logging
    os.environ["LOG_QUERIES"] = "true"
    
    print("Debug mode enabled")
    print("- Detailed logging to logs/debug.log")
    print("- Query logging enabled")
    print("- Performance metrics enabled")

if __name__ == "__main__":
    enable_debug_mode()
```

## Security Operations

### Security Checklist

#### Daily Tasks
- [ ] Review authentication logs for failed attempts
- [ ] Check for unusual API usage patterns
- [ ] Verify SSL certificate expiration dates
- [ ] Monitor disk space on all servers
- [ ] Review error logs for security exceptions

#### Weekly Tasks
- [ ] Update security patches
- [ ] Review user access permissions
- [ ] Audit API key usage
- [ ] Check for outdated dependencies
- [ ] Test backup restoration

#### Monthly Tasks
- [ ] Full security scan
- [ ] Penetration testing
- [ ] Review and update firewall rules
- [ ] Audit database access logs
- [ ] Update security documentation

### Security Monitoring

```bash
# Monitor failed authentication attempts
grep "Authentication failed" logs/*.log | tail -20

# Check for SQL injection attempts
grep -E "(UNION|SELECT.*FROM|DROP|INSERT|UPDATE|DELETE)" logs/*.log

# Monitor unusual request patterns
awk '{print $1}' logs/access.log | sort | uniq -c | sort -rn | head -20

# Check for large requests
awk '$10 > 1000000 {print $0}' logs/access.log
```

## Disaster Recovery

### Recovery Time Objectives

| Scenario | RTO | RPO | Priority |
|----------|-----|-----|----------|
| Service crash | 5 minutes | 0 | Critical |
| Database corruption | 30 minutes | 1 hour | High |
| Complete system failure | 2 hours | 4 hours | High |
| Data center failure | 4 hours | 4 hours | Medium |

### Disaster Recovery Procedures

#### Service Failure Recovery

```bash
#!/bin/bash
# scripts/service_recovery.sh

SERVICE=$1

case $SERVICE in
  "bff")
    echo "Recovering BFF service..."
    sudo systemctl restart spice-harvester-bff
    sleep 5
    curl -f http://localhost:8002/health || exit 1
    ;;
  "oms")
    echo "Recovering OMS service..."
    sudo systemctl restart spice-harvester-oms
    sleep 5
    curl -f http://localhost:8000/health || exit 1
    ;;
  "funnel")
    echo "Recovering Funnel service..."
    sudo systemctl restart spice-harvester-funnel
    sleep 5
    curl -f http://localhost:8003/health || exit 1
    ;;
  *)
    echo "Unknown service: $SERVICE"
    exit 1
    ;;
esac

echo "Service $SERVICE recovered successfully"
```

#### Database Recovery

```bash
#!/bin/bash
# scripts/database_recovery.sh

# Stop all services
docker-compose stop bff oms funnel

# Backup corrupted database
docker exec terminusdb terminusdb db dump admin/production \
  > /data/backups/corrupted_$(date +%Y%m%d_%H%M%S).dump

# Find latest good backup
LATEST_BACKUP=$(ls -t /data/backups/*/terminusdb.dump | head -1)

# Restore database
docker exec -i terminusdb terminusdb db delete admin/production
docker exec -i terminusdb terminusdb db create admin/production
docker exec -i terminusdb terminusdb db restore admin/production < $LATEST_BACKUP

# Start services
docker-compose start bff oms funnel

# Verify recovery
python scripts/verify_database_integrity.py
```

## Maintenance Procedures

### Scheduled Maintenance

#### Pre-Maintenance Checklist
1. [ ] Notify users 24 hours in advance
2. [ ] Prepare rollback plan
3. [ ] Backup current system state
4. [ ] Test updates in staging environment
5. [ ] Document expected downtime

#### Maintenance Window Procedure

```bash
#!/bin/bash
# scripts/maintenance_mode.sh

ACTION=$1

case $ACTION in
  "enable")
    # Enable maintenance mode
    echo '{"maintenance": true, "message": "System under maintenance"}' \
      > /var/www/maintenance.json
    
    # Configure nginx to serve maintenance page
    sudo ln -sf /etc/nginx/sites-available/maintenance \
      /etc/nginx/sites-enabled/default
    sudo nginx -s reload
    ;;
    
  "disable")
    # Disable maintenance mode
    rm -f /var/www/maintenance.json
    
    # Restore normal nginx configuration
    sudo ln -sf /etc/nginx/sites-available/production \
      /etc/nginx/sites-enabled/default
    sudo nginx -s reload
    ;;
    
  *)
    echo "Usage: $0 {enable|disable}"
    exit 1
    ;;
esac
```

### Update Procedures

#### Application Updates

```bash
#!/bin/bash
# scripts/update_application.sh

# Enable maintenance mode
./scripts/maintenance_mode.sh enable

# Backup current version
tar -czf /data/backups/app_backup_$(date +%Y%m%d_%H%M%S).tar.gz \
  --exclude=venv --exclude=logs --exclude=__pycache__ .

# Pull latest code
git pull origin production

# Update dependencies
source venv/bin/activate
pip install -r requirements.txt

# Run migrations if needed
python scripts/run_migrations.py

# Restart services
sudo systemctl restart spice-harvester-*

# Run health checks
python scripts/post_update_checks.py

# Disable maintenance mode
./scripts/maintenance_mode.sh disable
```

## Operational Checklists

### Daily Operations Checklist

```markdown
## Morning Checks (9:00 AM)
- [ ] Check all service health endpoints
- [ ] Review overnight error logs
- [ ] Verify backup completion
- [ ] Check disk space (>20% free)
- [ ] Monitor system resources

## Afternoon Checks (2:00 PM)
- [ ] Review API performance metrics
- [ ] Check database query performance
- [ ] Monitor active user sessions
- [ ] Verify data integrity

## Evening Checks (6:00 PM)
- [ ] Review daily usage statistics
- [ ] Check for security alerts
- [ ] Verify scheduled jobs completed
- [ ] Plan next day's maintenance
```

### Weekly Operations Checklist

```markdown
## Monday
- [ ] Review weekly performance trends
- [ ] Plan maintenance windows
- [ ] Update operational documentation

## Wednesday
- [ ] Test disaster recovery procedures
- [ ] Review and approve updates
- [ ] Audit user permissions

## Friday
- [ ] Comprehensive system health check
- [ ] Clean up old logs and backups
- [ ] Generate weekly report
```

### Incident Response Checklist

```markdown
## Initial Response (0-15 minutes)
- [ ] Acknowledge incident
- [ ] Assess severity (P1-P4)
- [ ] Notify stakeholders
- [ ] Begin investigation

## Investigation (15-60 minutes)
- [ ] Identify root cause
- [ ] Document timeline
- [ ] Implement temporary fix
- [ ] Test fix effectiveness

## Resolution (1-4 hours)
- [ ] Deploy permanent fix
- [ ] Verify system stability
- [ ] Update documentation
- [ ] Notify users of resolution

## Post-Incident (Next day)
- [ ] Conduct post-mortem
- [ ] Update runbooks
- [ ] Implement preventive measures
- [ ] Share lessons learned
```

## Appendices

### A. Service Ports Reference

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| OMS | 8000 | HTTP | Internal API |
| BFF | 8002 | HTTP | Public API |
| Funnel | 8003 | HTTP | Type inference |
| TerminusDB | 6363 | HTTP | Database |
| Redis | 6379 | TCP | Cache (optional) |
| PostgreSQL | 5433 | TCP | Idempotency/ordering/seq allocator |

### B. Log File Locations

| Service | Log Path | Rotation |
|---------|----------|----------|
| OMS | logs/oms.log | Daily, 7 days |
| BFF | logs/bff.log | Daily, 7 days |
| Funnel | logs/funnel.log | Daily, 7 days |
| Access | logs/access.log | Daily, 30 days |
| Error | logs/error.log | Daily, 30 days |

### C. Emergency Contacts

| Role | Contact | Availability |
|------|---------|--------------|
| System Administrator | ops@example.com | 24/7 |
| Database Administrator | dba@example.com | Business hours |
| Security Team | security@example.com | 24/7 |
| Development Lead | dev-lead@example.com | Business hours |

### D. External Dependencies

| Dependency | Purpose | Criticality |
|------------|---------|-------------|
| TerminusDB Cloud | Backup storage | Medium |
| AWS S3 | File storage | Low |
| SendGrid | Email notifications | Low |
| Datadog | Monitoring | Medium |
