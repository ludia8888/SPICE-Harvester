# 🔥 THINK ULTRA! Production Migration Runbook
## PostgreSQL to S3/MinIO Event Store Migration

---

## 📋 Table of Contents
1. [Executive Summary](#executive-summary)
2. [Pre-Migration Checklist](#pre-migration-checklist)
3. [Migration Phases](#migration-phases)
4. [Rollback Procedures](#rollback-procedures)
5. [Monitoring & Validation](#monitoring--validation)
6. [Troubleshooting Guide](#troubleshooting-guide)
7. [Post-Migration Tasks](#post-migration-tasks)

---

## Executive Summary

### Migration Goal
Migrate from incorrectly using PostgreSQL as an Event Store to using S3/MinIO as the Single Source of Truth (SSoT) for Event Sourcing, while maintaining PostgreSQL for the Outbox pattern (delivery guarantee only).

### Key Architecture Change
- **BEFORE**: PostgreSQL = Event Store (WRONG) + Outbox
- **AFTER**: S3/MinIO = Event Store (SSoT), PostgreSQL = Outbox only

### Migration Strategy
Zero-downtime migration using dual-write pattern with feature flags for safe rollback capability.

---

## Pre-Migration Checklist

### 1. Infrastructure Requirements
```bash
# Verify MinIO is running
curl -I http://localhost:9000/minio/health/live

# Verify PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify Kafka is running
kafka-topics --bootstrap-server localhost:9092 --list

# Verify Elasticsearch is running
curl -s http://localhost:9201/_cluster/health | jq .status
```

### 2. Backup Current State
```bash
# Backup PostgreSQL
pg_dump -h localhost -p 5433 -U spiceadmin -d spicedb > backup_$(date +%Y%m%d_%H%M%S).sql

# Backup MinIO (if existing data)
mc mirror minio/spice-event-store ./backup/minio_$(date +%Y%m%d_%H%M%S)/

# Document current configuration
echo "ENABLE_S3_EVENT_STORE=${ENABLE_S3_EVENT_STORE}" > current_config.txt
echo "ENABLE_DUAL_WRITE=${ENABLE_DUAL_WRITE}" >> current_config.txt
```

### 3. Verify Dependencies
```python
# Check Python dependencies
pip list | grep -E "aioboto3|boto3|asyncpg"

# Required versions:
# aioboto3 >= 11.0.0
# boto3 >= 1.26.0
# asyncpg >= 0.27.0
```

### 4. Team Communication
- [ ] Notify development team of migration schedule
- [ ] Notify operations team for monitoring
- [ ] Schedule maintenance window (if needed)
- [ ] Prepare incident response team

---

## Migration Phases

### Phase 1: Enable Dual-Write Mode (Current State)
**Duration**: 1-2 hours  
**Risk Level**: LOW  
**Rollback Time**: Immediate

#### Steps:
1. **Set Environment Variables**
   ```bash
   export ENABLE_S3_EVENT_STORE=true
   export ENABLE_DUAL_WRITE=true
   export MINIO_ENDPOINT_URL=http://localhost:9000
   export MINIO_ACCESS_KEY=admin
   export MINIO_SECRET_KEY=spice123!
   ```

2. **Deploy Services with Dual-Write**
   ```bash
   # Restart OMS with dual-write
   systemctl restart oms-service
   
   # Verify dual-write mode
   curl http://localhost:8000/health | jq .migration_mode
   # Should return: "dual_write"
   ```

3. **Validate Dual Writing**
   ```python
   # Test script to verify dual-write
   python test_migration_complete.py
   ```

4. **Monitor Initial Performance**
   ```bash
   # Check S3 event creation
   mc ls minio/spice-event-store/events/ --recursive | head -10
   
   # Check PostgreSQL outbox
   psql -c "SELECT COUNT(*) FROM spice_outbox.outbox WHERE created_at > NOW() - INTERVAL '10 minutes';"
   ```

### Phase 2: Validate S3 Event Storage
**Duration**: 24-48 hours  
**Risk Level**: LOW  
**Rollback Time**: Immediate

#### Steps:
1. **Monitor S3 Write Success Rate**
   ```bash
   # Check Grafana dashboard
   open http://localhost:3000/d/s3-event-store
   
   # Or use CLI monitoring
   python monitoring/s3_event_store_dashboard.py
   ```

2. **Verify Event Consistency**
   ```sql
   -- Compare event counts
   SELECT COUNT(*) as postgres_count FROM spice_outbox.outbox;
   ```
   
   ```python
   # Compare with S3
   import boto3
   s3 = boto3.client('s3', endpoint_url='http://localhost:9000')
   response = s3.list_objects_v2(Bucket='spice-event-store')
   print(f"S3 event count: {response['KeyCount']}")
   ```

3. **Test Worker S3 Reading**
   ```bash
   # Check worker logs for S3 reads
   grep "Reading from S3" /var/log/instance_worker.log | tail -20
   grep "S3 reference" /var/log/message_relay.log | tail -20
   ```

### Phase 3: Migrate Read Path to S3
**Duration**: 1 week  
**Risk Level**: MEDIUM  
**Rollback Time**: 1 hour

#### Steps:
1. **Update Worker Configuration**
   ```bash
   # Enable S3 reading in workers
   export WORKER_READ_FROM_S3=true
   systemctl restart instance-worker
   systemctl restart projection-worker
   ```

2. **Monitor Read Performance**
   ```python
   # Check read latencies
   from monitoring.s3_event_store_dashboard import S3EventStoreDashboard
   dashboard = S3EventStoreDashboard()
   metrics = await dashboard.collect_performance_metrics()
   print(f"Average read latency: {metrics['read_operations']['average_duration_ms']}ms")
   ```

3. **Validate Event Replay**
   ```python
   # Test event replay from S3
   from oms.services.event_store import event_store
   await event_store.connect()
   events = await event_store.replay_events(
       from_timestamp=datetime.utcnow() - timedelta(hours=1)
   )
   print(f"Replayed {len(list(events))} events from S3")
   ```

### Phase 4: Disable PostgreSQL Event Storage
**Duration**: 2-4 hours  
**Risk Level**: HIGH  
**Rollback Time**: 30 minutes

#### Steps:
1. **Switch to S3-Only Mode**
   ```bash
   # CRITICAL: Ensure all validations pass before this step!
   export ENABLE_DUAL_WRITE=false
   
   # Rolling restart of services
   systemctl restart oms-service
   sleep 30
   systemctl restart message-relay
   sleep 30
   systemctl restart instance-worker
   systemctl restart projection-worker
   ```

2. **Monitor for Errors**
   ```bash
   # Watch error rates closely
   watch -n 5 'grep ERROR /var/log/oms/*.log | tail -20'
   
   # Check Grafana alerts
   curl http://localhost:3000/api/alerts | jq '.[] | select(.state=="alerting")'
   ```

3. **Verify System Health**
   ```bash
   # Run comprehensive health check
   python test_infrastructure_ultra_verification.py
   ```

---

## Rollback Procedures

### Immediate Rollback (Any Phase)
```bash
# 1. Revert to dual-write mode
export ENABLE_S3_EVENT_STORE=true
export ENABLE_DUAL_WRITE=true

# 2. Restart services
systemctl restart oms-service message-relay instance-worker projection-worker

# 3. Verify rollback
curl http://localhost:8000/health | jq .migration_mode
# Should show: "dual_write"
```

### Emergency Rollback to Legacy Mode
```bash
# 1. Disable S3 completely
export ENABLE_S3_EVENT_STORE=false
export ENABLE_DUAL_WRITE=false

# 2. Restart all services
systemctl restart oms-service message-relay instance-worker projection-worker

# 3. Verify legacy mode
curl http://localhost:8000/health | jq .migration_mode
# Should show: "legacy"

# 4. Investigate and fix issues before retry
```

---

## Monitoring & Validation

### Key Metrics to Monitor

1. **S3 Write Success Rate**
   - Target: > 99.9%
   - Alert threshold: < 99%

2. **Event Processing Latency**
   - P50: < 50ms
   - P95: < 200ms
   - P99: < 500ms

3. **Storage Growth Rate**
   ```bash
   # Monitor S3 storage growth
   mc du minio/spice-event-store --depth 1
   ```

4. **Error Rates**
   ```promql
   # Prometheus query for error rate
   rate(event_store_errors_total[5m])
   ```

### Validation Scripts

```bash
# Run validation suite
python tests/integration/test_e2e_event_sourcing_s3.py
python test_migration_complete.py

# Check data consistency
python scripts/validate_migration_consistency.py
```

---

## Troubleshooting Guide

### Common Issues and Solutions

#### 1. S3 Connection Failures
```bash
# Symptom: "Connection refused" errors
# Solution:
curl -I http://localhost:9000/minio/health/live
# If failed, restart MinIO:
docker restart spice_minio
```

#### 2. Slow S3 Writes
```bash
# Symptom: High write latencies
# Solution: Check MinIO performance
mc admin trace minio/
# Consider scaling MinIO or optimizing batch sizes
```

#### 3. Missing Events in S3
```python
# Symptom: Events in PostgreSQL but not in S3
# Solution: Check dual-write logic
from oms.services.migration_helper import migration_helper
print(f"S3 enabled: {migration_helper.s3_enabled}")
print(f"Dual write: {migration_helper.dual_write}")
# Should both be True during migration
```

#### 4. Worker Can't Read from S3
```bash
# Symptom: Workers falling back to embedded payloads
# Solution: Check S3 credentials
aws s3 ls s3://spice-event-store/ --endpoint-url http://localhost:9000
# If failed, check AWS credentials configuration
```

---

## Post-Migration Tasks

### 1. Cleanup Legacy Code
```bash
# Remove legacy event storage code
rm -f backend/legacy_event_storage.py
rm -rf backend/tests/unit/validators/  # Already consolidated

# Update documentation
sed -i 's/PostgreSQL.*Event Store/S3\/MinIO Event Store/g' README.md
```

### 2. Optimize S3 Configuration
```python
# Set lifecycle policies for old events
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:9000')
s3.put_bucket_lifecycle_configuration(
    Bucket='spice-event-store',
    LifecycleConfiguration={
        'Rules': [{
            'ID': 'archive-old-events',
            'Status': 'Enabled',
            'Transitions': [{
                'Days': 90,
                'StorageClass': 'GLACIER'
            }]
        }]
    }
)
```

### 3. Update Monitoring Alerts
```yaml
# Update Prometheus alerts
groups:
  - name: event_store_alerts
    rules:
      - alert: S3EventStoreDown
        expr: up{job="minio"} == 0
        for: 5m
        annotations:
          summary: "S3 Event Store is down"
          
      - alert: HighEventWriteLatency
        expr: histogram_quantile(0.95, event_store_write_duration_seconds) > 0.5
        for: 10m
        annotations:
          summary: "Event write latency is high"
```

### 4. Performance Tuning
```bash
# Analyze event patterns
python scripts/analyze_event_patterns.py

# Optimize batch sizes based on analysis
export EVENT_BATCH_SIZE=100  # Adjust based on findings

# Consider implementing event snapshots for frequently accessed aggregates
python scripts/create_snapshots.py --aggregate-type Order --min-events 100
```

### 5. Documentation Updates
- [ ] Update architecture diagrams
- [ ] Update API documentation
- [ ] Update developer onboarding guide
- [ ] Create troubleshooting knowledge base

---

## Success Criteria

Migration is considered successful when:

1. ✅ All events are being written to S3 (100% success rate)
2. ✅ Workers successfully read from S3 references
3. ✅ PostgreSQL only contains outbox entries (no event storage)
4. ✅ System performance meets or exceeds baseline
5. ✅ Zero data loss during migration
6. ✅ All integration tests pass
7. ✅ Monitoring dashboards show healthy metrics

---

## Emergency Contacts

- **DevOps Lead**: [Contact Info]
- **Platform Team**: [Contact Info]
- **Database Admin**: [Contact Info]
- **On-Call Engineer**: [PagerDuty/Slack]

---

## Appendix

### A. Environment Variables Reference
```bash
ENABLE_S3_EVENT_STORE      # Enable S3 as Event Store (true/false)
ENABLE_DUAL_WRITE          # Enable dual-write mode (true/false)
MINIO_ENDPOINT_URL         # MinIO endpoint (http://localhost:9000)
MINIO_ACCESS_KEY           # MinIO access key
MINIO_SECRET_KEY           # MinIO secret key
EVENT_STORE_BUCKET         # S3 bucket name (spice-event-store)
```

### B. Useful Commands
```bash
# Check migration status
curl http://localhost:8000/api/v1/migration/status

# Force event replay from S3
python scripts/force_replay.py --from "2024-11-01" --to "2024-11-14"

# Export metrics for analysis
python monitoring/export_metrics.py --format csv --output metrics.csv
```

### C. References
- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [MinIO Documentation](https://min.io/docs/)
- [S3 API Reference](https://docs.aws.amazon.com/s3/index.html)

---

**Last Updated**: November 14, 2024  
**Version**: 1.0.0  
**Status**: READY FOR PRODUCTION

---

## 🔥 THINK ULTRA! 
### Remember: PostgreSQL is NOT an Event Store! S3/MinIO is our Single Source of Truth!