# Kafka Broker Configuration for Production
## Event Sourcing + CQRS with At-Least-Once Delivery

This document describes the required Kafka broker configuration for SPICE HARVESTER production deployment.

## 🚀 Quick Start

### 1. Broker Configuration (`server.properties`)

```properties
############################# Server Basics #############################
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:9092

############################# Log Basics #############################
log.dirs=/var/kafka-logs
num.partitions=3
default.replication.factor=3

############################# Durability Settings #############################
# CRITICAL: Minimum in-sync replicas for acks=all
min.insync.replicas=2

# Enable automatic leader election
unclean.leader.election.enable=false

############################# Delivery Semantics #############################
# At-least-once delivery; consumers are idempotent via processed_events.
# Enable idempotent producers if you operate a multi-broker cluster.
# transaction.* settings are optional and depend on your Kafka deployment.

############################# Log Retention #############################
log.retention.hours=168  # 7 days
log.segment.bytes=1073741824  # 1GB
log.retention.check.interval.ms=300000  # 5 minutes

############################# Compression #############################
compression.type=snappy

############################# Performance Tuning #############################
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Replica fetching
replica.fetch.min.bytes=1
replica.fetch.wait.max.ms=500
replica.lag.time.max.ms=30000

############################# Group Coordinator #############################
group.initial.rebalance.delay.ms=3000
offsets.topic.replication.factor=3
offsets.topic.num.partitions=50
```

## 📊 Topic Configuration

### Create Topics with Proper Settings

```bash
# Instance Events Topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic instance_events \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

# Instance Commands Topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic instance_commands \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config compression.type=snappy

# Ontology Events Topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic ontology_events \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

# Ontology Commands Topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic ontology_commands \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

# DLQ Topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic projection_failures_dlq \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000 \
  --config compression.type=snappy

# Poison Queue Topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic projection_failures_dlq.poison \
  --partitions 1 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=-1 \
  --config compression.type=gzip
```

## 🔧 Cluster Setup (3-node minimum)

### Node 1 Configuration
```properties
broker.id=1
listeners=PLAINTEXT://kafka1:9092
log.dirs=/var/kafka-logs-1
```

### Node 2 Configuration
```properties
broker.id=2
listeners=PLAINTEXT://kafka2:9092
log.dirs=/var/kafka-logs-2
```

### Node 3 Configuration
```properties
broker.id=3
listeners=PLAINTEXT://kafka3:9092
log.dirs=/var/kafka-logs-3
```

## 🎯 Critical Settings Explanation

### 1. **min.insync.replicas=2**
- Ensures at least 2 replicas acknowledge writes
- Prevents data loss if one broker fails
- Works with `acks=all` in producers

### 2. **unclean.leader.election.enable=false**
- Prevents out-of-sync replicas from becoming leader
- Guarantees no data loss (may impact availability)

### 3. **transaction.state.log.min.isr=2**
- Required only if you enable Kafka transactions
- Not required for SPICE HARVESTER correctness (handled by idempotent consumers)

### 4. **compression.type=snappy**
- Balance between compression ratio and CPU usage
- Reduces network and disk I/O

## 📈 Monitoring Metrics

### Key Metrics to Monitor

1. **Under-replicated Partitions**
   ```bash
   kafka-topics.sh --describe --under-replicated-partitions \
     --bootstrap-server localhost:9092
   ```

2. **Consumer Lag**
   ```bash
   kafka-consumer-groups.sh --describe \
     --group instance-worker-group \
     --bootstrap-server localhost:9092
   ```

3. **ISR (In-Sync Replicas)**
   ```bash
   kafka-topics.sh --describe --topic instance_events \
     --bootstrap-server localhost:9092
   ```

## 🚨 Alerts Configuration

Set up alerts for:

1. **High Consumer Lag** (> 10,000 messages)
2. **Under-replicated Partitions** (> 0)
3. **Offline Partitions** (> 0)
4. **ISR Shrink** (< configured replicas)
5. **Disk Usage** (> 80%)
6. **Transaction Coordinator Issues**

## 🔐 Security Configuration (Production)

```properties
# SSL Configuration
listeners=SSL://0.0.0.0:9093
security.inter.broker.protocol=SSL
ssl.keystore.location=/var/kafka/kafka.keystore.jks
ssl.keystore.password=your-keystore-password
ssl.key.password=your-key-password
ssl.truststore.location=/var/kafka/kafka.truststore.jks
ssl.truststore.password=your-truststore-password

# SASL Configuration (optional)
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
```

## 🎯 Performance Tuning

### JVM Settings (`kafka-server-start.sh`)

```bash
export KAFKA_HEAP_OPTS="-Xmx4G -Xms4G"
export KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"
```

### OS Tuning (Linux)

```bash
# Increase file descriptors
ulimit -n 100000

# Disable swapping
sudo swapoff -a

# Tune network stack
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
```

## 📝 Maintenance Tasks

### Weekly
- Check consumer lag trends
- Review disk usage
- Validate replication status

### Monthly
- Rebalance partitions if needed
- Clean up old log segments
- Review and adjust retention policies

### Quarterly
- Upgrade Kafka version (if needed)
- Review and optimize topic configurations
- Performance baseline testing

## 🆘 Troubleshooting

### High Consumer Lag
1. Check consumer group status
2. Increase consumer instances
3. Review processing logic for bottlenecks
4. Consider increasing partitions

### Under-replicated Partitions
1. Check broker health
2. Review network connectivity
3. Check disk space
4. Review replication throttles

### Transaction Timeouts
1. Increase `transaction.max.timeout.ms`
2. Review producer batch settings
3. Check network latency
4. Monitor broker load

## 📚 References

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Exactly-Once Semantics](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Kafka Performance Tuning](https://docs.confluent.io/platform/current/kafka/deployment.html)

---

**Last Updated**: 2024-08-12
**Version**: 1.0.0
**Author**: SPICE HARVESTER Team (THINK ULTRA³)
