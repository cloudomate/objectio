# Monitoring

Observability and monitoring for ObjectIO clusters.

## Metrics Overview

ObjectIO exposes Prometheus-compatible metrics from all components.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Monitoring Architecture                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Gateway  │  │ Metadata │  │   OSD    │  │   OSD    │        │
│  │ :9000    │  │  :9100   │  │  :9200   │  │  :9201   │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │               │
│       └─────────────┴─────────────┴─────────────┘               │
│                           │                                      │
│                           ▼                                      │
│                    ┌──────────────┐                             │
│                    │  Prometheus  │                             │
│                    │   :9090      │                             │
│                    └──────┬───────┘                             │
│                           │                                      │
│                           ▼                                      │
│                    ┌──────────────┐                             │
│                    │   Grafana    │                             │
│                    │   :3000      │                             │
│                    └──────────────┘                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Metrics Endpoints

| Component | Endpoint | Port |
|-----------|----------|------|
| Gateway | `/metrics` | 9000 |
| Metadata | `/metrics` | 9100 |
| OSD | `/metrics` | 9200+ |

```bash
# Example: scrape gateway metrics
curl http://localhost:9000/metrics
```

---

## Key Metrics

### Gateway Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `objectio_gateway_requests_total` | Counter | Total S3 API requests |
| `objectio_gateway_request_duration_seconds` | Histogram | Request latency |
| `objectio_gateway_request_size_bytes` | Histogram | Request body size |
| `objectio_gateway_response_size_bytes` | Histogram | Response body size |
| `objectio_gateway_active_connections` | Gauge | Current connections |
| `objectio_gateway_errors_total` | Counter | Error count by type |

**Example queries:**

```promql
# Request rate per second
rate(objectio_gateway_requests_total[5m])

# P99 latency
histogram_quantile(0.99, rate(objectio_gateway_request_duration_seconds_bucket[5m]))

# Error rate
rate(objectio_gateway_errors_total[5m]) / rate(objectio_gateway_requests_total[5m])
```

### Metadata Service Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `objectio_meta_raft_term` | Gauge | Current Raft term |
| `objectio_meta_raft_commit_index` | Gauge | Committed log index |
| `objectio_meta_raft_is_leader` | Gauge | 1 if leader, 0 otherwise |
| `objectio_meta_raft_peers` | Gauge | Number of peers |
| `objectio_meta_buckets_total` | Gauge | Total buckets |
| `objectio_meta_objects_total` | Gauge | Total objects |
| `objectio_meta_operations_total` | Counter | Metadata operations |

**Example queries:**

```promql
# Check for leader
objectio_meta_raft_is_leader == 1

# Log replication lag
objectio_meta_raft_commit_index{instance="meta1"} - ignoring(instance) objectio_meta_raft_commit_index

# Metadata operation rate
rate(objectio_meta_operations_total[5m])
```

### OSD Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `objectio_osd_disk_total_bytes` | Gauge | Total disk capacity |
| `objectio_osd_disk_used_bytes` | Gauge | Used disk space |
| `objectio_osd_disk_io_read_bytes_total` | Counter | Bytes read |
| `objectio_osd_disk_io_write_bytes_total` | Counter | Bytes written |
| `objectio_osd_disk_io_read_ops_total` | Counter | Read operations |
| `objectio_osd_disk_io_write_ops_total` | Counter | Write operations |
| `objectio_osd_objects_total` | Gauge | Objects stored |
| `objectio_osd_cache_hits_total` | Counter | Cache hits |
| `objectio_osd_cache_misses_total` | Counter | Cache misses |

**Example queries:**

```promql
# Disk usage percentage
objectio_osd_disk_used_bytes / objectio_osd_disk_total_bytes * 100

# Write throughput
rate(objectio_osd_disk_io_write_bytes_total[5m])

# Cache hit ratio
rate(objectio_osd_cache_hits_total[5m]) /
(rate(objectio_osd_cache_hits_total[5m]) + rate(objectio_osd_cache_misses_total[5m]))
```

### Erasure Coding Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `objectio_ec_encode_duration_seconds` | Histogram | Encode latency |
| `objectio_ec_decode_duration_seconds` | Histogram | Decode latency |
| `objectio_ec_repair_total` | Counter | Shards repaired |
| `objectio_ec_degraded_objects` | Gauge | Objects missing shards |

---

## Prometheus Configuration

```yaml
# /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'objectio-gateway'
    static_configs:
      - targets: ['gateway1:9000', 'gateway2:9000']

  - job_name: 'objectio-meta'
    static_configs:
      - targets: ['meta1:9100', 'meta2:9100', 'meta3:9100']

  - job_name: 'objectio-osd'
    static_configs:
      - targets: ['osd1:9200', 'osd2:9200', 'osd3:9200', 'osd4:9200', 'osd5:9200', 'osd6:9200', 'osd7:9200']
```

---

## Alerting Rules

### Critical Alerts

```yaml
# /etc/prometheus/rules/objectio.yml
groups:
  - name: objectio-critical
    rules:
      - alert: ObjectIOClusterDegraded
        expr: objectio_ec_degraded_objects > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "ObjectIO cluster has degraded objects"
          description: "{{ $value }} objects are missing shards"

      - alert: ObjectIONoLeader
        expr: sum(objectio_meta_raft_is_leader) == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "No Raft leader in metadata cluster"
          description: "Metadata cluster has no leader - writes will fail"

      - alert: ObjectIOOSDDown
        expr: up{job="objectio-osd"} == 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "OSD {{ $labels.instance }} is down"
          description: "OSD has been unreachable for 1 minute"

      - alert: ObjectIODiskFull
        expr: objectio_osd_disk_used_bytes / objectio_osd_disk_total_bytes > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "OSD disk {{ $labels.disk }} is 90% full"
          description: "Disk usage is {{ $value | humanizePercentage }}"

      - alert: ObjectIOHighLatency
        expr: histogram_quantile(0.99, rate(objectio_gateway_request_duration_seconds_bucket[5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High P99 latency on gateway"
          description: "P99 latency is {{ $value }}s"
```

### Capacity Planning Alerts

```yaml
      - alert: ObjectIOCapacityWarning
        expr: |
          (sum(objectio_osd_disk_used_bytes) / sum(objectio_osd_disk_total_bytes)) > 0.75
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "Cluster capacity above 75%"
          description: "Plan to add more storage"

      - alert: ObjectIOCapacityCritical
        expr: |
          (sum(objectio_osd_disk_used_bytes) / sum(objectio_osd_disk_total_bytes)) > 0.9
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: "Cluster capacity above 90%"
          description: "Add storage immediately"
```

---

## Grafana Dashboards

### Cluster Overview Dashboard

**Panels:**

1. **Cluster Health** (Stat)
   - Number of healthy OSDs
   - Raft leader status
   - Degraded object count

2. **Request Rate** (Graph)
   ```promql
   sum(rate(objectio_gateway_requests_total[5m])) by (method)
   ```

3. **Latency** (Graph)
   ```promql
   histogram_quantile(0.5, rate(objectio_gateway_request_duration_seconds_bucket[5m]))
   histogram_quantile(0.95, rate(objectio_gateway_request_duration_seconds_bucket[5m]))
   histogram_quantile(0.99, rate(objectio_gateway_request_duration_seconds_bucket[5m]))
   ```

4. **Throughput** (Graph)
   ```promql
   sum(rate(objectio_osd_disk_io_write_bytes_total[5m]))
   sum(rate(objectio_osd_disk_io_read_bytes_total[5m]))
   ```

5. **Storage Capacity** (Gauge)
   ```promql
   sum(objectio_osd_disk_used_bytes) / sum(objectio_osd_disk_total_bytes)
   ```

6. **OSD Status** (Table)
   ```promql
   objectio_osd_disk_used_bytes / objectio_osd_disk_total_bytes
   ```

### OSD Detail Dashboard

**Panels:**

1. **Disk I/O** (Graph)
   ```promql
   rate(objectio_osd_disk_io_read_ops_total{instance="$osd"}[5m])
   rate(objectio_osd_disk_io_write_ops_total{instance="$osd"}[5m])
   ```

2. **Cache Performance** (Graph)
   ```promql
   rate(objectio_osd_cache_hits_total{instance="$osd"}[5m])
   rate(objectio_osd_cache_misses_total{instance="$osd"}[5m])
   ```

3. **Object Count** (Stat)
   ```promql
   objectio_osd_objects_total{instance="$osd"}
   ```

---

## Logging

### Log Levels

| Level | Description |
|-------|-------------|
| `error` | Errors requiring attention |
| `warn` | Warnings about potential issues |
| `info` | Normal operational messages |
| `debug` | Detailed debugging information |
| `trace` | Very detailed tracing |

### Log Configuration

```toml
# /etc/objectio/gateway.toml
[logging]
level = "info"
format = "json"  # or "pretty"
```

### Structured Logging

All components emit JSON logs when `format = "json"`:

```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "level": "INFO",
  "target": "objectio_gateway::s3",
  "message": "Request completed",
  "request_id": "abc123",
  "method": "GET",
  "bucket": "my-bucket",
  "key": "file.txt",
  "status": 200,
  "duration_ms": 45
}
```

### Log Aggregation

Example Loki configuration:

```yaml
# /etc/promtail/config.yml
scrape_configs:
  - job_name: objectio
    static_configs:
      - targets:
          - localhost
        labels:
          job: objectio
          __path__: /var/log/objectio/*.log
    pipeline_stages:
      - json:
          expressions:
            level: level
            component: target
      - labels:
          level:
          component:
```

---

## Health Checks

### Gateway Health

```bash
# Simple health check
curl -f http://localhost:9000/health

# Response (healthy):
# {"status": "healthy", "version": "0.1.0"}

# Response (unhealthy):
# {"status": "unhealthy", "error": "cannot reach metadata service"}
```

### Deep Health Check

```bash
# Full cluster health
curl http://localhost:9000/health?deep=true

# Response:
# {
#   "status": "healthy",
#   "components": {
#     "metadata": {"status": "healthy", "leader": "meta1"},
#     "osds": {"healthy": 7, "total": 7},
#     "storage": {"used_bytes": 5000000000, "total_bytes": 10000000000}
#   }
# }
```

### Kubernetes Probes

```yaml
# Kubernetes deployment
spec:
  containers:
    - name: objectio-gateway
      livenessProbe:
        httpGet:
          path: /health
          port: 9000
        initialDelaySeconds: 10
        periodSeconds: 10
      readinessProbe:
        httpGet:
          path: /health?deep=true
          port: 9000
        initialDelaySeconds: 5
        periodSeconds: 5
```

---

## Distributed Tracing

ObjectIO supports OpenTelemetry tracing:

```toml
# /etc/objectio/gateway.toml
[tracing]
enabled = true
exporter = "otlp"
endpoint = "http://jaeger:4317"
service_name = "objectio-gateway"
sample_rate = 0.1  # 10% sampling
```

### Trace Example

```
Request: PUT /bucket/key
│
├── gateway.handle_request (45ms)
│   ├── auth.verify_signature (2ms)
│   ├── meta.get_placement (5ms)
│   ├── ec.encode (10ms)
│   │   ├── shard_1 (2ms)
│   │   ├── shard_2 (2ms)
│   │   └── ... (6 shards total)
│   └── osd.write_shards (25ms)
│       ├── osd1.write (8ms)
│       ├── osd2.write (7ms)
│       ├── osd3.write (8ms)
│       ├── osd4.write (9ms)
│       ├── osd5.write (7ms)
│       └── osd6.write (8ms)
└── Response: 200 OK
```

---

## Performance Tuning

### Identify Bottlenecks

1. **High latency at gateway?**
   ```promql
   histogram_quantile(0.99, rate(objectio_gateway_request_duration_seconds_bucket[5m]))
   ```
   → Check metadata service or OSD latency

2. **Slow metadata operations?**
   ```promql
   rate(objectio_meta_operations_total[5m])
   ```
   → Check Raft replication, disk I/O

3. **OSD I/O saturation?**
   ```promql
   rate(objectio_osd_disk_io_write_ops_total[5m])
   ```
   → Add OSDs, faster disks

4. **Low cache hit rate?**
   ```promql
   rate(objectio_osd_cache_hits_total[5m]) /
   (rate(objectio_osd_cache_hits_total[5m]) + rate(objectio_osd_cache_misses_total[5m]))
   ```
   → Increase cache size
