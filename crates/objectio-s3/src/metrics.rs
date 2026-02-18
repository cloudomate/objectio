//! S3 API metrics for Prometheus
//!
//! Tracks S3 operations, latencies, and error rates.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Protection configuration for capacity metrics
#[derive(Debug, Clone)]
pub struct ProtectionConfig {
    /// Protection scheme: "ec", "lrc", or "replication"
    pub scheme: String,
    /// Data shards (k) — for EC/LRC. For replication, 1.
    pub data_shards: u32,
    /// Total parity shards (m) — for EC: parity, for LRC: local+global, for replication: replicas-1
    pub parity_shards: u32,
    /// Total shards (data + parity). For replication: replicas.
    pub total_shards: u32,
    /// Storage efficiency ratio (0.0 - 1.0): data_shards / total_shards
    pub efficiency: f64,
    /// LRC local parity (0 if not LRC)
    pub lrc_local_parity: u32,
    /// LRC global parity (0 if not LRC)
    pub lrc_global_parity: u32,
}

/// Iceberg operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IcebergOperation {
    GetConfig,
    ListNamespaces,
    CreateNamespace,
    LoadNamespace,
    DropNamespace,
    NamespaceExists,
    UpdateNamespaceProperties,
    ListTables,
    CreateTable,
    LoadTable,
    UpdateTable,
    DropTable,
    TableExists,
    RenameTable,
}

impl IcebergOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            IcebergOperation::GetConfig => "GetConfig",
            IcebergOperation::ListNamespaces => "ListNamespaces",
            IcebergOperation::CreateNamespace => "CreateNamespace",
            IcebergOperation::LoadNamespace => "LoadNamespace",
            IcebergOperation::DropNamespace => "DropNamespace",
            IcebergOperation::NamespaceExists => "NamespaceExists",
            IcebergOperation::UpdateNamespaceProperties => "UpdateNamespaceProperties",
            IcebergOperation::ListTables => "ListTables",
            IcebergOperation::CreateTable => "CreateTable",
            IcebergOperation::LoadTable => "LoadTable",
            IcebergOperation::UpdateTable => "UpdateTable",
            IcebergOperation::DropTable => "DropTable",
            IcebergOperation::TableExists => "TableExists",
            IcebergOperation::RenameTable => "RenameTable",
        }
    }
}

/// S3 operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    ListObjects,
    GetObject,
    PutObject,
    DeleteObject,
    HeadObject,
    CopyObject,
    InitiateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,
    DeleteObjects,
}

impl S3Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            S3Operation::ListBuckets => "ListBuckets",
            S3Operation::CreateBucket => "CreateBucket",
            S3Operation::DeleteBucket => "DeleteBucket",
            S3Operation::HeadBucket => "HeadBucket",
            S3Operation::ListObjects => "ListObjects",
            S3Operation::GetObject => "GetObject",
            S3Operation::PutObject => "PutObject",
            S3Operation::DeleteObject => "DeleteObject",
            S3Operation::HeadObject => "HeadObject",
            S3Operation::CopyObject => "CopyObject",
            S3Operation::InitiateMultipartUpload => "InitiateMultipartUpload",
            S3Operation::UploadPart => "UploadPart",
            S3Operation::CompleteMultipartUpload => "CompleteMultipartUpload",
            S3Operation::AbortMultipartUpload => "AbortMultipartUpload",
            S3Operation::ListParts => "ListParts",
            S3Operation::DeleteObjects => "DeleteObjects",
        }
    }
}

/// Per-operation metrics
#[derive(Debug, Default)]
struct OperationMetrics {
    /// Total requests
    requests_total: AtomicU64,
    /// Successful requests (2xx)
    requests_success: AtomicU64,
    /// Client errors (4xx)
    requests_client_error: AtomicU64,
    /// Server errors (5xx)
    requests_server_error: AtomicU64,
    /// Total request bytes
    request_bytes_total: AtomicU64,
    /// Total response bytes
    response_bytes_total: AtomicU64,
    /// Latency sum in microseconds
    latency_sum_us: AtomicU64,
    /// Latency histogram buckets (cumulative counts)
    /// Buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 5s, 10s
    latency_buckets: [AtomicU64; 11],
}

const LATENCY_BUCKET_BOUNDARIES_MS: [u64; 11] =
    [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000];

impl OperationMetrics {
    #[allow(dead_code)]
    fn new() -> Self {
        Self::default()
    }

    fn record(&self, status_code: u16, request_bytes: u64, response_bytes: u64, latency_us: u64) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);

        if (200..300).contains(&status_code) {
            self.requests_success.fetch_add(1, Ordering::Relaxed);
        } else if (400..500).contains(&status_code) {
            self.requests_client_error.fetch_add(1, Ordering::Relaxed);
        } else if status_code >= 500 {
            self.requests_server_error.fetch_add(1, Ordering::Relaxed);
        }

        self.request_bytes_total
            .fetch_add(request_bytes, Ordering::Relaxed);
        self.response_bytes_total
            .fetch_add(response_bytes, Ordering::Relaxed);
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);

        // Update histogram buckets
        let latency_ms = latency_us / 1000;
        for (i, &boundary) in LATENCY_BUCKET_BOUNDARIES_MS.iter().enumerate() {
            if latency_ms <= boundary {
                self.latency_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Gateway-level metrics
#[derive(Debug, Default)]
struct GatewayMetrics {
    /// Active connections
    active_connections: AtomicU64,
    /// Total connections
    total_connections: AtomicU64,
    /// OSD pool connections per OSD
    osd_connections: RwLock<HashMap<String, u64>>,
    /// Scatter-gather operations
    scatter_gather_ops: AtomicU64,
    /// Scatter-gather latency sum
    scatter_gather_latency_us: AtomicU64,
}

/// Iceberg policy decision tracking key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PolicyDecisionKey {
    action: String,
    decision: String,
}

/// S3 metrics collector
#[derive(Debug)]
pub struct S3Metrics {
    /// Per-operation metrics
    operations: RwLock<HashMap<S3Operation, OperationMetrics>>,
    /// Per-Iceberg-operation metrics
    iceberg_operations: RwLock<HashMap<IcebergOperation, OperationMetrics>>,
    /// Iceberg policy decision counters
    iceberg_policy_decisions: RwLock<HashMap<PolicyDecisionKey, AtomicU64>>,
    /// Gateway metrics
    gateway: GatewayMetrics,
    /// Start time for uptime calculation
    start_time: Instant,
    /// Protection configuration for capacity calculations
    protection: RwLock<Option<ProtectionConfig>>,
}

impl S3Metrics {
    /// Create a new S3 metrics collector
    pub fn new() -> Self {
        Self {
            operations: RwLock::new(HashMap::new()),
            iceberg_operations: RwLock::new(HashMap::new()),
            iceberg_policy_decisions: RwLock::new(HashMap::new()),
            gateway: GatewayMetrics::default(),
            start_time: Instant::now(),
            protection: RwLock::new(None),
        }
    }

    /// Set the protection configuration (called once at startup)
    pub fn set_protection_config(&self, config: ProtectionConfig) {
        *self.protection.write().unwrap() = Some(config);
    }

    /// Record an S3 operation
    pub fn record_operation(
        &self,
        op: S3Operation,
        status_code: u16,
        request_bytes: u64,
        response_bytes: u64,
        latency_us: u64,
    ) {
        let mut ops = self.operations.write().unwrap();
        let metrics = ops.entry(op).or_default();
        metrics.record(status_code, request_bytes, response_bytes, latency_us);
    }

    /// Record a simple operation (no body sizes)
    pub fn record_simple(&self, op: S3Operation, status_code: u16, latency_us: u64) {
        self.record_operation(op, status_code, 0, 0, latency_us);
    }

    /// Record an Iceberg policy decision
    pub fn record_iceberg_policy_decision(&self, action: &str, decision: &str) {
        let key = PolicyDecisionKey {
            action: action.to_string(),
            decision: decision.to_string(),
        };
        let mut decisions = self.iceberg_policy_decisions.write().unwrap();
        decisions
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record an Iceberg operation
    pub fn record_iceberg_operation(
        &self,
        op: IcebergOperation,
        status_code: u16,
        latency_us: u64,
    ) {
        let mut ops = self.iceberg_operations.write().unwrap();
        let metrics = ops.entry(op).or_default();
        metrics.record(status_code, 0, 0, latency_us);
    }

    /// Increment active connections
    pub fn connection_opened(&self) {
        self.gateway
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        self.gateway
            .total_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connections
    pub fn connection_closed(&self) {
        self.gateway
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }

    /// Update OSD connection count
    pub fn update_osd_connections(&self, osd_id: &str, count: u64) {
        self.gateway
            .osd_connections
            .write()
            .unwrap()
            .insert(osd_id.to_string(), count);
    }

    /// Record scatter-gather operation
    pub fn record_scatter_gather(&self, latency_us: u64) {
        self.gateway
            .scatter_gather_ops
            .fetch_add(1, Ordering::Relaxed);
        self.gateway
            .scatter_gather_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::with_capacity(8 * 1024);

        // Gateway uptime
        let uptime_secs = self.start_time.elapsed().as_secs();
        writeln!(
            output,
            "# HELP objectio_gateway_uptime_seconds Gateway uptime in seconds"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_gateway_uptime_seconds counter").unwrap();
        writeln!(output, "objectio_gateway_uptime_seconds {}", uptime_secs).unwrap();

        // Active connections
        writeln!(
            output,
            "# HELP objectio_gateway_active_connections Current active connections"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_gateway_active_connections gauge").unwrap();
        writeln!(
            output,
            "objectio_gateway_active_connections {}",
            self.gateway.active_connections.load(Ordering::Relaxed)
        )
        .unwrap();

        // Total connections
        writeln!(
            output,
            "# HELP objectio_gateway_connections_total Total connections since start"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_gateway_connections_total counter").unwrap();
        writeln!(
            output,
            "objectio_gateway_connections_total {}",
            self.gateway.total_connections.load(Ordering::Relaxed)
        )
        .unwrap();

        // OSD connections
        let osd_conns = self.gateway.osd_connections.read().unwrap();
        if !osd_conns.is_empty() {
            writeln!(
                output,
                "# HELP objectio_gateway_osd_connections Connections to each OSD"
            )
            .unwrap();
            writeln!(output, "# TYPE objectio_gateway_osd_connections gauge").unwrap();
            for (osd_id, count) in osd_conns.iter() {
                writeln!(
                    output,
                    "objectio_gateway_osd_connections{{osd_id=\"{}\"}} {}",
                    osd_id, count
                )
                .unwrap();
            }
        }

        // Scatter-gather metrics
        let sg_ops = self.gateway.scatter_gather_ops.load(Ordering::Relaxed);
        if sg_ops > 0 {
            writeln!(
                output,
                "# HELP objectio_gateway_scatter_gather_total Total scatter-gather operations"
            )
            .unwrap();
            writeln!(
                output,
                "# TYPE objectio_gateway_scatter_gather_total counter"
            )
            .unwrap();
            writeln!(output, "objectio_gateway_scatter_gather_total {}", sg_ops).unwrap();

            let sg_latency = self
                .gateway
                .scatter_gather_latency_us
                .load(Ordering::Relaxed);
            writeln!(output, "# HELP objectio_gateway_scatter_gather_latency_seconds_sum Sum of scatter-gather latencies").unwrap();
            writeln!(
                output,
                "# TYPE objectio_gateway_scatter_gather_latency_seconds_sum counter"
            )
            .unwrap();
            writeln!(
                output,
                "objectio_gateway_scatter_gather_latency_seconds_sum {}",
                sg_latency as f64 / 1_000_000.0
            )
            .unwrap();
        }

        // Protection configuration metrics
        if let Some(ref prot) = *self.protection.read().unwrap() {
            writeln!(
                output,
                "# HELP objectio_gateway_protection_data_shards Number of data shards (k)"
            )
            .unwrap();
            writeln!(
                output,
                "# TYPE objectio_gateway_protection_data_shards gauge"
            )
            .unwrap();
            writeln!(
                output,
                "objectio_gateway_protection_data_shards{{scheme=\"{}\"}} {}",
                prot.scheme, prot.data_shards
            )
            .unwrap();

            writeln!(
                output,
                "# HELP objectio_gateway_protection_parity_shards Number of parity shards (m)"
            )
            .unwrap();
            writeln!(
                output,
                "# TYPE objectio_gateway_protection_parity_shards gauge"
            )
            .unwrap();
            writeln!(
                output,
                "objectio_gateway_protection_parity_shards{{scheme=\"{}\"}} {}",
                prot.scheme, prot.parity_shards
            )
            .unwrap();

            writeln!(
                output,
                "# HELP objectio_gateway_protection_total_shards Total shards (data + parity)"
            )
            .unwrap();
            writeln!(
                output,
                "# TYPE objectio_gateway_protection_total_shards gauge"
            )
            .unwrap();
            writeln!(
                output,
                "objectio_gateway_protection_total_shards{{scheme=\"{}\"}} {}",
                prot.scheme, prot.total_shards
            )
            .unwrap();

            writeln!(output, "# HELP objectio_gateway_protection_efficiency Storage efficiency ratio (data/total)").unwrap();
            writeln!(
                output,
                "# TYPE objectio_gateway_protection_efficiency gauge"
            )
            .unwrap();
            writeln!(
                output,
                "objectio_gateway_protection_efficiency{{scheme=\"{}\"}} {:.6}",
                prot.scheme, prot.efficiency
            )
            .unwrap();

            if prot.scheme == "lrc" {
                writeln!(
                    output,
                    "# HELP objectio_gateway_protection_lrc_local_parity LRC local parity shards"
                )
                .unwrap();
                writeln!(
                    output,
                    "# TYPE objectio_gateway_protection_lrc_local_parity gauge"
                )
                .unwrap();
                writeln!(
                    output,
                    "objectio_gateway_protection_lrc_local_parity {}",
                    prot.lrc_local_parity
                )
                .unwrap();

                writeln!(
                    output,
                    "# HELP objectio_gateway_protection_lrc_global_parity LRC global parity shards"
                )
                .unwrap();
                writeln!(
                    output,
                    "# TYPE objectio_gateway_protection_lrc_global_parity gauge"
                )
                .unwrap();
                writeln!(
                    output,
                    "objectio_gateway_protection_lrc_global_parity {}",
                    prot.lrc_global_parity
                )
                .unwrap();
            }
        }

        // S3 operation metrics
        let ops = self.operations.read().unwrap();

        // Requests total
        writeln!(
            output,
            "# HELP objectio_s3_requests_total Total S3 requests by operation and status"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_s3_requests_total counter").unwrap();
        for (op, metrics) in ops.iter() {
            let op_name: &str = op.as_str();
            let success = metrics.requests_success.load(Ordering::Relaxed);
            let client_err = metrics.requests_client_error.load(Ordering::Relaxed);
            let server_err = metrics.requests_server_error.load(Ordering::Relaxed);

            writeln!(
                output,
                "objectio_s3_requests_total{{operation=\"{}\",status=\"success\"}} {}",
                op_name, success
            )
            .unwrap();
            writeln!(
                output,
                "objectio_s3_requests_total{{operation=\"{}\",status=\"client_error\"}} {}",
                op_name, client_err
            )
            .unwrap();
            writeln!(
                output,
                "objectio_s3_requests_total{{operation=\"{}\",status=\"server_error\"}} {}",
                op_name, server_err
            )
            .unwrap();
        }

        // Request/response bytes
        writeln!(
            output,
            "# HELP objectio_s3_request_bytes_total Total request body bytes"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_s3_request_bytes_total counter").unwrap();
        for (op, metrics) in ops.iter() {
            let op_name: &str = op.as_str();
            writeln!(
                output,
                "objectio_s3_request_bytes_total{{operation=\"{}\"}} {}",
                op_name,
                metrics.request_bytes_total.load(Ordering::Relaxed)
            )
            .unwrap();
        }

        writeln!(
            output,
            "# HELP objectio_s3_response_bytes_total Total response body bytes"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_s3_response_bytes_total counter").unwrap();
        for (op, metrics) in ops.iter() {
            let op_name: &str = op.as_str();
            writeln!(
                output,
                "objectio_s3_response_bytes_total{{operation=\"{}\"}} {}",
                op_name,
                metrics.response_bytes_total.load(Ordering::Relaxed)
            )
            .unwrap();
        }

        // Latency histogram
        writeln!(
            output,
            "# HELP objectio_s3_request_duration_seconds S3 request duration histogram"
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE objectio_s3_request_duration_seconds histogram"
        )
        .unwrap();
        for (op, metrics) in ops.iter() {
            let op_name: &str = op.as_str();
            let total = metrics.requests_total.load(Ordering::Relaxed);
            let sum_us = metrics.latency_sum_us.load(Ordering::Relaxed);

            // Buckets
            let mut cumulative = 0u64;
            for (i, &boundary_ms) in LATENCY_BUCKET_BOUNDARIES_MS.iter().enumerate() {
                cumulative += metrics.latency_buckets[i].load(Ordering::Relaxed);
                writeln!(
                    output,
                    "objectio_s3_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {}",
                    op_name,
                    boundary_ms as f64 / 1000.0,
                    cumulative
                )
                .unwrap();
            }
            writeln!(
                output,
                "objectio_s3_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {}",
                op_name, total
            )
            .unwrap();
            writeln!(
                output,
                "objectio_s3_request_duration_seconds_sum{{operation=\"{}\"}} {}",
                op_name,
                sum_us as f64 / 1_000_000.0
            )
            .unwrap();
            writeln!(
                output,
                "objectio_s3_request_duration_seconds_count{{operation=\"{}\"}} {}",
                op_name, total
            )
            .unwrap();
        }

        // Iceberg operation metrics
        let iceberg_ops = self.iceberg_operations.read().unwrap();
        if !iceberg_ops.is_empty() {
            writeln!(
                output,
                "# HELP objectio_iceberg_requests_total Total Iceberg requests by operation and status"
            )
            .unwrap();
            writeln!(output, "# TYPE objectio_iceberg_requests_total counter").unwrap();
            for (op, metrics) in iceberg_ops.iter() {
                let op_name = op.as_str();
                let success = metrics.requests_success.load(Ordering::Relaxed);
                let client_err = metrics.requests_client_error.load(Ordering::Relaxed);
                let server_err = metrics.requests_server_error.load(Ordering::Relaxed);
                writeln!(
                    output,
                    "objectio_iceberg_requests_total{{operation=\"{}\",status=\"success\"}} {}",
                    op_name, success
                )
                .unwrap();
                writeln!(
                    output,
                    "objectio_iceberg_requests_total{{operation=\"{}\",status=\"client_error\"}} {}",
                    op_name, client_err
                )
                .unwrap();
                writeln!(
                    output,
                    "objectio_iceberg_requests_total{{operation=\"{}\",status=\"server_error\"}} {}",
                    op_name, server_err
                )
                .unwrap();
            }

            writeln!(
                output,
                "# HELP objectio_iceberg_request_duration_seconds Iceberg request duration histogram"
            )
            .unwrap();
            writeln!(
                output,
                "# TYPE objectio_iceberg_request_duration_seconds histogram"
            )
            .unwrap();
            for (op, metrics) in iceberg_ops.iter() {
                let op_name = op.as_str();
                let total = metrics.requests_total.load(Ordering::Relaxed);
                let sum_us = metrics.latency_sum_us.load(Ordering::Relaxed);

                let mut cumulative = 0u64;
                for (i, &boundary_ms) in LATENCY_BUCKET_BOUNDARIES_MS.iter().enumerate() {
                    cumulative += metrics.latency_buckets[i].load(Ordering::Relaxed);
                    writeln!(
                        output,
                        "objectio_iceberg_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {}",
                        op_name,
                        boundary_ms as f64 / 1000.0,
                        cumulative
                    )
                    .unwrap();
                }
                writeln!(
                    output,
                    "objectio_iceberg_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {}",
                    op_name, total
                )
                .unwrap();
                writeln!(
                    output,
                    "objectio_iceberg_request_duration_seconds_sum{{operation=\"{}\"}} {}",
                    op_name,
                    sum_us as f64 / 1_000_000.0
                )
                .unwrap();
                writeln!(
                    output,
                    "objectio_iceberg_request_duration_seconds_count{{operation=\"{}\"}} {}",
                    op_name, total
                )
                .unwrap();
            }
        }

        // Iceberg policy decision metrics
        let policy_decisions = self.iceberg_policy_decisions.read().unwrap();
        if !policy_decisions.is_empty() {
            writeln!(
                output,
                "# HELP objectio_iceberg_policy_decisions_total Total Iceberg policy decisions by action and decision"
            )
            .unwrap();
            writeln!(
                output,
                "# TYPE objectio_iceberg_policy_decisions_total counter"
            )
            .unwrap();
            for (key, count) in policy_decisions.iter() {
                writeln!(
                    output,
                    "objectio_iceberg_policy_decisions_total{{action=\"{}\",decision=\"{}\"}} {}",
                    key.action,
                    key.decision,
                    count.load(Ordering::Relaxed)
                )
                .unwrap();
            }
        }

        output
    }
}

impl Default for S3Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Global S3 metrics instance
static S3_METRICS: std::sync::OnceLock<S3Metrics> = std::sync::OnceLock::new();

/// Get the global S3 metrics instance
pub fn s3_metrics() -> &'static S3Metrics {
    S3_METRICS.get_or_init(S3Metrics::new)
}

/// RAII guard for timing an operation
pub struct OperationTimer {
    op: S3Operation,
    start: Instant,
    request_bytes: u64,
}

impl OperationTimer {
    /// Start timing an operation
    pub fn new(op: S3Operation) -> Self {
        Self {
            op,
            start: Instant::now(),
            request_bytes: 0,
        }
    }

    /// Set the request body size
    pub fn with_request_bytes(mut self, bytes: u64) -> Self {
        self.request_bytes = bytes;
        self
    }

    /// Complete the operation with a response
    pub fn complete(self, status_code: u16, response_bytes: u64) {
        let latency_us = self.start.elapsed().as_micros() as u64;
        s3_metrics().record_operation(
            self.op,
            status_code,
            self.request_bytes,
            response_bytes,
            latency_us,
        );
    }

    /// Complete with just status code
    pub fn complete_simple(self, status_code: u16) {
        self.complete(status_code, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_operation() {
        let metrics = S3Metrics::new();
        metrics.record_operation(S3Operation::GetObject, 200, 0, 1024, 5000);
        metrics.record_operation(S3Operation::GetObject, 404, 0, 0, 1000);
        metrics.record_operation(S3Operation::PutObject, 200, 2048, 0, 10000);

        let output = metrics.export_prometheus();
        assert!(output.contains("objectio_s3_requests_total"));
        assert!(output.contains("GetObject"));
        assert!(output.contains("PutObject"));
    }

    #[test]
    fn test_record_iceberg_operation() {
        let metrics = S3Metrics::new();
        metrics.record_iceberg_operation(IcebergOperation::ListNamespaces, 200, 3000);
        metrics.record_iceberg_operation(IcebergOperation::CreateTable, 409, 5000);
        metrics.record_iceberg_operation(IcebergOperation::LoadTable, 500, 10000);

        let output = metrics.export_prometheus();
        assert!(output.contains("objectio_iceberg_requests_total"));
        assert!(output.contains("ListNamespaces"));
        assert!(output.contains("CreateTable"));
        assert!(output.contains("objectio_iceberg_request_duration_seconds"));
    }

    #[test]
    fn test_latency_histogram() {
        let metrics = S3Metrics::new();

        // Record operations with different latencies
        metrics.record_operation(S3Operation::GetObject, 200, 0, 100, 500); // 0.5ms
        metrics.record_operation(S3Operation::GetObject, 200, 0, 100, 5000); // 5ms
        metrics.record_operation(S3Operation::GetObject, 200, 0, 100, 50000); // 50ms
        metrics.record_operation(S3Operation::GetObject, 200, 0, 100, 500000); // 500ms

        let output = metrics.export_prometheus();
        assert!(output.contains("objectio_s3_request_duration_seconds_bucket"));
        assert!(output.contains("le=\"0.001\"")); // 1ms bucket
        assert!(output.contains("le=\"0.05\"")); // 50ms bucket
    }
}
