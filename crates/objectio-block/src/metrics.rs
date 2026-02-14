//! Metrics collection and Prometheus exporter for block storage
//!
//! This module provides:
//! - **MetricsCollector**: Aggregates metrics from volumes, OSDs, and cluster
//! - **PrometheusExporter**: Formats metrics in Prometheus text format
//!
//! # Prometheus Metrics
//!
//! ## Volume Metrics
//! - `objectio_volume_read_iops` - Current read IOPS per volume
//! - `objectio_volume_write_iops` - Current write IOPS per volume
//! - `objectio_volume_read_bytes_total` - Total bytes read per volume
//! - `objectio_volume_write_bytes_total` - Total bytes written per volume
//! - `objectio_volume_read_latency_seconds` - Read latency histogram
//! - `objectio_volume_write_latency_seconds` - Write latency histogram
//! - `objectio_volume_throttled_ios_total` - Throttled I/Os due to QoS
//! - `objectio_volume_size_bytes` - Provisioned volume size
//! - `objectio_volume_used_bytes` - Used space per volume
//!
//! ## OSD Metrics
//! - `objectio_osd_capacity_bytes` - Total OSD capacity
//! - `objectio_osd_used_bytes` - Used OSD capacity
//! - `objectio_osd_read_iops` - Current read IOPS per OSD
//! - `objectio_osd_write_iops` - Current write IOPS per OSD
//! - `objectio_osd_disk_health` - Disk health status (0=error, 1=warning, 2=healthy)
//!
//! ## Cluster Metrics
//! - `objectio_cluster_total_capacity_bytes` - Total cluster capacity
//! - `objectio_cluster_used_capacity_bytes` - Used cluster capacity
//! - `objectio_cluster_healthy_osds` - Number of healthy OSDs
//! - `objectio_cluster_total_volumes` - Total number of volumes

use std::collections::HashMap;
use std::fmt::Write;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::qos::{LatencyHistogram, LatencyPercentiles, VolumeRateLimiter};

/// Bucket boundaries for Prometheus histogram (in seconds)
const PROMETHEUS_BUCKET_BOUNDARIES: &[f64] = &[
    0.00001, // 10us
    0.00002, // 20us
    0.00005, // 50us
    0.0001,  // 100us
    0.0002,  // 200us
    0.0005,  // 500us
    0.001,   // 1ms
    0.002,   // 2ms
    0.005,   // 5ms
    0.01,    // 10ms
    0.02,    // 20ms
    0.05,    // 50ms
    0.1,     // 100ms
    0.2,     // 200ms
    0.5,     // 500ms
];

/// Volume metrics snapshot
#[derive(Debug, Clone, Default)]
pub struct VolumeMetrics {
    pub volume_id: String,
    pub name: String,
    pub pool: String,
    pub size_bytes: u64,
    pub used_bytes: u64,
    pub read_ops: u64,
    pub write_ops: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_iops: u64,
    pub write_iops: u64,
    pub throttled_ios: u64,
    pub read_latency: LatencyPercentiles,
    pub write_latency: LatencyPercentiles,
    pub qos_max_iops: u64,
    pub qos_min_iops: u64,
    pub iops_utilization: f64,
}

/// OSD metrics snapshot
#[derive(Debug, Clone, Default)]
pub struct OsdMetrics {
    pub osd_id: String,
    pub node_name: String,
    pub rack: String,
    pub datacenter: String,
    pub region: String,
    pub status: HealthStatus,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub read_iops: u64,
    pub write_iops: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub pending_ios: u64,
    pub active_ios: u64,
    pub read_latency: LatencyPercentiles,
    pub write_latency: LatencyPercentiles,
    pub disk_count: u32,
    pub healthy_disks: u32,
}

/// Health status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HealthStatus {
    #[default]
    Unknown = 0,
    Healthy = 1,
    Warning = 2,
    Error = 3,
}

impl HealthStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            HealthStatus::Unknown => "unknown",
            HealthStatus::Healthy => "healthy",
            HealthStatus::Warning => "warning",
            HealthStatus::Error => "error",
        }
    }

    pub fn as_value(&self) -> f64 {
        match self {
            HealthStatus::Unknown => 0.0,
            HealthStatus::Healthy => 1.0,
            HealthStatus::Warning => 0.5,
            HealthStatus::Error => 0.0,
        }
    }
}

/// Cluster metrics snapshot
#[derive(Debug, Clone, Default)]
pub struct ClusterMetrics {
    pub timestamp: u64,
    pub total_osds: u32,
    pub healthy_osds: u32,
    pub warning_osds: u32,
    pub error_osds: u32,
    pub total_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub total_read_iops: u64,
    pub total_write_iops: u64,
    pub total_volumes: u32,
    pub attached_volumes: u32,
    pub total_provisioned_bytes: u64,
    pub total_throttled_ios: u64,
    pub total_reserved_iops: u64,
    pub degraded_objects: u32,
    pub recovering_objects: u32,
}

/// Metrics collector that aggregates metrics from various sources
#[derive(Debug)]
pub struct MetricsCollector {
    /// Volume metrics indexed by volume_id
    volumes: RwLock<HashMap<String, VolumeMetrics>>,
    /// OSD metrics indexed by osd_id
    osds: RwLock<HashMap<String, OsdMetrics>>,
    /// Last cluster metrics snapshot
    cluster: RwLock<ClusterMetrics>,
    /// Collection interval
    collection_interval: Duration,
    /// Last collection time
    last_collection: RwLock<Instant>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(collection_interval: Duration) -> Self {
        Self {
            volumes: RwLock::new(HashMap::new()),
            osds: RwLock::new(HashMap::new()),
            cluster: RwLock::new(ClusterMetrics::default()),
            collection_interval,
            last_collection: RwLock::new(Instant::now()),
        }
    }

    /// Update volume metrics from rate limiter
    pub fn update_volume_from_limiter(
        &self,
        limiter: &VolumeRateLimiter,
        size_bytes: u64,
        used_bytes: u64,
    ) {
        let stats = limiter.stats();
        let config = limiter.config();

        let metrics = VolumeMetrics {
            volume_id: limiter.volume_id().to_string(),
            name: String::new(), // Set by caller if needed
            pool: String::new(),
            size_bytes,
            used_bytes,
            read_ops: stats.read_ops(),
            write_ops: stats.write_ops(),
            read_bytes: stats.read_bytes(),
            write_bytes: stats.write_bytes(),
            read_iops: stats.current_iops(), // Approximation
            write_iops: 0,
            throttled_ios: stats.throttled_ops(),
            read_latency: stats.read_latency_percentiles(),
            write_latency: stats.write_latency_percentiles(),
            qos_max_iops: config.max_iops,
            qos_min_iops: config.min_iops,
            iops_utilization: limiter.iops_utilization(),
        };

        self.volumes
            .write()
            .insert(limiter.volume_id().to_string(), metrics);
    }

    /// Update volume metrics directly
    pub fn update_volume(&self, metrics: VolumeMetrics) {
        self.volumes
            .write()
            .insert(metrics.volume_id.clone(), metrics);
    }

    /// Update OSD metrics
    pub fn update_osd(&self, metrics: OsdMetrics) {
        self.osds.write().insert(metrics.osd_id.clone(), metrics);
    }

    /// Update cluster metrics
    pub fn update_cluster(&self, metrics: ClusterMetrics) {
        *self.cluster.write() = metrics;
    }

    /// Remove volume metrics
    pub fn remove_volume(&self, volume_id: &str) {
        self.volumes.write().remove(volume_id);
    }

    /// Remove OSD metrics
    pub fn remove_osd(&self, osd_id: &str) {
        self.osds.write().remove(osd_id);
    }

    /// Get all volume metrics
    pub fn get_volumes(&self) -> Vec<VolumeMetrics> {
        self.volumes.read().values().cloned().collect()
    }

    /// Get all OSD metrics
    pub fn get_osds(&self) -> Vec<OsdMetrics> {
        self.osds.read().values().cloned().collect()
    }

    /// Get cluster metrics
    pub fn get_cluster(&self) -> ClusterMetrics {
        self.cluster.read().clone()
    }

    /// Get volume metrics by ID
    pub fn get_volume(&self, volume_id: &str) -> Option<VolumeMetrics> {
        self.volumes.read().get(volume_id).cloned()
    }

    /// Get OSD metrics by ID
    pub fn get_osd(&self, osd_id: &str) -> Option<OsdMetrics> {
        self.osds.read().get(osd_id).cloned()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new(Duration::from_secs(15))
    }
}

/// Prometheus text format exporter
#[derive(Debug)]
pub struct PrometheusExporter {
    /// Metrics prefix (e.g., "objectio")
    prefix: String,
}

impl PrometheusExporter {
    /// Create a new Prometheus exporter with the given prefix
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Export metrics in Prometheus text format
    pub fn export(&self, collector: &MetricsCollector) -> String {
        let mut output = String::with_capacity(16 * 1024);

        self.export_volume_metrics(&mut output, collector);
        self.export_osd_metrics(&mut output, collector);
        self.export_cluster_metrics(&mut output, collector);

        output
    }

    fn export_volume_metrics(&self, output: &mut String, collector: &MetricsCollector) {
        let volumes = collector.get_volumes();
        if volumes.is_empty() {
            return;
        }

        // Volume size
        self.write_help(
            output,
            "volume_size_bytes",
            "Provisioned volume size in bytes",
        );
        self.write_type(output, "volume_size_bytes", "gauge");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_size_bytes",
                vol.size_bytes as f64,
                &[("volume_id", &vol.volume_id), ("pool", &vol.pool)],
            );
        }

        // Volume used bytes
        self.write_help(output, "volume_used_bytes", "Used space in volume");
        self.write_type(output, "volume_used_bytes", "gauge");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_used_bytes",
                vol.used_bytes as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }

        // Read operations total
        self.write_help(output, "volume_read_ops_total", "Total read operations");
        self.write_type(output, "volume_read_ops_total", "counter");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_read_ops_total",
                vol.read_ops as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }

        // Write operations total
        self.write_help(output, "volume_write_ops_total", "Total write operations");
        self.write_type(output, "volume_write_ops_total", "counter");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_write_ops_total",
                vol.write_ops as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }

        // Read bytes total
        self.write_help(output, "volume_read_bytes_total", "Total bytes read");
        self.write_type(output, "volume_read_bytes_total", "counter");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_read_bytes_total",
                vol.read_bytes as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }

        // Write bytes total
        self.write_help(output, "volume_write_bytes_total", "Total bytes written");
        self.write_type(output, "volume_write_bytes_total", "counter");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_write_bytes_total",
                vol.write_bytes as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }

        // Throttled I/Os
        self.write_help(
            output,
            "volume_throttled_ios_total",
            "Total I/Os throttled by QoS",
        );
        self.write_type(output, "volume_throttled_ios_total", "counter");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_throttled_ios_total",
                vol.throttled_ios as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }

        // IOPS utilization
        self.write_help(
            output,
            "volume_iops_utilization",
            "Current IOPS utilization (0-1)",
        );
        self.write_type(output, "volume_iops_utilization", "gauge");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_iops_utilization",
                vol.iops_utilization,
                &[("volume_id", &vol.volume_id)],
            );
        }

        // Read latency percentiles (as summary)
        self.write_help(
            output,
            "volume_read_latency_seconds",
            "Read latency in seconds",
        );
        self.write_type(output, "volume_read_latency_seconds", "summary");
        for vol in &volumes {
            self.export_latency_summary(
                output,
                "volume_read_latency_seconds",
                &vol.volume_id,
                &vol.read_latency,
            );
        }

        // Write latency percentiles (as summary)
        self.write_help(
            output,
            "volume_write_latency_seconds",
            "Write latency in seconds",
        );
        self.write_type(output, "volume_write_latency_seconds", "summary");
        for vol in &volumes {
            self.export_latency_summary(
                output,
                "volume_write_latency_seconds",
                &vol.volume_id,
                &vol.write_latency,
            );
        }

        // QoS configuration
        self.write_help(output, "volume_qos_max_iops", "Maximum IOPS limit");
        self.write_type(output, "volume_qos_max_iops", "gauge");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_qos_max_iops",
                vol.qos_max_iops as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }

        self.write_help(output, "volume_qos_min_iops", "Guaranteed minimum IOPS");
        self.write_type(output, "volume_qos_min_iops", "gauge");
        for vol in &volumes {
            self.write_metric_with_labels(
                output,
                "volume_qos_min_iops",
                vol.qos_min_iops as f64,
                &[("volume_id", &vol.volume_id)],
            );
        }
    }

    fn export_osd_metrics(&self, output: &mut String, collector: &MetricsCollector) {
        let osds = collector.get_osds();
        if osds.is_empty() {
            return;
        }

        // OSD health status
        self.write_help(
            output,
            "osd_health",
            "OSD health status (1=healthy, 0.5=warning, 0=error)",
        );
        self.write_type(output, "osd_health", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_health",
                osd.status.as_value(),
                &[
                    ("osd_id", &osd.osd_id),
                    ("node", &osd.node_name),
                    ("rack", &osd.rack),
                    ("datacenter", &osd.datacenter),
                ],
            );
        }

        // OSD capacity
        self.write_help(output, "osd_capacity_bytes", "Total OSD capacity in bytes");
        self.write_type(output, "osd_capacity_bytes", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_capacity_bytes",
                osd.capacity_bytes as f64,
                &[("osd_id", &osd.osd_id)],
            );
        }

        // OSD used bytes
        self.write_help(output, "osd_used_bytes", "Used space on OSD in bytes");
        self.write_type(output, "osd_used_bytes", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_used_bytes",
                osd.used_bytes as f64,
                &[("osd_id", &osd.osd_id)],
            );
        }

        // OSD read IOPS
        self.write_help(output, "osd_read_iops", "Current read IOPS");
        self.write_type(output, "osd_read_iops", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_read_iops",
                osd.read_iops as f64,
                &[("osd_id", &osd.osd_id)],
            );
        }

        // OSD write IOPS
        self.write_help(output, "osd_write_iops", "Current write IOPS");
        self.write_type(output, "osd_write_iops", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_write_iops",
                osd.write_iops as f64,
                &[("osd_id", &osd.osd_id)],
            );
        }

        // OSD pending I/Os
        self.write_help(output, "osd_pending_ios", "I/Os waiting in queue");
        self.write_type(output, "osd_pending_ios", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_pending_ios",
                osd.pending_ios as f64,
                &[("osd_id", &osd.osd_id)],
            );
        }

        // OSD disk count
        self.write_help(output, "osd_disk_count", "Number of disks in OSD");
        self.write_type(output, "osd_disk_count", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_disk_count",
                osd.disk_count as f64,
                &[("osd_id", &osd.osd_id)],
            );
        }

        // OSD healthy disks
        self.write_help(output, "osd_healthy_disks", "Number of healthy disks");
        self.write_type(output, "osd_healthy_disks", "gauge");
        for osd in &osds {
            self.write_metric_with_labels(
                output,
                "osd_healthy_disks",
                osd.healthy_disks as f64,
                &[("osd_id", &osd.osd_id)],
            );
        }

        // OSD latency summaries
        self.write_help(output, "osd_read_latency_seconds", "OSD read latency");
        self.write_type(output, "osd_read_latency_seconds", "summary");
        for osd in &osds {
            self.export_latency_summary(
                output,
                "osd_read_latency_seconds",
                &osd.osd_id,
                &osd.read_latency,
            );
        }

        self.write_help(output, "osd_write_latency_seconds", "OSD write latency");
        self.write_type(output, "osd_write_latency_seconds", "summary");
        for osd in &osds {
            self.export_latency_summary(
                output,
                "osd_write_latency_seconds",
                &osd.osd_id,
                &osd.write_latency,
            );
        }
    }

    fn export_cluster_metrics(&self, output: &mut String, collector: &MetricsCollector) {
        let cluster = collector.get_cluster();

        // Cluster OSD counts
        self.write_help(output, "cluster_osds_total", "Total number of OSDs");
        self.write_type(output, "cluster_osds_total", "gauge");
        self.write_metric(output, "cluster_osds_total", cluster.total_osds as f64);

        self.write_help(output, "cluster_osds_healthy", "Number of healthy OSDs");
        self.write_type(output, "cluster_osds_healthy", "gauge");
        self.write_metric(output, "cluster_osds_healthy", cluster.healthy_osds as f64);

        self.write_help(
            output,
            "cluster_osds_warning",
            "Number of OSDs in warning state",
        );
        self.write_type(output, "cluster_osds_warning", "gauge");
        self.write_metric(output, "cluster_osds_warning", cluster.warning_osds as f64);

        self.write_help(
            output,
            "cluster_osds_error",
            "Number of OSDs in error state",
        );
        self.write_type(output, "cluster_osds_error", "gauge");
        self.write_metric(output, "cluster_osds_error", cluster.error_osds as f64);

        // Cluster capacity
        self.write_help(output, "cluster_capacity_bytes", "Total cluster capacity");
        self.write_type(output, "cluster_capacity_bytes", "gauge");
        self.write_metric(
            output,
            "cluster_capacity_bytes",
            cluster.total_capacity_bytes as f64,
        );

        self.write_help(output, "cluster_used_bytes", "Used cluster capacity");
        self.write_type(output, "cluster_used_bytes", "gauge");
        self.write_metric(
            output,
            "cluster_used_bytes",
            cluster.used_capacity_bytes as f64,
        );

        self.write_help(
            output,
            "cluster_available_bytes",
            "Available cluster capacity",
        );
        self.write_type(output, "cluster_available_bytes", "gauge");
        self.write_metric(
            output,
            "cluster_available_bytes",
            cluster.available_capacity_bytes as f64,
        );

        // Cluster I/O
        self.write_help(output, "cluster_read_iops", "Total cluster read IOPS");
        self.write_type(output, "cluster_read_iops", "gauge");
        self.write_metric(output, "cluster_read_iops", cluster.total_read_iops as f64);

        self.write_help(output, "cluster_write_iops", "Total cluster write IOPS");
        self.write_type(output, "cluster_write_iops", "gauge");
        self.write_metric(
            output,
            "cluster_write_iops",
            cluster.total_write_iops as f64,
        );

        // Volumes
        self.write_help(output, "cluster_volumes_total", "Total number of volumes");
        self.write_type(output, "cluster_volumes_total", "gauge");
        self.write_metric(
            output,
            "cluster_volumes_total",
            cluster.total_volumes as f64,
        );

        self.write_help(
            output,
            "cluster_volumes_attached",
            "Number of attached volumes",
        );
        self.write_type(output, "cluster_volumes_attached", "gauge");
        self.write_metric(
            output,
            "cluster_volumes_attached",
            cluster.attached_volumes as f64,
        );

        self.write_help(
            output,
            "cluster_provisioned_bytes",
            "Total provisioned capacity",
        );
        self.write_type(output, "cluster_provisioned_bytes", "gauge");
        self.write_metric(
            output,
            "cluster_provisioned_bytes",
            cluster.total_provisioned_bytes as f64,
        );

        // QoS
        self.write_help(
            output,
            "cluster_throttled_ios_total",
            "Total throttled I/Os",
        );
        self.write_type(output, "cluster_throttled_ios_total", "counter");
        self.write_metric(
            output,
            "cluster_throttled_ios_total",
            cluster.total_throttled_ios as f64,
        );

        self.write_help(
            output,
            "cluster_reserved_iops",
            "Total reserved IOPS (min_iops sum)",
        );
        self.write_type(output, "cluster_reserved_iops", "gauge");
        self.write_metric(
            output,
            "cluster_reserved_iops",
            cluster.total_reserved_iops as f64,
        );

        // Recovery
        self.write_help(
            output,
            "cluster_degraded_objects",
            "Number of degraded objects",
        );
        self.write_type(output, "cluster_degraded_objects", "gauge");
        self.write_metric(
            output,
            "cluster_degraded_objects",
            cluster.degraded_objects as f64,
        );

        self.write_help(
            output,
            "cluster_recovering_objects",
            "Objects being recovered",
        );
        self.write_type(output, "cluster_recovering_objects", "gauge");
        self.write_metric(
            output,
            "cluster_recovering_objects",
            cluster.recovering_objects as f64,
        );
    }

    fn export_latency_summary(
        &self,
        output: &mut String,
        name: &str,
        id: &str,
        latency: &LatencyPercentiles,
    ) {
        let id_label = if name.starts_with("volume") {
            "volume_id"
        } else {
            "osd_id"
        };

        // p50
        let _ = writeln!(
            output,
            "{}_{}{{{id_label}=\"{id}\",quantile=\"0.5\"}} {}",
            self.prefix,
            name,
            latency.p50 as f64 / 1_000_000.0
        );
        // p90
        let _ = writeln!(
            output,
            "{}_{}{{{id_label}=\"{id}\",quantile=\"0.9\"}} {}",
            self.prefix,
            name,
            latency.p90 as f64 / 1_000_000.0
        );
        // p95
        let _ = writeln!(
            output,
            "{}_{}{{{id_label}=\"{id}\",quantile=\"0.95\"}} {}",
            self.prefix,
            name,
            latency.p95 as f64 / 1_000_000.0
        );
        // p99
        let _ = writeln!(
            output,
            "{}_{}{{{id_label}=\"{id}\",quantile=\"0.99\"}} {}",
            self.prefix,
            name,
            latency.p99 as f64 / 1_000_000.0
        );
        // p999
        let _ = writeln!(
            output,
            "{}_{}{{{id_label}=\"{id}\",quantile=\"0.999\"}} {}",
            self.prefix,
            name,
            latency.p999 as f64 / 1_000_000.0
        );
    }

    fn write_help(&self, output: &mut String, name: &str, help: &str) {
        let _ = writeln!(output, "# HELP {}_{} {}", self.prefix, name, help);
    }

    fn write_type(&self, output: &mut String, name: &str, metric_type: &str) {
        let _ = writeln!(output, "# TYPE {}_{} {}", self.prefix, name, metric_type);
    }

    fn write_metric(&self, output: &mut String, name: &str, value: f64) {
        let _ = writeln!(output, "{}_{} {}", self.prefix, name, value);
    }

    fn write_metric_with_labels(
        &self,
        output: &mut String,
        name: &str,
        value: f64,
        labels: &[(&str, &str)],
    ) {
        let labels_str: Vec<String> = labels
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, v))
            .collect();
        let _ = writeln!(
            output,
            "{}_{}{{{}}} {}",
            self.prefix,
            name,
            labels_str.join(","),
            value
        );
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new("objectio")
    }
}

/// Export latency histogram in Prometheus histogram format
pub fn export_histogram_prometheus(
    output: &mut String,
    prefix: &str,
    name: &str,
    labels: &[(&str, &str)],
    histogram: &LatencyHistogram,
) {
    let labels_str: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, v))
        .collect();
    let base_labels = if labels_str.is_empty() {
        String::new()
    } else {
        labels_str.join(",")
    };

    // Export bucket counts (cumulative)
    let buckets = histogram.bucket_counts();
    let mut cumulative = 0u64;

    for (i, (boundary_us, count)) in buckets.iter().enumerate() {
        cumulative += count;
        let boundary_secs = *boundary_us as f64 / 1_000_000.0;

        if base_labels.is_empty() {
            let _ = writeln!(
                output,
                "{prefix}_{name}_bucket{{le=\"{boundary_secs}\"}} {cumulative}"
            );
        } else {
            let _ = writeln!(
                output,
                "{prefix}_{name}_bucket{{{base_labels},le=\"{boundary_secs}\"}} {cumulative}"
            );
        }
    }

    // +Inf bucket
    if base_labels.is_empty() {
        let _ = writeln!(
            output,
            "{prefix}_{name}_bucket{{le=\"+Inf\"}} {}",
            histogram.count()
        );
    } else {
        let _ = writeln!(
            output,
            "{prefix}_{name}_bucket{{{base_labels},le=\"+Inf\"}} {}",
            histogram.count()
        );
    }

    // Sum and count
    let sum_secs = histogram.sum() as f64 / 1_000_000.0;
    if base_labels.is_empty() {
        let _ = writeln!(output, "{prefix}_{name}_sum {sum_secs}");
        let _ = writeln!(output, "{prefix}_{name}_count {}", histogram.count());
    } else {
        let _ = writeln!(output, "{prefix}_{name}_sum{{{base_labels}}} {sum_secs}");
        let _ = writeln!(
            output,
            "{prefix}_{name}_count{{{base_labels}}} {}",
            histogram.count()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_export() {
        let collector = MetricsCollector::default();

        // Add volume metrics
        collector.update_volume(VolumeMetrics {
            volume_id: "vol-1".to_string(),
            name: "test-volume".to_string(),
            pool: "default".to_string(),
            size_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            used_bytes: 10 * 1024 * 1024 * 1024,  // 10GB
            read_ops: 1000,
            write_ops: 500,
            read_bytes: 4 * 1024 * 1024,
            write_bytes: 2 * 1024 * 1024,
            read_iops: 100,
            write_iops: 50,
            throttled_ios: 10,
            read_latency: LatencyPercentiles {
                avg: 100,
                min: 10,
                max: 1000,
                p50: 80,
                p90: 200,
                p95: 300,
                p99: 500,
                p999: 900,
            },
            write_latency: LatencyPercentiles::default(),
            qos_max_iops: 1000,
            qos_min_iops: 100,
            iops_utilization: 0.15,
        });

        let exporter = PrometheusExporter::default();
        let output = exporter.export(&collector);

        // Verify output contains expected metrics
        assert!(output.contains("objectio_volume_size_bytes"));
        assert!(output.contains("vol-1"));
        assert!(output.contains("107374182400")); // 100GB
    }

    #[test]
    fn test_cluster_metrics() {
        let collector = MetricsCollector::default();

        collector.update_cluster(ClusterMetrics {
            total_osds: 10,
            healthy_osds: 8,
            warning_osds: 1,
            error_osds: 1,
            total_capacity_bytes: 1024 * 1024 * 1024 * 1024, // 1TB
            used_capacity_bytes: 512 * 1024 * 1024 * 1024,   // 512GB
            available_capacity_bytes: 512 * 1024 * 1024 * 1024,
            total_volumes: 50,
            attached_volumes: 30,
            ..Default::default()
        });

        let exporter = PrometheusExporter::default();
        let output = exporter.export(&collector);

        assert!(output.contains("cluster_osds_total"));
        assert!(output.contains("cluster_osds_healthy"));
        assert!(output.contains(" 8")); // 8 healthy OSDs
    }

    #[test]
    fn test_histogram_export() {
        let histogram = LatencyHistogram::new();
        histogram.record(50); // 50us
        histogram.record(150); // 150us
        histogram.record(5000); // 5ms

        let mut output = String::new();
        export_histogram_prometheus(
            &mut output,
            "objectio",
            "test_latency_seconds",
            &[("volume_id", "vol-1")],
            &histogram,
        );

        assert!(output.contains("objectio_test_latency_seconds_bucket"));
        assert!(output.contains("objectio_test_latency_seconds_sum"));
        assert!(output.contains("objectio_test_latency_seconds_count"));
        assert!(output.contains("volume_id=\"vol-1\""));
    }
}
