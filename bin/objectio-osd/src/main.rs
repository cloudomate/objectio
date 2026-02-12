//! ObjectIO OSD - Object Storage Daemon
//!
//! This binary provides the storage node service.

mod service;

use anyhow::Result;
use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use clap::Parser;
use objectio_block::metrics::{MetricsCollector, OsdMetrics, PrometheusExporter, HealthStatus};
use objectio_proto::metadata::{
    metadata_service_client::MetadataServiceClient, FailureDomainInfo, RegisterOsdRequest,
};
use objectio_proto::storage::storage_service_server::StorageServiceServer;
use objectio_storage::SmartMonitor;
use serde::Deserialize;
use service::OsdService;
use std::fmt::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "objectio-osd")]
#[command(about = "ObjectIO Object Storage Daemon")]
#[command(version)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "/etc/objectio/osd.toml")]
    config: String,

    /// Listen address for gRPC
    #[arg(short, long)]
    listen: Option<String>,

    /// Advertise address (how other services reach this OSD)
    /// If not set, derived from listen address
    #[arg(long)]
    advertise_addr: Option<String>,

    /// Disk paths to use for storage
    #[arg(long)]
    disks: Vec<String>,

    /// Metadata service endpoint
    #[arg(long)]
    meta_endpoint: Option<String>,

    /// Data directory for OSD metadata
    #[arg(long)]
    data_dir: Option<String>,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Metrics server port (Prometheus)
    #[arg(long, default_value = "9201")]
    metrics_port: u16,
}

/// Configuration file structure
#[derive(Debug, Deserialize, Default)]
struct Config {
    #[serde(default)]
    osd: OsdConfig,
    #[serde(default)]
    storage: StorageConfig,
    #[serde(default)]
    logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Default)]
struct OsdConfig {
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    node_name: Option<String>,
    #[serde(default = "default_listen")]
    listen: String,
    /// Address to advertise to metadata service (how other services reach this OSD)
    #[serde(default)]
    advertise_addr: Option<String>,
    #[serde(default = "default_meta_endpoint")]
    meta_endpoint: String,
    #[serde(default)]
    failure_domain: FailureDomainConfig,
    #[serde(default = "default_weight")]
    weight: f64,
}

/// Failure domain configuration - defines where this OSD sits in the topology
#[derive(Debug, Deserialize, Clone)]
struct FailureDomainConfig {
    #[serde(default = "default_region")]
    region: String,
    #[serde(default = "default_datacenter")]
    datacenter: String,
    #[serde(default = "default_rack")]
    rack: String,
}

impl Default for FailureDomainConfig {
    fn default() -> Self {
        Self {
            region: default_region(),
            datacenter: default_datacenter(),
            rack: default_rack(),
        }
    }
}

fn default_region() -> String {
    "default".to_string()
}

fn default_datacenter() -> String {
    "default".to_string()
}

fn default_rack() -> String {
    "default".to_string()
}

fn default_weight() -> f64 {
    1.0
}

#[derive(Debug, Deserialize, Default)]
struct StorageConfig {
    #[serde(default)]
    disks: Vec<String>,
    #[serde(default = "default_block_size")]
    block_size: usize,
    #[serde(default = "default_data_dir")]
    data_dir: String,
}

#[derive(Debug, Deserialize, Default)]
struct LoggingConfig {
    #[serde(default = "default_log_level")]
    level: String,
}

fn default_listen() -> String {
    "0.0.0.0:9002".to_string()
}

fn default_meta_endpoint() -> String {
    "http://localhost:9001".to_string()
}

fn default_block_size() -> usize {
    4194304
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_data_dir() -> String {
    "./osd-data".to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Load config file if it exists
    let config: Config = if std::path::Path::new(&args.config).exists() {
        let config_str = std::fs::read_to_string(&args.config)?;
        toml::from_str(&config_str).unwrap_or_else(|e| {
            eprintln!("Warning: Failed to parse config file: {}", e);
            Config::default()
        })
    } else {
        Config::default()
    };

    // Merge CLI args with config file (CLI takes precedence)
    let listen = args.listen.unwrap_or(config.osd.listen);
    let meta_endpoint = args.meta_endpoint.unwrap_or(config.osd.meta_endpoint);
    let disks = if args.disks.is_empty() {
        config.storage.disks
    } else {
        args.disks
    };
    let block_size = config.storage.block_size;
    let data_dir = args.data_dir.unwrap_or(config.storage.data_dir);
    let log_level = if args.log_level != "info" {
        args.log_level
    } else {
        config.logging.level
    };

    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting ObjectIO OSD");
    info!("Config file: {}", args.config);
    info!("Disks: {:?}", disks);
    info!("Block size: {} bytes ({} MB)", block_size, block_size / 1024 / 1024);

    if disks.is_empty() {
        error!("No disks specified. Use --disks or configure in {}", args.config);
        std::process::exit(1);
    }

    // Initialize OSD service
    let data_path = PathBuf::from(&data_dir);
    info!("Data directory: {}", data_dir);
    let disk_paths = disks.clone();
    let osd_service = match OsdService::new(disk_paths, block_size as u32, data_path) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to initialize OSD: {}", e);
            std::process::exit(1);
        }
    };

    let node_id_bytes = *osd_service.node_id();
    let node_id = hex::encode(&node_id_bytes);
    info!("OSD node ID: {}", node_id);

    // Get disk IDs for registration
    let disk_ids: Vec<Vec<u8>> = osd_service.disk_ids().iter().map(|d| d.to_vec()).collect();
    info!("OSD managing {} disks", disk_ids.len());

    // Parse listen address
    let addr = listen.parse().map_err(|e| {
        anyhow::anyhow!("Invalid listen address {}: {}", listen, e)
    })?;

    // Determine the address to advertise to the metadata service
    // Priority: CLI --advertise-addr > config advertise_addr > derived from listen address
    let advertise_addr = if let Some(addr) = args.advertise_addr {
        // Use explicit CLI advertise address
        if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else {
            format!("http://{}", addr)
        }
    } else if let Some(addr) = &config.osd.advertise_addr {
        // Use config file advertise address
        if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.clone()
        } else {
            format!("http://{}", addr)
        }
    } else if listen.starts_with("0.0.0.0") {
        // Fallback: use localhost when listening on all interfaces
        format!("http://127.0.0.1:{}", listen.split(':').last().unwrap_or("9002"))
    } else {
        format!("http://{}", listen)
    };
    info!("Advertising at: {}", advertise_addr);

    // Log failure domain configuration
    let failure_domain = config.osd.failure_domain.clone();
    let node_name = config.osd.node_name.clone();
    let weight = config.osd.weight;
    info!(
        "Failure domain: region={}, datacenter={}, rack={}",
        failure_domain.region, failure_domain.datacenter, failure_domain.rack
    );

    // Register with metadata service
    let meta_endpoint_clone = meta_endpoint.clone();
    let node_id_for_reg = node_id_bytes;
    let disk_ids_for_reg = disk_ids.clone();
    let advertise_addr_clone = advertise_addr.clone();
    let failure_domain_for_reg = failure_domain.clone();
    let node_name_for_reg = node_name.clone();

    // Spawn registration task
    let registration_handle = tokio::spawn(async move {
        register_with_meta(
            &meta_endpoint_clone,
            &node_id_for_reg,
            &advertise_addr_clone,
            &disk_ids_for_reg,
            &failure_domain_for_reg,
            node_name_for_reg.as_deref(),
            weight,
        )
        .await
    });

    // Wrap service in Arc for sharing
    let osd_service = Arc::new(osd_service);

    // Create SMART monitor
    let smart_monitor = Arc::new(SmartMonitor::new(&node_id, Duration::from_secs(300)));
    let disk_devices = disks.clone();

    // Check if SMART monitoring is available
    if SmartMonitor::is_available() {
        info!("SMART monitoring available - disk health will be tracked");
    } else {
        warn!("SMART monitoring unavailable (smartctl not found) - disk health metrics will be limited");
    }

    // Create metrics state
    let metrics_collector = Arc::new(MetricsCollector::default());
    let metrics_state = Arc::new(OsdMetricsState {
        osd_service: osd_service.clone(),
        collector: metrics_collector.clone(),
        exporter: PrometheusExporter::default(),
        osd_id: node_id.clone(),
        node_name: node_name.clone().unwrap_or_else(|| "unknown".to_string()),
        failure_domain: failure_domain.clone(),
        start_time: std::time::Instant::now(),
        smart_monitor: smart_monitor.clone(),
        disk_devices: disk_devices.clone(),
    });

    // Start metrics server
    let metrics_port = args.metrics_port;
    let metrics_state_clone = metrics_state.clone();
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(metrics_port, metrics_state_clone).await {
            error!("Metrics server error: {}", e);
        }
    });

    info!("Starting gRPC server on {}", addr);
    info!("Metrics available at http://0.0.0.0:{}/metrics", metrics_port);

    // Start heartbeat task
    let heartbeat_meta_endpoint = meta_endpoint.clone();
    let heartbeat_node_id = node_id_bytes;
    let heartbeat_disk_ids = disk_ids.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_loop(&heartbeat_meta_endpoint, &heartbeat_node_id, &heartbeat_disk_ids).await
    });

    // Start gRPC server with increased message size limit (100MB for large objects)
    let max_message_size = 100 * 1024 * 1024; // 100 MB
    let storage_service = StorageServiceServer::from_arc(osd_service)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);

    let server_future = Server::builder()
        .add_service(storage_service)
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
        });

    // Wait for registration to complete (with timeout)
    tokio::select! {
        result = registration_handle => {
            match result {
                Ok(Ok(())) => info!("Registered with metadata service"),
                Ok(Err(e)) => warn!("Failed to register with metadata service: {} (continuing anyway)", e),
                Err(e) => warn!("Registration task error: {} (continuing anyway)", e),
            }
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            warn!("Registration timed out (continuing anyway)");
        }
    }

    // Run server (heartbeat continues in background)
    server_future.await?;

    // Cancel heartbeat on shutdown
    heartbeat_handle.abort();

    info!("OSD shut down gracefully");

    Ok(())
}

/// Register this OSD with the metadata service
async fn register_with_meta(
    meta_endpoint: &str,
    node_id: &[u8; 16],
    address: &str,
    disk_ids: &[Vec<u8>],
    failure_domain: &FailureDomainConfig,
    node_name: Option<&str>,
    weight: f64,
) -> Result<(), String> {
    info!(
        "Registering OSD with metadata service at {}",
        meta_endpoint
    );

    // Connect to metadata service
    let mut client = MetadataServiceClient::connect(meta_endpoint.to_string())
        .await
        .map_err(|e| format!("Failed to connect to metadata service: {}", e))?;

    // Call RegisterOsd RPC with failure domain
    let response = client
        .register_osd(RegisterOsdRequest {
            node_id: node_id.to_vec(),
            address: address.to_string(),
            disk_ids: disk_ids.to_vec(),
            failure_domain: Some(FailureDomainInfo {
                region: failure_domain.region.clone(),
                datacenter: failure_domain.datacenter.clone(),
                rack: failure_domain.rack.clone(),
            }),
            node_name: node_name.unwrap_or_default().to_string(),
            weight,
        })
        .await
        .map_err(|e| format!("Failed to register OSD: {}", e))?;

    info!(
        "OSD {} with {} disks ready to serve at {} (topology v{})",
        hex::encode(&node_id[..4]),
        disk_ids.len(),
        address,
        response.into_inner().topology_version
    );

    Ok(())
}

/// OSD metrics state for the HTTP server
struct OsdMetricsState {
    osd_service: Arc<OsdService>,
    collector: Arc<MetricsCollector>,
    exporter: PrometheusExporter,
    osd_id: String,
    node_name: String,
    failure_domain: FailureDomainConfig,
    start_time: std::time::Instant,
    smart_monitor: Arc<SmartMonitor>,
    disk_devices: Vec<String>,
}

/// Metrics HTTP handler
async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<Arc<OsdMetricsState>>,
) -> impl IntoResponse {
    let mut output = String::with_capacity(16 * 1024);

    // OSD info
    writeln!(output, "# HELP objectio_osd_info OSD information").unwrap();
    writeln!(output, "# TYPE objectio_osd_info gauge").unwrap();
    writeln!(
        output,
        "objectio_osd_info{{osd_id=\"{}\",node=\"{}\",rack=\"{}\",datacenter=\"{}\",region=\"{}\"}} 1",
        state.osd_id, state.node_name, state.failure_domain.rack,
        state.failure_domain.datacenter, state.failure_domain.region
    ).unwrap();

    // OSD uptime
    let uptime = state.start_time.elapsed().as_secs();
    writeln!(output, "# HELP objectio_osd_uptime_seconds OSD uptime in seconds").unwrap();
    writeln!(output, "# TYPE objectio_osd_uptime_seconds counter").unwrap();
    writeln!(output, "objectio_osd_uptime_seconds{{osd_id=\"{}\"}} {}", state.osd_id, uptime).unwrap();

    // Get disk stats from OSD service
    let status = state.osd_service.status();

    // Total capacity and used
    writeln!(output, "# HELP objectio_osd_capacity_bytes Total OSD capacity").unwrap();
    writeln!(output, "# TYPE objectio_osd_capacity_bytes gauge").unwrap();
    writeln!(output, "objectio_osd_capacity_bytes{{osd_id=\"{}\"}} {}", state.osd_id, status.total_capacity).unwrap();

    writeln!(output, "# HELP objectio_osd_used_bytes Used space on OSD").unwrap();
    writeln!(output, "# TYPE objectio_osd_used_bytes gauge").unwrap();
    writeln!(output, "objectio_osd_used_bytes{{osd_id=\"{}\"}} {}", state.osd_id, status.total_used).unwrap();

    // Shard count
    writeln!(output, "# HELP objectio_osd_shards_total Total shards stored").unwrap();
    writeln!(output, "# TYPE objectio_osd_shards_total gauge").unwrap();
    writeln!(output, "objectio_osd_shards_total{{osd_id=\"{}\"}} {}", state.osd_id, status.total_shards).unwrap();

    // Per-disk metrics
    writeln!(output, "# HELP objectio_disk_capacity_bytes Disk capacity").unwrap();
    writeln!(output, "# TYPE objectio_disk_capacity_bytes gauge").unwrap();
    writeln!(output, "# HELP objectio_disk_used_bytes Disk used space").unwrap();
    writeln!(output, "# TYPE objectio_disk_used_bytes gauge").unwrap();
    writeln!(output, "# HELP objectio_disk_shards_total Shards on disk").unwrap();
    writeln!(output, "# TYPE objectio_disk_shards_total gauge").unwrap();
    writeln!(output, "# HELP objectio_disk_healthy Disk health status (1=healthy, 0=unhealthy)").unwrap();
    writeln!(output, "# TYPE objectio_disk_healthy gauge").unwrap();

    for disk in &status.disks {
        let healthy = if disk.status == "healthy" { 1 } else { 0 };
        writeln!(
            output,
            "objectio_disk_capacity_bytes{{osd_id=\"{}\",disk=\"{}\"}} {}",
            state.osd_id, disk.path, disk.capacity
        ).unwrap();
        writeln!(
            output,
            "objectio_disk_used_bytes{{osd_id=\"{}\",disk=\"{}\"}} {}",
            state.osd_id, disk.path, disk.used
        ).unwrap();
        writeln!(
            output,
            "objectio_disk_shards_total{{osd_id=\"{}\",disk=\"{}\"}} {}",
            state.osd_id, disk.path, disk.shard_count
        ).unwrap();
        writeln!(
            output,
            "objectio_disk_healthy{{osd_id=\"{}\",disk=\"{}\"}} {}",
            state.osd_id, disk.path, healthy
        ).unwrap();
    }

    // Export block metrics if available
    output.push_str(&state.exporter.export(&state.collector));

    // Export gRPC metrics
    output.push_str(&state.osd_service.grpc_metrics().export_prometheus(&state.osd_id));

    // Check SMART metrics (will only poll if interval has elapsed)
    state.smart_monitor.check_if_needed(&state.disk_devices);
    output.push_str(&state.smart_monitor.export_prometheus());

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        output,
    )
}

/// Health check handler
async fn health_handler(
    axum::extract::State(state): axum::extract::State<Arc<OsdMetricsState>>,
) -> impl IntoResponse {
    let status = state.osd_service.status();
    let healthy = status.disks.iter().all(|d| d.status == "healthy");

    if healthy {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "UNHEALTHY")
    }
}

/// Start the metrics HTTP server
async fn start_metrics_server(
    port: u16,
    state: Arc<OsdMetricsState>,
) -> Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    info!("Starting metrics server on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Send periodic heartbeats to the metadata service
async fn heartbeat_loop(meta_endpoint: &str, node_id: &[u8; 16], _disk_ids: &[Vec<u8>]) {
    let heartbeat_interval = Duration::from_secs(10);

    loop {
        tokio::time::sleep(heartbeat_interval).await;

        // In a full implementation, we would:
        // 1. Connect to metadata service
        // 2. Send HeartbeatRequest with node status and disk health
        // 3. Process HeartbeatResponse for cluster map updates
        //
        // For now, just log that we would send a heartbeat
        tracing::trace!(
            "Would send heartbeat for node {} to {}",
            hex::encode(&node_id[..4]),
            meta_endpoint
        );
    }
}
