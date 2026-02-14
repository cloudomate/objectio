//! ObjectIO Metadata Service
//!
//! This binary provides the Raft-based metadata service for both
//! object storage and block storage.

mod block_service;
mod service;

use anyhow::Result;
use axum::{
    Router,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use block_service::BlockMetaService;
use clap::Parser;
use objectio_meta_store::{MetaStore, OsdNode};
use objectio_proto::block::block_service_server::BlockServiceServer;
use objectio_proto::metadata::metadata_service_server::MetadataServiceServer;
use service::MetaService;
use std::fmt::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "objectio-meta")]
#[command(about = "ObjectIO Metadata Service")]
#[command(version)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "/etc/objectio/meta.toml")]
    config: String,

    /// Node ID (for Raft)
    #[arg(long)]
    node_id: Option<u64>,

    /// Listen address for gRPC
    #[arg(short, long, default_value = "0.0.0.0:9001")]
    listen: String,

    /// Peer addresses for Raft cluster
    #[arg(long)]
    peers: Vec<String>,

    /// OSD addresses to register (host:port)
    #[arg(long)]
    osd: Vec<String>,

    /// Erasure coding data shards (k)
    #[arg(long, default_value = "4")]
    ec_k: u8,

    /// Erasure coding parity shards (m)
    #[arg(long, default_value = "2")]
    ec_m: u8,

    /// Replication count (use instead of EC for simple replication)
    /// Set to 1 for single-disk mode (no redundancy)
    /// Set to 3 for 3-way replication
    /// When set, overrides ec_k and ec_m
    #[arg(long)]
    replication: Option<u8>,

    /// Data directory for persistent metadata (redb)
    #[arg(long, default_value = "/var/lib/objectio/meta")]
    data_dir: PathBuf,

    /// Admin user name (creates default admin on startup if no users exist)
    #[arg(long, default_value = "admin")]
    admin_user: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Metrics server port (Prometheus)
    #[arg(long, default_value = "9101")]
    metrics_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting ObjectIO Metadata Service");

    // Initialize metadata service with EC config
    // Replication mode takes precedence over EC settings
    let ec_config = if let Some(replication_count) = args.replication {
        info!("Storage mode: Replication (count={})", replication_count);
        objectio_meta_store::EcConfig::Replication {
            count: replication_count,
        }
    } else {
        info!(
            "Storage mode: Erasure coding (k={}, m={})",
            args.ec_k, args.ec_m
        );
        objectio_meta_store::EcConfig::Mds {
            k: args.ec_k,
            m: args.ec_m,
        }
    };

    // Open persistent store
    let store_path = args.data_dir.join("meta.redb");
    info!("Opening metadata store at {}", store_path.display());
    let store = Arc::new(MetaStore::open(&store_path).unwrap_or_else(|e| {
        panic!(
            "Failed to open metadata store at {}: {}",
            store_path.display(),
            e
        )
    }));
    info!("Metadata store opened successfully");

    let meta_service = MetaService::with_store(ec_config, store.clone());

    // Ensure admin user exists (creates on first startup, returns existing on restarts)
    if let Some((access_key_id, secret_access_key)) = meta_service.ensure_admin(&args.admin_user) {
        info!("============================================");
        info!("Admin credentials (save these!):");
        info!("  Access Key ID:     {}", access_key_id);
        info!("  Secret Access Key: {}", secret_access_key);
        info!("============================================");
    } else {
        info!("Admin user '{}' already exists", args.admin_user);
    }

    // Register OSD nodes from CLI args (skip if store already has nodes)
    if !args.osd.is_empty() && !meta_service.has_persisted_osds() {
        for osd_addr in &args.osd {
            // In a real implementation, we would connect to the OSD and get its info
            // For now, create a placeholder with generated IDs
            let node = OsdNode {
                node_id: *uuid::Uuid::new_v4().as_bytes(),
                address: osd_addr.clone(),
                disk_ids: vec![*uuid::Uuid::new_v4().as_bytes()],
                failure_domain: None,
            };
            meta_service.register_osd(node);
        }
    } else if meta_service.has_persisted_osds() && !args.osd.is_empty() {
        info!(
            "Skipping --osd registration: {} OSD nodes already loaded from store",
            meta_service.stats().osd_count
        );
    }

    // Parse listen address
    let addr = args
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid listen address {}: {}", args.listen, e))?;

    // Initialize block metadata service with persistent store
    let block_service = BlockMetaService::with_store(store);
    info!("Block storage service initialized");

    // Wrap services in Arc for sharing
    let meta_service = Arc::new(meta_service);
    let block_service = Arc::new(block_service);

    // Create metrics state
    let metrics_state = Arc::new(MetaMetricsState {
        meta_service: meta_service.clone(),
        block_service: block_service.clone(),
        start_time: std::time::Instant::now(),
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
    info!(
        "Metrics available at http://0.0.0.0:{}/metrics",
        metrics_port
    );

    // Start gRPC server with both metadata and block services
    Server::builder()
        .add_service(MetadataServiceServer::from_arc(meta_service))
        .add_service(BlockServiceServer::from_arc(block_service))
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
        })
        .await?;

    info!("Metadata Service shut down gracefully");

    Ok(())
}

/// Metrics state for the Meta service
struct MetaMetricsState {
    meta_service: Arc<MetaService>,
    block_service: Arc<BlockMetaService>,
    start_time: std::time::Instant,
}

/// Metrics HTTP handler
async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<Arc<MetaMetricsState>>,
) -> impl IntoResponse {
    let mut output = String::with_capacity(8 * 1024);

    // Meta service uptime
    let uptime = state.start_time.elapsed().as_secs();
    writeln!(
        output,
        "# HELP objectio_meta_uptime_seconds Metadata service uptime"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_meta_uptime_seconds counter").unwrap();
    writeln!(output, "objectio_meta_uptime_seconds {}", uptime).unwrap();

    // Get stats from meta service
    let stats = state.meta_service.stats();

    // Bucket and object counts
    writeln!(
        output,
        "# HELP objectio_meta_buckets_total Total number of buckets"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_meta_buckets_total gauge").unwrap();
    writeln!(output, "objectio_meta_buckets_total {}", stats.bucket_count).unwrap();

    writeln!(
        output,
        "# HELP objectio_meta_objects_total Total number of objects"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_meta_objects_total gauge").unwrap();
    writeln!(output, "objectio_meta_objects_total {}", stats.object_count).unwrap();

    // OSD counts
    writeln!(
        output,
        "# HELP objectio_meta_osds_total Total registered OSDs"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_meta_osds_total gauge").unwrap();
    writeln!(output, "objectio_meta_osds_total {}", stats.osd_count).unwrap();

    // User counts
    writeln!(output, "# HELP objectio_meta_users_total Total users").unwrap();
    writeln!(output, "# TYPE objectio_meta_users_total gauge").unwrap();
    writeln!(output, "objectio_meta_users_total {}", stats.user_count).unwrap();

    // Get block service stats
    let block_stats = state.block_service.stats();

    // --- Volume inventory metrics ---

    writeln!(
        output,
        "# HELP objectio_block_volumes_total Total block volumes"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volumes_total gauge").unwrap();
    writeln!(
        output,
        "objectio_block_volumes_total {}",
        block_stats.volume_count
    )
    .unwrap();

    writeln!(
        output,
        "# HELP objectio_block_volumes_by_state Volume count by state"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volumes_by_state gauge").unwrap();
    for state_name in &[
        "available",
        "attached",
        "creating",
        "error",
        "deleting",
        "unknown",
    ] {
        let count = block_stats
            .volumes_by_state
            .get(*state_name)
            .copied()
            .unwrap_or(0);
        writeln!(
            output,
            "objectio_block_volumes_by_state{{state=\"{}\"}} {}",
            state_name, count
        )
        .unwrap();
    }

    writeln!(
        output,
        "# HELP objectio_block_volumes_provisioned_bytes Total provisioned bytes across all volumes"
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE objectio_block_volumes_provisioned_bytes gauge"
    )
    .unwrap();
    writeln!(
        output,
        "objectio_block_volumes_provisioned_bytes {}",
        block_stats.volumes_provisioned_bytes
    )
    .unwrap();

    writeln!(
        output,
        "# HELP objectio_block_volumes_used_bytes Total used bytes across all volumes"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volumes_used_bytes gauge").unwrap();
    writeln!(
        output,
        "objectio_block_volumes_used_bytes {}",
        block_stats.volumes_used_bytes
    )
    .unwrap();

    writeln!(
        output,
        "# HELP objectio_block_volume_size_bytes Provisioned size per volume"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volume_size_bytes gauge").unwrap();
    writeln!(
        output,
        "# HELP objectio_block_volume_used_bytes Used bytes per volume"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volume_used_bytes gauge").unwrap();
    writeln!(
        output,
        "# HELP objectio_block_volume_qos_max_iops Configured max IOPS per volume"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volume_qos_max_iops gauge").unwrap();
    writeln!(
        output,
        "# HELP objectio_block_volume_qos_min_iops Configured min IOPS per volume"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volume_qos_min_iops gauge").unwrap();
    writeln!(
        output,
        "# HELP objectio_block_volume_qos_max_bandwidth_bps Configured max bandwidth per volume"
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE objectio_block_volume_qos_max_bandwidth_bps gauge"
    )
    .unwrap();
    writeln!(
        output,
        "# HELP objectio_block_volume_qos_burst_iops Configured burst IOPS per volume"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_volume_qos_burst_iops gauge").unwrap();

    for vol in &block_stats.volumes {
        writeln!(
            output,
            "objectio_block_volume_size_bytes{{volume_id=\"{}\",name=\"{}\",pool=\"{}\"}} {}",
            vol.volume_id, vol.name, vol.pool, vol.size_bytes
        )
        .unwrap();
        writeln!(
            output,
            "objectio_block_volume_used_bytes{{volume_id=\"{}\",name=\"{}\",pool=\"{}\"}} {}",
            vol.volume_id, vol.name, vol.pool, vol.used_bytes
        )
        .unwrap();

        if let Some(qos) = &vol.qos {
            writeln!(
                output,
                "objectio_block_volume_qos_max_iops{{volume_id=\"{}\",name=\"{}\"}} {}",
                vol.volume_id, vol.name, qos.max_iops
            )
            .unwrap();
            writeln!(
                output,
                "objectio_block_volume_qos_min_iops{{volume_id=\"{}\",name=\"{}\"}} {}",
                vol.volume_id, vol.name, qos.min_iops
            )
            .unwrap();
            writeln!(
                output,
                "objectio_block_volume_qos_max_bandwidth_bps{{volume_id=\"{}\",name=\"{}\"}} {}",
                vol.volume_id, vol.name, qos.max_bandwidth_bps
            )
            .unwrap();
            writeln!(
                output,
                "objectio_block_volume_qos_burst_iops{{volume_id=\"{}\",name=\"{}\"}} {}",
                vol.volume_id, vol.name, qos.burst_iops
            )
            .unwrap();
        }
    }

    // --- Snapshot metrics ---

    writeln!(
        output,
        "# HELP objectio_block_snapshots_total Total snapshots"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_snapshots_total gauge").unwrap();
    writeln!(
        output,
        "objectio_block_snapshots_total {}",
        block_stats.snapshot_count
    )
    .unwrap();

    writeln!(
        output,
        "# HELP objectio_block_snapshots_space_bytes Total snapshot space"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_snapshots_space_bytes gauge").unwrap();
    writeln!(
        output,
        "objectio_block_snapshots_space_bytes {}",
        block_stats.snapshots_space_bytes
    )
    .unwrap();

    writeln!(
        output,
        "# HELP objectio_block_snapshot_size_bytes Snapshot logical size"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_snapshot_size_bytes gauge").unwrap();
    for snap in &block_stats.snapshots {
        writeln!(output,
            "objectio_block_snapshot_size_bytes{{snapshot_id=\"{}\",volume_id=\"{}\",name=\"{}\"}} {}",
            snap.snapshot_id, snap.volume_id, snap.name, snap.size_bytes
        ).unwrap();
    }

    // --- Attachment metrics ---

    writeln!(
        output,
        "# HELP objectio_block_attachments_total Total active attachments"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_attachments_total gauge").unwrap();
    writeln!(
        output,
        "objectio_block_attachments_total {}",
        block_stats.attachment_count
    )
    .unwrap();

    writeln!(
        output,
        "# HELP objectio_block_attachments_by_type Attachment count by target type"
    )
    .unwrap();
    writeln!(output, "# TYPE objectio_block_attachments_by_type gauge").unwrap();
    for type_name in &["iscsi", "nvmeof", "nbd", "unknown"] {
        let count = block_stats
            .attachments_by_type
            .get(*type_name)
            .copied()
            .unwrap_or(0);
        writeln!(
            output,
            "objectio_block_attachments_by_type{{type=\"{}\"}} {}",
            type_name, count
        )
        .unwrap();
    }

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

/// Health check handler
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Start the metrics HTTP server
async fn start_metrics_server(port: u16, state: Arc<MetaMetricsState>) -> Result<()> {
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
