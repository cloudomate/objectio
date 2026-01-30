//! ObjectIO Metadata Service
//!
//! This binary provides the Raft-based metadata service for both
//! object storage and block storage.

mod block_service;
mod service;

use anyhow::Result;
use block_service::BlockMetaService;
use clap::Parser;
use objectio_proto::block::block_service_server::BlockServiceServer;
use objectio_proto::metadata::metadata_service_server::MetadataServiceServer;
use service::{MetaService, OsdNode};
use tonic::transport::Server;
use tracing::info;
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

    /// Admin user name (creates default admin on startup if no users exist)
    #[arg(long, default_value = "admin")]
    admin_user: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
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
        service::EcConfig::Replication { count: replication_count }
    } else {
        info!("Storage mode: Erasure coding (k={}, m={})", args.ec_k, args.ec_m);
        service::EcConfig::Mds { k: args.ec_k, m: args.ec_m }
    };
    let meta_service = MetaService::with_ec_config(ec_config);

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

    // Register OSD nodes
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

    // Parse listen address
    let addr = args.listen.parse().map_err(|e| {
        anyhow::anyhow!("Invalid listen address {}: {}", args.listen, e)
    })?;

    // Initialize block metadata service
    let block_service = BlockMetaService::new();
    info!("Block storage service initialized");

    info!("Starting gRPC server on {}", addr);

    // Start gRPC server with both metadata and block services
    Server::builder()
        .add_service(MetadataServiceServer::new(meta_service))
        .add_service(BlockServiceServer::new(block_service))
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
        })
        .await?;

    info!("Metadata Service shut down gracefully");

    Ok(())
}
