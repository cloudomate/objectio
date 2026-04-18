#![allow(clippy::result_large_err)]
//! ObjectIO Block Gateway
//!
//! Accepts block I/O over gRPC (BlockService) and NBD, buffers writes in an
//! in-memory WriteCache, and flushes 4 MB chunks as EC objects to the OSDs.

mod ec_io;
mod flush;
mod nbd;
mod osd_pool;
mod service;
mod store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use objectio_block::chunk::ChunkMapper;
use objectio_block::{CacheConfig, VolumeManager, WriteCache};
use objectio_proto::block::block_service_server::BlockServiceServer;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::osd_pool::OsdPool;
use crate::service::BlockGatewayState;
use crate::store::BlockStore;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(
    name = "objectio-block-gateway",
    about = "ObjectIO Block Storage Gateway"
)]
struct Args {
    /// gRPC listen address (BlockService)
    #[arg(long, default_value = "0.0.0.0:9300")]
    listen: String,

    /// NBD TCP listen address
    #[arg(long, default_value = "0.0.0.0:10809")]
    nbd_listen: String,

    /// Host advertised in NBD attachment URLs (defaults to listen host)
    #[arg(long, default_value = "")]
    advertise_host: String,

    /// Meta service endpoint
    #[arg(long, default_value = "http://localhost:9100")]
    meta_endpoint: String,

    /// Data directory (Redb store + journal)
    #[arg(long, default_value = "./block-gw-data")]
    data_dir: std::path::PathBuf,

    /// Write-cache size in bytes
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    cache_bytes: usize,

    /// Background flush interval in seconds
    #[arg(long, default_value_t = 5)]
    flush_interval_s: u64,

    /// EC data shards (k)
    #[arg(long, default_value_t = 4)]
    ec_k: u32,

    /// EC parity shards (m)
    #[arg(long, default_value_t = 2)]
    ec_m: u32,

    /// Log level (trace / debug / info / warn / error)
    #[arg(long, default_value = "info")]
    log_level: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(&args.log_level).unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("Starting ObjectIO Block Gateway");

    // ── Data directory ────────────────────────────────────────────────────────
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("create data_dir {:?}", args.data_dir))?;

    // ── Persistent store ──────────────────────────────────────────────────────
    let store =
        Arc::new(BlockStore::open(args.data_dir.join("block.db")).context("open block store")?);

    // ── Write cache ───────────────────────────────────────────────────────────
    let journal_path = args.data_dir.join("block.journal");
    let cache_config = CacheConfig {
        max_cache_bytes: args.cache_bytes as u64,
        journal_path: Some(journal_path.to_string_lossy().to_string()),
        ..CacheConfig::default()
    };
    let chunk_mapper = Arc::new(ChunkMapper::default());
    let cache = Arc::new(WriteCache::new(chunk_mapper, cache_config));

    // ── Volume manager ────────────────────────────────────────────────────────
    let volume_manager = Arc::new(VolumeManager::new());
    store
        .restore_volumes(&volume_manager)
        .context("restore volumes")?;

    // Ensure caches exist for all restored volumes
    for vol in volume_manager.list_volumes() {
        cache.init_volume(&vol.volume_id);
    }

    // ── Meta gRPC client ──────────────────────────────────────────────────────
    let meta_channel = tonic::transport::Endpoint::new(args.meta_endpoint.clone())
        .context("parse meta endpoint")?
        .connect()
        .await
        .context("connect to meta service")?;

    let meta_client = Arc::new(Mutex::new(
        objectio_proto::metadata::metadata_service_client::MetadataServiceClient::new(meta_channel),
    ));

    // ── OSD pool ──────────────────────────────────────────────────────────────
    let osd_pool = Arc::new(OsdPool::new());

    // ── NBD advertise host / port ─────────────────────────────────────────────
    let advertise_host = if args.advertise_host.is_empty() {
        args.listen
            .split(':')
            .next()
            .unwrap_or("localhost")
            .to_string()
    } else {
        args.advertise_host.clone()
    };
    let nbd_port: u16 = args
        .nbd_listen
        .split(':')
        .next_back()
        .unwrap_or("10809")
        .parse()
        .unwrap_or(10809);

    // ── NBD server ────────────────────────────────────────────────────────────
    let nbd_server = Arc::new(nbd::NbdServer::new(
        Arc::clone(&cache),
        Arc::clone(&store),
        Arc::clone(&osd_pool),
        Arc::clone(&meta_client),
        args.ec_k,
        args.ec_m,
    ));

    // ── Gateway state ─────────────────────────────────────────────────────────
    let state = Arc::new(BlockGatewayState {
        meta_client,
        osd_pool,
        cache,
        store,
        volume_manager,
        nbd_server: Arc::clone(&nbd_server),
        advertise_host,
        nbd_port,
        ec_k: args.ec_k,
        ec_m: args.ec_m,
    });

    // ── Background flush loop ─────────────────────────────────────────────────
    {
        let flush_state = Arc::clone(&state);
        let interval = Duration::from_secs(args.flush_interval_s);
        tokio::spawn(flush::flush_loop(flush_state, interval));
    }

    // ── NBD TCP listener ──────────────────────────────────────────────────────
    let nbd_addr: SocketAddr = args
        .nbd_listen
        .parse()
        .context("parse NBD listen address")?;
    tokio::spawn(nbd::NbdServer::serve(Arc::clone(&nbd_server), nbd_addr));

    // ── gRPC server ───────────────────────────────────────────────────────────
    let grpc_addr: SocketAddr = args.listen.parse().context("parse gRPC listen address")?;
    info!("Block gateway gRPC on {grpc_addr}");
    info!("NBD server on {nbd_addr}");

    let svc = service::BlockGatewayService::new(Arc::clone(&state));
    Server::builder()
        .add_service(BlockServiceServer::new(svc))
        .serve(grpc_addr)
        .await
        .context("gRPC server error")?;

    Ok(())
}
