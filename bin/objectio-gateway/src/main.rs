//! ObjectIO Gateway - S3 API Gateway
//!
//! This binary provides the S3-compatible HTTP API.
//! Credentials are managed by the metadata service for persistence.

mod auth_middleware;
mod osd_pool;
mod s3;
mod scatter_gather;

use anyhow::Result;
use auth_middleware::{auth_layer, AuthState};
use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, head, post, put},
    Router,
};
use clap::Parser;
use objectio_auth::policy::PolicyEvaluator;
use objectio_proto::metadata::metadata_service_client::MetadataServiceClient;
use osd_pool::OsdPool;
use s3::AppState;
use scatter_gather::ScatterGatherEngine;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "objectio-gateway")]
#[command(about = "ObjectIO S3 API Gateway")]
#[command(version)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "/etc/objectio/gateway.toml")]
    config: String,

    /// Listen address for S3 API
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    listen: String,

    /// Metadata service endpoint
    #[arg(long, default_value = "http://localhost:9001")]
    meta_endpoint: String,

    /// OSD endpoint (initial OSD, more discovered via metadata service)
    #[arg(long, default_value = "http://localhost:9002")]
    osd_endpoint: String,

    /// Erasure coding data shards (k)
    #[arg(long, default_value = "4")]
    ec_k: u32,

    /// Erasure coding parity shards (m)
    #[arg(long, default_value = "2")]
    ec_m: u32,

    /// Disable authentication (for development)
    #[arg(long, default_value_t = false)]
    no_auth: bool,

    /// AWS region for SigV4 verification
    #[arg(long, default_value = "us-east-1")]
    region: String,

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

    info!("Starting ObjectIO Gateway");
    info!("Metadata endpoint: {}", args.meta_endpoint);
    info!("OSD endpoint: {}", args.osd_endpoint);

    // Connect to metadata service
    let meta_client = MetadataServiceClient::connect(args.meta_endpoint.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to metadata service: {}", e))?;

    info!("Connected to metadata service");
    info!("Credentials are managed by the metadata service");

    // Create OSD connection pool
    let osd_pool = Arc::new(OsdPool::new());

    // Connect to initial OSD (more will be discovered via placement)
    // Generate a temporary node ID for the initial OSD
    let initial_node_id = uuid::Uuid::new_v4();
    if let Err(e) = osd_pool
        .connect(
            osd_pool::NodeId::from(*initial_node_id.as_bytes()),
            &args.osd_endpoint,
        )
        .await
    {
        tracing::warn!("Failed to connect to initial OSD: {}. Will connect on demand.", e);
    } else {
        info!("Connected to initial OSD at {}", args.osd_endpoint);
    }

    // Create auth state using metadata service for credential lookup
    let auth_state = Arc::new(AuthState::new(meta_client.clone(), &args.region));

    // Create scatter-gather engine with a signing key derived from region
    // In production, this should come from a secure configuration
    let signing_key = format!("objectio-scatter-gather-{}", args.region);
    let scatter_gather = ScatterGatherEngine::new(osd_pool.clone(), signing_key.as_bytes());

    // Create application state
    let state = Arc::new(AppState {
        meta_client,
        osd_pool,
        ec_k: args.ec_k,
        ec_m: args.ec_m,
        policy_evaluator: PolicyEvaluator::new(),
        scatter_gather,
    });

    // Build router
    // Allow up to 100MB for single-part uploads (larger objects need multipart)
    let body_limit = DefaultBodyLimit::max(100 * 1024 * 1024);
    info!("Max single-part upload size: 100 MB");

    let app = if !args.no_auth {
        info!("Authentication is ENABLED (credentials from metadata service)");
        info!("Admin API is ENABLED (requires 'admin' user credentials)");
        Router::new()
            // Service endpoint (list buckets)
            .route("/", get(s3::list_buckets))
            // Bucket operations (including ?policy and ?uploads query params)
            .route("/{bucket}", put(s3::create_bucket))
            .route("/{bucket}", delete(s3::delete_bucket))
            .route("/{bucket}", head(s3::head_bucket))
            .route("/{bucket}", get(s3::list_objects))
            // POST /{bucket}?delete - batch delete objects
            .route("/{bucket}", post(s3::post_bucket))
            // Bucket with trailing slash (s3fs compatibility)
            .route("/{bucket}/", head(s3::head_bucket_trailing))
            .route("/{bucket}/", get(s3::list_objects_trailing))
            // Object operations (with multipart upload support via query params)
            // PUT /{bucket}/{key} - simple upload
            // PUT /{bucket}/{key}?uploadId=X&partNumber=N - upload part
            .route("/{bucket}/{*key}", put(s3::put_object_with_params))
            // GET /{bucket}/{key} - get object
            // GET /{bucket}/{key}?uploadId=X - list parts
            .route("/{bucket}/{*key}", get(s3::get_object_with_params))
            .route("/{bucket}/{*key}", head(s3::head_object))
            // DELETE /{bucket}/{key} - delete object
            // DELETE /{bucket}/{key}?uploadId=X - abort multipart upload
            .route("/{bucket}/{*key}", delete(s3::delete_object_with_params))
            // POST /{bucket}/{key}?uploads - initiate multipart upload
            // POST /{bucket}/{key}?uploadId=X - complete multipart upload
            .route("/{bucket}/{*key}", post(s3::post_object))
            // Health endpoint (no auth)
            .route("/health", get(s3::health_check))
            // Admin API (requires 'admin' user - checked in handlers)
            .route("/_admin/users", get(s3::admin_list_users))
            .route("/_admin/users", post(s3::admin_create_user))
            .route("/_admin/users/{user_id}", delete(s3::admin_delete_user))
            .route("/_admin/users/{user_id}/access-keys", get(s3::admin_list_access_keys))
            .route("/_admin/users/{user_id}/access-keys", post(s3::admin_create_access_key))
            .route("/_admin/access-keys/{access_key_id}", delete(s3::admin_delete_access_key))
            .layer(body_limit.clone())
            .layer(middleware::from_fn_with_state(auth_state, auth_layer))
            .layer(TraceLayer::new_for_http())
            .with_state(state)
    } else {
        info!("Authentication is DISABLED (development mode)");
        info!("Admin API is ENABLED (no auth required in dev mode)");
        Router::new()
            // Service endpoint (list buckets)
            .route("/", get(s3::list_buckets))
            // Bucket operations (including ?policy and ?uploads query params)
            .route("/{bucket}", put(s3::create_bucket))
            .route("/{bucket}", delete(s3::delete_bucket))
            .route("/{bucket}", head(s3::head_bucket))
            .route("/{bucket}", get(s3::list_objects))
            // POST /{bucket}?delete - batch delete objects
            .route("/{bucket}", post(s3::post_bucket))
            // Bucket with trailing slash (s3fs compatibility)
            .route("/{bucket}/", head(s3::head_bucket_trailing))
            .route("/{bucket}/", get(s3::list_objects_trailing))
            // Object operations (with multipart upload support via query params)
            // PUT /{bucket}/{key} - simple upload
            // PUT /{bucket}/{key}?uploadId=X&partNumber=N - upload part
            .route("/{bucket}/{*key}", put(s3::put_object_with_params))
            // GET /{bucket}/{key} - get object
            // GET /{bucket}/{key}?uploadId=X - list parts
            .route("/{bucket}/{*key}", get(s3::get_object_with_params))
            .route("/{bucket}/{*key}", head(s3::head_object))
            // DELETE /{bucket}/{key} - delete object
            // DELETE /{bucket}/{key}?uploadId=X - abort multipart upload
            .route("/{bucket}/{*key}", delete(s3::delete_object_with_params))
            // POST /{bucket}/{key}?uploads - initiate multipart upload
            // POST /{bucket}/{key}?uploadId=X - complete multipart upload
            .route("/{bucket}/{*key}", post(s3::post_object))
            // Health endpoint
            .route("/health", get(s3::health_check))
            // Admin API (no auth in dev mode)
            .route("/_admin/users", get(s3::admin_list_users))
            .route("/_admin/users", post(s3::admin_create_user))
            .route("/_admin/users/{user_id}", delete(s3::admin_delete_user))
            .route("/_admin/users/{user_id}/access-keys", get(s3::admin_list_access_keys))
            .route("/_admin/users/{user_id}/access-keys", post(s3::admin_create_access_key))
            .route("/_admin/access-keys/{access_key_id}", delete(s3::admin_delete_access_key))
            .layer(body_limit)
            .layer(TraceLayer::new_for_http())
            .with_state(state)
    };

    // Parse listen address
    let addr: SocketAddr = args.listen.parse().map_err(|e| {
        anyhow::anyhow!("Invalid listen address {}: {}", args.listen, e)
    })?;

    info!("Starting S3 API server on {}", addr);

    // Start server
    let listener = TcpListener::bind(addr).await?;

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
        })
        .await?;

    info!("Gateway shut down gracefully");

    Ok(())
}
