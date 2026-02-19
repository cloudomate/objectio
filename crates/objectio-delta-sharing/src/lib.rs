//! Delta Sharing protocol server for `ObjectIO`.
//!
//! Implements the Delta Sharing REST API v1 as an Axum sub-router.
//! The router is nested at `/delta-sharing` in the gateway and is served
//! **outside** the `SigV4` authentication middleware (Delta Sharing uses bearer tokens).
//!
//! Reference: <https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md>

pub mod access;
pub mod catalog;
pub mod error;
pub mod handlers;
pub mod types;

use axum::Router;
use axum::routing::{delete, get, post};
use catalog::DeltaCatalog;
use handlers::DeltaState;
use objectio_proto::metadata::metadata_service_client::MetadataServiceClient;
use std::sync::Arc;
use tonic::transport::Channel;

/// Configuration for the Delta Sharing router.
pub struct DeltaSharingConfig {
    /// Gateway public endpoint used to construct presigned URLs
    /// (e.g. `http://localhost:9000` or `https://objectio.example.com`).
    pub endpoint: String,
    /// AWS region string for `SigV4` presigned URL credential scope.
    pub region: String,
    /// Admin access key ID for presigning (the generated URLs are verified by
    /// the existing `SigV4` auth middleware using these credentials).
    pub access_key_id: String,
    /// Admin secret access key for presigning.
    pub secret_access_key: String,
}

/// Build the Delta Sharing REST API router.
///
/// The returned router should be nested at `/delta-sharing` by the gateway.
/// It must be mounted **outside** the `SigV4` middleware layer; bearer token
/// validation is handled internally by the `access::authenticate_request` helper.
///
/// Admin management endpoints are nested under `/_admin/delta-sharing/` in the
/// main gateway router (not this router).
pub fn router(meta_client: MetadataServiceClient<Channel>, config: DeltaSharingConfig) -> Router {
    let catalog = DeltaCatalog::new(meta_client.clone());
    let state = Arc::new(DeltaState {
        catalog,
        meta_client,
        endpoint: config.endpoint,
        region: config.region,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
    });

    Router::new()
        // Shares
        .route("/v1/shares", get(handlers::list_shares))
        // Schemas in a share
        .route("/v1/shares/{share}/schemas", get(handlers::list_schemas))
        // Tables in a share
        .route(
            "/v1/shares/{share}/schemas/{schema}/tables",
            get(handlers::list_tables),
        )
        .route(
            "/v1/shares/{share}/all-tables",
            get(handlers::list_all_tables),
        )
        // Table version
        .route(
            "/v1/shares/{share}/schemas/{schema}/tables/{table}/version",
            get(handlers::get_table_version),
        )
        // Table metadata (NDJSON: protocol + metadata lines)
        .route(
            "/v1/shares/{share}/schemas/{schema}/tables/{table}/metadata",
            get(handlers::get_table_metadata),
        )
        // Table query: returns presigned file URLs (NDJSON)
        .route(
            "/v1/shares/{share}/schemas/{schema}/tables/{table}/query",
            post(handlers::query_table),
        )
        .with_state(state)
}

/// Build the admin router for Delta Sharing management.
///
/// Should be nested at `/_admin/delta-sharing` in the gateway, behind
/// the admin authentication middleware.
pub fn admin_router(
    meta_client: MetadataServiceClient<Channel>,
    config: DeltaSharingConfig,
) -> Router {
    let catalog = DeltaCatalog::new(meta_client.clone());
    let state = Arc::new(DeltaState {
        catalog,
        meta_client,
        endpoint: config.endpoint,
        region: config.region,
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
    });

    Router::new()
        // Share management
        .route("/shares", post(handlers::admin_create_share))
        .route("/shares/{share}", delete(handlers::admin_drop_share))
        .route("/shares/{share}/tables", post(handlers::admin_add_table))
        .route(
            "/shares/{share}/schemas/{schema}/tables/{table}",
            delete(handlers::admin_remove_table),
        )
        // Recipient management
        .route("/recipients", post(handlers::admin_create_recipient))
        .route("/recipients/{name}", delete(handlers::admin_drop_recipient))
        .with_state(state)
}
