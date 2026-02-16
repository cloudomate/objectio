//! Iceberg REST Catalog for `ObjectIO`.
//!
//! Provides an Axum sub-router implementing the Apache Iceberg REST Catalog API.
//! The router is nested at `/iceberg` in the gateway and delegates catalog state
//! to the Meta gRPC service.

pub mod catalog;
pub mod error;
pub mod handlers;
pub mod types;

use axum::Router;
use axum::routing::{delete, get, head, post};
use catalog::IcebergCatalog;
use handlers::IcebergState;
use objectio_proto::metadata::metadata_service_client::MetadataServiceClient;
use std::sync::Arc;
use tonic::transport::Channel;

/// Build the Iceberg REST Catalog router.
///
/// The returned router should be nested at `/iceberg` by the gateway.
/// Routes follow the Iceberg REST Catalog `OpenAPI` spec (v1 prefix).
pub fn router(meta_client: MetadataServiceClient<Channel>, warehouse_location: String) -> Router {
    let catalog = IcebergCatalog::new(meta_client);
    let state = Arc::new(IcebergState {
        catalog,
        warehouse_location,
    });

    Router::new()
        // Config
        .route("/v1/config", get(handlers::get_config))
        // Namespace operations
        .route("/v1/namespaces", get(handlers::list_namespaces))
        .route("/v1/namespaces", post(handlers::create_namespace))
        .route("/v1/namespaces/{namespace}", get(handlers::load_namespace))
        .route(
            "/v1/namespaces/{namespace}",
            head(handlers::namespace_exists),
        )
        .route(
            "/v1/namespaces/{namespace}",
            delete(handlers::drop_namespace),
        )
        .route(
            "/v1/namespaces/{namespace}/properties",
            post(handlers::update_namespace_properties),
        )
        // Table operations
        .route(
            "/v1/namespaces/{namespace}/tables",
            get(handlers::list_tables),
        )
        .route(
            "/v1/namespaces/{namespace}/tables",
            post(handlers::create_table),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            get(handlers::load_table),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            post(handlers::update_table),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            head(handlers::table_exists),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            delete(handlers::drop_table),
        )
        // Rename table
        .route("/v1/tables/rename", post(handlers::rename_table))
        .with_state(state)
}
