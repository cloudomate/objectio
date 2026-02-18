//! Iceberg REST Catalog for `ObjectIO`.
//!
//! Provides an Axum sub-router implementing the Apache Iceberg REST Catalog API.
//! The router is nested at `/iceberg` in the gateway and delegates catalog state
//! to the Meta gRPC service.

pub mod access;
pub mod catalog;
pub mod error;
pub mod filters;
pub mod handlers;
pub mod metadata;
pub mod roles;
pub mod types;

use axum::Router;
use axum::routing::{delete, get, head, post, put};
use catalog::IcebergCatalog;
use handlers::IcebergState;
use objectio_auth::policy::PolicyEvaluator;
use objectio_proto::metadata::metadata_service_client::MetadataServiceClient;
use std::sync::Arc;
use tonic::transport::Channel;

/// Build the Iceberg REST Catalog router.
///
/// The returned router should be nested at `/iceberg` by the gateway.
/// Routes follow the Iceberg REST Catalog `OpenAPI` spec (v1 prefix).
#[allow(clippy::too_many_lines)]
pub fn router(
    meta_client: MetadataServiceClient<Channel>,
    warehouse_location: String,
    policy_evaluator: PolicyEvaluator,
) -> Router {
    let catalog = IcebergCatalog::new(meta_client);
    let state = Arc::new(IcebergState {
        catalog,
        warehouse_location,
        policy_evaluator,
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
        // Namespace policy management
        .route(
            "/v1/namespaces/{namespace}/policy",
            put(handlers::set_namespace_policy),
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
        // Table policy management
        .route(
            "/v1/namespaces/{namespace}/tables/{table}/policy",
            put(handlers::set_table_policy),
        )
        // Tag management
        .route(
            "/v1/namespaces/{namespace}/tags",
            put(handlers::set_namespace_tags),
        )
        .route(
            "/v1/namespaces/{namespace}/tags",
            get(handlers::get_namespace_tags),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}/tags",
            put(handlers::set_table_tags),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}/tags",
            get(handlers::get_table_tags),
        )
        // Quota management
        .route(
            "/v1/namespaces/{namespace}/quota",
            put(handlers::set_namespace_quota),
        )
        .route(
            "/v1/namespaces/{namespace}/quota",
            get(handlers::get_namespace_quota),
        )
        // Encryption policy management
        .route(
            "/v1/namespaces/{namespace}/encryption-policy",
            put(handlers::set_encryption_policy),
        )
        .route(
            "/v1/namespaces/{namespace}/encryption-policy",
            get(handlers::get_encryption_policy),
        )
        // Data filter management (admin only)
        .route(
            "/v1/namespaces/{namespace}/tables/{table}/data-filters",
            put(handlers::create_data_filter),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}/data-filters",
            get(handlers::list_data_filters),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}/data-filters/{filter_id}",
            delete(handlers::delete_data_filter),
        )
        // Role binding
        .route(
            "/v1/namespaces/{namespace}/role-binding",
            put(handlers::set_role_binding),
        )
        // Catalog-level policy management
        .route("/v1/catalog/policy", put(handlers::set_catalog_policy))
        // Effective policy (admin)
        .route(
            "/v1/namespaces/{namespace}/effective-policy",
            get(handlers::get_effective_policy),
        )
        // Policy simulation (admin)
        .route("/v1/simulate-policy", post(handlers::simulate_policy))
        // Rename table
        .route("/v1/tables/rename", post(handlers::rename_table))
        .with_state(state)
}
