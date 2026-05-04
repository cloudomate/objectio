//! Unity Catalog REST API for `ObjectIO`.
//!
//! Implements a subset of the Databricks Unity Catalog REST API at
//! `/api/2.1/unity-catalog/*` as an Axum sub-router. Three-level namespacing
//! (`catalog.schema.table`) maps directly onto the meta-service
//! `Unity{Catalog,Schema,Table}` proto messages introduced in PR2.
//!
//! Reference: <https://docs.databricks.com/api/workspace/catalogs>

pub mod access;
pub mod catalog;
pub mod error;
pub mod handlers;
pub mod types;

use axum::Router;
use axum::routing::{get, post};
use catalog::UnityCatalogClient;
use handlers::UnityState;
use objectio_auth::policy::PolicyEvaluator;
use objectio_proto::metadata::metadata_service_client::MetadataServiceClient;
use std::sync::Arc;
use tonic::transport::Channel;

/// Build the Unity Catalog REST router.
///
/// The returned router should be nested at the root of the gateway and will
/// register paths beginning with `/api/2.1/unity-catalog/`. Auth must be
/// applied externally; handlers expect `Option<Extension<AuthResult>>` so
/// `--no-auth` mode works the same as for Iceberg.
pub fn router(
    meta_client: MetadataServiceClient<Channel>,
    policy_evaluator: PolicyEvaluator,
    admin_principals: Vec<String>,
    sts_provider: Option<objectio_auth::sts::StsProvider>,
    s3_endpoint: String,
) -> Router {
    let catalog = UnityCatalogClient::new(meta_client);
    let state = Arc::new(UnityState {
        catalog,
        policy_evaluator,
        admin_principals,
        sts_provider,
        s3_endpoint,
    });

    Router::new()
        // Caller identity (engines use this to populate UDF built-ins:
        // current_user(), is_member(g), is_account_group_member(g))
        .route(
            "/api/2.1/unity-catalog/me",
            get(handlers::current_caller_identity),
        )
        // Catalog CRUD
        .route(
            "/api/2.1/unity-catalog/catalogs",
            get(handlers::list_catalogs).post(handlers::create_catalog),
        )
        .route(
            "/api/2.1/unity-catalog/catalogs/{name}",
            get(handlers::get_catalog)
                .patch(handlers::update_catalog)
                .delete(handlers::delete_catalog),
        )
        // Schema CRUD (full_name = "{catalog}.{schema}")
        .route(
            "/api/2.1/unity-catalog/schemas",
            get(handlers::list_schemas).post(handlers::create_schema),
        )
        .route(
            "/api/2.1/unity-catalog/schemas/{full_name}",
            get(handlers::get_schema)
                .patch(handlers::update_schema)
                .delete(handlers::delete_schema),
        )
        // Table CRUD (full_name = "{catalog}.{schema}.{table}")
        .route(
            "/api/2.1/unity-catalog/tables",
            get(handlers::list_tables).post(handlers::create_table),
        )
        .route(
            "/api/2.1/unity-catalog/tables/{full_name}",
            get(handlers::get_table).delete(handlers::delete_table),
        )
        // Function CRUD (full_name = "{catalog}.{schema}.{function}")
        .route(
            "/api/2.1/unity-catalog/functions",
            get(handlers::list_functions).post(handlers::create_function),
        )
        .route(
            "/api/2.1/unity-catalog/functions/{full_name}",
            get(handlers::get_function).delete(handlers::delete_function),
        )
        // Volume CRUD (full_name = "{catalog}.{schema}.{volume}")
        .route(
            "/api/2.1/unity-catalog/volumes",
            get(handlers::list_volumes).post(handlers::create_volume),
        )
        .route(
            "/api/2.1/unity-catalog/volumes/{full_name}",
            get(handlers::get_volume).delete(handlers::delete_volume),
        )
        // Model + ModelVersion CRUD (full_name = "{catalog}.{schema}.{model}")
        .route(
            "/api/2.1/unity-catalog/models",
            get(handlers::list_models).post(handlers::create_model),
        )
        .route(
            "/api/2.1/unity-catalog/models/{full_name}",
            get(handlers::get_model).delete(handlers::delete_model),
        )
        .route(
            "/api/2.1/unity-catalog/models/{full_name}/versions",
            get(handlers::list_model_versions).post(handlers::create_model_version),
        )
        .route(
            "/api/2.1/unity-catalog/models/{full_name}/versions/{version}",
            get(handlers::get_model_version)
                .patch(handlers::update_model_version_status)
                .delete(handlers::delete_model_version),
        )
        // Vended data-plane credentials (admin/policy gated)
        .route(
            "/api/2.1/unity-catalog/temporary-table-credentials",
            post(handlers::temporary_table_credentials),
        )
        .route(
            "/api/2.1/unity-catalog/temporary-volume-credentials",
            post(handlers::temporary_volume_credentials),
        )
        // Policy management (out-of-spec convenience endpoints, mirror Iceberg)
        .route(
            "/api/2.1/unity-catalog/catalogs/{name}/policy",
            get(handlers::get_catalog_policy).put(handlers::set_catalog_policy),
        )
        .route(
            "/api/2.1/unity-catalog/schemas/{full_name}/policy",
            get(handlers::get_schema_policy).put(handlers::set_schema_policy),
        )
        .route(
            "/api/2.1/unity-catalog/tables/{full_name}/policy",
            get(handlers::get_table_policy).put(handlers::set_table_policy),
        )
        // Row-filter / column-mask bindings. Replace-on-write — empty body
        // clears both. Engines read these out of the GET /tables response.
        .route(
            "/api/2.1/unity-catalog/tables/{full_name}/security",
            axum::routing::put(handlers::set_table_security),
        )
        .with_state(state)
}
