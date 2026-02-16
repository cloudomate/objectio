//! Axum route handlers for the 13 Iceberg REST API endpoints.

use crate::catalog::IcebergCatalog;
use crate::error::IcebergError;
use crate::types::{
    CatalogConfig, CommitTableRequest, CommitTableResponse, CreateNamespaceRequest,
    CreateNamespaceResponse, CreateTableRequest, ListNamespacesParams, ListNamespacesResponse,
    ListTablesResponse, LoadNamespaceResponse, LoadTableResponse, PurgeParams, RenameTableRequest,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

type Result<T> = std::result::Result<T, IcebergError>;

/// Shared state for Iceberg handlers — wraps the catalog.
pub struct IcebergState {
    pub catalog: IcebergCatalog,
    pub warehouse_location: String,
}

// ---- Config ----

/// `GET /v1/config` — return catalog configuration.
pub async fn get_config(State(state): State<Arc<IcebergState>>) -> impl IntoResponse {
    let config = CatalogConfig {
        defaults: HashMap::from([("warehouse".to_string(), state.warehouse_location.clone())]),
        overrides: HashMap::new(),
    };
    Json(config)
}

// ---- Namespace handlers ----

/// Parse a namespace path segment like "ns1%1Fns2" into levels.
/// The Iceberg REST spec uses U+001F (unit separator) to encode multi-level namespaces in URLs.
fn parse_namespace(encoded: &str) -> Vec<String> {
    encoded.split('\x1F').map(String::from).collect()
}

/// `GET /v1/namespaces` — list namespaces, optionally under a parent.
///
/// # Errors
/// Returns `IcebergError` if the parent namespace is not found.
pub async fn list_namespaces(
    State(state): State<Arc<IcebergState>>,
    Query(params): Query<ListNamespacesParams>,
) -> Result<Json<ListNamespacesResponse>> {
    let parent = params.parent.map(|p| parse_namespace(&p));
    let namespaces = state.catalog.list_namespaces(parent).await?;
    Ok(Json(ListNamespacesResponse { namespaces }))
}

/// `POST /v1/namespaces` — create a new namespace.
///
/// # Errors
/// Returns `IcebergError` if the namespace already exists or parent is missing.
pub async fn create_namespace(
    State(state): State<Arc<IcebergState>>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<CreateNamespaceResponse>)> {
    let resp = state
        .catalog
        .create_namespace(req.namespace, req.properties)
        .await?;
    Ok((StatusCode::OK, Json(resp)))
}

/// `GET /v1/namespaces/{namespace}` — load namespace properties.
///
/// # Errors
/// Returns `IcebergError` if the namespace is not found.
pub async fn load_namespace(
    State(state): State<Arc<IcebergState>>,
    Path(namespace): Path<String>,
) -> Result<Json<LoadNamespaceResponse>> {
    let levels = parse_namespace(&namespace);
    let resp = state.catalog.load_namespace(levels).await?;
    Ok(Json(resp))
}

/// `HEAD /v1/namespaces/{namespace}` — check if namespace exists.
///
/// # Errors
/// Returns `IcebergError` on gRPC communication failure.
pub async fn namespace_exists(
    State(state): State<Arc<IcebergState>>,
    Path(namespace): Path<String>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    let exists = state.catalog.namespace_exists(levels).await?;
    if exists {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

/// `DELETE /v1/namespaces/{namespace}` — drop a namespace.
///
/// # Errors
/// Returns `IcebergError` if the namespace is not found or not empty.
pub async fn drop_namespace(
    State(state): State<Arc<IcebergState>>,
    Path(namespace): Path<String>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    state.catalog.drop_namespace(levels).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/namespaces/{namespace}/properties` — update namespace properties.
///
/// # Errors
/// Returns `IcebergError` if the namespace is not found.
pub async fn update_namespace_properties(
    State(state): State<Arc<IcebergState>>,
    Path(namespace): Path<String>,
    Json(req): Json<UpdateNamespacePropertiesRequest>,
) -> Result<Json<UpdateNamespacePropertiesResponse>> {
    let levels = parse_namespace(&namespace);
    let resp = state
        .catalog
        .update_namespace_properties(levels, req.removals, req.updates)
        .await?;
    Ok(Json(resp))
}

// ---- Table handlers ----

/// `GET /v1/namespaces/{namespace}/tables` — list tables in namespace.
///
/// # Errors
/// Returns `IcebergError` if the namespace is not found.
pub async fn list_tables(
    State(state): State<Arc<IcebergState>>,
    Path(namespace): Path<String>,
) -> Result<Json<ListTablesResponse>> {
    let levels = parse_namespace(&namespace);
    let identifiers = state.catalog.list_tables(levels).await?;
    Ok(Json(ListTablesResponse { identifiers }))
}

/// `POST /v1/namespaces/{namespace}/tables` — create a new table.
///
/// # Errors
/// Returns `IcebergError` if the namespace is not found or table already exists.
#[allow(clippy::cast_possible_truncation)]
pub async fn create_table(
    State(state): State<Arc<IcebergState>>,
    Path(namespace): Path<String>,
    Json(req): Json<CreateTableRequest>,
) -> Result<(StatusCode, Json<LoadTableResponse>)> {
    let levels = parse_namespace(&namespace);

    // Build initial table metadata (Iceberg v2 format)
    let table_uuid = uuid::Uuid::new_v4().to_string();
    let location = req.location.unwrap_or_else(|| {
        let ns_path = levels.join("/");
        format!("{}/{}/{}", state.warehouse_location, ns_path, req.name)
    });

    let schema = if req.schema.is_null() {
        json!({
            "type": "struct",
            "schema-id": 0,
            "fields": []
        })
    } else if req.schema.get("schema-id").is_none() {
        let mut s = req.schema.clone();
        if let Some(obj) = s.as_object_mut() {
            obj.insert("schema-id".to_string(), json!(0));
        }
        s
    } else {
        req.schema
    };

    let partition_spec = if req.partition_spec.is_null() {
        json!({"spec-id": 0, "fields": []})
    } else {
        req.partition_spec
    };

    let sort_order = if req.write_order.is_null() {
        json!({"order-id": 0, "fields": []})
    } else {
        req.write_order
    };

    let metadata = json!({
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": chrono_now_ms(),
        "last-column-id": last_column_id(&schema),
        "current-schema-id": 0,
        "schemas": [schema],
        "default-spec-id": 0,
        "partition-specs": [partition_spec],
        "last-partition-id": 0,
        "default-sort-order-id": 0,
        "sort-orders": [sort_order],
        "properties": req.properties,
        "current-snapshot-id": -1,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    });

    let metadata_location = format!(
        "s3://{}/{}/metadata/00000-{}.metadata.json",
        "objectio-warehouse",
        location.trim_start_matches("s3://"),
        table_uuid
    );

    let _stored_location = state
        .catalog
        .create_table(levels, &req.name, &metadata_location)
        .await?;

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location,
            metadata,
            config: None,
        }),
    ))
}

/// `GET /v1/namespaces/{namespace}/tables/{table}` — load a table.
///
/// # Errors
/// Returns `IcebergError` if the table is not found.
pub async fn load_table(
    State(state): State<Arc<IcebergState>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<LoadTableResponse>> {
    let levels = parse_namespace(&namespace);
    let metadata_location = state.catalog.load_table(levels, &table).await?;

    let metadata = json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": "",
        "schemas": [],
        "current-schema-id": 0,
        "partition-specs": [],
        "default-spec-id": 0,
        "sort-orders": [],
        "default-sort-order-id": 0,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    });

    Ok(Json(LoadTableResponse {
        metadata_location,
        metadata,
        config: None,
    }))
}

/// `POST /v1/namespaces/{namespace}/tables/{table}` — commit table update (CAS).
///
/// # Errors
/// Returns `IcebergError` if the table is not found or the commit conflicts.
pub async fn update_table(
    State(state): State<Arc<IcebergState>>,
    Path((namespace, table)): Path<(String, String)>,
    Json(_req): Json<CommitTableRequest>,
) -> Result<Json<CommitTableResponse>> {
    let levels = parse_namespace(&namespace);

    let current_location = state.catalog.load_table(levels.clone(), &table).await?;

    let new_location = format!(
        "{}-{}",
        current_location.trim_end_matches(".metadata.json"),
        uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap_or("0")
    ) + ".metadata.json";

    let committed_location = state
        .catalog
        .commit_table(levels, &table, &current_location, &new_location)
        .await?;

    let metadata = json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": "",
        "schemas": [],
        "current-schema-id": 0,
        "partition-specs": [],
        "default-spec-id": 0,
        "sort-orders": [],
        "default-sort-order-id": 0,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    });

    Ok(Json(CommitTableResponse {
        metadata_location: committed_location,
        metadata,
    }))
}

/// `HEAD /v1/namespaces/{namespace}/tables/{table}` — check if table exists.
///
/// # Errors
/// Returns `IcebergError` on gRPC communication failure.
pub async fn table_exists(
    State(state): State<Arc<IcebergState>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    let exists = state.catalog.table_exists(levels, &table).await?;
    if exists {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

/// `DELETE /v1/namespaces/{namespace}/tables/{table}` — drop a table.
///
/// # Errors
/// Returns `IcebergError` if the table is not found.
pub async fn drop_table(
    State(state): State<Arc<IcebergState>>,
    Path((namespace, table)): Path<(String, String)>,
    Query(params): Query<PurgeParams>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    state
        .catalog
        .drop_table(levels, &table, params.purge_requested)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/tables/rename` — rename a table across namespaces.
///
/// # Errors
/// Returns `IcebergError` if the source or destination namespace/table has issues.
pub async fn rename_table(
    State(state): State<Arc<IcebergState>>,
    Json(req): Json<RenameTableRequest>,
) -> Result<StatusCode> {
    state
        .catalog
        .rename_table(
            req.source.namespace,
            &req.source.name,
            req.destination.namespace,
            &req.destination.name,
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Helpers ----

#[allow(clippy::cast_possible_truncation)]
fn chrono_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn last_column_id(schema: &serde_json::Value) -> i64 {
    schema
        .get("fields")
        .and_then(|f| f.as_array())
        .and_then(|fields| fields.iter().filter_map(|f| f.get("id")?.as_i64()).max())
        .unwrap_or(0)
}
