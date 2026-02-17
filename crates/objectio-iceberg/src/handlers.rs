//! Axum route handlers for the 13 Iceberg REST API endpoints.

use crate::access::{build_iceberg_arn, check_iceberg_policy};
use crate::catalog::IcebergCatalog;
use crate::error::IcebergError;
use crate::metadata;
use crate::types::{
    CatalogConfig, CommitTableRequest, CommitTableResponse, CreateNamespaceRequest,
    CreateNamespaceResponse, CreateTableRequest, ListNamespacesParams, ListNamespacesResponse,
    ListTablesParams, ListTablesResponse, LoadNamespaceResponse, LoadTableResponse, PurgeParams,
    RenameTableRequest, SetPolicyRequest, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
};
use axum::Extension;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use objectio_auth::AuthResult;
use objectio_auth::policy::PolicyEvaluator;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

type Result<T> = std::result::Result<T, IcebergError>;

/// Shared state for Iceberg handlers — wraps the catalog.
pub struct IcebergState {
    pub catalog: IcebergCatalog,
    pub warehouse_location: String,
    pub policy_evaluator: PolicyEvaluator,
}

/// Check namespace-level policy. Returns Ok(()) if allowed, Err on deny.
/// Skips if auth is None (--no-auth mode).
async fn check_ns_policy(
    state: &IcebergState,
    auth: Option<&Extension<AuthResult>>,
    ns_levels: &[String],
    action: &str,
    resource_arn: &str,
) -> Result<()> {
    let Some(Extension(auth_result)) = auth else {
        return Ok(());
    };

    // Load namespace properties to get __policy
    let policy_json = match state.catalog.load_namespace(ns_levels.to_vec()).await {
        Ok(resp) => resp.properties.get("__policy").cloned(),
        Err(_) => None,
    };

    check_iceberg_policy(
        &state.policy_evaluator,
        policy_json.as_deref(),
        &auth_result.user_arn,
        action,
        resource_arn,
    )
}

/// Check table-level policy (after namespace policy). Returns Ok(()) if allowed.
async fn check_table_policy(
    state: &IcebergState,
    auth: Option<&Extension<AuthResult>>,
    ns_levels: &[String],
    table_name: &str,
    action: &str,
    resource_arn: &str,
) -> Result<()> {
    let Some(Extension(auth_result)) = auth else {
        return Ok(());
    };

    // Check namespace-level policy first
    let ns_arn = build_iceberg_arn(ns_levels, None);
    check_ns_policy(state, auth, ns_levels, action, &ns_arn).await?;

    // Then check table-level policy
    let policy_bytes = state
        .catalog
        .get_table_policy(ns_levels.to_vec(), table_name)
        .await?;

    let policy_str = policy_bytes.and_then(|b| {
        if b.is_empty() {
            None
        } else {
            String::from_utf8(b).ok()
        }
    });

    check_iceberg_policy(
        &state.policy_evaluator,
        policy_str.as_deref(),
        &auth_result.user_arn,
        action,
        resource_arn,
    )
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
    auth: Option<Extension<AuthResult>>,
    Query(params): Query<ListNamespacesParams>,
) -> Result<Json<ListNamespacesResponse>> {
    let parent = params.parent.as_ref().map(|p| parse_namespace(p));
    if let Some(ref parent_levels) = parent {
        let arn = build_iceberg_arn(parent_levels, None);
        check_ns_policy(
            &state,
            auth.as_ref(),
            parent_levels,
            "iceberg:ListNamespaces",
            &arn,
        )
        .await?;
    }
    let (namespaces, next_page_token) = state
        .catalog
        .list_namespaces(parent, params.page_token, params.page_size)
        .await?;
    Ok(Json(ListNamespacesResponse {
        namespaces,
        next_page_token,
    }))
}

/// `POST /v1/namespaces` — create a new namespace.
///
/// # Errors
/// Returns `IcebergError` if the namespace already exists or parent is missing.
pub async fn create_namespace(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<CreateNamespaceResponse>)> {
    // Check parent namespace policy if multi-level
    if req.namespace.len() > 1 {
        let parent = &req.namespace[..req.namespace.len() - 1];
        let arn = build_iceberg_arn(parent, None);
        check_ns_policy(
            &state,
            auth.as_ref(),
            parent,
            "iceberg:CreateNamespace",
            &arn,
        )
        .await?;
    }
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
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
) -> Result<Json<LoadNamespaceResponse>> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, None);
    check_ns_policy(
        &state,
        auth.as_ref(),
        &levels,
        "iceberg:LoadNamespace",
        &arn,
    )
    .await?;
    let resp = state.catalog.load_namespace(levels).await?;
    Ok(Json(resp))
}

/// `HEAD /v1/namespaces/{namespace}` — check if namespace exists.
///
/// # Errors
/// Returns `IcebergError` on gRPC communication failure.
pub async fn namespace_exists(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, None);
    check_ns_policy(
        &state,
        auth.as_ref(),
        &levels,
        "iceberg:NamespaceExists",
        &arn,
    )
    .await?;
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
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, None);
    check_ns_policy(
        &state,
        auth.as_ref(),
        &levels,
        "iceberg:DropNamespace",
        &arn,
    )
    .await?;
    state.catalog.drop_namespace(levels).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/namespaces/{namespace}/properties` — update namespace properties.
///
/// # Errors
/// Returns `IcebergError` if the namespace is not found.
pub async fn update_namespace_properties(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Json(req): Json<UpdateNamespacePropertiesRequest>,
) -> Result<Json<UpdateNamespacePropertiesResponse>> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, None);
    check_ns_policy(
        &state,
        auth.as_ref(),
        &levels,
        "iceberg:UpdateNamespaceProperties",
        &arn,
    )
    .await?;
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
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Query(params): Query<ListTablesParams>,
) -> Result<Json<ListTablesResponse>> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, None);
    check_ns_policy(&state, auth.as_ref(), &levels, "iceberg:ListTables", &arn).await?;
    let (identifiers, next_page_token) = state
        .catalog
        .list_tables(levels, params.page_token, params.page_size)
        .await?;
    Ok(Json(ListTablesResponse {
        identifiers,
        next_page_token,
    }))
}

/// `POST /v1/namespaces/{namespace}/tables` — create a new table.
///
/// # Errors
/// Returns `IcebergError` if the namespace is not found or table already exists.
#[allow(clippy::cast_possible_truncation)]
pub async fn create_table(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Json(req): Json<CreateTableRequest>,
) -> Result<(StatusCode, Json<LoadTableResponse>)> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, None);
    check_ns_policy(&state, auth.as_ref(), &levels, "iceberg:CreateTable", &arn).await?;

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

    let metadata_bytes = serde_json::to_vec(&metadata)
        .map_err(|e| IcebergError::internal(format!("failed to serialize metadata: {e}")))?;

    let _stored = state
        .catalog
        .create_table(levels, &req.name, &metadata_location, metadata_bytes)
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
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<LoadTableResponse>> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, Some(&table));
    check_table_policy(
        &state,
        auth.as_ref(),
        &levels,
        &table,
        "iceberg:LoadTable",
        &arn,
    )
    .await?;
    let (metadata_location, metadata_bytes) = state.catalog.load_table(levels, &table).await?;

    let metadata: serde_json::Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| IcebergError::internal(format!("failed to deserialize metadata: {e}")))?;

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
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
    Json(req): Json<CommitTableRequest>,
) -> Result<Json<CommitTableResponse>> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, Some(&table));
    check_table_policy(
        &state,
        auth.as_ref(),
        &levels,
        &table,
        "iceberg:UpdateTable",
        &arn,
    )
    .await?;

    // 1. Load current metadata
    let (current_location, metadata_bytes) =
        state.catalog.load_table(levels.clone(), &table).await?;
    let mut md: serde_json::Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| IcebergError::internal(format!("failed to deserialize metadata: {e}")))?;

    // 2. Validate requirements against current metadata
    metadata::validate_requirements(&md, &req.requirements)?;

    // 3. Apply updates
    metadata::apply_updates(&mut md, &req.updates)?;

    // 4. Increment sequence number and update timestamp
    let seq = md
        .get("last-sequence-number")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
        + 1;
    md["last-sequence-number"] = json!(seq);
    md["last-updated-ms"] = json!(chrono_now_ms());

    // 5. Append current location to metadata-log
    if let Some(log) = md
        .get_mut("metadata-log")
        .and_then(serde_json::Value::as_array_mut)
    {
        log.push(json!({
            "metadata-file": current_location,
            "timestamp-ms": chrono_now_ms()
        }));
    }

    // 6. Generate new metadata location with sequence number
    let new_location = format!(
        "{}/metadata/{:05}-{}.metadata.json",
        md.get("location")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(""),
        seq,
        uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap_or("0")
    );

    // 7. Serialize and commit
    let new_bytes = serde_json::to_vec(&md)
        .map_err(|e| IcebergError::internal(format!("failed to serialize metadata: {e}")))?;

    let (committed_location, _) = state
        .catalog
        .commit_table(levels, &table, &current_location, &new_location, new_bytes)
        .await?;

    Ok(Json(CommitTableResponse {
        metadata_location: committed_location,
        metadata: md,
    }))
}

/// `HEAD /v1/namespaces/{namespace}/tables/{table}` — check if table exists.
///
/// # Errors
/// Returns `IcebergError` on gRPC communication failure.
pub async fn table_exists(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, Some(&table));
    check_table_policy(
        &state,
        auth.as_ref(),
        &levels,
        &table,
        "iceberg:TableExists",
        &arn,
    )
    .await?;
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
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
    Query(params): Query<PurgeParams>,
) -> Result<StatusCode> {
    let levels = parse_namespace(&namespace);
    let arn = build_iceberg_arn(&levels, Some(&table));
    check_table_policy(
        &state,
        auth.as_ref(),
        &levels,
        &table,
        "iceberg:DropTable",
        &arn,
    )
    .await?;
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
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<RenameTableRequest>,
) -> Result<StatusCode> {
    // Check policy on source table (need DropTable equivalent)
    let src_arn = build_iceberg_arn(&req.source.namespace, Some(&req.source.name));
    check_table_policy(
        &state,
        auth.as_ref(),
        &req.source.namespace,
        &req.source.name,
        "iceberg:RenameTable",
        &src_arn,
    )
    .await?;
    // Check policy on destination namespace (need CreateTable equivalent)
    let dst_arn = build_iceberg_arn(&req.destination.namespace, None);
    check_ns_policy(
        &state,
        auth.as_ref(),
        &req.destination.namespace,
        "iceberg:RenameTable",
        &dst_arn,
    )
    .await?;
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

// ---- Policy management endpoints ----

/// `PUT /v1/namespaces/{namespace}/policy` — set namespace-level access policy.
///
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if namespace not found or user is not admin.
pub async fn set_namespace_policy(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Json(req): Json<SetPolicyRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);

    // Store policy as __policy property on the namespace
    let mut updates = HashMap::new();
    updates.insert("__policy".to_string(), req.policy);
    state
        .catalog
        .update_namespace_properties(levels, Vec::new(), updates)
        .await?;
    Ok(StatusCode::OK)
}

/// `PUT /v1/namespaces/{namespace}/tables/{table}/policy` — set table-level access policy.
///
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if table not found or user is not admin.
pub async fn set_table_policy(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
    Json(req): Json<SetPolicyRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);
    state
        .catalog
        .set_table_policy(levels, &table, req.policy.into_bytes())
        .await?;
    Ok(StatusCode::OK)
}

/// Require admin user. Returns 403 if auth is present but not admin.
/// In --no-auth mode (auth is None), allows all access.
fn require_admin(auth: Option<&Extension<AuthResult>>) -> Result<()> {
    if let Some(Extension(auth_result)) = auth
        && !auth_result.user_arn.ends_with("user/admin")
    {
        return Err(IcebergError::forbidden(
            "Only admin user can manage policies",
        ));
    }
    Ok(())
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
