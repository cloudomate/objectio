//! Axum route handlers for the 13 Iceberg REST API endpoints.

use crate::access::{
    CATALOG_POLICY_NAMESPACE, HierarchicalPolicyCheck, IcebergPolicyCheck, TAG_PREFIX,
    build_iceberg_arn, check_hierarchical_policies, check_iceberg_policy, extract_tags,
    validate_iceberg_policy,
};
use crate::catalog::{IcebergCatalog, NewDataFilter};
use crate::error::IcebergError;
use crate::filters;
use crate::metadata;
use crate::roles::IcebergRole;
use crate::types::{
    CatalogConfig, CommitTableRequest, CommitTableResponse,
    CreateDataFilterRequest as CreateDataFilterRestRequest, CreateNamespaceRequest,
    CreateNamespaceResponse, CreateTableRequest, DataFilterResponse, EffectivePolicyEntry,
    EffectivePolicyParams, EffectivePolicyResponse, GetEncryptionPolicyResponse, GetQuotaResponse,
    GetTagsResponse, ListDataFiltersResponse as ListDataFiltersRestResponse, ListNamespacesParams,
    ListNamespacesResponse, ListTablesParams, ListTablesResponse, LoadNamespaceResponse,
    LoadTableResponse, PurgeParams, RenameTableRequest, SetEncryptionPolicyRequest,
    SetPolicyRequest, SetQuotaRequest, SetRoleBindingRequest, SetTagsRequest,
    SimulateMatchedStatement, SimulatePolicyRequest, SimulatePolicyResponse,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use axum::Extension;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use objectio_auth::AuthResult;
use objectio_auth::policy::{BucketPolicy, PolicyEvaluator, RequestContext};
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

/// Load the catalog-level default policy (from `__catalog` namespace).
async fn load_catalog_policy(state: &IcebergState) -> Option<String> {
    match state
        .catalog
        .load_namespace(vec![CATALOG_POLICY_NAMESPACE.to_string()])
        .await
    {
        Ok(resp) => resp.properties.get("__policy").cloned(),
        Err(_) => None,
    }
}

/// Collected namespace policies and merged resource tags from the hierarchy.
struct NamespaceHierarchy {
    policies: Vec<(String, Option<String>)>,
    tags: HashMap<String, String>,
}

/// Collect namespace policies and resource tags for the full hierarchy.
///
/// For namespace `["a", "b", "c"]`, loads properties for `a`, `a.b`, `a.b.c`,
/// extracting `__policy` entries and merging `__tag:*` entries (child tags
/// override parent tags with the same key).
async fn collect_namespace_hierarchy(
    state: &IcebergState,
    ns_levels: &[String],
) -> NamespaceHierarchy {
    let mut policies = Vec::with_capacity(ns_levels.len());
    let mut tags = HashMap::new();
    for i in 1..=ns_levels.len() {
        let prefix = &ns_levels[..i];
        let label = format!("namespace:{}", prefix.join("."));
        match state.catalog.load_namespace(prefix.to_vec()).await {
            Ok(resp) => {
                let policy_json = resp.properties.get("__policy").cloned();
                // Merge tags from this level (child overrides parent)
                for (k, v) in &resp.properties {
                    if let Some(tag_key) = k.strip_prefix(TAG_PREFIX) {
                        tags.insert(tag_key.to_string(), v.clone());
                    }
                }
                policies.push((label, policy_json));
            }
            Err(_) => {
                policies.push((label, None));
            }
        }
    }
    NamespaceHierarchy { policies, tags }
}

/// Check namespace-level policy with full hierarchy. Returns Ok(()) if allowed, Err on deny.
/// Skips if auth is None (--no-auth mode).
///
/// Checks in order: catalog policy → each ancestor namespace → target namespace.
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

    // Extract table name from resource ARN if present (after last /)
    let table_name = resource_arn
        .strip_prefix("arn:obio:iceberg:::")
        .and_then(|rest| {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() > 1 {
                Some(parts[1])
            } else {
                None
            }
        });

    let catalog_policy = load_catalog_policy(state).await;
    let hierarchy = collect_namespace_hierarchy(state, ns_levels).await;

    check_hierarchical_policies(&HierarchicalPolicyCheck {
        evaluator: &state.policy_evaluator,
        catalog_policy_json: catalog_policy.as_deref(),
        ancestor_policies: &hierarchy.policies,
        user_arn: &auth_result.user_arn,
        action,
        resource_arn,
        ns_levels,
        table_name,
        resource_tags: &hierarchy.tags,
    })
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

    // Check namespace-level policy first (use the table ARN so ns/* patterns match)
    check_ns_policy(state, auth, ns_levels, action, resource_arn).await?;

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

    check_iceberg_policy(&IcebergPolicyCheck {
        evaluator: &state.policy_evaluator,
        policy_json: policy_str.as_deref(),
        user_arn: &auth_result.user_arn,
        action,
        resource_arn,
        ns_levels,
        table_name: Some(table_name),
        policy_source: "table",
        resource_tags: &HashMap::new(),
    })
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

    // Check parent namespace policy for multi-level namespaces
    if levels.len() > 1 {
        let parent = &levels[..levels.len() - 1];
        let parent_arn = build_iceberg_arn(parent, None);
        check_ns_policy(
            &state,
            auth.as_ref(),
            parent,
            "iceberg:DropNamespace",
            &parent_arn,
        )
        .await?;
    }

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

    // Check parent namespace policy for multi-level namespaces
    if levels.len() > 1 {
        let parent = &levels[..levels.len() - 1];
        let parent_arn = build_iceberg_arn(parent, None);
        check_ns_policy(
            &state,
            auth.as_ref(),
            parent,
            "iceberg:UpdateNamespaceProperties",
            &parent_arn,
        )
        .await?;
    }

    check_ns_policy(
        &state,
        auth.as_ref(),
        &levels,
        "iceberg:UpdateNamespaceProperties",
        &arn,
    )
    .await?;

    // Reject non-admin attempts to set or remove the __policy property
    let touches_policy =
        req.updates.contains_key("__policy") || req.removals.iter().any(|k| k == "__policy");
    if touches_policy {
        require_admin(auth.as_ref())?;
    }

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
    let arn = build_iceberg_arn(&levels, Some(&req.name));
    check_ns_policy(&state, auth.as_ref(), &levels, "iceberg:CreateTable", &arn).await?;

    // Check namespace quota
    let ns_props = state.catalog.load_namespace(levels.clone()).await?;
    if let Some(max_str) = ns_props.properties.get(QUOTA_MAX_TABLES_KEY)
        && let Ok(max_tables) = max_str.parse::<u32>()
    {
        let (existing_tables, _) = state
            .catalog
            .list_tables(levels.clone(), None, None)
            .await?;
        #[allow(clippy::cast_possible_truncation)]
        let count = existing_tables.len() as u32;
        if count >= max_tables {
            return Err(IcebergError::bad_request(format!(
                "Namespace table quota exceeded: {count}/{max_tables} tables"
            )));
        }
    }

    // Build initial table metadata (Iceberg v2 format)
    let table_uuid = uuid::Uuid::new_v4().to_string();
    let location = req.location.unwrap_or_else(|| {
        let ns_path = levels.join("/");
        format!("{}/{}/{}", state.warehouse_location, ns_path, req.name)
    });

    // Check encryption policy — location must start with required prefix
    if let Some(required_prefix) = ns_props.properties.get(ENCRYPTION_PREFIX_KEY)
        && !location.starts_with(required_prefix.as_str())
    {
        return Err(IcebergError::bad_request(format!(
            "Table location \"{location}\" does not match required encryption prefix \"{required_prefix}\""
        )));
    }

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

    let metadata_location = format!("{location}/metadata/00000-{table_uuid}.metadata.json");

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
/// When data filters are configured for the calling principal, the response
/// metadata is modified: schema fields are filtered (column security) and a
/// `obio.row-filter` property is injected (row security).
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
    let (metadata_location, metadata_bytes) =
        state.catalog.load_table(levels.clone(), &table).await?;

    if metadata_bytes.is_empty() {
        return Err(IcebergError::internal(
            "table metadata is empty — this table was likely created with an older catalog \
             version that did not persist inline metadata; please drop and re-create the table",
        ));
    }

    let mut metadata: serde_json::Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| IcebergError::internal(format!("failed to deserialize metadata: {e}")))?;

    // Apply data filters if the caller is authenticated and not admin
    if let Some(Extension(ref auth_result)) = auth
        && !auth_result.user_arn.ends_with("user/admin")
    {
        let applicable_filters = state
            .catalog
            .get_data_filters_for_principal(
                levels,
                &table,
                &auth_result.user_arn,
                auth_result.group_arns.clone(),
            )
            .await
            .unwrap_or_default();

        for filter in &applicable_filters {
            filters::apply_column_filter(
                &mut metadata,
                &filter.allowed_columns,
                &filter.excluded_columns,
            );
            filters::apply_row_filter(&mut metadata, &filter.row_filter_expression);
        }
    }

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

    if metadata_bytes.is_empty() {
        return Err(IcebergError::internal(
            "table metadata is empty — this table was likely created with an older catalog \
             version that did not persist inline metadata; please drop and re-create the table",
        ));
    }

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
    // Check policy on destination namespace (rename creates a table in the destination)
    let dst_arn = build_iceberg_arn(&req.destination.namespace, Some(&req.destination.name));
    check_ns_policy(
        &state,
        auth.as_ref(),
        &req.destination.namespace,
        "iceberg:CreateTable",
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

    // Validate policy before persisting
    if let Err(e) = validate_iceberg_policy(&req.policy) {
        return Err(IcebergError::bad_request(format!(
            "Invalid iceberg policy: {e}"
        )));
    }

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

    // Validate policy before persisting
    if let Err(e) = validate_iceberg_policy(&req.policy) {
        return Err(IcebergError::bad_request(format!(
            "Invalid iceberg policy: {e}"
        )));
    }

    state
        .catalog
        .set_table_policy(levels, &table, req.policy.into_bytes())
        .await?;
    Ok(StatusCode::OK)
}

/// `PUT /v1/catalog/policy` — set catalog-level default policy.
///
/// Stores the policy as `__policy` property on the reserved `__catalog` namespace.
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if user is not admin or policy is invalid.
pub async fn set_catalog_policy(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<SetPolicyRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;

    // Validate policy before persisting
    if let Err(e) = validate_iceberg_policy(&req.policy) {
        return Err(IcebergError::bad_request(format!(
            "Invalid iceberg policy: {e}"
        )));
    }

    // Ensure __catalog namespace exists
    let catalog_ns = vec![CATALOG_POLICY_NAMESPACE.to_string()];
    if !state
        .catalog
        .namespace_exists(catalog_ns.clone())
        .await
        .unwrap_or(false)
    {
        let _ = state
            .catalog
            .create_namespace(catalog_ns.clone(), HashMap::new())
            .await;
    }

    // Store policy as __policy property on __catalog namespace
    let mut updates = HashMap::new();
    updates.insert("__policy".to_string(), req.policy);
    state
        .catalog
        .update_namespace_properties(catalog_ns, Vec::new(), updates)
        .await?;
    Ok(StatusCode::OK)
}

/// `GET /v1/namespaces/{namespace}/effective-policy` — show effective merged policy.
///
/// Returns the catalog-level policy plus each namespace policy in the hierarchy.
/// Read-only, admin-only.
///
/// # Errors
/// Returns `IcebergError` if user is not admin.
pub async fn get_effective_policy(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Query(_params): Query<EffectivePolicyParams>,
) -> Result<Json<EffectivePolicyResponse>> {
    require_admin(auth.as_ref())?;

    let levels = parse_namespace(&namespace);

    // Load catalog-level policy
    let catalog_policy_str = load_catalog_policy(&state).await;
    let catalog_policy = catalog_policy_str
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());

    // Load each namespace policy in the hierarchy
    let mut namespace_policies = Vec::new();
    for i in 1..=levels.len() {
        let prefix = &levels[..i];
        let ns_name = prefix.join(".");
        let policy_json = match state.catalog.load_namespace(prefix.to_vec()).await {
            Ok(resp) => resp.properties.get("__policy").cloned(),
            Err(_) => None,
        };
        let policy = policy_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        namespace_policies.push(EffectivePolicyEntry {
            namespace: ns_name,
            policy,
        });
    }

    Ok(Json(EffectivePolicyResponse {
        namespace: levels.join("."),
        catalog_policy,
        namespace_policies,
    }))
}

/// `POST /v1/simulate-policy` — simulate a policy decision without executing.
///
/// Returns the decision and which policy statement caused it.
/// Admin-only.
///
/// # Errors
/// Returns `IcebergError` if user is not admin or input is invalid.
pub async fn simulate_policy(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<SimulatePolicyRequest>,
) -> Result<Json<SimulatePolicyResponse>> {
    require_admin(auth.as_ref())?;

    // Parse the resource ARN to extract namespace/table
    let resource_path = req
        .resource
        .strip_prefix("arn:obio:iceberg:::")
        .unwrap_or(&req.resource);

    let parts: Vec<&str> = resource_path.split('/').collect();
    if parts.is_empty() {
        return Err(IcebergError::bad_request(
            "resource must be a valid Iceberg ARN (arn:obio:iceberg:::ns/table)",
        ));
    }

    let ns_levels: Vec<String> = if parts.len() > 1 {
        parts[..parts.len() - 1]
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    } else {
        parts.iter().map(|s| (*s).to_string()).collect()
    };
    let table_name = if parts.len() > 1 {
        Some(parts[parts.len() - 1])
    } else {
        None
    };

    // Build context
    let mut context = RequestContext::new(&req.user_arn, &req.action, &req.resource);
    context
        .variables
        .insert("iceberg:namespace".to_string(), ns_levels.join("."));
    if let Some(t) = table_name {
        context
            .variables
            .insert("iceberg:tableName".to_string(), t.to_string());
    }
    context
        .variables
        .insert("iceberg:operation".to_string(), req.action.clone());

    // Check catalog policy
    let catalog_policy_str = load_catalog_policy(&state).await;
    if let Some(ref json) = catalog_policy_str
        && let Ok(policy) = BucketPolicy::from_json(json)
    {
        let explanation = state
            .policy_evaluator
            .evaluate_with_explanation(&policy, &context, "catalog");
        if explanation.decision == objectio_auth::policy::PolicyDecision::Deny {
            return Ok(Json(explanation_to_response(explanation)));
        }
    }

    // Check namespace hierarchy
    for i in 1..=ns_levels.len() {
        let prefix = &ns_levels[..i];
        let ns_label = format!("namespace:{}", prefix.join("."));
        let policy_json = match state.catalog.load_namespace(prefix.to_vec()).await {
            Ok(resp) => resp.properties.get("__policy").cloned(),
            Err(_) => None,
        };
        if let Some(ref json) = policy_json
            && let Ok(policy) = BucketPolicy::from_json(json)
        {
            let explanation = state
                .policy_evaluator
                .evaluate_with_explanation(&policy, &context, &ns_label);
            if explanation.decision == objectio_auth::policy::PolicyDecision::Deny
                || explanation.decision == objectio_auth::policy::PolicyDecision::Allow
            {
                return Ok(Json(explanation_to_response(explanation)));
            }
        }
    }

    // Check table policy if applicable
    if let Some(t) = table_name {
        let policy_bytes = state
            .catalog
            .get_table_policy(ns_levels.clone(), t)
            .await
            .ok()
            .flatten();

        if let Some(bytes) = policy_bytes
            && let Ok(json_str) = String::from_utf8(bytes)
            && let Ok(policy) = BucketPolicy::from_json(&json_str)
        {
            let source = format!("table:{}", ns_levels.join(".") + "/" + t);
            let explanation = state
                .policy_evaluator
                .evaluate_with_explanation(&policy, &context, &source);
            if explanation.decision != objectio_auth::policy::PolicyDecision::ImplicitDeny {
                return Ok(Json(explanation_to_response(explanation)));
            }
        }
    }

    // No matching policy
    Ok(Json(SimulatePolicyResponse {
        decision: "implicit_deny".to_string(),
        matched_statement: None,
    }))
}

fn explanation_to_response(
    explanation: objectio_auth::policy::PolicyExplanation,
) -> SimulatePolicyResponse {
    let decision = match explanation.decision {
        objectio_auth::policy::PolicyDecision::Allow => "allow",
        objectio_auth::policy::PolicyDecision::Deny => "deny",
        objectio_auth::policy::PolicyDecision::ImplicitDeny => "implicit_deny",
    };
    SimulatePolicyResponse {
        decision: decision.to_string(),
        matched_statement: explanation
            .matched_statement
            .map(|ms| SimulateMatchedStatement {
                sid: ms.sid,
                effect: match ms.effect {
                    objectio_auth::policy::Effect::Allow => "Allow".to_string(),
                    objectio_auth::policy::Effect::Deny => "Deny".to_string(),
                },
                source: ms.source,
            }),
    }
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

// ---- Role binding ----

/// `PUT /v1/namespaces/{namespace}/role-binding` — bind a built-in role to principals.
///
/// Generates the equivalent policy document and stores it as the namespace policy.
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if the role is unknown or the user is not admin.
pub async fn set_role_binding(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Json(req): Json<SetRoleBindingRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;

    let role = IcebergRole::from_name(&req.role).ok_or_else(|| {
        IcebergError::bad_request(format!(
            "Unknown role \"{}\". Valid roles: CatalogAdmin, NamespaceOwner, TableWriter, TableReader",
            req.role
        ))
    })?;

    if req.principals.is_empty() {
        return Err(IcebergError::bad_request("principals list cannot be empty"));
    }

    let levels = parse_namespace(&namespace);
    let ns_name = levels.join(".");
    let policy_json = role.to_policy_json(&req.principals, &ns_name);

    // Store as namespace policy
    let mut updates = HashMap::new();
    updates.insert("__policy".to_string(), policy_json);
    state
        .catalog
        .update_namespace_properties(levels, Vec::new(), updates)
        .await?;

    tracing::info!(
        namespace = %ns_name,
        role = %role.name(),
        principals = ?req.principals,
        "iceberg.role_binding.set"
    );

    Ok(StatusCode::OK)
}

// ---- Tag management endpoints ----

/// `PUT /v1/namespaces/{namespace}/tags` — set namespace tags.
///
/// Tags are stored as `__tag:{key}` properties on the namespace.
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if namespace not found or user is not admin.
pub async fn set_namespace_tags(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Json(req): Json<SetTagsRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);

    let updates: HashMap<String, String> = req
        .tags
        .into_iter()
        .map(|(k, v)| (format!("{TAG_PREFIX}{k}"), v))
        .collect();

    state
        .catalog
        .update_namespace_properties(levels, Vec::new(), updates)
        .await?;
    Ok(StatusCode::OK)
}

/// `GET /v1/namespaces/{namespace}/tags` — get namespace tags.
///
/// # Errors
/// Returns `IcebergError` if namespace not found.
pub async fn get_namespace_tags(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
) -> Result<Json<GetTagsResponse>> {
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
    let tags = extract_tags(&resp.properties);
    Ok(Json(GetTagsResponse { tags }))
}

/// `PUT /v1/namespaces/{namespace}/tables/{table}/tags` — set table tags.
///
/// Table tags are stored as `__tag:{key}` entries in the table's Iceberg
/// metadata `properties` map.
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if table not found or user is not admin.
pub async fn set_table_tags(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
    Json(req): Json<SetTagsRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);

    // Load current metadata, inject tags, commit
    let (current_location, metadata_bytes) =
        state.catalog.load_table(levels.clone(), &table).await?;

    if metadata_bytes.is_empty() {
        return Err(IcebergError::internal("table metadata is empty"));
    }

    let mut md: serde_json::Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| IcebergError::internal(format!("failed to deserialize metadata: {e}")))?;

    // Ensure properties object exists
    if md.get("properties").is_none() {
        md["properties"] = json!({});
    }
    if let Some(props) = md
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
    {
        for (k, v) in &req.tags {
            props.insert(
                format!("{TAG_PREFIX}{k}"),
                serde_json::Value::String(v.clone()),
            );
        }
    }

    let new_bytes = serde_json::to_vec(&md)
        .map_err(|e| IcebergError::internal(format!("failed to serialize metadata: {e}")))?;

    let new_location = format!(
        "{}/metadata/tags-{}.metadata.json",
        md.get("location")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(""),
        uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap_or("0")
    );

    state
        .catalog
        .commit_table(levels, &table, &current_location, &new_location, new_bytes)
        .await?;

    Ok(StatusCode::OK)
}

/// `GET /v1/namespaces/{namespace}/tables/{table}/tags` — get table tags.
///
/// # Errors
/// Returns `IcebergError` if table not found.
pub async fn get_table_tags(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<GetTagsResponse>> {
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

    let (_location, metadata_bytes) = state.catalog.load_table(levels, &table).await?;
    if metadata_bytes.is_empty() {
        return Ok(Json(GetTagsResponse {
            tags: HashMap::new(),
        }));
    }

    let md: serde_json::Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| IcebergError::internal(format!("failed to deserialize metadata: {e}")))?;

    let tags = md
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .map(|props| {
            props
                .iter()
                .filter_map(|(k, v)| {
                    k.strip_prefix(TAG_PREFIX).map(|tag_key| {
                        (
                            tag_key.to_string(),
                            v.as_str().unwrap_or_default().to_string(),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Json(GetTagsResponse { tags }))
}

// ---- Quota management endpoints ----

/// Property key for maximum tables quota.
const QUOTA_MAX_TABLES_KEY: &str = "__quota_max_tables";

/// `PUT /v1/namespaces/{namespace}/quota` — set namespace quota.
///
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if namespace not found or user is not admin.
pub async fn set_namespace_quota(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Json(req): Json<SetQuotaRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);

    let mut updates = HashMap::new();
    let mut removals = Vec::new();

    if let Some(max) = req.max_tables {
        updates.insert(QUOTA_MAX_TABLES_KEY.to_string(), max.to_string());
    } else {
        removals.push(QUOTA_MAX_TABLES_KEY.to_string());
    }

    state
        .catalog
        .update_namespace_properties(levels, removals, updates)
        .await?;
    Ok(StatusCode::OK)
}

/// `GET /v1/namespaces/{namespace}/quota` — get namespace quota and current usage.
///
/// # Errors
/// Returns `IcebergError` if namespace not found.
#[allow(clippy::cast_possible_truncation)]
pub async fn get_namespace_quota(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
) -> Result<Json<GetQuotaResponse>> {
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

    let resp = state.catalog.load_namespace(levels.clone()).await?;
    let max_tables = resp
        .properties
        .get(QUOTA_MAX_TABLES_KEY)
        .and_then(|v| v.parse::<u32>().ok());

    // Count current tables
    let (tables, _) = state.catalog.list_tables(levels, None, None).await?;
    let current_tables = tables.len() as u32;

    Ok(Json(GetQuotaResponse {
        max_tables,
        current_tables,
    }))
}

// ---- Encryption policy endpoints ----

/// Property key for required encryption location prefix.
const ENCRYPTION_PREFIX_KEY: &str = "__encryption_required_prefix";

/// `PUT /v1/namespaces/{namespace}/encryption-policy` — set encryption requirement.
///
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if namespace not found or user is not admin.
pub async fn set_encryption_policy(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
    Json(req): Json<SetEncryptionPolicyRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);

    let mut updates = HashMap::new();
    updates.insert(
        ENCRYPTION_PREFIX_KEY.to_string(),
        req.required_location_prefix,
    );

    state
        .catalog
        .update_namespace_properties(levels, Vec::new(), updates)
        .await?;
    Ok(StatusCode::OK)
}

/// `GET /v1/namespaces/{namespace}/encryption-policy` — get encryption requirement.
///
/// # Errors
/// Returns `IcebergError` if namespace not found.
pub async fn get_encryption_policy(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path(namespace): Path<String>,
) -> Result<Json<GetEncryptionPolicyResponse>> {
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
    let required_location_prefix = resp.properties.get(ENCRYPTION_PREFIX_KEY).cloned();

    Ok(Json(GetEncryptionPolicyResponse {
        required_location_prefix,
    }))
}

// ---- Data filter endpoints ----

/// `PUT /v1/namespaces/{namespace}/tables/{table}/data-filters` — create a data filter.
///
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if user is not admin or table is not found.
pub async fn create_data_filter(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
    Json(req): Json<CreateDataFilterRestRequest>,
) -> Result<(StatusCode, Json<DataFilterResponse>)> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);

    let filter = state
        .catalog
        .create_data_filter(NewDataFilter {
            ns_levels: levels,
            table_name: &table,
            filter_name: &req.filter_name,
            principal_arns: req.principal_arns,
            allowed_columns: req.allowed_columns,
            excluded_columns: req.excluded_columns,
            row_filter_expression: &req.row_filter_expression,
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(DataFilterResponse {
            filter_id: filter.filter_id,
            filter_name: filter.filter_name,
            namespace: filter.namespace_levels,
            table_name: filter.table_name,
            principal_arns: filter.principal_arns,
            allowed_columns: filter.allowed_columns,
            excluded_columns: filter.excluded_columns,
            row_filter_expression: filter.row_filter_expression,
            created_at: filter.created_at,
            updated_at: filter.updated_at,
        }),
    ))
}

/// `GET /v1/namespaces/{namespace}/tables/{table}/data-filters` — list data filters.
///
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if user is not admin.
pub async fn list_data_filters(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<ListDataFiltersRestResponse>> {
    require_admin(auth.as_ref())?;
    let levels = parse_namespace(&namespace);

    let proto_filters = state.catalog.list_data_filters(levels, &table).await?;

    let filters = proto_filters
        .into_iter()
        .map(|f| DataFilterResponse {
            filter_id: f.filter_id,
            filter_name: f.filter_name,
            namespace: f.namespace_levels,
            table_name: f.table_name,
            principal_arns: f.principal_arns,
            allowed_columns: f.allowed_columns,
            excluded_columns: f.excluded_columns,
            row_filter_expression: f.row_filter_expression,
            created_at: f.created_at,
            updated_at: f.updated_at,
        })
        .collect();

    Ok(Json(ListDataFiltersRestResponse { filters }))
}

/// `DELETE /v1/namespaces/{namespace}/tables/{table}/data-filters/{filter_id}` — delete a filter.
///
/// Requires admin user.
///
/// # Errors
/// Returns `IcebergError` if user is not admin or filter not found.
pub async fn delete_data_filter(
    State(state): State<Arc<IcebergState>>,
    auth: Option<Extension<AuthResult>>,
    Path((_namespace, _table, filter_id)): Path<(String, String, String)>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref())?;

    let deleted = state.catalog.delete_data_filter(&filter_id).await?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(IcebergError::not_found(format!(
            "Data filter {filter_id} not found"
        )))
    }
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
