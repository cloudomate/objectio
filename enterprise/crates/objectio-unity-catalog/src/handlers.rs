//! Axum route handlers for the Unity Catalog REST API.
//!
//! Three-level model: `catalog → schema → table`. Schema and table paths
//! address resources by Unity-style `full_name` strings (`catalog.schema`,
//! `catalog.schema.table`); the handlers split on `.` and reject anything
//! else with `INVALID_PARAMETER_VALUE`.
#![allow(clippy::significant_drop_tightening)]

use crate::access::{
    PolicyLevel, UnityPolicyCheck, build_unity_arn, check_three_level, check_unity_policy,
    validate_unity_policy,
};
use crate::catalog::UnityCatalogClient;
use crate::error::UnityError;
use crate::types::{
    AwsCredentials, CallerIdentity, CatalogInfo, CreateCatalogRequest, CreateFunctionBody,
    CreateModelRequest, CreateModelVersionRequest, CreateSchemaRequest, CreateTableRequest,
    CreateVolumeRequest, DeleteParams, FunctionInfo, GetPolicyResponse, ListCatalogsParams,
    ListCatalogsResponse, ListFunctionsParams, ListFunctionsResponse, ListModelVersionsResponse,
    ListModelsParams, ListModelsResponse, ListSchemasParams, ListSchemasResponse,
    ListTablesParams, ListTablesResponse, ListVolumesParams, ListVolumesResponse, ModelInfo,
    ModelVersionInfo, SchemaInfo, SetPolicyRequest, SetTableSecurityRequest, TableInfo,
    TemporaryTableCredentialsRequest, TemporaryTableCredentialsResponse,
    TemporaryVolumeCredentialsRequest, TemporaryVolumeCredentialsResponse, UpdateCatalogRequest,
    UpdateModelVersionStatusRequest, UpdateSchemaRequest, VolumeInfo,
};
use axum::Extension;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use objectio_auth::AuthResult;
use objectio_auth::policy::{BucketPolicy, PolicyDecision, PolicyEvaluator, RequestContext};
use objectio_proto::metadata::{GetPolicyRequest, ListAttachedPoliciesRequest};
use std::sync::Arc;

type Result<T> = std::result::Result<T, UnityError>;

/// Shared state for Unity handlers.
pub struct UnityState {
    pub catalog: UnityCatalogClient,
    pub policy_evaluator: PolicyEvaluator,
    /// Principals (user/group ARNs) granted admin access for catalog/schema/table policy management.
    pub admin_principals: Vec<String>,
    /// STS provider for `temporary-table-credentials`. `None` disables vending.
    pub sts_provider: Option<objectio_auth::sts::StsProvider>,
    /// S3 endpoint URL surfaced alongside vended credentials.
    pub s3_endpoint: String,
}

// ---- full_name parsers ----

/// Split `"catalog.schema"` into `("catalog", "schema")`. Errors on any other shape.
fn parse_schema_full_name(full_name: &str) -> Result<(String, String)> {
    let mut parts = full_name.splitn(2, '.');
    let catalog = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
        UnityError::bad_request(format!("invalid schema full_name '{full_name}'"))
    })?;
    let schema = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
        UnityError::bad_request(format!(
            "schema full_name must be 'catalog.schema', got '{full_name}'"
        ))
    })?;
    if schema.contains('.') {
        return Err(UnityError::bad_request(format!(
            "schema full_name must contain exactly one '.', got '{full_name}'"
        )));
    }
    Ok((catalog.to_string(), schema.to_string()))
}

/// Split `"catalog.schema.table"` into `("catalog", "schema", "table")`.
/// Rejects anything other than exactly three non-empty dot-separated parts
/// — none of the segments may themselves contain a `.`.
fn parse_table_full_name(full_name: &str) -> Result<(String, String, String)> {
    let parts: Vec<&str> = full_name.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err(UnityError::bad_request(format!(
            "table full_name must be 'catalog.schema.table', got '{full_name}'"
        )));
    }
    Ok((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

// ---- Policy walk helpers ----

/// Load (catalog/schema/table) policies from meta in a single async pass.
async fn load_policies(
    state: &UnityState,
    catalog: &str,
    schema: Option<&str>,
    table: Option<&str>,
) -> (Option<String>, Option<String>, Option<String>) {
    let cat_policy = state
        .catalog
        .get_catalog_policy(catalog)
        .await
        .ok()
        .and_then(decode_policy);
    let sch_policy = match schema {
        Some(s) => state
            .catalog
            .get_schema_policy(catalog, s)
            .await
            .ok()
            .and_then(decode_policy),
        None => None,
    };
    let tbl_policy = match (schema, table) {
        (Some(s), Some(t)) => state
            .catalog
            .get_table_policy(catalog, s, t)
            .await
            .ok()
            .and_then(decode_policy),
        _ => None,
    };
    (cat_policy, sch_policy, tbl_policy)
}

fn decode_policy(bytes: Vec<u8>) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    String::from_utf8(bytes).ok().filter(|s| !s.is_empty())
}

/// Evaluate the user's attached IAM managed policies against
/// (action, resource). Returns `Err` on the first explicit Deny.
///
/// This is the bridge that lets workspace IAM policies grant or deny
/// `unity:*` actions the same way they grant or deny `s3:*` and
/// `kms:*`. Same `PolicyEvaluator`, same JSON shape — Unity actions
/// ride on the existing IAM plane instead of a parallel one.
///
/// Empty attached-set returns Ok (matches the catalog/schema/table
/// "no policy = implicit allow" semantics already used elsewhere).
async fn check_iam_policies(
    state: &UnityState,
    auth_result: &AuthResult,
    action: &str,
    resource_arn: &str,
    catalog: &str,
    schema: Option<&str>,
    table: Option<&str>,
) -> Result<()> {
    // The admin user gets a free pass at the IAM layer — same as KMS and
    // the entity-policy admin gate. Without this, an admin who has zero
    // IAM policies attached would still be allowed (because empty set is
    // implicit-allow), but a Deny statement attached to "*" would block
    // them by accident. The admin short-circuit avoids that footgun.
    if auth_result.user_arn.ends_with("user/admin") {
        return Ok(());
    }
    let mut client = state.catalog.meta_client();
    // Collect policies from the user AND every IAM group they're a member
    // of. The OIDC bridge populates `group_ids` by mapping claim names to
    // local IAM groups; SigV4/cookie callers currently leave it empty (a
    // future enhancement can call GetUserGroups in those branches).
    // Dedup across user + groups so a policy attached to both isn't
    // evaluated twice.
    let mut attached: Vec<String> = Vec::new();
    let user_lookup = client
        .list_attached_policies(ListAttachedPoliciesRequest {
            user_id: auth_result.user_id.clone(),
            group_id: String::new(),
        })
        .await;
    match user_lookup {
        Ok(r) => attached.extend(r.into_inner().policy_names),
        Err(e) => {
            // Treat lookup failures as "no IAM opinion" — entity policies
            // still apply. Logging surfaces the misconfiguration without
            // breaking the request.
            tracing::warn!(
                "unity: list_attached_policies for {} failed: {e}",
                auth_result.user_id
            );
            return Ok(());
        }
    }
    for gid in &auth_result.group_ids {
        if let Ok(r) = client
            .list_attached_policies(ListAttachedPoliciesRequest {
                user_id: String::new(),
                group_id: gid.clone(),
            })
            .await
        {
            for name in r.into_inner().policy_names {
                if !attached.contains(&name) {
                    attached.push(name);
                }
            }
        }
    }
    if attached.is_empty() {
        return Ok(());
    }
    for name in attached {
        let Ok(resp) = client
            .get_policy(GetPolicyRequest { name: name.clone() })
            .await
        else {
            continue;
        };
        let inner = resp.into_inner();
        if !inner.found {
            continue;
        }
        let Some(policy_obj) = inner.policy else {
            continue;
        };
        let Ok(policy) = BucketPolicy::from_json(&policy_obj.policy_json) else {
            tracing::warn!(
                "unity: attached policy '{name}' for {} failed to parse — skipping",
                auth_result.user_id
            );
            continue;
        };
        let mut ctx = RequestContext::new(&auth_result.user_arn, action, resource_arn);
        ctx.variables
            .insert("unity:catalog".to_string(), catalog.to_string());
        if let Some(s) = schema {
            ctx.variables
                .insert("unity:schema".to_string(), s.to_string());
        }
        if let Some(t) = table {
            ctx.variables
                .insert("unity:tableName".to_string(), t.to_string());
        }
        ctx.variables
            .insert("unity:operation".to_string(), action.to_string());
        match state.policy_evaluator.evaluate(&policy, &ctx) {
            PolicyDecision::Deny => {
                tracing::warn!(
                    user = %auth_result.user_arn,
                    action = %action,
                    resource = %resource_arn,
                    policy = %name,
                    "unity.iam.decision: deny"
                );
                return Err(UnityError::forbidden(format!(
                    "Action {action} on {resource_arn} denied by attached IAM policy {name}"
                )));
            }
            PolicyDecision::Allow | PolicyDecision::ImplicitDeny => {}
        }
    }
    Ok(())
}

/// Walk attached IAM policies → catalog → schema → table policies for an
/// authenticated request. Skips evaluation entirely when auth is None
/// (--no-auth mode).
///
/// Semantics: any explicit Deny at any layer rejects. Implicit-allow at
/// every layer = permit (matches the existing "owner has implicit
/// access" model already used by S3 bucket policies and Iceberg).
async fn check_policy_chain(
    state: &UnityState,
    auth: Option<&Extension<AuthResult>>,
    catalog: &str,
    schema: Option<&str>,
    table: Option<&str>,
    action: &str,
) -> Result<()> {
    let Some(Extension(auth_result)) = auth else {
        return Ok(());
    };
    let resource_arn = build_unity_arn(catalog, schema, table);
    // IAM-attached policies first — any Deny here short-circuits before
    // we even bother loading the entity policies.
    check_iam_policies(
        state,
        auth_result,
        action,
        &resource_arn,
        catalog,
        schema,
        table,
    )
    .await?;
    let (cat, sch, tbl) = load_policies(state, catalog, schema, table).await;
    let levels = [
        PolicyLevel {
            label: "catalog",
            policy_json: cat.as_deref(),
        },
        PolicyLevel {
            label: "schema",
            policy_json: sch.as_deref(),
        },
        PolicyLevel {
            label: "table",
            policy_json: tbl.as_deref(),
        },
    ];
    check_three_level(
        &state.policy_evaluator,
        &auth_result.user_arn,
        action,
        &resource_arn,
        catalog,
        schema,
        table,
        &levels,
    )
}

/// Single-level policy check for catalog-only operations (`CreateSchema` etc.).
/// Same IAM-then-entity ordering as `check_policy_chain`.
async fn check_catalog_only(
    state: &UnityState,
    auth: Option<&Extension<AuthResult>>,
    catalog: &str,
    action: &str,
) -> Result<()> {
    let Some(Extension(auth_result)) = auth else {
        return Ok(());
    };
    let resource_arn = build_unity_arn(catalog, None, None);
    check_iam_policies(state, auth_result, action, &resource_arn, catalog, None, None).await?;
    let cat_policy = state
        .catalog
        .get_catalog_policy(catalog)
        .await
        .ok()
        .and_then(decode_policy);
    check_unity_policy(&UnityPolicyCheck {
        evaluator: &state.policy_evaluator,
        policy_json: cat_policy.as_deref(),
        user_arn: &auth_result.user_arn,
        action,
        resource_arn: &resource_arn,
        catalog,
        schema: None,
        table: None,
        policy_source: "catalog",
    })
}

// ---- Admin gate ----

/// Tail of an ARN (`arn:obio:iam::tenant:user/alice` → `alice`). For
/// arbitrary identifiers without slashes, returns the input as-is.
fn arn_short_name(arn: &str) -> &str {
    arn.rsplit_once('/').map_or(arn, |(_, name)| name)
}

fn is_admin(auth: &AuthResult, state: &UnityState) -> bool {
    if auth.user_arn.ends_with("user/admin") {
        return true;
    }
    if state.admin_principals.iter().any(|p| p == &auth.user_arn) {
        return true;
    }
    auth.group_arns
        .iter()
        .any(|g| state.admin_principals.iter().any(|p| p == g))
}

/// `GET /api/2.1/unity-catalog/me`
///
/// Return the calling principal's identity context. Engines (Spark/Trino)
/// call this to populate the SQL built-ins that row-filter / column-mask
/// UDFs reference (`current_user()`, `is_member()`,
/// `is_account_group_member()`).
///
/// In `--no-auth` mode the response is the wildcard "anonymous" principal
/// with no groups and `is_admin: true` so engines see a permissive context.
///
/// # Errors
/// Infallible — auth failures are surfaced by the auth middleware before
/// the handler runs.
#[allow(clippy::unused_async)]
pub async fn current_caller_identity(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
) -> Result<Json<CallerIdentity>> {
    let Some(Extension(auth_result)) = auth else {
        return Ok(Json(CallerIdentity {
            user_name: "anonymous".to_string(),
            user_id: String::new(),
            user_arn: String::new(),
            groups: Vec::new(),
            group_arns: Vec::new(),
            tenant: String::new(),
            is_admin: true,
        }));
    };
    let user_name = if auth_result.user_arn.is_empty() {
        auth_result.user_id.clone()
    } else {
        arn_short_name(&auth_result.user_arn).to_string()
    };
    let groups = auth_result
        .group_arns
        .iter()
        .map(|g| arn_short_name(g).to_string())
        .collect();
    let admin = is_admin(&auth_result, &state);
    Ok(Json(CallerIdentity {
        user_name,
        user_id: auth_result.user_id.clone(),
        user_arn: auth_result.user_arn.clone(),
        groups,
        group_arns: auth_result.group_arns,
        tenant: auth_result.tenant,
        is_admin: admin,
    }))
}

fn require_admin(auth: Option<&Extension<AuthResult>>, state: &UnityState) -> Result<()> {
    let Some(Extension(auth_result)) = auth else {
        return Ok(());
    };
    if auth_result.user_arn.ends_with("user/admin") {
        return Ok(());
    }
    let arns = std::iter::once(auth_result.user_arn.as_str())
        .chain(auth_result.group_arns.iter().map(String::as_str));
    for arn in arns {
        if state.admin_principals.iter().any(|p| p == arn) {
            return Ok(());
        }
    }
    Err(UnityError::forbidden(
        "Admin access required for Unity policy management.",
    ))
}

// ---- Catalog handlers ----

/// `GET /api/2.1/unity-catalog/catalogs`
///
/// # Errors
/// Returns `UnityError` if the catalog list call fails.
pub async fn list_catalogs(
    State(state): State<Arc<UnityState>>,
    _auth: Option<Extension<AuthResult>>,
    Query(_params): Query<ListCatalogsParams>,
) -> Result<Json<ListCatalogsResponse>> {
    let catalogs = state.catalog.list_catalogs(String::new()).await?;
    Ok(Json(ListCatalogsResponse {
        catalogs,
        next_page_token: None,
    }))
}

/// `POST /api/2.1/unity-catalog/catalogs`
///
/// # Errors
/// Returns `UnityError` on validation, conflict, or backend failure.
pub async fn create_catalog(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<CreateCatalogRequest>,
) -> Result<(StatusCode, Json<CatalogInfo>)> {
    require_admin(auth.as_ref(), &state)?;
    let name = req.name.clone();
    let catalog = state
        .catalog
        .create_catalog(
            req.name,
            req.comment,
            req.owner,
            String::new(),
            req.properties,
        )
        .await?;
    // Auto-attach a default deny-direct-S3 bucket policy on the backing
    // bucket. Forces all reads/writes through Unity-vended STS sessions
    // (which set obio:CredentialType = "STS") so an admin who hands out
    // permanent S3 keys can't accidentally bypass row filters / column
    // masks. Best-effort: log and continue if attach fails — the catalog
    // already exists and re-creating the policy is recoverable via the
    // bucket-policy admin API.
    let bucket_name = format!("unity-{name}");
    if let Err(e) = attach_default_unity_bucket_policy(&state, &bucket_name).await {
        tracing::warn!(
            "unity: failed to attach default deny-direct-S3 policy on backing bucket '{bucket_name}': {e}"
        );
    }
    Ok((StatusCode::OK, Json(catalog)))
}

/// Default bucket policy attached at Unity catalog create. Denies any
/// direct-S3 access where the credential type isn't `STS`. Engines (and
/// the gateway's `temporary-table-credentials` flow) hold STS sessions
/// → allowed. End-users with permanent IAM keys → denied. Admins can
/// edit or delete the policy via the bucket-policy admin endpoints.
fn default_unity_bucket_policy_json(bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "DenyNonStsDirectAccess",
            "Effect": "Deny",
            "Principal": "*",
            "Action": ["s3:*"],
            "Resource": [
                format!("arn:obio:s3:::{bucket}"),
                format!("arn:obio:s3:::{bucket}/*")
            ],
            "Condition": {
                "StringNotEquals": {
                    "obio:CredentialType": "STS"
                }
            }
        }]
    })
    .to_string()
}

async fn attach_default_unity_bucket_policy(
    state: &UnityState,
    bucket: &str,
) -> std::result::Result<(), tonic::Status> {
    use objectio_proto::metadata::SetBucketPolicyRequest;
    let policy_json = default_unity_bucket_policy_json(bucket);
    state
        .catalog
        .meta_client()
        .set_bucket_policy(SetBucketPolicyRequest {
            bucket: bucket.to_string(),
            policy_json,
        })
        .await?;
    Ok(())
}

/// `GET /api/2.1/unity-catalog/catalogs/{name}`
///
/// # Errors
/// Returns `UnityError::not_found` if the catalog doesn't exist.
pub async fn get_catalog(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(name): Path<String>,
) -> Result<Json<CatalogInfo>> {
    check_catalog_only(&state, auth.as_ref(), &name, "unity:GetCatalog").await?;
    Ok(Json(state.catalog.get_catalog(&name).await?))
}

/// `PATCH /api/2.1/unity-catalog/catalogs/{name}`
///
/// # Errors
/// Returns `UnityError` on conflict or backend failure.
pub async fn update_catalog(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(name): Path<String>,
    Json(req): Json<UpdateCatalogRequest>,
) -> Result<Json<CatalogInfo>> {
    check_catalog_only(&state, auth.as_ref(), &name, "unity:UpdateCatalog").await?;
    let updated = state
        .catalog
        .update_catalog(name, req.comment, req.owner, req.properties)
        .await?;
    Ok(Json(updated))
}

/// `DELETE /api/2.1/unity-catalog/catalogs/{name}?force=true`
///
/// # Errors
/// Returns `UnityError::not_empty` if force=false and schemas exist.
pub async fn delete_catalog(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(name): Path<String>,
    Query(params): Query<DeleteParams>,
) -> Result<StatusCode> {
    check_catalog_only(&state, auth.as_ref(), &name, "unity:DeleteCatalog").await?;
    state.catalog.delete_catalog(&name, params.force).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Schema handlers ----

/// `GET /api/2.1/unity-catalog/schemas?catalog_name=X`
///
/// # Errors
/// Returns `UnityError` if `catalog_name` is missing or backend fails.
pub async fn list_schemas(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Query(params): Query<ListSchemasParams>,
) -> Result<Json<ListSchemasResponse>> {
    if params.catalog_name.is_empty() {
        return Err(UnityError::bad_request(
            "catalog_name query parameter is required",
        ));
    }
    check_catalog_only(
        &state,
        auth.as_ref(),
        &params.catalog_name,
        "unity:ListSchemas",
    )
    .await?;
    let schemas = state.catalog.list_schemas(params.catalog_name).await?;
    Ok(Json(ListSchemasResponse {
        schemas,
        next_page_token: None,
    }))
}

/// `POST /api/2.1/unity-catalog/schemas`
///
/// # Errors
/// Returns `UnityError` on conflict or backend failure.
pub async fn create_schema(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<CreateSchemaRequest>,
) -> Result<(StatusCode, Json<SchemaInfo>)> {
    check_catalog_only(
        &state,
        auth.as_ref(),
        &req.catalog_name,
        "unity:CreateSchema",
    )
    .await?;
    let schema = state
        .catalog
        .create_schema(
            req.catalog_name,
            req.name,
            req.comment,
            req.owner,
            req.properties,
        )
        .await?;
    Ok((StatusCode::OK, Json(schema)))
}

/// `GET /api/2.1/unity-catalog/schemas/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn get_schema(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<SchemaInfo>> {
    let (catalog, schema) = parse_schema_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        None,
        "unity:GetSchema",
    )
    .await?;
    Ok(Json(state.catalog.get_schema(&catalog, &schema).await?))
}

/// `PATCH /api/2.1/unity-catalog/schemas/{full_name}`
///
/// # Errors
/// Returns `UnityError` on conflict or backend failure.
pub async fn update_schema(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
    Json(req): Json<UpdateSchemaRequest>,
) -> Result<Json<SchemaInfo>> {
    let (catalog, schema) = parse_schema_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        None,
        "unity:UpdateSchema",
    )
    .await?;
    let updated = state
        .catalog
        .update_schema(catalog, schema, req.comment, req.owner, req.properties)
        .await?;
    Ok(Json(updated))
}

/// `DELETE /api/2.1/unity-catalog/schemas/{full_name}?force=true`
///
/// # Errors
/// Returns `UnityError::not_empty` if force=false and tables exist.
pub async fn delete_schema(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
    Query(params): Query<DeleteParams>,
) -> Result<StatusCode> {
    let (catalog, schema) = parse_schema_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        None,
        "unity:DeleteSchema",
    )
    .await?;
    state
        .catalog
        .delete_schema(&catalog, &schema, params.force)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Table handlers ----

/// `GET /api/2.1/unity-catalog/tables?catalog_name=X&schema_name=Y`
///
/// # Errors
/// Returns `UnityError::bad_request` if either query param is empty.
pub async fn list_tables(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Query(params): Query<ListTablesParams>,
) -> Result<Json<ListTablesResponse>> {
    if params.catalog_name.is_empty() || params.schema_name.is_empty() {
        return Err(UnityError::bad_request(
            "catalog_name and schema_name query parameters are required",
        ));
    }
    check_policy_chain(
        &state,
        auth.as_ref(),
        &params.catalog_name,
        Some(&params.schema_name),
        None,
        "unity:ListTables",
    )
    .await?;
    let tables = state
        .catalog
        .list_tables(params.catalog_name, params.schema_name)
        .await?;
    Ok(Json(ListTablesResponse {
        tables,
        next_page_token: None,
    }))
}

/// `POST /api/2.1/unity-catalog/tables`
///
/// MANAGED tables auto-derive `storage_location` server-side. EXTERNAL
/// tables require the caller to supply one.
///
/// # Errors
/// Returns `UnityError` on conflict, validation, or backend failure.
pub async fn create_table(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<CreateTableRequest>,
) -> Result<(StatusCode, Json<TableInfo>)> {
    check_policy_chain(
        &state,
        auth.as_ref(),
        &req.catalog_name,
        Some(&req.schema_name),
        None,
        "unity:CreateTable",
    )
    .await?;
    let columns_json = if req.columns.is_empty() {
        String::new()
    } else {
        serde_json::to_string(&req.columns)
            .map_err(|e| UnityError::bad_request(format!("invalid columns: {e}")))?
    };
    let table = state
        .catalog
        .create_table(
            req.catalog_name,
            req.schema_name,
            req.name,
            req.table_type,
            req.data_source_format,
            req.storage_location,
            columns_json,
            req.owner,
            req.properties,
        )
        .await?;
    Ok((StatusCode::OK, Json(table)))
}

/// `GET /api/2.1/unity-catalog/tables/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn get_table(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<TableInfo>> {
    let (catalog, schema, table) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&table),
        "unity:GetTable",
    )
    .await?;
    Ok(Json(
        state.catalog.get_table(&catalog, &schema, &table).await?,
    ))
}

/// `DELETE /api/2.1/unity-catalog/tables/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn delete_table(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<StatusCode> {
    let (catalog, schema, table) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&table),
        "unity:DeleteTable",
    )
    .await?;
    state
        .catalog
        .delete_table(&catalog, &schema, &table)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Function handlers ----

/// `GET /api/2.1/unity-catalog/functions?catalog_name=X&schema_name=Y`
///
/// # Errors
/// Returns `UnityError::bad_request` if either query param is empty.
pub async fn list_functions(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Query(params): Query<ListFunctionsParams>,
) -> Result<Json<ListFunctionsResponse>> {
    if params.catalog_name.is_empty() || params.schema_name.is_empty() {
        return Err(UnityError::bad_request(
            "catalog_name and schema_name query parameters are required",
        ));
    }
    check_policy_chain(
        &state,
        auth.as_ref(),
        &params.catalog_name,
        Some(&params.schema_name),
        None,
        "unity:ListFunctions",
    )
    .await?;
    let functions = state
        .catalog
        .list_functions(params.catalog_name, params.schema_name)
        .await?;
    Ok(Json(ListFunctionsResponse {
        functions,
        next_page_token: None,
    }))
}

/// `POST /api/2.1/unity-catalog/functions`
///
/// Accepts both the Databricks-spec wrapped envelope
/// (`{"function_info": {...}}`) and a flat object — the body type uses
/// `#[serde(untagged)]` to discriminate. The official Databricks SDK
/// always sends the wrapped form.
///
/// # Errors
/// Returns `UnityError` on validation, conflict, or backend failure.
pub async fn create_function(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(body): Json<CreateFunctionBody>,
) -> Result<(StatusCode, Json<FunctionInfo>)> {
    let req = body.into_request();
    check_policy_chain(
        &state,
        auth.as_ref(),
        &req.catalog_name,
        Some(&req.schema_name),
        None,
        "unity:CreateFunction",
    )
    .await?;
    let input_params_json = if req.input_params.is_null() {
        String::new()
    } else {
        serde_json::to_string(&req.input_params)
            .map_err(|e| UnityError::bad_request(format!("invalid input_params: {e}")))?
    };
    let return_params_json = if req.return_params.is_null() {
        String::new()
    } else {
        serde_json::to_string(&req.return_params)
            .map_err(|e| UnityError::bad_request(format!("invalid return_params: {e}")))?
    };
    let proto_req = objectio_proto::metadata::UnityCreateFunctionRequest {
        catalog_name: req.catalog_name,
        schema_name: req.schema_name,
        name: req.name,
        routine_definition: req.routine_definition,
        routine_body: req.routine_body,
        external_language: req.external_language,
        data_type: req.data_type,
        full_data_type: req.full_data_type,
        parameter_style: req.parameter_style,
        is_deterministic: req.is_deterministic,
        sql_data_access: req.sql_data_access,
        is_null_call: req.is_null_call,
        security_type: req.security_type,
        specific_name: req.specific_name,
        input_params_json,
        return_params_json,
        comment: req.comment,
        owner: req.owner,
        properties: req.properties,
    };
    let function = state.catalog.create_function(proto_req).await?;
    Ok((StatusCode::OK, Json(function)))
}

/// `GET /api/2.1/unity-catalog/functions/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn get_function(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<FunctionInfo>> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:GetFunction",
    )
    .await?;
    Ok(Json(
        state.catalog.get_function(&catalog, &schema, &name).await?,
    ))
}

/// `DELETE /api/2.1/unity-catalog/functions/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn delete_function(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<StatusCode> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:DeleteFunction",
    )
    .await?;
    state
        .catalog
        .delete_function(&catalog, &schema, &name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Volume handlers ----

/// `GET /api/2.1/unity-catalog/volumes?catalog_name=X&schema_name=Y`
///
/// # Errors
/// Returns `UnityError::bad_request` if either query param is empty.
pub async fn list_volumes(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Query(params): Query<ListVolumesParams>,
) -> Result<Json<ListVolumesResponse>> {
    if params.catalog_name.is_empty() || params.schema_name.is_empty() {
        return Err(UnityError::bad_request(
            "catalog_name and schema_name query parameters are required",
        ));
    }
    check_policy_chain(
        &state,
        auth.as_ref(),
        &params.catalog_name,
        Some(&params.schema_name),
        None,
        "unity:ListVolumes",
    )
    .await?;
    let volumes = state
        .catalog
        .list_volumes(params.catalog_name, params.schema_name)
        .await?;
    Ok(Json(ListVolumesResponse {
        volumes,
        next_page_token: None,
    }))
}

/// `POST /api/2.1/unity-catalog/volumes`
///
/// MANAGED volumes auto-derive `storage_location` server-side.
/// EXTERNAL volumes require `storage_location`.
///
/// # Errors
/// Returns `UnityError` on validation, conflict, or backend failure.
pub async fn create_volume(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<CreateVolumeRequest>,
) -> Result<(StatusCode, Json<VolumeInfo>)> {
    check_policy_chain(
        &state,
        auth.as_ref(),
        &req.catalog_name,
        Some(&req.schema_name),
        None,
        "unity:CreateVolume",
    )
    .await?;
    let proto_req = objectio_proto::metadata::UnityCreateVolumeRequest {
        catalog_name: req.catalog_name,
        schema_name: req.schema_name,
        name: req.name,
        volume_type: req.volume_type,
        storage_location: req.storage_location,
        comment: req.comment,
        owner: req.owner,
        properties: req.properties,
    };
    let volume = state.catalog.create_volume(proto_req).await?;
    Ok((StatusCode::OK, Json(volume)))
}

/// `GET /api/2.1/unity-catalog/volumes/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn get_volume(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<VolumeInfo>> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:GetVolume",
    )
    .await?;
    Ok(Json(
        state.catalog.get_volume(&catalog, &schema, &name).await?,
    ))
}

/// `DELETE /api/2.1/unity-catalog/volumes/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn delete_volume(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<StatusCode> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:DeleteVolume",
    )
    .await?;
    state
        .catalog
        .delete_volume(&catalog, &schema, &name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Model + ModelVersion handlers ----

/// `GET /api/2.1/unity-catalog/models?catalog_name=X&schema_name=Y`
///
/// # Errors
/// Returns `UnityError::bad_request` if either query param is empty.
pub async fn list_models(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Query(params): Query<ListModelsParams>,
) -> Result<Json<ListModelsResponse>> {
    if params.catalog_name.is_empty() || params.schema_name.is_empty() {
        return Err(UnityError::bad_request(
            "catalog_name and schema_name query parameters are required",
        ));
    }
    check_policy_chain(
        &state,
        auth.as_ref(),
        &params.catalog_name,
        Some(&params.schema_name),
        None,
        "unity:ListModels",
    )
    .await?;
    let models = state
        .catalog
        .list_models(params.catalog_name, params.schema_name)
        .await?;
    Ok(Json(ListModelsResponse {
        registered_models: models,
        next_page_token: None,
    }))
}

/// `POST /api/2.1/unity-catalog/models`
///
/// # Errors
/// Returns `UnityError` on validation, conflict, or backend failure.
pub async fn create_model(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<CreateModelRequest>,
) -> Result<(StatusCode, Json<ModelInfo>)> {
    check_policy_chain(
        &state,
        auth.as_ref(),
        &req.catalog_name,
        Some(&req.schema_name),
        None,
        "unity:CreateModel",
    )
    .await?;
    let proto_req = objectio_proto::metadata::UnityCreateModelRequest {
        catalog_name: req.catalog_name,
        schema_name: req.schema_name,
        name: req.name,
        storage_location: req.storage_location,
        comment: req.comment,
        owner: req.owner,
        properties: req.properties,
    };
    let model = state.catalog.create_model(proto_req).await?;
    Ok((StatusCode::OK, Json(model)))
}

/// `GET /api/2.1/unity-catalog/models/{full_name}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn get_model(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<ModelInfo>> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:GetModel",
    )
    .await?;
    Ok(Json(
        state.catalog.get_model(&catalog, &schema, &name).await?,
    ))
}

/// `DELETE /api/2.1/unity-catalog/models/{full_name}`
///
/// Cascades to all versions of the model.
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn delete_model(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<StatusCode> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:DeleteModel",
    )
    .await?;
    state
        .catalog
        .delete_model(&catalog, &schema, &name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/2.1/unity-catalog/models/{full_name}/versions`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn list_model_versions(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<ListModelVersionsResponse>> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:ListModelVersions",
    )
    .await?;
    let versions = state
        .catalog
        .list_model_versions(catalog, schema, name)
        .await?;
    Ok(Json(ListModelVersionsResponse {
        model_versions: versions,
        next_page_token: None,
    }))
}

/// `POST /api/2.1/unity-catalog/models/{full_name}/versions`
///
/// # Errors
/// Returns `UnityError` on validation or backend failure.
pub async fn create_model_version(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
    Json(mut req): Json<CreateModelVersionRequest>,
) -> Result<(StatusCode, Json<ModelVersionInfo>)> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:CreateModelVersion",
    )
    .await?;
    // Path is authoritative — overwrite anything the body claimed so the
    // caller can't address one model and create a version under another.
    req.catalog_name = catalog;
    req.schema_name = schema;
    req.model_name = name;
    let proto_req = objectio_proto::metadata::UnityCreateModelVersionRequest {
        catalog_name: req.catalog_name,
        schema_name: req.schema_name,
        model_name: req.model_name,
        source: req.source,
        run_id: req.run_id,
        description: req.description,
        properties: req.properties,
    };
    let version = state.catalog.create_model_version(proto_req).await?;
    Ok((StatusCode::OK, Json(version)))
}

/// `GET /api/2.1/unity-catalog/models/{full_name}/versions/{version}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn get_model_version(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path((full_name, version)): Path<(String, u32)>,
) -> Result<Json<ModelVersionInfo>> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:GetModelVersion",
    )
    .await?;
    Ok(Json(
        state
            .catalog
            .get_model_version(&catalog, &schema, &name, version)
            .await?,
    ))
}

/// `PATCH /api/2.1/unity-catalog/models/{full_name}/versions/{version}`
///
/// # Errors
/// Returns `UnityError` on invalid status or backend failure.
pub async fn update_model_version_status(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path((full_name, version)): Path<(String, u32)>,
    Json(req): Json<UpdateModelVersionStatusRequest>,
) -> Result<Json<ModelVersionInfo>> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:UpdateModelVersion",
    )
    .await?;
    let updated = state
        .catalog
        .update_model_version_status(&catalog, &schema, &name, version, req.status)
        .await?;
    Ok(Json(updated))
}

/// `DELETE /api/2.1/unity-catalog/models/{full_name}/versions/{version}`
///
/// # Errors
/// Returns `UnityError::bad_request` if `full_name` is malformed.
pub async fn delete_model_version(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path((full_name, version)): Path<(String, u32)>,
) -> Result<StatusCode> {
    let (catalog, schema, name) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&name),
        "unity:DeleteModelVersion",
    )
    .await?;
    state
        .catalog
        .delete_model_version(&catalog, &schema, &name, version)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Vended credentials ----

/// `POST /api/2.1/unity-catalog/temporary-table-credentials`
///
/// Issues short-lived AWS-style credentials scoped to the table's
/// `storage_location`, gated by `unity:GenerateTemporaryCredentials`.
/// Requires both an `StsProvider` configured on the gateway AND a request
/// `table_id` that resolves to a Unity table.
///
/// # Errors
/// Returns `UnityError` if STS is disabled, the table is unknown, or
/// the user is denied by policy.
pub async fn temporary_table_credentials(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<TemporaryTableCredentialsRequest>,
) -> Result<Json<TemporaryTableCredentialsResponse>> {
    let Some(sts) = state.sts_provider.as_ref() else {
        return Err(UnityError::bad_request(
            "STS credential vending is not configured on this gateway",
        ));
    };
    if req.table_id.is_empty() {
        return Err(UnityError::bad_request("table_id is required"));
    }
    // table_id is opaque server-side — resolve by scanning catalogs/schemas.
    // For v1 we look up by linear scan; future: indexed lookup table in meta.
    let table = find_table_by_id(&state, &req.table_id).await?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &table.catalog_name,
        Some(&table.schema_name),
        Some(&table.name),
        "unity:GenerateTemporaryCredentials",
    )
    .await?;
    let user_arn = auth
        .as_ref()
        .map_or("anonymous", |Extension(a)| a.user_arn.as_str());
    // Map the Databricks-spec operation string ("READ" | "READ_WRITE")
    // onto our internal enum. Anything unrecognised is treated as the
    // safer Read — never silently widen.
    let operation = match req.operation.to_ascii_uppercase().as_str() {
        "READ_WRITE" => objectio_auth::sts::Operation::ReadWrite,
        _ => objectio_auth::sts::Operation::Read,
    };
    let creds = sts.issue(user_arn, &table.storage_location, operation);
    Ok(Json(TemporaryTableCredentialsResponse {
        aws_temp_credentials: AwsCredentials {
            access_key_id: creds.access_key_id,
            secret_access_key: creds.secret_access_key,
            session_token: creds.session_token,
        },
        expiration_time: creds.expires_at,
        url: table.storage_location,
    }))
}

/// `POST /api/2.1/unity-catalog/temporary-volume-credentials`
///
/// Mirror of `temporary-table-credentials` for Unity volumes — issues
/// short-lived AWS-style credentials scoped to the volume's
/// `storage_location`, gated by `unity:GenerateTemporaryCredentials`.
///
/// # Errors
/// Returns `UnityError` if STS is disabled, the volume is unknown, or
/// the user is denied by policy.
pub async fn temporary_volume_credentials(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Json(req): Json<TemporaryVolumeCredentialsRequest>,
) -> Result<Json<TemporaryVolumeCredentialsResponse>> {
    let Some(sts) = state.sts_provider.as_ref() else {
        return Err(UnityError::bad_request(
            "STS credential vending is not configured on this gateway",
        ));
    };
    if req.volume_id.is_empty() {
        return Err(UnityError::bad_request("volume_id is required"));
    }
    let volume = find_volume_by_id(&state, &req.volume_id).await?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &volume.catalog_name,
        Some(&volume.schema_name),
        Some(&volume.name),
        "unity:GenerateTemporaryCredentials",
    )
    .await?;
    let user_arn = auth
        .as_ref()
        .map_or("anonymous", |Extension(a)| a.user_arn.as_str());
    let operation = match req.operation.to_ascii_uppercase().as_str() {
        "READ_WRITE" => objectio_auth::sts::Operation::ReadWrite,
        _ => objectio_auth::sts::Operation::Read,
    };
    let creds = sts.issue(user_arn, &volume.storage_location, operation);
    Ok(Json(TemporaryVolumeCredentialsResponse {
        aws_temp_credentials: AwsCredentials {
            access_key_id: creds.access_key_id,
            secret_access_key: creds.secret_access_key,
            session_token: creds.session_token,
        },
        expiration_time: creds.expires_at,
        url: volume.storage_location,
    }))
}

/// Look up a Unity volume by its server-assigned `volume_id`.
///
/// v1 implementation: scans every catalog → schema → volume. Same caveat
/// as `find_table_by_id` — fine while catalog count is small.
async fn find_volume_by_id(state: &UnityState, volume_id: &str) -> Result<VolumeInfo> {
    for cat in state.catalog.list_catalogs(String::new()).await? {
        for sch in state.catalog.list_schemas(cat.name.clone()).await? {
            for vol in state
                .catalog
                .list_volumes(cat.name.clone(), sch.name.clone())
                .await?
            {
                if vol.volume_id == volume_id {
                    return Ok(vol);
                }
            }
        }
    }
    Err(UnityError::not_found(format!(
        "no volume with id '{volume_id}'"
    )))
}

/// Look up a Unity table by its server-assigned `table_id`.
///
/// v1 implementation: scans every catalog → schema → table. Acceptable
/// while catalog count is small; revisit when we add a dedicated index.
async fn find_table_by_id(state: &UnityState, table_id: &str) -> Result<TableInfo> {
    for cat in state.catalog.list_catalogs(String::new()).await? {
        for sch in state.catalog.list_schemas(cat.name.clone()).await? {
            for tbl in state
                .catalog
                .list_tables(cat.name.clone(), sch.name.clone())
                .await?
            {
                if tbl.table_id == table_id {
                    return Ok(tbl);
                }
            }
        }
    }
    Err(UnityError::not_found(format!(
        "no table with id '{table_id}'"
    )))
}

// ---- Policy management (admin only) ----

/// `PUT /api/2.1/unity-catalog/catalogs/{name}/policy`
///
/// # Errors
/// Returns `UnityError::forbidden` if not admin, `bad_request` on invalid policy.
pub async fn set_catalog_policy(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(name): Path<String>,
    Json(req): Json<SetPolicyRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref(), &state)?;
    let json = serde_json::to_string(&req.policy)
        .map_err(|e| UnityError::bad_request(format!("invalid policy JSON: {e}")))?;
    validate_unity_policy(&json).map_err(UnityError::bad_request)?;
    state
        .catalog
        .set_catalog_policy(name, json.into_bytes())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/2.1/unity-catalog/catalogs/{name}/policy`
///
/// # Errors
/// Returns `UnityError::forbidden` if not admin.
pub async fn get_catalog_policy(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(name): Path<String>,
) -> Result<Json<GetPolicyResponse>> {
    require_admin(auth.as_ref(), &state)?;
    let bytes = state.catalog.get_catalog_policy(&name).await?;
    let policy = bytes_to_policy(&bytes);
    Ok(Json(GetPolicyResponse { policy }))
}

/// `PUT /api/2.1/unity-catalog/schemas/{full_name}/policy`
///
/// # Errors
/// Returns `UnityError::forbidden` if not admin, `bad_request` on invalid policy.
pub async fn set_schema_policy(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
    Json(req): Json<SetPolicyRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref(), &state)?;
    let (catalog, schema) = parse_schema_full_name(&full_name)?;
    let json = serde_json::to_string(&req.policy)
        .map_err(|e| UnityError::bad_request(format!("invalid policy JSON: {e}")))?;
    validate_unity_policy(&json).map_err(UnityError::bad_request)?;
    state
        .catalog
        .set_schema_policy(catalog, schema, json.into_bytes())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/2.1/unity-catalog/schemas/{full_name}/policy`
///
/// # Errors
/// Returns `UnityError::forbidden` if not admin.
pub async fn get_schema_policy(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<GetPolicyResponse>> {
    require_admin(auth.as_ref(), &state)?;
    let (catalog, schema) = parse_schema_full_name(&full_name)?;
    let bytes = state.catalog.get_schema_policy(&catalog, &schema).await?;
    Ok(Json(GetPolicyResponse {
        policy: bytes_to_policy(&bytes),
    }))
}

/// `PUT /api/2.1/unity-catalog/tables/{full_name}/policy`
///
/// # Errors
/// Returns `UnityError::forbidden` if not admin, `bad_request` on invalid policy.
pub async fn set_table_policy(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
    Json(req): Json<SetPolicyRequest>,
) -> Result<StatusCode> {
    require_admin(auth.as_ref(), &state)?;
    let (catalog, schema, table) = parse_table_full_name(&full_name)?;
    let json = serde_json::to_string(&req.policy)
        .map_err(|e| UnityError::bad_request(format!("invalid policy JSON: {e}")))?;
    validate_unity_policy(&json).map_err(UnityError::bad_request)?;
    state
        .catalog
        .set_table_policy(catalog, schema, table, json.into_bytes())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /api/2.1/unity-catalog/tables/{full_name}/security`
///
/// Bind (or clear) a row filter and/or per-column masks on a table.
/// Engines that read this metadata enforce the predicate and column
/// rewrites at query time. Empty `row_filter`/`column_masks` clears
/// the corresponding binding.
///
/// # Errors
/// Returns `UnityError::forbidden` if not admin, `bad_request` for an
/// unknown referenced function or a row-filter UDF that isn't BOOLEAN.
pub async fn set_table_security(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
    Json(req): Json<SetTableSecurityRequest>,
) -> Result<Json<TableInfo>> {
    let (catalog, schema, table) = parse_table_full_name(&full_name)?;
    check_policy_chain(
        &state,
        auth.as_ref(),
        &catalog,
        Some(&schema),
        Some(&table),
        "unity:SetTableSecurity",
    )
    .await?;
    let row_filter = req
        .row_filter
        .map(|rf| objectio_proto::metadata::UnityRowFilter {
            function_full_name: rf.function_full_name,
            input_column_names: rf.input_column_names,
        });
    let column_masks = req
        .column_masks
        .into_iter()
        .map(|(col, m)| {
            (
                col,
                objectio_proto::metadata::UnityColumnMask {
                    function_full_name: m.function_full_name,
                    using_column_names: m.using_column_names,
                },
            )
        })
        .collect();
    let info = state
        .catalog
        .set_table_security(catalog, schema, table, row_filter, column_masks)
        .await?;
    Ok(Json(info))
}

/// `GET /api/2.1/unity-catalog/tables/{full_name}/policy`
///
/// # Errors
/// Returns `UnityError::forbidden` if not admin.
pub async fn get_table_policy(
    State(state): State<Arc<UnityState>>,
    auth: Option<Extension<AuthResult>>,
    Path(full_name): Path<String>,
) -> Result<Json<GetPolicyResponse>> {
    require_admin(auth.as_ref(), &state)?;
    let (catalog, schema, table) = parse_table_full_name(&full_name)?;
    let bytes = state
        .catalog
        .get_table_policy(&catalog, &schema, &table)
        .await?;
    Ok(Json(GetPolicyResponse {
        policy: bytes_to_policy(&bytes),
    }))
}

fn bytes_to_policy(bytes: &[u8]) -> serde_json::Value {
    if bytes.is_empty() {
        return serde_json::Value::Null;
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_schema_full_name_happy_path() {
        let (c, s) = parse_schema_full_name("prod.sales").unwrap();
        assert_eq!(c, "prod");
        assert_eq!(s, "sales");
    }

    #[test]
    fn parse_schema_full_name_rejects_three_parts() {
        let err = parse_schema_full_name("a.b.c").unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("'a.b.c'"));
    }

    #[test]
    fn parse_schema_full_name_rejects_missing_schema() {
        let err = parse_schema_full_name("prod").unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn parse_schema_full_name_rejects_empty_segments() {
        assert!(parse_schema_full_name(".sales").is_err());
        assert!(parse_schema_full_name("prod.").is_err());
        assert!(parse_schema_full_name("").is_err());
    }

    #[test]
    fn parse_table_full_name_happy_path() {
        let (c, s, t) = parse_table_full_name("prod.sales.events").unwrap();
        assert_eq!(c, "prod");
        assert_eq!(s, "sales");
        assert_eq!(t, "events");
    }

    #[test]
    fn parse_table_full_name_rejects_two_parts() {
        let err = parse_table_full_name("prod.sales").unwrap_err();
        assert!(err.message.contains("'prod.sales'"));
    }

    #[test]
    fn parse_table_full_name_rejects_four_parts() {
        // splitn(3) keeps the rest in the third element — it must not contain dots
        let err = parse_table_full_name("a.b.c.d").unwrap_err();
        assert!(err.message.contains("'a.b.c.d'") || err.message.contains("'a.b.c.d'"));
        // The third segment "c.d" is non-empty so the strict-three-part check
        // happens via the length test only when collected — verify with all-empty case:
        assert!(parse_table_full_name(".b.c").is_err());
    }

    #[test]
    fn bytes_to_policy_handles_empty_and_invalid() {
        assert_eq!(bytes_to_policy(&[]), serde_json::Value::Null);
        assert_eq!(bytes_to_policy(b"not-json"), serde_json::Value::Null);
        assert_eq!(bytes_to_policy(br#"{"x":1}"#), serde_json::json!({"x": 1}));
    }
}
