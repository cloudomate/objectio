//! Axum handlers for the Delta Sharing REST protocol.
//!
//! Protocol: <https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md>
//!
//! All routes are authenticated via bearer tokens (not `SigV4`).
//! The `authenticate_request` helper validates the token against the Meta service
//! and checks that the recipient has access to the requested share.

use crate::access::authenticate_request;
use crate::catalog::DeltaCatalog;
use crate::error::DeltaError;
use crate::types::{
    AddTableRequest, CreateRecipientRequest, CreateRecipientResponse, CreateShareRequest,
    FileEntry, FileLine, Format, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
    MetadataLine, Protocol, ProtocolLine, Schema, Share, Table, TableMetadata,
    TableVersionResponse,
};
use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use objectio_auth::presign::presign_get;
use objectio_proto::metadata::{
    IcebergLoadTableRequest, metadata_service_client::MetadataServiceClient,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{error, info};

type Result<T> = std::result::Result<T, DeltaError>;

/// Shared state for Delta Sharing handlers.
pub struct DeltaState {
    pub catalog: DeltaCatalog,
    /// Meta client for Iceberg table lookups
    pub meta_client: MetadataServiceClient<Channel>,
    /// Gateway public endpoint (e.g. `http://localhost:9000`)
    pub endpoint: String,
    /// AWS region for presigned URL credential scope
    pub region: String,
    /// Admin access key ID for presigning
    pub access_key_id: String,
    /// Admin secret access key for presigning
    pub secret_access_key: String,
}

impl DeltaState {
    fn presign(&self, bucket: &str, key: &str, expires_in: Duration) -> String {
        presign_get(
            &self.endpoint,
            &self.region,
            &self.access_key_id,
            &self.secret_access_key,
            bucket,
            key,
            expires_in,
        )
    }

    /// Load an Iceberg table's metadata JSON.
    async fn load_iceberg_table_meta(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_load_table(IcebergLoadTableRequest {
                namespace_levels: vec![namespace.to_string()],
                table_name: table_name.to_string(),
            })
            .await
            .map_err(DeltaError::from)?;

        let bytes = resp.into_inner().metadata_json;
        if bytes.is_empty() {
            return Ok(None);
        }
        let val: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| DeltaError::internal(format!("invalid iceberg metadata: {e}")))?;
        Ok(Some(val))
    }
}

// ---- Delta Sharing protocol endpoints ----

/// GET /delta-sharing/v1/shares
///
/// # Errors
/// Returns `DeltaError` if authentication fails or the catalog call fails.
pub async fn list_shares(
    State(state): State<Arc<DeltaState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let ctx = authenticate_request(&headers, &state.catalog, None).await?;

    let all_shares = state.catalog.list_shares().await?;

    let items: Vec<Share> = all_shares
        .into_iter()
        .filter(|s| ctx.has_share(&s.name))
        .map(|s| Share {
            name: s.name.clone(),
            id: Some(s.name),
        })
        .collect();

    Ok(Json(ListSharesResponse {
        items,
        next_page_token: None,
    }))
}

/// GET /delta-sharing/v1/shares/{share}/schemas
///
/// # Errors
/// Returns `DeltaError` if authentication fails or the catalog call fails.
pub async fn list_schemas(
    State(state): State<Arc<DeltaState>>,
    Path(share): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    authenticate_request(&headers, &state.catalog, Some(&share)).await?;

    let tables = state.catalog.list_all_tables_in_share(&share).await?;

    // Deduplicate schemas
    let mut seen = std::collections::HashSet::new();
    let items: Vec<Schema> = tables
        .into_iter()
        .filter(|t| seen.insert(t.schema.clone()))
        .map(|t| Schema {
            name: t.schema,
            share: share.clone(),
        })
        .collect();

    Ok(Json(ListSchemasResponse {
        items,
        next_page_token: None,
    }))
}

/// GET /delta-sharing/v1/shares/{share}/schemas/{schema}/tables
///
/// # Errors
/// Returns `DeltaError` if authentication fails or the catalog call fails.
pub async fn list_tables(
    State(state): State<Arc<DeltaState>>,
    Path((share, schema)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    authenticate_request(&headers, &state.catalog, Some(&share)).await?;

    let tables = state.catalog.list_tables(&share, &schema).await?;
    let items: Vec<Table> = tables
        .into_iter()
        .map(|t| Table {
            name: t.table_name.clone(),
            schema: t.schema,
            share: t.share,
            share_id: if t.share_id.is_empty() {
                None
            } else {
                Some(t.share_id)
            },
            id: Some(t.table_name),
        })
        .collect();

    Ok(Json(ListTablesResponse {
        items,
        next_page_token: None,
    }))
}

/// GET /delta-sharing/v1/shares/{share}/all-tables
///
/// # Errors
/// Returns `DeltaError` if authentication fails or the catalog call fails.
pub async fn list_all_tables(
    State(state): State<Arc<DeltaState>>,
    Path(share): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    authenticate_request(&headers, &state.catalog, Some(&share)).await?;

    let tables = state.catalog.list_all_tables_in_share(&share).await?;
    let items: Vec<Table> = tables
        .into_iter()
        .map(|t| Table {
            name: t.table_name.clone(),
            schema: t.schema,
            share: t.share,
            share_id: if t.share_id.is_empty() {
                None
            } else {
                Some(t.share_id)
            },
            id: Some(t.table_name),
        })
        .collect();

    Ok(Json(ListTablesResponse {
        items,
        next_page_token: None,
    }))
}

/// GET /delta-sharing/v1/shares/{share}/schemas/{schema}/tables/{table}/version
///
/// # Errors
/// Returns `DeltaError` if authentication fails, the table is not found, or the catalog call fails.
pub async fn get_table_version(
    State(state): State<Arc<DeltaState>>,
    Path((share, schema, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    authenticate_request(&headers, &state.catalog, Some(&share)).await?;

    // Verify the table is in this share
    validate_table_in_share(&state, &share, &schema, &table).await?;

    let version = get_snapshot_id(&state, &schema, &table).await?;
    Ok(Json(TableVersionResponse {
        delta_table_version: version,
    }))
}

/// GET /delta-sharing/v1/shares/{share}/schemas/{schema}/tables/{table}/metadata
///
/// # Errors
/// Returns `DeltaError` if authentication fails, the table is not found, or the metadata is missing.
pub async fn get_table_metadata(
    State(state): State<Arc<DeltaState>>,
    Path((share, schema, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    authenticate_request(&headers, &state.catalog, Some(&share)).await?;
    validate_table_in_share(&state, &share, &schema, &table).await?;

    let meta = state
        .load_iceberg_table_meta(&schema, &table)
        .await?
        .ok_or_else(|| DeltaError::not_found(format!("table {schema}.{table} has no metadata")))?;

    let (protocol_line, metadata_line) = build_metadata_lines(&table, &meta);

    // Delta Sharing metadata response: NDJSON with protocol + metadata lines
    let body = format!(
        "{}\n{}\n",
        serde_json::to_string(&protocol_line).unwrap_or_default(),
        serde_json::to_string(&metadata_line).unwrap_or_default(),
    );

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
        body,
    ))
}

/// POST /delta-sharing/v1/shares/{share}/schemas/{schema}/tables/{table}/query
///
/// # Errors
/// Returns `DeltaError` if authentication fails, the table is not found, or the metadata is missing.
pub async fn query_table(
    State(state): State<Arc<DeltaState>>,
    Path((share, schema, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    authenticate_request(&headers, &state.catalog, Some(&share)).await?;
    validate_table_in_share(&state, &share, &schema, &table).await?;

    let meta = state
        .load_iceberg_table_meta(&schema, &table)
        .await?
        .ok_or_else(|| DeltaError::not_found(format!("table {schema}.{table} has no metadata")))?;

    let (protocol_line, metadata_line) = build_metadata_lines(&table, &meta);

    // Extract Parquet file paths from the current snapshot
    let file_lines = extract_file_lines(&state, &schema, &meta);

    let mut body = String::new();
    body.push_str(&serde_json::to_string(&protocol_line).unwrap_or_default());
    body.push('\n');
    body.push_str(&serde_json::to_string(&metadata_line).unwrap_or_default());
    body.push('\n');
    for file_line in file_lines {
        body.push_str(&serde_json::to_string(&file_line).unwrap_or_default());
        body.push('\n');
    }

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
        body,
    ))
}

// ---- Admin endpoints ----

/// POST /_admin/delta-sharing/shares
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_create_share(
    State(state): State<Arc<DeltaState>>,
    Json(body): Json<CreateShareRequest>,
) -> Result<impl IntoResponse> {
    let share = state
        .catalog
        .create_share(&body.name, &body.comment)
        .await?;
    info!("Created delta share '{}'", share.name);
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "name": share.name, "comment": share.comment })),
    ))
}

/// POST /_admin/delta-sharing/shares/{share}/tables
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_add_table(
    State(state): State<Arc<DeltaState>>,
    Path(share): Path<String>,
    Json(body): Json<AddTableRequest>,
) -> Result<impl IntoResponse> {
    let table = state
        .catalog
        .add_table(&share, &body.schema, &body.name)
        .await?;
    info!(
        "Added table '{}.{}' to delta share '{}'",
        table.schema, table.table_name, share
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "share": table.share,
            "schema": table.schema,
            "name": table.table_name,
        })),
    ))
}

/// DELETE /_admin/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_remove_table(
    State(state): State<Arc<DeltaState>>,
    Path((share, schema, table)): Path<(String, String, String)>,
) -> Result<impl IntoResponse> {
    state.catalog.remove_table(&share, &schema, &table).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /_admin/delta-sharing/shares/{share}
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_drop_share(
    State(state): State<Arc<DeltaState>>,
    Path(share): Path<String>,
) -> Result<impl IntoResponse> {
    state.catalog.drop_share(&share).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /_admin/delta-sharing/recipients
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_create_recipient(
    State(state): State<Arc<DeltaState>>,
    Json(body): Json<CreateRecipientRequest>,
) -> Result<impl IntoResponse> {
    let (recipient, raw_token) = state
        .catalog
        .create_recipient(&body.name, body.shares.clone())
        .await?;
    info!("Created delta recipient '{}'", recipient.name);
    Ok((
        StatusCode::CREATED,
        Json(CreateRecipientResponse {
            name: recipient.name,
            shares: recipient.shares,
            bearer_token: raw_token,
        }),
    ))
}

/// DELETE /_admin/delta-sharing/recipients/{name}
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_drop_recipient(
    State(state): State<Arc<DeltaState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse> {
    state.catalog.drop_recipient(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Helpers ----

/// Verify that `table` exists in `share/schema`.
async fn validate_table_in_share(
    state: &DeltaState,
    share: &str,
    schema: &str,
    table: &str,
) -> Result<()> {
    let tables = state.catalog.list_tables(share, schema).await?;
    let found = tables
        .iter()
        .any(|t| t.table_name == table && t.schema == schema);
    if found {
        Ok(())
    } else {
        Err(DeltaError::not_found(format!(
            "table '{schema}.{table}' not found in share '{share}'"
        )))
    }
}

/// Get the current snapshot ID from Iceberg table metadata.
async fn get_snapshot_id(state: &DeltaState, schema: &str, table: &str) -> Result<i64> {
    let meta = state
        .load_iceberg_table_meta(schema, table)
        .await?
        .ok_or_else(|| DeltaError::not_found(format!("table {schema}.{table} has no metadata")))?;

    Ok(meta
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0))
}

/// Build NDJSON protocol + metadata lines from Iceberg table metadata JSON.
fn build_metadata_lines(
    table_name: &str,
    meta: &serde_json::Value,
) -> (ProtocolLine, MetadataLine) {
    let schema_string =
        meta.get("schema")
            .or_else(|| {
                // Iceberg v2: schemas array + current-schema-id
                let schema_id = meta.get("current-schema-id")?.as_i64()?;
                meta.get("schemas")?.as_array()?.iter().find(|s| {
                    s.get("schema-id").and_then(serde_json::Value::as_i64) == Some(schema_id)
                })
            })
            .map_or_else(
                || "{}".to_string(),
                |s| serde_json::to_string(s).unwrap_or_default(),
            );

    let partition_columns: Vec<String> = meta
        .get("partition-specs")
        .and_then(serde_json::Value::as_array)
        .and_then(|arr| arr.first())
        .and_then(|spec| spec.get("fields"))
        .and_then(serde_json::Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|f| {
                    f.get("name")
                        .and_then(serde_json::Value::as_str)
                        .map(std::string::ToString::to_string)
                })
                .collect()
        })
        .unwrap_or_default();

    let table_id = meta
        .get("table-uuid")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(table_name)
        .to_string();

    let created_time = meta
        .get("last-updated-ms")
        .and_then(serde_json::Value::as_i64);

    let protocol = ProtocolLine {
        protocol: Protocol {
            min_reader_version: 1,
        },
    };
    let metadata = MetadataLine {
        metadata: TableMetadata {
            id: table_id,
            format: Format {
                provider: "parquet".to_string(),
            },
            schema_string,
            partition_columns,
            configuration: None,
            created_time,
            name: Some(table_name.to_string()),
            description: None,
            num_files: None,
            size: None,
            num_records: None,
        },
    };

    (protocol, metadata)
}

/// Extract file entries from Iceberg metadata for the current snapshot.
///
/// Phase 1: Returns presigned URLs for data files referenced in the snapshot summary.
/// Full manifest/Avro reading is not implemented; this is a best-effort stub that
/// returns an empty file list when manifests are not readable inline.
fn extract_file_lines(
    state: &DeltaState,
    _schema: &str,
    meta: &serde_json::Value,
) -> Vec<FileLine> {
    // Get current snapshot
    let current_snapshot_id = match meta
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
    {
        Some(id) if id > 0 => id,
        _ => return vec![],
    };

    let Some(snapshots) = meta.get("snapshots").and_then(serde_json::Value::as_array) else {
        return vec![];
    };

    let Some(snapshot) = snapshots.iter().find(|s| {
        s.get("snapshot-id").and_then(serde_json::Value::as_i64) == Some(current_snapshot_id)
    }) else {
        return vec![];
    };

    // The manifest-list is an Avro file — we can return a presigned URL for it.
    // Full clients will read manifests themselves; we return a presigned manifest-list URL
    // using the `added-data-files` summary count if available.
    let Some(manifest_list) = snapshot
        .get("manifest-list")
        .and_then(serde_json::Value::as_str)
    else {
        return vec![];
    };

    // Parse S3 URL: s3://bucket/key
    let Some(without_scheme) = manifest_list.strip_prefix("s3://") else {
        error!("unexpected manifest-list scheme: {}", manifest_list);
        return vec![];
    };
    let Some((bucket, key)) = without_scheme.split_once('/') else {
        return vec![];
    };

    let size = snapshot
        .get("summary")
        .and_then(|s| s.get("total-files-size"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    let presigned_url = state.presign(bucket, key, Duration::from_secs(3600));
    let file_id = format!("manifest-list-{current_snapshot_id}");

    vec![FileLine {
        file: FileEntry {
            url: presigned_url,
            id: file_id,
            partition_values: std::collections::HashMap::default(),
            size,
            stats: None,
        },
    }]
}
