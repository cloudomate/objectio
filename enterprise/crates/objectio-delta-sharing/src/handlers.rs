//! Axum handlers for the Delta Sharing REST protocol.
//!
//! Protocol: <https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md>
//!
//! All routes are authenticated via bearer tokens (not `SigV4`).
//! The `authenticate_request` helper validates the token against the Meta service
//! and checks that the recipient has access to the requested share.

use crate::access::authenticate_request;
use crate::catalog::DeltaCatalog;
use crate::delta_log::{self, DeltaSnapshot};
use crate::error::DeltaError;
use crate::presigned_reader::{DEFAULT_LOG_URL_TTL, PresignedHttpReader};
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
    DeltaShareTableEntry, IcebergLoadTableRequest, metadata_service_client::MetadataServiceClient,
};
use sha2::{Digest, Sha256};
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
    /// HTTP client used to fetch `_delta_log/` files via presigned URLs
    pub http: reqwest::Client,
    /// Default lifetime of presigned data-file URLs returned in /query responses.
    /// Operators can tune this for long-running Spark/Databricks queries.
    pub default_url_ttl: Duration,
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
                warehouse: String::new(),
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

    let all_shares = state.catalog.list_shares_filtered("").await?;

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

    let entry = find_share_table(&state, &share, &schema, &table).await?;

    let version = match table_kind(&entry)? {
        TableKind::Delta => {
            let snapshot = build_delta_snapshot(&state, &entry).await?;
            snapshot.version
        }
        TableKind::Iceberg => get_snapshot_id(&state, &schema, &table).await?,
    };

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
    let entry = find_share_table(&state, &share, &schema, &table).await?;

    let (protocol_line, metadata_line) = match table_kind(&entry)? {
        TableKind::Delta => {
            let snapshot = build_delta_snapshot(&state, &entry).await?;
            build_delta_metadata_lines(&table, &snapshot)
        }
        TableKind::Iceberg => {
            let meta = state
                .load_iceberg_table_meta(&schema, &table)
                .await?
                .ok_or_else(|| {
                    DeltaError::not_found(format!("table {schema}.{table} has no metadata"))
                })?;
            build_metadata_lines(&table, &meta)
        }
    };

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
    let entry = find_share_table(&state, &share, &schema, &table).await?;

    let (protocol_line, metadata_line, file_lines) = match table_kind(&entry)? {
        TableKind::Delta => {
            let snapshot = build_delta_snapshot(&state, &entry).await?;
            let (p, m) = build_delta_metadata_lines(&table, &snapshot);
            let files = build_delta_file_lines(&state, &entry, &snapshot);
            (p, m, files)
        }
        TableKind::Iceberg => {
            let meta = state
                .load_iceberg_table_meta(&schema, &table)
                .await?
                .ok_or_else(|| {
                    DeltaError::not_found(format!("table {schema}.{table} has no metadata"))
                })?;
            let (p, m) = build_metadata_lines(&table, &meta);
            let files = extract_file_lines(&state, &schema, &meta);
            (p, m, files)
        }
    };

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

/// GET /_admin/delta-sharing/shares
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_list_shares(
    State(state): State<Arc<DeltaState>>,
) -> Result<Json<serde_json::Value>> {
    let shares = state.catalog.list_shares_filtered("").await?;
    let list: Vec<serde_json::Value> = shares
        .iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "comment": s.comment,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "shares": list })))
}

/// GET /_admin/delta-sharing/shares/{share}/tables
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_list_share_tables(
    State(state): State<Arc<DeltaState>>,
    Path(share): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let tables = state.catalog.list_all_tables_in_share(&share).await?;
    let list: Vec<serde_json::Value> = tables
        .iter()
        .map(|t| {
            serde_json::json!({
                "share": t.share,
                "schema": t.schema,
                "name": t.table_name,
                "table_type": t.table_type,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "tables": list })))
}

/// GET /_admin/delta-sharing/recipients
///
/// # Errors
/// Returns `DeltaError` if the catalog call fails.
pub async fn admin_list_recipients(
    State(state): State<Arc<DeltaState>>,
) -> Result<Json<serde_json::Value>> {
    let recipients = state.catalog.list_recipients().await?;
    let list: Vec<serde_json::Value> = recipients
        .iter()
        .map(|r| {
            serde_json::json!({
                "name": r.name,
                "shares": r.shares,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "recipients": list })))
}

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
        .create_share(&body.name, &body.comment, &body.tenant)
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
    use objectio_proto::metadata::DeltaAddTableRequest;
    let table = state
        .catalog
        .add_table(DeltaAddTableRequest {
            share: share.clone(),
            schema: body.schema.clone(),
            table_name: body.name.clone(),
            table_type: body.table_type.clone(),
            bucket: body.bucket.clone(),
            path: body.path.clone(),
            warehouse: body.warehouse.clone(),
            namespace: body.namespace.clone(),
        })
        .await?;
    info!(
        "Added table '{}.{}' (type={}) to share '{}'",
        table.schema,
        table.table_name,
        if table.table_type.is_empty() {
            "iceberg"
        } else {
            &table.table_type
        },
        share
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "share": table.share,
            "schema": table.schema,
            "name": table.table_name,
            "table_type": table.table_type,
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

/// Find the share table entry for `share/schema/table`, or 404.
async fn find_share_table(
    state: &DeltaState,
    share: &str,
    schema: &str,
    table: &str,
) -> Result<DeltaShareTableEntry> {
    let tables = state.catalog.list_tables(share, schema).await?;
    tables
        .into_iter()
        .find(|t| t.table_name == table && t.schema == schema)
        .ok_or_else(|| {
            DeltaError::not_found(format!(
                "table '{schema}.{table}' not found in share '{share}'"
            ))
        })
}

/// Backing-table dispatch kind. Determined by `entry.table_type`.
#[derive(Debug)]
enum TableKind {
    /// Native Delta Lake table (`_delta_log/` JSON commits in `bucket`/`path`)
    Delta,
    /// Iceberg table loaded via the warehouse meta service
    Iceberg,
}

fn table_kind(entry: &DeltaShareTableEntry) -> Result<TableKind> {
    match entry.table_type.as_str() {
        "delta" => Ok(TableKind::Delta),
        "" | "iceberg" | "uniform" => Ok(TableKind::Iceberg),
        other => Err(DeltaError::bad_request(format!(
            "unknown share table_type: '{other}'"
        ))),
    }
}

/// Build a Delta snapshot for a delta-typed share table entry by reading
/// `_delta_log/` from `entry.bucket` + `entry.path` via presigned URLs.
async fn build_delta_snapshot(
    state: &DeltaState,
    entry: &DeltaShareTableEntry,
) -> Result<DeltaSnapshot> {
    if entry.bucket.is_empty() {
        return Err(DeltaError::bad_request(format!(
            "share table '{}.{}' has table_type=delta but no bucket configured",
            entry.schema, entry.table_name,
        )));
    }
    let reader = PresignedHttpReader::new(
        &state.endpoint,
        &state.region,
        &state.access_key_id,
        &state.secret_access_key,
        DEFAULT_LOG_URL_TTL,
        &state.http,
    );
    delta_log::build_snapshot(&reader, &entry.bucket, &entry.path).await
}

/// Build Protocol + Metadata NDJSON lines from a Delta snapshot.
fn build_delta_metadata_lines(
    table_name: &str,
    snapshot: &DeltaSnapshot,
) -> (ProtocolLine, MetadataLine) {
    let configuration = if snapshot.metadata.configuration.is_empty() {
        None
    } else {
        Some(snapshot.metadata.configuration.clone())
    };
    let provider = snapshot
        .metadata
        .format
        .as_ref()
        .map_or_else(|| "parquet".to_string(), |f| f.provider.clone());

    let protocol = ProtocolLine {
        protocol: Protocol {
            min_reader_version: snapshot.protocol.min_reader_version,
        },
    };
    let metadata = MetadataLine {
        metadata: TableMetadata {
            id: if snapshot.metadata.id.is_empty() {
                table_name.to_string()
            } else {
                snapshot.metadata.id.clone()
            },
            format: Format { provider },
            schema_string: snapshot.metadata.schema_string.clone(),
            partition_columns: snapshot.metadata.partition_columns.clone(),
            configuration,
            created_time: snapshot.metadata.created_time,
            name: snapshot
                .metadata
                .name
                .clone()
                .or_else(|| Some(table_name.to_string())),
            description: snapshot.metadata.description.clone(),
            num_files: i64::try_from(snapshot.adds.len()).ok(),
            size: Some(snapshot.adds.iter().map(|a| a.size).sum()),
            num_records: None,
        },
    };
    (protocol, metadata)
}

/// Build one File NDJSON line per live `add` action in the snapshot. Each
/// `url` is a fresh presigned GET against `entry.bucket` + `entry.path` +
/// `add.path`, valid for `state.default_url_ttl`.
fn build_delta_file_lines(
    state: &DeltaState,
    entry: &DeltaShareTableEntry,
    snapshot: &DeltaSnapshot,
) -> Vec<FileLine> {
    snapshot
        .adds
        .iter()
        .map(|add| {
            let key = file_key(&entry.path, &add.path);
            let url = presign_get(
                &state.endpoint,
                &state.region,
                &state.access_key_id,
                &state.secret_access_key,
                &entry.bucket,
                &key,
                state.default_url_ttl,
            );
            // Stable per-path id — recipients use it for cross-page dedup.
            let id = hex::encode(Sha256::digest(add.path.as_bytes()));
            // Drop None partition values — Delta Sharing protocol has no
            // representation for tombstoned partitions in File entries.
            let partition_values = add
                .partition_values
                .iter()
                .filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
                .collect();
            FileLine {
                file: FileEntry {
                    url,
                    id,
                    partition_values,
                    size: add.size,
                    stats: add.stats.clone(),
                },
            }
        })
        .collect()
}

/// Join `table_root` and `add.path` (which is relative to the table root) into
/// a full S3 object key. Both inputs may or may not have trailing/leading slashes.
fn file_key(table_root: &str, file_path: &str) -> String {
    let root_trim = table_root.trim_end_matches('/');
    let file_trim = file_path.trim_start_matches('/');
    if root_trim.is_empty() {
        file_trim.to_string()
    } else {
        format!("{root_trim}/{file_trim}")
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

#[cfg(test)]
#[allow(clippy::significant_drop_tightening)] // test fixtures intentionally hold state across asserts
mod tests {
    use super::*;
    use crate::delta_log::{AddAction, MetadataAction, ProtocolAction};
    use std::collections::HashMap;

    fn delta_entry() -> DeltaShareTableEntry {
        DeltaShareTableEntry {
            share: "s".into(),
            schema: "sc".into(),
            table_name: "t".into(),
            share_id: String::new(),
            table_type: "delta".into(),
            bucket: "lake".into(),
            path: "warehouse/sales/events/".into(),
            warehouse: String::new(),
            namespace: String::new(),
        }
    }

    #[test]
    fn table_kind_dispatches_known_types() {
        let mut e = delta_entry();
        assert!(matches!(table_kind(&e).unwrap(), TableKind::Delta));
        e.table_type = String::new();
        assert!(matches!(table_kind(&e).unwrap(), TableKind::Iceberg));
        e.table_type = "iceberg".into();
        assert!(matches!(table_kind(&e).unwrap(), TableKind::Iceberg));
        e.table_type = "uniform".into();
        assert!(matches!(table_kind(&e).unwrap(), TableKind::Iceberg));
    }

    #[test]
    fn table_kind_rejects_unknown_type() {
        let mut e = delta_entry();
        e.table_type = "hudi".into();
        let err = table_kind(&e).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("hudi"));
    }

    #[test]
    fn file_key_joins_root_and_relative_path() {
        assert_eq!(
            file_key("warehouse/sales/events/", "part-00000.parquet"),
            "warehouse/sales/events/part-00000.parquet"
        );
        assert_eq!(
            file_key("warehouse/sales/events", "part-00000.parquet"),
            "warehouse/sales/events/part-00000.parquet"
        );
        assert_eq!(
            file_key("warehouse/sales/events/", "/part-00000.parquet"),
            "warehouse/sales/events/part-00000.parquet"
        );
        assert_eq!(
            file_key("warehouse", "country=US/part-0.parquet"),
            "warehouse/country=US/part-0.parquet"
        );
        assert_eq!(file_key("", "part.parquet"), "part.parquet");
    }

    fn snapshot_with_two_adds() -> DeltaSnapshot {
        let mut p1 = HashMap::new();
        p1.insert("country".to_string(), Some("US".to_string()));
        let mut p2 = HashMap::new();
        p2.insert("country".to_string(), Some("CA".to_string()));
        // a partition value that's None — must be dropped from the wire format
        p2.insert("region".to_string(), None);
        DeltaSnapshot {
            version: 7,
            protocol: ProtocolAction {
                min_reader_version: 3,
                min_writer_version: 5,
            },
            metadata: MetadataAction {
                id: "abc".into(),
                schema_string: r#"{"type":"struct"}"#.into(),
                partition_columns: vec!["country".into(), "region".into()],
                created_time: Some(1_700_000_000_000),
                ..MetadataAction::default()
            },
            adds: vec![
                AddAction {
                    path: "country=US/part-0.parquet".into(),
                    partition_values: p1,
                    size: 100,
                    modification_time: 0,
                    stats: Some(r#"{"numRecords":10}"#.into()),
                    data_change: true,
                },
                AddAction {
                    path: "country=CA/part-0.parquet".into(),
                    partition_values: p2,
                    size: 200,
                    modification_time: 0,
                    stats: None,
                    data_change: true,
                },
            ],
        }
    }

    #[test]
    fn build_delta_metadata_lines_carries_snapshot_state() {
        let snap = snapshot_with_two_adds();
        let (proto, meta) = build_delta_metadata_lines("events", &snap);
        assert_eq!(proto.protocol.min_reader_version, 3);
        assert_eq!(meta.metadata.id, "abc");
        assert_eq!(meta.metadata.format.provider, "parquet");
        assert_eq!(meta.metadata.partition_columns.len(), 2);
        assert_eq!(meta.metadata.num_files, Some(2));
        assert_eq!(meta.metadata.size, Some(300));
        assert_eq!(meta.metadata.created_time, Some(1_700_000_000_000));
        assert_eq!(meta.metadata.name.as_deref(), Some("events"));
    }

    #[test]
    fn build_delta_metadata_lines_falls_back_to_table_name_when_id_missing() {
        let mut snap = snapshot_with_two_adds();
        snap.metadata.id = String::new();
        let (_, meta) = build_delta_metadata_lines("events", &snap);
        assert_eq!(meta.metadata.id, "events");
    }

    fn fake_state() -> DeltaState {
        // Only the presigning fields are exercised by build_delta_file_lines —
        // the rest can be defaults / dummies.
        let (channel, _) = tonic::transport::Channel::balance_channel::<u32>(1);
        DeltaState {
            catalog: DeltaCatalog::new(MetadataServiceClient::new(channel.clone())),
            meta_client: MetadataServiceClient::new(channel),
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key_id: "AKID".into(),
            secret_access_key: "secret".into(),
            http: reqwest::Client::new(),
            default_url_ttl: Duration::from_secs(900),
        }
    }

    #[tokio::test]
    async fn build_delta_file_lines_emits_one_per_add_with_presigned_urls() {
        let snap = snapshot_with_two_adds();
        let entry = delta_entry();
        let state = fake_state();
        let files = build_delta_file_lines(&state, &entry, &snap);
        assert_eq!(files.len(), 2);
        for f in &files {
            assert!(
                f.file
                    .url
                    .starts_with("http://localhost:9000/lake/warehouse/sales/events/"),
            );
            assert!(f.file.url.contains("X-Amz-Signature="));
            assert!(f.file.url.contains("X-Amz-Expires=900"));
        }
        assert_eq!(files[0].file.size, 100);
        assert_eq!(files[0].file.stats.as_deref(), Some(r#"{"numRecords":10}"#));
        assert_eq!(files[1].file.size, 200);
        assert!(files[1].file.stats.is_none());
    }

    #[tokio::test]
    async fn build_delta_file_lines_drops_none_partition_values() {
        let snap = snapshot_with_two_adds();
        let entry = delta_entry();
        let state = fake_state();
        let files = build_delta_file_lines(&state, &entry, &snap);
        // First add has only Some("US")
        assert_eq!(
            files[0]
                .file
                .partition_values
                .get("country")
                .map(String::as_str),
            Some("US"),
        );
        // Second add had Some("CA") + None for region — region must be filtered out
        assert_eq!(
            files[1]
                .file
                .partition_values
                .get("country")
                .map(String::as_str),
            Some("CA"),
        );
        assert!(!files[1].file.partition_values.contains_key("region"));
    }

    #[tokio::test]
    async fn build_delta_file_lines_uses_stable_path_hash_for_id() {
        let snap = snapshot_with_two_adds();
        let entry = delta_entry();
        let state = fake_state();
        let files = build_delta_file_lines(&state, &entry, &snap);
        // SHA-256 hex digest is 64 chars; identical input → identical id.
        assert_eq!(files[0].file.id.len(), 64);
        let again = build_delta_file_lines(&state, &entry, &snap);
        assert_eq!(files[0].file.id, again[0].file.id);
        assert_ne!(files[0].file.id, files[1].file.id);
    }
}
