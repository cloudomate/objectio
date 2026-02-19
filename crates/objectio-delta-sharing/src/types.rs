//! Delta Sharing REST API JSON types.
//!
//! Follows the Delta Sharing Protocol spec:
//! <https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md>

use serde::{Deserialize, Serialize};

// ---- Shares ----

#[derive(Debug, Serialize, Deserialize)]
pub struct Share {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListSharesResponse {
    pub items: Vec<Share>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

// ---- Schemas ----

#[derive(Debug, Serialize)]
pub struct Schema {
    pub name: String,
    pub share: String,
}

#[derive(Debug, Serialize)]
pub struct ListSchemasResponse {
    pub items: Vec<Schema>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

// ---- Tables ----

#[derive(Debug, Serialize)]
pub struct Table {
    pub name: String,
    pub schema: String,
    pub share: String,
    #[serde(rename = "shareId", skip_serializing_if = "Option::is_none")]
    pub share_id: Option<String>,
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    pub items: Vec<Table>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

// ---- Table metadata ----

#[derive(Debug, Serialize)]
pub struct ProtocolLine {
    pub protocol: Protocol,
}

#[derive(Debug, Serialize)]
pub struct Protocol {
    #[serde(rename = "minReaderVersion")]
    pub min_reader_version: i32,
}

#[derive(Debug, Serialize)]
pub struct MetadataLine {
    pub metadata: TableMetadata,
}

#[derive(Debug, Serialize)]
pub struct TableMetadata {
    pub id: String,
    pub format: Format,
    #[serde(rename = "schemaString")]
    pub schema_string: String,
    #[serde(rename = "partitionColumns")]
    pub partition_columns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<std::collections::HashMap<String, String>>,
    #[serde(rename = "createdTime", skip_serializing_if = "Option::is_none")]
    pub created_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "numFiles", skip_serializing_if = "Option::is_none")]
    pub num_files: Option<i64>,
    #[serde(rename = "size", skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(rename = "numRecords", skip_serializing_if = "Option::is_none")]
    pub num_records: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct Format {
    pub provider: String,
}

// ---- Query response (presigned file URLs) ----

#[derive(Debug, Serialize)]
pub struct FileLine {
    pub file: FileEntry,
}

#[derive(Debug, Serialize)]
pub struct FileEntry {
    pub url: String,
    pub id: String,
    #[serde(rename = "partitionValues")]
    pub partition_values: std::collections::HashMap<String, String>,
    pub size: i64,
    #[serde(rename = "stats", skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
}

// ---- Table version ----

#[derive(Debug, Serialize)]
pub struct TableVersionResponse {
    #[serde(rename = "deltaTableVersion")]
    pub delta_table_version: i64,
}

// ---- Query request ----

#[derive(Debug, Deserialize)]
pub struct QueryTableRequest {
    #[serde(rename = "predicateHints", default)]
    pub predicate_hints: Vec<String>,
    #[serde(rename = "limitHint")]
    pub limit_hint: Option<i64>,
    pub version: Option<i64>,
}

// ---- Admin request types ----

#[derive(Debug, Deserialize)]
pub struct CreateShareRequest {
    pub name: String,
    #[serde(default)]
    pub comment: String,
}

#[derive(Debug, Deserialize)]
pub struct AddTableRequest {
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateRecipientRequest {
    pub name: String,
    #[serde(default)]
    pub shares: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateRecipientResponse {
    pub name: String,
    pub shares: Vec<String>,
    pub bearer_token: String,
}
