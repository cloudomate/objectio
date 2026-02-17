//! Iceberg REST API request/response types (serde).
//!
//! These match the Iceberg REST `OpenAPI` spec and are used for JSON
//! serialization/deserialization in the Axum handlers.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---- Config ----

#[derive(Debug, Serialize)]
pub struct CatalogConfig {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}

// ---- Namespace ----

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    pub namespace: Vec<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct CreateNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct LoadNamespaceResponse {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListNamespacesParams {
    pub parent: Option<String>,
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateNamespacePropertiesRequest {
    #[serde(default)]
    pub removals: Vec<String>,
    #[serde(default)]
    pub updates: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateNamespacePropertiesResponse {
    pub updated: Vec<String>,
    pub removed: Vec<String>,
    pub missing: Vec<String>,
}

// ---- Table ----

#[derive(Debug, Serialize, Deserialize)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    #[serde(default)]
    pub schema: serde_json::Value,
    #[serde(default, rename = "partition-spec")]
    pub partition_spec: serde_json::Value,
    #[serde(default, rename = "write-order")]
    pub write_order: serde_json::Value,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    #[serde(default, rename = "stage-create")]
    pub stage_create: bool,
}

#[derive(Debug, Serialize)]
pub struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    pub metadata: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct CommitTableRequest {
    #[serde(default)]
    pub identifier: Option<TableIdentifier>,
    #[serde(default)]
    pub requirements: Vec<TableRequirement>,
    #[serde(default)]
    pub updates: Vec<TableUpdate>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(clippy::enum_variant_names)]
pub enum TableRequirement {
    #[serde(rename = "assert-create")]
    AssertCreate,
    #[serde(rename = "assert-table-uuid")]
    AssertTableUuid { uuid: String },
    #[serde(rename = "assert-ref-snapshot-id")]
    AssertRefSnapshotId {
        r#ref: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    #[serde(rename = "assert-last-assigned-field-id")]
    AssertLastAssignedFieldId {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i64,
    },
    #[serde(rename = "assert-current-schema-id")]
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i64,
    },
    #[serde(rename = "assert-last-assigned-partition-id")]
    AssertLastAssignedPartitionId {
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i64,
    },
    #[serde(rename = "assert-default-spec-id")]
    AssertDefaultSpecId {
        #[serde(rename = "default-spec-id")]
        default_spec_id: i64,
    },
    #[serde(rename = "assert-default-sort-order-id")]
    AssertDefaultSortOrderId {
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i64,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action")]
pub enum TableUpdate {
    #[serde(rename = "assign-uuid")]
    AssignUuid { uuid: String },
    #[serde(rename = "upgrade-format-version")]
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
    #[serde(rename = "add-schema")]
    AddSchema {
        schema: serde_json::Value,
        #[serde(rename = "last-column-id")]
        last_column_id: Option<i64>,
    },
    #[serde(rename = "set-current-schema")]
    SetCurrentSchema {
        #[serde(rename = "schema-id")]
        schema_id: i64,
    },
    #[serde(rename = "add-spec")]
    AddSpec { spec: serde_json::Value },
    #[serde(rename = "set-default-spec")]
    SetDefaultSpec {
        #[serde(rename = "spec-id")]
        spec_id: i64,
    },
    #[serde(rename = "add-sort-order")]
    AddSortOrder {
        #[serde(rename = "sort-order")]
        sort_order: serde_json::Value,
    },
    #[serde(rename = "set-default-sort-order")]
    SetDefaultSortOrder {
        #[serde(rename = "sort-order-id")]
        sort_order_id: i64,
    },
    #[serde(rename = "add-snapshot")]
    AddSnapshot { snapshot: serde_json::Value },
    #[serde(rename = "set-snapshot-ref")]
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(flatten)]
        rest: serde_json::Value,
    },
    #[serde(rename = "remove-snapshots")]
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    #[serde(rename = "remove-snapshot-ref")]
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
    #[serde(rename = "set-location")]
    SetLocation { location: String },
    #[serde(rename = "set-properties")]
    SetProperties { updates: HashMap<String, String> },
    #[serde(rename = "remove-properties")]
    RemoveProperties { removals: Vec<String> },
}

#[derive(Debug, Serialize)]
pub struct CommitTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct RenameTableRequest {
    pub source: TableIdentifier,
    pub destination: TableIdentifier,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesParams {
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct PurgeParams {
    #[serde(rename = "purgeRequested", default)]
    pub purge_requested: bool,
}

// ---- Policy ----

#[derive(Debug, Deserialize)]
pub struct SetPolicyRequest {
    pub policy: String,
}
