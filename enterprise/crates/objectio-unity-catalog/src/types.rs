//! Unity Catalog REST API request/response types (serde).
//!
//! Field shape follows the Databricks Unity Catalog `OpenAPI` spec so that
//! existing Unity-aware clients (`PySpark`, Trino Unity connector, Daft,
//! Databricks SQL) recognize the responses without modification.
//!
//! Wire format is `snake_case` (Unity's choice); the proto messages stored
//! in the meta service also use `snake_case`, so the conversion is mostly
//! direct field copies.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---- Catalog ----

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct CatalogInfo {
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comment: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    /// Storage root for MANAGED tables created under this catalog.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub storage_root: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    /// `MANAGED_CATALOG` (we don't currently model FOREIGN/DELTASHARING here).
    #[serde(default = "default_catalog_type")]
    pub catalog_type: String,
}

fn default_catalog_type() -> String {
    "MANAGED_CATALOG".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateCatalogRequest {
    pub name: String,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct UpdateCatalogRequest {
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ListCatalogsResponse {
    pub catalogs: Vec<CatalogInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ListCatalogsParams {
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DeleteParams {
    #[serde(default)]
    pub force: bool,
}

// ---- Schema ----

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct SchemaInfo {
    pub name: String,
    pub catalog_name: String,
    /// Conventionally `"{catalog}.{schema}"` — surfaced to clients as the
    /// canonical addressable form.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub full_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comment: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateSchemaRequest {
    pub name: String,
    pub catalog_name: String,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct UpdateSchemaRequest {
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ListSchemasResponse {
    pub schemas: Vec<SchemaInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ListSchemasParams {
    pub catalog_name: String,
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub page_token: Option<String>,
}

// ---- Table ----

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ColumnInfo {
    pub name: String,
    /// Human-readable type, e.g. "string", "int", "decimal(10,2)".
    pub type_text: String,
    /// JSON-serialized type per Unity spec. Optional — we don't validate.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub type_json: String,
    /// Logical type name (e.g. "STRING", "INT", "DECIMAL").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub type_name: String,
    pub position: i32,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_index: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct TableInfo {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    /// "{catalog}.{schema}.{table}".
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub full_name: String,
    /// "MANAGED" | "EXTERNAL" | "VIEW"
    pub table_type: String,
    /// "DELTA" | "ICEBERG" | "PARQUET" | "CSV" | "JSON"
    pub data_source_format: String,
    pub storage_location: String,
    #[serde(default)]
    pub columns: Vec<ColumnInfo>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    /// Server-assigned UUID at create time.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table_id: String,
    /// Row filter binding. Engines that consume this metadata
    /// inject `WHERE function_full_name(input_columns)` into reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<RowFilterBinding>,
    /// Column masks keyed by column name. Engine wraps reads of
    /// each masked column with `function_full_name(col, ...using_cols)`.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub column_masks: HashMap<String, ColumnMaskBinding>,
}

/// Databricks-shape row filter binding. Mirror of the proto type, with
/// `snake_case` JSON for the REST surface.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct RowFilterBinding {
    pub function_full_name: String,
    #[serde(default)]
    pub input_column_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ColumnMaskBinding {
    pub function_full_name: String,
    #[serde(default)]
    pub using_column_names: Vec<String>,
}

/// Body of `PUT /api/2.1/unity-catalog/tables/{full_name}/security`.
/// Empty/missing fields = clear the binding.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct SetTableSecurityRequest {
    #[serde(default)]
    pub row_filter: Option<RowFilterBinding>,
    #[serde(default)]
    pub column_masks: HashMap<String, ColumnMaskBinding>,
}

/// Effective identity of the calling principal.
///
/// Resolved by the gateway from a `SigV4` access key or an OIDC bearer.
/// Engines (Spark/Trino) read this to populate the SQL built-ins that
/// row-filter and column-mask UDFs reference:
///
/// | Built-in                          | Source field         |
/// |-----------------------------------|----------------------|
/// | `current_user()`                  | `user_name`          |
/// | `is_member(g)`                    | `groups` contains `g`|
/// | `is_account_group_member(g)`      | `groups` contains `g`|
///
/// `is_member` and `is_account_group_member` resolve to the same set —
/// we don't separate workspace vs account groups.
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct CallerIdentity {
    /// Short principal name (best-effort: parsed from `user_arn` tail,
    /// falls back to `user_id`). What `current_user()` resolves to.
    pub user_name: String,
    pub user_id: String,
    pub user_arn: String,
    /// Group short names — what `is_member()` / `is_account_group_member()`
    /// resolve against. Parsed from `group_arns` tails.
    pub groups: Vec<String>,
    /// Full ARNs (debug / advanced predicates).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_arns: Vec<String>,
    /// Tenant the principal belongs to. Empty = system admin.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tenant: String,
    /// True when the principal is in the gateway's admin allow-list.
    pub is_admin: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateTableRequest {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    /// Defaults to "MANAGED" if omitted.
    #[serde(default)]
    pub table_type: String,
    /// Defaults to "DELTA" if omitted.
    #[serde(default)]
    pub data_source_format: String,
    /// Required for EXTERNAL tables; ignored for MANAGED.
    #[serde(default)]
    pub storage_location: String,
    #[serde(default)]
    pub columns: Vec<ColumnInfo>,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ListTablesResponse {
    pub tables: Vec<TableInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ListTablesParams {
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub page_token: Option<String>,
}

// ---- Functions (UDFs) ----

/// Function metadata returned from `GET /functions/{full_name}` and friends.
///
/// Field shape mirrors the Databricks Unity Catalog `FunctionInfo` exactly
/// so the official Databricks SDK deserializes it without modification.
/// In particular: `routine_definition` carries the source text and
/// `routine_body` carries the dispatch flag (`"SQL"` | `"EXTERNAL"`).
#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct FunctionInfo {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub full_name: String,
    /// Source text — the SQL/code body the engine executes.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub routine_definition: String,
    /// `"SQL"` | `"EXTERNAL"`. Dispatch flag, not the source text.
    pub routine_body: String,
    /// Runtime for `EXTERNAL` bodies (e.g. `"PYTHON"`); empty for SQL.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub external_language: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub full_data_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parameter_style: String,
    #[serde(default)]
    pub is_deterministic: bool,
    /// `"CONTAINS_SQL"` | `"READS_SQL_DATA"` | `"MODIFIES_SQL_DATA"` | `"NO_SQL"`
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sql_data_access: String,
    #[serde(default)]
    pub is_null_call: bool,
    /// `"DEFINER"` | `"INVOKER"`
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub security_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub specific_name: String,
    /// `{"parameters": [...]}` per Databricks spec; pass-through opaque JSON.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub input_params: serde_json::Value,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub return_params: serde_json::Value,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comment: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub function_id: String,
}

/// Wire-shape compat: the Databricks SDK posts function creates as
/// `{"function_info": {...}}`. We accept either the wrapped envelope or
/// a flat object so curl-style callers still work.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CreateFunctionBody {
    Wrapped {
        function_info: CreateFunctionRequest,
    },
    Flat(CreateFunctionRequest),
}

impl CreateFunctionBody {
    #[must_use]
    pub fn into_request(self) -> CreateFunctionRequest {
        match self {
            Self::Wrapped { function_info } => function_info,
            Self::Flat(req) => req,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateFunctionRequest {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    /// Source text the engine will execute. Required.
    pub routine_definition: String,
    /// `"SQL"` | `"EXTERNAL"`. Defaults to `"SQL"` when omitted.
    #[serde(default)]
    pub routine_body: String,
    /// Runtime for `EXTERNAL` bodies (e.g. `"PYTHON"`); ignored for SQL.
    #[serde(default)]
    pub external_language: String,
    #[serde(default)]
    pub data_type: String,
    #[serde(default)]
    pub full_data_type: String,
    #[serde(default)]
    pub parameter_style: String,
    #[serde(default)]
    pub is_deterministic: bool,
    #[serde(default)]
    pub sql_data_access: String,
    #[serde(default)]
    pub is_null_call: bool,
    #[serde(default)]
    pub security_type: String,
    #[serde(default)]
    pub specific_name: String,
    #[serde(default)]
    pub input_params: serde_json::Value,
    #[serde(default)]
    pub return_params: serde_json::Value,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ListFunctionsResponse {
    pub functions: Vec<FunctionInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ListFunctionsParams {
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub page_token: Option<String>,
}

// ---- Volumes ----

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct VolumeInfo {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub full_name: String,
    /// "MANAGED" | "EXTERNAL"
    pub volume_type: String,
    pub storage_location: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comment: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub volume_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateVolumeRequest {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    /// Defaults to "MANAGED" when omitted.
    #[serde(default)]
    pub volume_type: String,
    /// Required for EXTERNAL volumes; ignored for MANAGED.
    #[serde(default)]
    pub storage_location: String,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ListVolumesResponse {
    pub volumes: Vec<VolumeInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ListVolumesParams {
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TemporaryVolumeCredentialsRequest {
    pub volume_id: String,
    /// Requested operation: `"READ"` | `"READ_WRITE"`.
    #[serde(default = "default_operation")]
    pub operation: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TemporaryVolumeCredentialsResponse {
    pub aws_temp_credentials: AwsCredentials,
    pub expiration_time: u64,
    pub url: String,
}

// ---- Models + ModelVersions ----

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ModelInfo {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub full_name: String,
    pub storage_location: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comment: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateModelRequest {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    /// Empty → MANAGED, server-derived under the catalog bucket.
    #[serde(default)]
    pub storage_location: String,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ListModelsResponse {
    pub registered_models: Vec<ModelInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ListModelsParams {
    pub catalog_name: String,
    pub schema_name: String,
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ModelVersionInfo {
    pub model_name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub version: u32,
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    /// `"PENDING_REGISTRATION"` | `"READY"` | `"FAILED_REGISTRATION"`
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateModelVersionRequest {
    pub model_name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub source: String,
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateModelVersionStatusRequest {
    pub status: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ListModelVersionsResponse {
    pub model_versions: Vec<ModelVersionInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

// ---- Vended credentials ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TemporaryTableCredentialsRequest {
    pub table_id: String,
    /// Requested operation: `"READ"` | `"READ_WRITE"` (Unity convention).
    #[serde(default = "default_operation")]
    pub operation: String,
}

fn default_operation() -> String {
    "READ".to_string()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TemporaryTableCredentialsResponse {
    pub aws_temp_credentials: AwsCredentials,
    pub expiration_time: u64,
    /// S3 URL the recipient should use for I/O against this table.
    pub url: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

// ---- Policy management (out-of-spec, mirrors Iceberg's `/policy` endpoints) ----

#[derive(Debug, Deserialize)]
pub struct SetPolicyRequest {
    /// JSON-encoded `BucketPolicy` body (passed through verbatim after validation).
    pub policy: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct GetPolicyResponse {
    pub policy: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_info_uses_snake_case() {
        let c = CatalogInfo {
            name: "prod".into(),
            comment: "ok".into(),
            storage_root: "s3://unity-prod".into(),
            created_at: 1,
            updated_at: 2,
            ..CatalogInfo::default()
        };
        let json = serde_json::to_string(&c).unwrap();
        // Unity uses snake_case wire format — assert key names directly.
        assert!(json.contains(r#""name":"prod""#));
        assert!(json.contains(r#""storage_root":"s3://unity-prod""#));
        assert!(json.contains(r#""created_at":1"#));
        assert!(json.contains(r#""updated_at":2"#));
    }

    #[test]
    fn empty_optional_fields_are_omitted() {
        let c = CatalogInfo {
            name: "x".into(),
            ..CatalogInfo::default()
        };
        let json = serde_json::to_string(&c).unwrap();
        assert!(!json.contains("storage_root"));
        assert!(!json.contains("comment"));
        assert!(!json.contains("owner"));
        assert!(!json.contains("properties"));
    }

    #[test]
    fn table_info_round_trips_columns() {
        let t = TableInfo {
            name: "events".into(),
            catalog_name: "prod".into(),
            schema_name: "sales".into(),
            full_name: "prod.sales.events".into(),
            table_type: "MANAGED".into(),
            data_source_format: "DELTA".into(),
            storage_location: "s3://unity-prod/sales/events/".into(),
            columns: vec![ColumnInfo {
                name: "id".into(),
                type_text: "bigint".into(),
                type_name: "LONG".into(),
                position: 0,
                nullable: false,
                ..ColumnInfo {
                    name: String::new(),
                    type_text: String::new(),
                    type_json: String::new(),
                    type_name: String::new(),
                    position: 0,
                    nullable: false,
                    comment: String::new(),
                    partition_index: None,
                }
            }],
            ..TableInfo::default()
        };
        let json = serde_json::to_string(&t).unwrap();
        let back: TableInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.full_name, "prod.sales.events");
        assert_eq!(back.columns.len(), 1);
        assert_eq!(back.columns[0].type_name, "LONG");
    }
}
