//! Thin gRPC wrapper around the meta service's `Unity*` RPCs.
//!
//! Translates between the proto messages and the Unity REST shapes in
//! `crate::types`. Each method is a 1:1 wrapper so the handlers stay simple.
// Every method here returns `Result<T, UnityError>` and the error is the
// uniform "meta gRPC failure mapped via `From<tonic::Status>`" — no need for
// a per-method `# Errors` docblock describing the same thing 22 times.
#![allow(clippy::missing_errors_doc)]
// Unity Catalog's create_table accepts the full set of Unity table fields;
// bundling them into a struct just to satisfy clippy adds noise without
// improving the API.
#![allow(clippy::too_many_arguments)]

use crate::error::UnityError;
use crate::types::{
    CatalogInfo, ColumnInfo, FunctionInfo, ModelInfo, ModelVersionInfo, SchemaInfo, TableInfo,
    VolumeInfo,
};
use objectio_proto::metadata::{
    UnityCatalog as ProtoCatalog, UnityCreateCatalogRequest, UnityCreateFunctionRequest,
    UnityCreateModelRequest, UnityCreateModelVersionRequest, UnityCreateSchemaRequest,
    UnityCreateTableRequest, UnityCreateVolumeRequest, UnityDeleteCatalogRequest,
    UnityDeleteFunctionRequest, UnityDeleteModelRequest, UnityDeleteModelVersionRequest,
    UnityDeleteSchemaRequest, UnityDeleteTableRequest, UnityDeleteVolumeRequest,
    UnityFunction as ProtoFunction, UnityGetCatalogPolicyRequest, UnityGetCatalogRequest,
    UnityGetFunctionRequest, UnityGetModelRequest, UnityGetModelVersionRequest,
    UnityGetSchemaPolicyRequest, UnityGetSchemaRequest, UnityGetTablePolicyRequest,
    UnityGetTableRequest, UnityGetVolumeRequest, UnityListCatalogsRequest,
    UnityListFunctionsRequest, UnityListModelVersionsRequest, UnityListModelsRequest,
    UnityListSchemasRequest, UnityListTablesRequest, UnityListVolumesRequest,
    UnityModel as ProtoModel, UnityModelVersion as ProtoModelVersion, UnitySchema as ProtoSchema,
    UnitySetCatalogPolicyRequest, UnitySetSchemaPolicyRequest, UnitySetTablePolicyRequest,
    UnitySetTableSecurityRequest, UnityTable as ProtoTable, UnityUpdateCatalogRequest,
    UnityUpdateModelVersionStatusRequest,
    UnityUpdateSchemaRequest, UnityVolume as ProtoVolume,
    metadata_service_client::MetadataServiceClient,
};
use tonic::transport::Channel;

type Result<T> = std::result::Result<T, UnityError>;

#[derive(Clone)]
pub struct UnityCatalogClient {
    meta_client: MetadataServiceClient<Channel>,
}

impl UnityCatalogClient {
    #[must_use]
    pub const fn new(meta_client: MetadataServiceClient<Channel>) -> Self {
        Self { meta_client }
    }

    pub fn meta_client(&self) -> MetadataServiceClient<Channel> {
        self.meta_client.clone()
    }

    // ---- Catalog ----

    pub async fn create_catalog(
        &self,
        name: String,
        comment: String,
        owner: String,
        tenant: String,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<CatalogInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_create_catalog(UnityCreateCatalogRequest {
                name,
                comment,
                owner,
                tenant,
                properties,
            })
            .await?
            .into_inner();
        let catalog = resp
            .catalog
            .ok_or_else(|| UnityError::internal("meta returned no catalog"))?;
        Ok(catalog_to_info(catalog))
    }

    pub async fn list_catalogs(&self, tenant: String) -> Result<Vec<CatalogInfo>> {
        let resp = self
            .meta_client
            .clone()
            .unity_list_catalogs(UnityListCatalogsRequest {
                max_results: 0,
                page_token: String::new(),
                tenant,
            })
            .await?
            .into_inner();
        Ok(resp.catalogs.into_iter().map(catalog_to_info).collect())
    }

    pub async fn get_catalog(&self, name: &str) -> Result<CatalogInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_catalog(UnityGetCatalogRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner();
        let catalog = resp
            .catalog
            .ok_or_else(|| UnityError::internal("meta returned no catalog"))?;
        Ok(catalog_to_info(catalog))
    }

    pub async fn update_catalog(
        &self,
        name: String,
        new_comment: String,
        new_owner: String,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<CatalogInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_update_catalog(UnityUpdateCatalogRequest {
                name,
                new_comment,
                new_owner,
                properties,
            })
            .await?
            .into_inner();
        let catalog = resp
            .catalog
            .ok_or_else(|| UnityError::internal("meta returned no catalog"))?;
        Ok(catalog_to_info(catalog))
    }

    pub async fn delete_catalog(&self, name: &str, force: bool) -> Result<()> {
        self.meta_client
            .clone()
            .unity_delete_catalog(UnityDeleteCatalogRequest {
                name: name.to_string(),
                force,
            })
            .await?;
        Ok(())
    }

    // ---- Schema ----

    pub async fn create_schema(
        &self,
        catalog_name: String,
        name: String,
        comment: String,
        owner: String,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<SchemaInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_create_schema(UnityCreateSchemaRequest {
                catalog_name,
                name,
                comment,
                owner,
                properties,
            })
            .await?
            .into_inner();
        let schema = resp
            .schema
            .ok_or_else(|| UnityError::internal("meta returned no schema"))?;
        Ok(schema_to_info(schema))
    }

    pub async fn list_schemas(&self, catalog_name: String) -> Result<Vec<SchemaInfo>> {
        let resp = self
            .meta_client
            .clone()
            .unity_list_schemas(UnityListSchemasRequest {
                catalog_name,
                max_results: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner();
        Ok(resp.schemas.into_iter().map(schema_to_info).collect())
    }

    pub async fn get_schema(&self, catalog_name: &str, name: &str) -> Result<SchemaInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_schema(UnityGetSchemaRequest {
                catalog_name: catalog_name.to_string(),
                name: name.to_string(),
            })
            .await?
            .into_inner();
        let schema = resp
            .schema
            .ok_or_else(|| UnityError::internal("meta returned no schema"))?;
        Ok(schema_to_info(schema))
    }

    pub async fn update_schema(
        &self,
        catalog_name: String,
        name: String,
        new_comment: String,
        new_owner: String,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<SchemaInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_update_schema(UnityUpdateSchemaRequest {
                catalog_name,
                name,
                new_comment,
                new_owner,
                properties,
            })
            .await?
            .into_inner();
        let schema = resp
            .schema
            .ok_or_else(|| UnityError::internal("meta returned no schema"))?;
        Ok(schema_to_info(schema))
    }

    pub async fn delete_schema(&self, catalog_name: &str, name: &str, force: bool) -> Result<()> {
        self.meta_client
            .clone()
            .unity_delete_schema(UnityDeleteSchemaRequest {
                catalog_name: catalog_name.to_string(),
                name: name.to_string(),
                force,
            })
            .await?;
        Ok(())
    }

    // ---- Table ----

    pub async fn create_table(
        &self,
        catalog_name: String,
        schema_name: String,
        name: String,
        table_type: String,
        data_source_format: String,
        storage_location: String,
        columns_json: String,
        owner: String,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<TableInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_create_table(UnityCreateTableRequest {
                catalog_name,
                schema_name,
                name,
                table_type,
                data_source_format,
                storage_location,
                columns_json,
                owner,
                properties,
            })
            .await?
            .into_inner();
        let table = resp
            .table
            .ok_or_else(|| UnityError::internal("meta returned no table"))?;
        Ok(table_to_info(table))
    }

    pub async fn list_tables(
        &self,
        catalog_name: String,
        schema_name: String,
    ) -> Result<Vec<TableInfo>> {
        let resp = self
            .meta_client
            .clone()
            .unity_list_tables(UnityListTablesRequest {
                catalog_name,
                schema_name,
                max_results: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner();
        Ok(resp.tables.into_iter().map(table_to_info).collect())
    }

    pub async fn get_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<TableInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_table(UnityGetTableRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?
            .into_inner();
        let table = resp
            .table
            .ok_or_else(|| UnityError::internal("meta returned no table"))?;
        Ok(table_to_info(table))
    }

    pub async fn delete_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_delete_table(UnityDeleteTableRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    // ---- Function ----

    pub async fn create_function(&self, req: UnityCreateFunctionRequest) -> Result<FunctionInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_create_function(req)
            .await?
            .into_inner();
        let function = resp
            .function
            .ok_or_else(|| UnityError::internal("meta returned no function"))?;
        Ok(function_to_info(function))
    }

    pub async fn list_functions(
        &self,
        catalog_name: String,
        schema_name: String,
    ) -> Result<Vec<FunctionInfo>> {
        let resp = self
            .meta_client
            .clone()
            .unity_list_functions(UnityListFunctionsRequest {
                catalog_name,
                schema_name,
                max_results: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner();
        Ok(resp.functions.into_iter().map(function_to_info).collect())
    }

    pub async fn get_function(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<FunctionInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_function(UnityGetFunctionRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?
            .into_inner();
        let function = resp
            .function
            .ok_or_else(|| UnityError::internal("meta returned no function"))?;
        Ok(function_to_info(function))
    }

    pub async fn delete_function(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_delete_function(UnityDeleteFunctionRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    // ---- Volume ----

    pub async fn create_volume(&self, req: UnityCreateVolumeRequest) -> Result<VolumeInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_create_volume(req)
            .await?
            .into_inner();
        let volume = resp
            .volume
            .ok_or_else(|| UnityError::internal("meta returned no volume"))?;
        Ok(volume_to_info(volume))
    }

    pub async fn list_volumes(
        &self,
        catalog_name: String,
        schema_name: String,
    ) -> Result<Vec<VolumeInfo>> {
        let resp = self
            .meta_client
            .clone()
            .unity_list_volumes(UnityListVolumesRequest {
                catalog_name,
                schema_name,
                max_results: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner();
        Ok(resp.volumes.into_iter().map(volume_to_info).collect())
    }

    pub async fn get_volume(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<VolumeInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_volume(UnityGetVolumeRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?
            .into_inner();
        let volume = resp
            .volume
            .ok_or_else(|| UnityError::internal("meta returned no volume"))?;
        Ok(volume_to_info(volume))
    }

    pub async fn delete_volume(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_delete_volume(UnityDeleteVolumeRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    // ---- Model + ModelVersion ----

    pub async fn create_model(&self, req: UnityCreateModelRequest) -> Result<ModelInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_create_model(req)
            .await?
            .into_inner();
        let model = resp
            .model
            .ok_or_else(|| UnityError::internal("meta returned no model"))?;
        Ok(model_to_info(model))
    }

    pub async fn list_models(
        &self,
        catalog_name: String,
        schema_name: String,
    ) -> Result<Vec<ModelInfo>> {
        let resp = self
            .meta_client
            .clone()
            .unity_list_models(UnityListModelsRequest {
                catalog_name,
                schema_name,
                max_results: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner();
        Ok(resp.models.into_iter().map(model_to_info).collect())
    }

    pub async fn get_model(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<ModelInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_model(UnityGetModelRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?
            .into_inner();
        let model = resp
            .model
            .ok_or_else(|| UnityError::internal("meta returned no model"))?;
        Ok(model_to_info(model))
    }

    pub async fn delete_model(
        &self,
        catalog_name: &str,
        schema_name: &str,
        name: &str,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_delete_model(UnityDeleteModelRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    pub async fn create_model_version(
        &self,
        req: UnityCreateModelVersionRequest,
    ) -> Result<ModelVersionInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_create_model_version(req)
            .await?
            .into_inner();
        let version = resp
            .version
            .ok_or_else(|| UnityError::internal("meta returned no model version"))?;
        Ok(model_version_to_info(version))
    }

    pub async fn list_model_versions(
        &self,
        catalog_name: String,
        schema_name: String,
        model_name: String,
    ) -> Result<Vec<ModelVersionInfo>> {
        let resp = self
            .meta_client
            .clone()
            .unity_list_model_versions(UnityListModelVersionsRequest {
                catalog_name,
                schema_name,
                model_name,
                max_results: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner();
        Ok(resp
            .versions
            .into_iter()
            .map(model_version_to_info)
            .collect())
    }

    pub async fn get_model_version(
        &self,
        catalog_name: &str,
        schema_name: &str,
        model_name: &str,
        version: u32,
    ) -> Result<ModelVersionInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_model_version(UnityGetModelVersionRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                model_name: model_name.to_string(),
                version,
            })
            .await?
            .into_inner();
        let version = resp
            .version
            .ok_or_else(|| UnityError::internal("meta returned no model version"))?;
        Ok(model_version_to_info(version))
    }

    pub async fn update_model_version_status(
        &self,
        catalog_name: &str,
        schema_name: &str,
        model_name: &str,
        version: u32,
        new_status: String,
    ) -> Result<ModelVersionInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_update_model_version_status(UnityUpdateModelVersionStatusRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                model_name: model_name.to_string(),
                version,
                new_status,
            })
            .await?
            .into_inner();
        let version = resp
            .version
            .ok_or_else(|| UnityError::internal("meta returned no model version"))?;
        Ok(model_version_to_info(version))
    }

    pub async fn delete_model_version(
        &self,
        catalog_name: &str,
        schema_name: &str,
        model_name: &str,
        version: u32,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_delete_model_version(UnityDeleteModelVersionRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                model_name: model_name.to_string(),
                version,
            })
            .await?;
        Ok(())
    }

    // ---- Policy management ----

    pub async fn set_catalog_policy(
        &self,
        catalog_name: String,
        policy_json: Vec<u8>,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_set_catalog_policy(UnitySetCatalogPolicyRequest {
                catalog_name,
                policy_json,
            })
            .await?;
        Ok(())
    }

    pub async fn get_catalog_policy(&self, catalog_name: &str) -> Result<Vec<u8>> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_catalog_policy(UnityGetCatalogPolicyRequest {
                catalog_name: catalog_name.to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.policy_json)
    }

    pub async fn set_schema_policy(
        &self,
        catalog_name: String,
        schema_name: String,
        policy_json: Vec<u8>,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_set_schema_policy(UnitySetSchemaPolicyRequest {
                catalog_name,
                schema_name,
                policy_json,
            })
            .await?;
        Ok(())
    }

    pub async fn get_schema_policy(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<u8>> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_schema_policy(UnityGetSchemaPolicyRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.policy_json)
    }

    pub async fn set_table_policy(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
        policy_json: Vec<u8>,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .unity_set_table_policy(UnitySetTablePolicyRequest {
                catalog_name,
                schema_name,
                table_name,
                policy_json,
            })
            .await?;
        Ok(())
    }

    /// Replace a table's row-filter / column-mask bindings. Empty
    /// `row_filter` + empty `column_masks` clears both. Server validates
    /// the referenced functions exist and (for row filter) return BOOLEAN.
    pub async fn set_table_security(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
        row_filter: Option<objectio_proto::metadata::UnityRowFilter>,
        column_masks: std::collections::HashMap<
            String,
            objectio_proto::metadata::UnityColumnMask,
        >,
    ) -> Result<TableInfo> {
        let resp = self
            .meta_client
            .clone()
            .unity_set_table_security(UnitySetTableSecurityRequest {
                catalog_name,
                schema_name,
                name: table_name,
                row_filter,
                column_masks,
            })
            .await?
            .into_inner();
        let table = resp
            .table
            .ok_or_else(|| UnityError::internal("meta returned no table on set-security"))?;
        Ok(table_to_info(table))
    }

    pub async fn get_table_policy(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<u8>> {
        let resp = self
            .meta_client
            .clone()
            .unity_get_table_policy(UnityGetTablePolicyRequest {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.policy_json)
    }
}

// ---- Proto → wire shape converters ----

fn catalog_to_info(c: ProtoCatalog) -> CatalogInfo {
    CatalogInfo {
        name: c.name,
        comment: c.comment,
        owner: c.owner,
        storage_root: c.location,
        created_at: c.created_at,
        updated_at: c.updated_at,
        properties: c.properties,
        catalog_type: "MANAGED_CATALOG".to_string(),
    }
}

fn schema_to_info(s: ProtoSchema) -> SchemaInfo {
    let full_name = format!("{}.{}", s.catalog_name, s.name);
    SchemaInfo {
        name: s.name,
        catalog_name: s.catalog_name,
        full_name,
        comment: s.comment,
        owner: s.owner,
        created_at: s.created_at,
        updated_at: s.updated_at,
        properties: s.properties,
    }
}

fn table_to_info(t: ProtoTable) -> TableInfo {
    let full_name = format!("{}.{}.{}", t.catalog_name, t.schema_name, t.name);
    let columns: Vec<ColumnInfo> = if t.columns_json.is_empty() {
        Vec::new()
    } else {
        // Best-effort — Unity stores columns as opaque JSON in our proto.
        // Drop on parse failure rather than failing the whole response.
        serde_json::from_str(&t.columns_json).unwrap_or_default()
    };
    let row_filter = t.row_filter.map(|rf| crate::types::RowFilterBinding {
        function_full_name: rf.function_full_name,
        input_column_names: rf.input_column_names,
    });
    let column_masks = t
        .column_masks
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                crate::types::ColumnMaskBinding {
                    function_full_name: v.function_full_name,
                    using_column_names: v.using_column_names,
                },
            )
        })
        .collect();
    TableInfo {
        name: t.name,
        catalog_name: t.catalog_name,
        schema_name: t.schema_name,
        full_name,
        table_type: t.table_type,
        data_source_format: t.data_source_format,
        storage_location: t.storage_location,
        columns,
        owner: t.owner,
        created_at: t.created_at,
        updated_at: t.updated_at,
        properties: t.properties,
        table_id: t.table_id,
        row_filter,
        column_masks,
    }
}

fn function_to_info(f: ProtoFunction) -> FunctionInfo {
    let full_name = format!("{}.{}.{}", f.catalog_name, f.schema_name, f.name);
    // Pass-through JSON for the param descriptors. Bad JSON becomes Null
    // rather than failing the whole response — same tolerance as columns.
    let input_params = if f.input_params_json.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&f.input_params_json).unwrap_or(serde_json::Value::Null)
    };
    let return_params = if f.return_params_json.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&f.return_params_json).unwrap_or(serde_json::Value::Null)
    };
    FunctionInfo {
        name: f.name,
        catalog_name: f.catalog_name,
        schema_name: f.schema_name,
        full_name,
        routine_definition: f.routine_definition,
        routine_body: f.routine_body,
        external_language: f.external_language,
        data_type: f.data_type,
        full_data_type: f.full_data_type,
        parameter_style: f.parameter_style,
        is_deterministic: f.is_deterministic,
        sql_data_access: f.sql_data_access,
        is_null_call: f.is_null_call,
        security_type: f.security_type,
        specific_name: f.specific_name,
        input_params,
        return_params,
        comment: f.comment,
        owner: f.owner,
        created_at: f.created_at,
        updated_at: f.updated_at,
        properties: f.properties,
        function_id: f.function_id,
    }
}

fn volume_to_info(v: ProtoVolume) -> VolumeInfo {
    let full_name = format!("{}.{}.{}", v.catalog_name, v.schema_name, v.name);
    VolumeInfo {
        name: v.name,
        catalog_name: v.catalog_name,
        schema_name: v.schema_name,
        full_name,
        volume_type: v.volume_type,
        storage_location: v.storage_location,
        comment: v.comment,
        owner: v.owner,
        created_at: v.created_at,
        updated_at: v.updated_at,
        properties: v.properties,
        volume_id: v.volume_id,
    }
}

fn model_to_info(m: ProtoModel) -> ModelInfo {
    let full_name = format!("{}.{}.{}", m.catalog_name, m.schema_name, m.name);
    ModelInfo {
        name: m.name,
        catalog_name: m.catalog_name,
        schema_name: m.schema_name,
        full_name,
        storage_location: m.storage_location,
        comment: m.comment,
        owner: m.owner,
        created_at: m.created_at,
        updated_at: m.updated_at,
        properties: m.properties,
        model_id: m.model_id,
    }
}

fn model_version_to_info(v: ProtoModelVersion) -> ModelVersionInfo {
    ModelVersionInfo {
        model_name: v.model_name,
        catalog_name: v.catalog_name,
        schema_name: v.schema_name,
        version: v.version,
        source: v.source,
        run_id: v.run_id,
        status: v.status,
        description: v.description,
        created_at: v.created_at,
        updated_at: v.updated_at,
        properties: v.properties,
        version_id: v.version_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_to_info_builds_full_name() {
        let s = ProtoSchema {
            catalog_name: "prod".into(),
            name: "sales".into(),
            ..ProtoSchema::default()
        };
        let info = schema_to_info(s);
        assert_eq!(info.full_name, "prod.sales");
    }

    #[test]
    fn table_to_info_parses_columns_json() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            type_text: "bigint".into(),
            type_json: String::new(),
            type_name: "LONG".into(),
            position: 0,
            nullable: false,
            comment: String::new(),
            partition_index: None,
        }];
        let columns_json = serde_json::to_string(&columns).unwrap();
        let t = ProtoTable {
            catalog_name: "prod".into(),
            schema_name: "sales".into(),
            name: "events".into(),
            columns_json,
            ..ProtoTable::default()
        };
        let info = table_to_info(t);
        assert_eq!(info.full_name, "prod.sales.events");
        assert_eq!(info.columns.len(), 1);
        assert_eq!(info.columns[0].name, "id");
    }

    #[test]
    fn table_to_info_tolerates_invalid_columns_json() {
        let t = ProtoTable {
            catalog_name: "p".into(),
            schema_name: "s".into(),
            name: "t".into(),
            columns_json: "not-json".into(),
            ..ProtoTable::default()
        };
        let info = table_to_info(t);
        assert!(info.columns.is_empty());
    }
}
