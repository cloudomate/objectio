//! Catalog logic: wraps Meta gRPC calls for Iceberg namespace and table operations.

use crate::error::IcebergError;
use crate::types;
use objectio_proto::metadata::{
    CreateDataFilterRequest, DeleteDataFilterRequest, GetDataFiltersForPrincipalRequest,
    IcebergCommitTableRequest, IcebergCreateNamespaceRequest, IcebergCreateTableRequest,
    IcebergDataFilter, IcebergDropNamespaceRequest, IcebergDropTableRequest,
    IcebergGetTablePolicyRequest, IcebergListNamespacesRequest, IcebergListTablesRequest,
    IcebergLoadNamespaceRequest, IcebergLoadTableRequest, IcebergNamespaceExistsRequest,
    IcebergRenameTableRequest, IcebergSetTablePolicyRequest, IcebergTableExistsRequest,
    IcebergTableIdentifier, IcebergUpdateNamespacePropertiesRequest, ListDataFiltersRequest,
    metadata_service_client::MetadataServiceClient,
};
use tonic::transport::Channel;

type Result<T> = std::result::Result<T, IcebergError>;

/// Parameters for creating a new data filter.
pub struct NewDataFilter<'a> {
    pub ns_levels: Vec<String>,
    pub table_name: &'a str,
    pub filter_name: &'a str,
    pub principal_arns: Vec<String>,
    pub allowed_columns: Vec<String>,
    pub excluded_columns: Vec<String>,
    pub row_filter_expression: &'a str,
}

/// Thin catalog layer that delegates to the Meta gRPC service.
#[derive(Clone)]
pub struct IcebergCatalog {
    meta_client: MetadataServiceClient<Channel>,
}

impl IcebergCatalog {
    #[must_use]
    pub const fn new(meta_client: MetadataServiceClient<Channel>) -> Self {
        Self { meta_client }
    }

    // ---- Namespace operations ----

    /// Create a new namespace with the given levels and properties.
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace already exists or parent is missing.
    pub async fn create_namespace(
        &self,
        levels: Vec<String>,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<types::CreateNamespaceResponse> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_create_namespace(IcebergCreateNamespaceRequest {
                namespace_levels: levels,
                properties,
            })
            .await?
            .into_inner();

        Ok(types::CreateNamespaceResponse {
            namespace: resp.namespace_levels,
            properties: resp.properties,
        })
    }

    /// Load namespace properties.
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace is not found.
    pub async fn load_namespace(
        &self,
        levels: Vec<String>,
    ) -> Result<types::LoadNamespaceResponse> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_load_namespace(IcebergLoadNamespaceRequest {
                namespace_levels: levels,
            })
            .await?
            .into_inner();

        Ok(types::LoadNamespaceResponse {
            namespace: resp.namespace_levels,
            properties: resp.properties,
        })
    }

    /// Drop a namespace.
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace is not found or not empty.
    pub async fn drop_namespace(&self, levels: Vec<String>) -> Result<()> {
        self.meta_client
            .clone()
            .iceberg_drop_namespace(IcebergDropNamespaceRequest {
                namespace_levels: levels,
            })
            .await?;
        Ok(())
    }

    /// List namespaces, optionally under a parent, with pagination.
    ///
    /// # Errors
    /// Returns `IcebergError` if the parent namespace is not found.
    pub async fn list_namespaces(
        &self,
        parent: Option<Vec<String>>,
        page_token: Option<String>,
        page_size: Option<u32>,
    ) -> Result<(Vec<Vec<String>>, Option<String>)> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_list_namespaces(IcebergListNamespacesRequest {
                parent_levels: parent.unwrap_or_default(),
                page_token: page_token.unwrap_or_default(),
                page_size: page_size.unwrap_or(0),
            })
            .await?
            .into_inner();

        let next = if resp.next_page_token.is_empty() {
            None
        } else {
            Some(resp.next_page_token)
        };

        Ok((
            resp.namespaces.into_iter().map(|ns| ns.levels).collect(),
            next,
        ))
    }

    /// Update namespace properties (add/remove keys).
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace is not found.
    pub async fn update_namespace_properties(
        &self,
        levels: Vec<String>,
        removals: Vec<String>,
        updates: std::collections::HashMap<String, String>,
    ) -> Result<types::UpdateNamespacePropertiesResponse> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_update_namespace_properties(IcebergUpdateNamespacePropertiesRequest {
                namespace_levels: levels,
                removals,
                updates,
            })
            .await?
            .into_inner();

        Ok(types::UpdateNamespacePropertiesResponse {
            updated: resp.updated,
            removed: resp.removed,
            missing: resp.missing,
        })
    }

    /// Check if a namespace exists.
    ///
    /// # Errors
    /// Returns `IcebergError` on gRPC communication failure.
    pub async fn namespace_exists(&self, levels: Vec<String>) -> Result<bool> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_namespace_exists(IcebergNamespaceExistsRequest {
                namespace_levels: levels,
            })
            .await?
            .into_inner();
        Ok(resp.exists)
    }

    // ---- Table operations ----

    /// Register a new table in the catalog with its metadata JSON.
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace is not found or table already exists.
    pub async fn create_table(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
        metadata_location: &str,
        metadata_json: Vec<u8>,
    ) -> Result<(String, Vec<u8>)> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_create_table(IcebergCreateTableRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
                metadata_location: metadata_location.to_string(),
                metadata_json,
            })
            .await?
            .into_inner();

        Ok((resp.metadata_location, resp.metadata_json))
    }

    /// Load a table's current metadata location and metadata JSON.
    ///
    /// # Errors
    /// Returns `IcebergError` if the table is not found.
    pub async fn load_table(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
    ) -> Result<(String, Vec<u8>)> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_load_table(IcebergLoadTableRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
            })
            .await?
            .into_inner();

        Ok((resp.metadata_location, resp.metadata_json))
    }

    /// Atomically commit a table metadata update (CAS) with new metadata JSON.
    ///
    /// # Errors
    /// Returns `IcebergError` if the table is not found or the commit conflicts.
    pub async fn commit_table(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
        current_metadata_location: &str,
        new_metadata_location: &str,
        new_metadata_json: Vec<u8>,
    ) -> Result<(String, Vec<u8>)> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_commit_table(IcebergCommitTableRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
                current_metadata_location: current_metadata_location.to_string(),
                new_metadata_location: new_metadata_location.to_string(),
                new_metadata_json,
            })
            .await?
            .into_inner();

        Ok((resp.metadata_location, resp.metadata_json))
    }

    /// Drop a table from the catalog.
    ///
    /// # Errors
    /// Returns `IcebergError` if the table is not found.
    pub async fn drop_table(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
        purge: bool,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .iceberg_drop_table(IcebergDropTableRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
                purge,
            })
            .await?;
        Ok(())
    }

    /// Rename a table, possibly across namespaces.
    ///
    /// # Errors
    /// Returns `IcebergError` if source/destination has issues.
    pub async fn rename_table(
        &self,
        source_ns: Vec<String>,
        source_name: &str,
        dest_ns: Vec<String>,
        dest_name: &str,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .iceberg_rename_table(IcebergRenameTableRequest {
                source: Some(IcebergTableIdentifier {
                    namespace_levels: source_ns,
                    name: source_name.to_string(),
                }),
                destination: Some(IcebergTableIdentifier {
                    namespace_levels: dest_ns,
                    name: dest_name.to_string(),
                }),
            })
            .await?;
        Ok(())
    }

    /// List all tables in a namespace with pagination.
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace is not found.
    pub async fn list_tables(
        &self,
        ns_levels: Vec<String>,
        page_token: Option<String>,
        page_size: Option<u32>,
    ) -> Result<(Vec<types::TableIdentifier>, Option<String>)> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_list_tables(IcebergListTablesRequest {
                namespace_levels: ns_levels,
                page_token: page_token.unwrap_or_default(),
                page_size: page_size.unwrap_or(0),
            })
            .await?
            .into_inner();

        let next = if resp.next_page_token.is_empty() {
            None
        } else {
            Some(resp.next_page_token)
        };

        Ok((
            resp.identifiers
                .into_iter()
                .map(|id| types::TableIdentifier {
                    namespace: id.namespace_levels,
                    name: id.name,
                })
                .collect(),
            next,
        ))
    }

    /// Check if a table exists.
    ///
    /// # Errors
    /// Returns `IcebergError` on gRPC communication failure.
    pub async fn table_exists(&self, ns_levels: Vec<String>, table_name: &str) -> Result<bool> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_table_exists(IcebergTableExistsRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.exists)
    }

    /// Get a table's access policy JSON.
    ///
    /// # Errors
    /// Returns `IcebergError` if the table is not found.
    pub async fn get_table_policy(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
    ) -> Result<Option<Vec<u8>>> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_get_table_policy(IcebergGetTablePolicyRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
            })
            .await?
            .into_inner();
        if resp.policy_json.is_empty() {
            Ok(None)
        } else {
            Ok(Some(resp.policy_json))
        }
    }

    /// Set a table's access policy JSON.
    ///
    /// # Errors
    /// Returns `IcebergError` if the table is not found.
    pub async fn set_table_policy(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
        policy_json: Vec<u8>,
    ) -> Result<()> {
        self.meta_client
            .clone()
            .iceberg_set_table_policy(IcebergSetTablePolicyRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
                policy_json,
            })
            .await?;
        Ok(())
    }

    // ---- Data filter operations ----

    /// Create a data filter for a table.
    ///
    /// # Errors
    /// Returns `IcebergError` on gRPC failure.
    pub async fn create_data_filter(&self, params: NewDataFilter<'_>) -> Result<IcebergDataFilter> {
        let resp = self
            .meta_client
            .clone()
            .create_data_filter(CreateDataFilterRequest {
                namespace_levels: params.ns_levels,
                table_name: params.table_name.to_string(),
                filter_name: params.filter_name.to_string(),
                principal_arns: params.principal_arns,
                allowed_columns: params.allowed_columns,
                excluded_columns: params.excluded_columns,
                row_filter_expression: params.row_filter_expression.to_string(),
            })
            .await?
            .into_inner();

        resp.filter
            .ok_or_else(|| IcebergError::internal("missing filter in response"))
    }

    /// List data filters for a table.
    ///
    /// # Errors
    /// Returns `IcebergError` on gRPC failure.
    pub async fn list_data_filters(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
    ) -> Result<Vec<IcebergDataFilter>> {
        let resp = self
            .meta_client
            .clone()
            .list_data_filters(ListDataFiltersRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
            })
            .await?
            .into_inner();

        Ok(resp.filters)
    }

    /// Delete a data filter by ID.
    ///
    /// # Errors
    /// Returns `IcebergError` on gRPC failure.
    pub async fn delete_data_filter(&self, filter_id: &str) -> Result<bool> {
        let resp = self
            .meta_client
            .clone()
            .delete_data_filter(DeleteDataFilterRequest {
                filter_id: filter_id.to_string(),
            })
            .await?
            .into_inner();

        Ok(resp.success)
    }

    /// Get data filters applicable to a principal for a given table.
    ///
    /// # Errors
    /// Returns `IcebergError` on gRPC failure.
    pub async fn get_data_filters_for_principal(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
        principal_arn: &str,
        group_arns: Vec<String>,
    ) -> Result<Vec<IcebergDataFilter>> {
        let resp = self
            .meta_client
            .clone()
            .get_data_filters_for_principal(GetDataFiltersForPrincipalRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
                principal_arn: principal_arn.to_string(),
                group_arns,
            })
            .await?
            .into_inner();

        Ok(resp.filters)
    }
}
