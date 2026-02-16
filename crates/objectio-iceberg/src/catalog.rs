//! Catalog logic: wraps Meta gRPC calls for Iceberg namespace and table operations.

use crate::error::IcebergError;
use crate::types;
use objectio_proto::metadata::{
    IcebergCommitTableRequest, IcebergCreateNamespaceRequest, IcebergCreateTableRequest,
    IcebergDropNamespaceRequest, IcebergDropTableRequest, IcebergListNamespacesRequest,
    IcebergListTablesRequest, IcebergLoadNamespaceRequest, IcebergLoadTableRequest,
    IcebergNamespaceExistsRequest, IcebergRenameTableRequest, IcebergTableExistsRequest,
    IcebergTableIdentifier, IcebergUpdateNamespacePropertiesRequest,
    metadata_service_client::MetadataServiceClient,
};
use tonic::transport::Channel;

type Result<T> = std::result::Result<T, IcebergError>;

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

    /// List namespaces, optionally under a parent.
    ///
    /// # Errors
    /// Returns `IcebergError` if the parent namespace is not found.
    pub async fn list_namespaces(&self, parent: Option<Vec<String>>) -> Result<Vec<Vec<String>>> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_list_namespaces(IcebergListNamespacesRequest {
                parent_levels: parent.unwrap_or_default(),
            })
            .await?
            .into_inner();

        Ok(resp.namespaces.into_iter().map(|ns| ns.levels).collect())
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

    /// Register a new table in the catalog.
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace is not found or table already exists.
    pub async fn create_table(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
        metadata_location: &str,
    ) -> Result<String> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_create_table(IcebergCreateTableRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
                metadata_location: metadata_location.to_string(),
            })
            .await?
            .into_inner();

        Ok(resp.metadata_location)
    }

    /// Load a table's current metadata location.
    ///
    /// # Errors
    /// Returns `IcebergError` if the table is not found.
    pub async fn load_table(&self, ns_levels: Vec<String>, table_name: &str) -> Result<String> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_load_table(IcebergLoadTableRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
            })
            .await?
            .into_inner();

        Ok(resp.metadata_location)
    }

    /// Atomically commit a table metadata update (CAS).
    ///
    /// # Errors
    /// Returns `IcebergError` if the table is not found or the commit conflicts.
    pub async fn commit_table(
        &self,
        ns_levels: Vec<String>,
        table_name: &str,
        current_metadata_location: &str,
        new_metadata_location: &str,
    ) -> Result<String> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_commit_table(IcebergCommitTableRequest {
                namespace_levels: ns_levels,
                table_name: table_name.to_string(),
                current_metadata_location: current_metadata_location.to_string(),
                new_metadata_location: new_metadata_location.to_string(),
            })
            .await?
            .into_inner();

        Ok(resp.metadata_location)
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

    /// List all tables in a namespace.
    ///
    /// # Errors
    /// Returns `IcebergError` if the namespace is not found.
    pub async fn list_tables(&self, ns_levels: Vec<String>) -> Result<Vec<types::TableIdentifier>> {
        let resp = self
            .meta_client
            .clone()
            .iceberg_list_tables(IcebergListTablesRequest {
                namespace_levels: ns_levels,
            })
            .await?
            .into_inner();

        Ok(resp
            .identifiers
            .into_iter()
            .map(|id| types::TableIdentifier {
                namespace: id.namespace_levels,
                name: id.name,
            })
            .collect())
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
}
