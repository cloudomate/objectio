//! Delta Sharing catalog — thin gRPC wrapper around the Meta service.

use crate::error::DeltaError;
use objectio_proto::metadata::{
    DeltaAddTableRequest, DeltaCreateRecipientRequest, DeltaCreateShareRequest,
    DeltaDropRecipientRequest, DeltaDropShareRequest, DeltaGetRecipientByTokenRequest,
    DeltaGetShareRequest, DeltaListRecipientsRequest, DeltaListSharesRequest,
    DeltaListTablesRequest, DeltaRecipientEntry, DeltaRemoveTableRequest, DeltaShareEntry,
    DeltaShareTableEntry, metadata_service_client::MetadataServiceClient,
};
use tonic::transport::Channel;

#[derive(Clone)]
pub struct DeltaCatalog {
    client: MetadataServiceClient<Channel>,
}

impl DeltaCatalog {
    #[must_use]
    pub const fn new(client: MetadataServiceClient<Channel>) -> Self {
        Self { client }
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails or the response contains no share.
    pub async fn create_share(&self, name: &str, comment: &str) -> Result<DeltaShareEntry, DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_create_share(DeltaCreateShareRequest {
                name: name.to_string(),
                comment: comment.to_string(),
            })
            .await?;
        resp.into_inner().share.ok_or_else(|| DeltaError::internal("no share in response"))
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails or the share is not found.
    pub async fn get_share(&self, name: &str) -> Result<DeltaShareEntry, DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_get_share(DeltaGetShareRequest { name: name.to_string() })
            .await?;
        resp.into_inner().share.ok_or_else(|| DeltaError::not_found(format!("share not found: {name}")))
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn list_shares(&self) -> Result<Vec<DeltaShareEntry>, DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_list_shares(DeltaListSharesRequest { max_results: 0, page_token: String::new() })
            .await?;
        Ok(resp.into_inner().shares)
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn drop_share(&self, name: &str) -> Result<(), DeltaError> {
        self.client
            .clone()
            .delta_drop_share(DeltaDropShareRequest { name: name.to_string() })
            .await?;
        Ok(())
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails or the response contains no table.
    pub async fn add_table(
        &self,
        share: &str,
        schema: &str,
        name: &str,
    ) -> Result<DeltaShareTableEntry, DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_add_table(DeltaAddTableRequest {
                share: share.to_string(),
                schema: schema.to_string(),
                table_name: name.to_string(),
            })
            .await?;
        resp.into_inner().table.ok_or_else(|| DeltaError::internal("no table in response"))
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn remove_table(&self, share: &str, schema: &str, name: &str) -> Result<(), DeltaError> {
        self.client
            .clone()
            .delta_remove_table(DeltaRemoveTableRequest {
                share: share.to_string(),
                schema: schema.to_string(),
                table_name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn list_tables(
        &self,
        share: &str,
        schema: &str,
    ) -> Result<Vec<DeltaShareTableEntry>, DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_list_tables(DeltaListTablesRequest {
                share: share.to_string(),
                schema: schema.to_string(),
                max_results: 0,
                page_token: String::new(),
            })
            .await?;
        Ok(resp.into_inner().tables)
    }

    /// List all tables across all schemas in a share.
    ///
    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn list_all_tables_in_share(
        &self,
        share: &str,
    ) -> Result<Vec<DeltaShareTableEntry>, DeltaError> {
        self.list_tables(share, "").await
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails or the response contains no recipient.
    pub async fn create_recipient(
        &self,
        name: &str,
        shares: Vec<String>,
    ) -> Result<(DeltaRecipientEntry, String), DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_create_recipient(DeltaCreateRecipientRequest {
                name: name.to_string(),
                shares,
            })
            .await?;
        let inner = resp.into_inner();
        let recipient = inner
            .recipient
            .ok_or_else(|| DeltaError::internal("no recipient in response"))?;
        Ok((recipient, inner.raw_token))
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn get_recipient_by_token(
        &self,
        token: &str,
    ) -> Result<Option<DeltaRecipientEntry>, DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_get_recipient_by_token(DeltaGetRecipientByTokenRequest {
                raw_token: token.to_string(),
            })
            .await?;
        let inner = resp.into_inner();
        if inner.found {
            Ok(inner.recipient)
        } else {
            Ok(None)
        }
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn list_recipients(&self) -> Result<Vec<DeltaRecipientEntry>, DeltaError> {
        let resp = self
            .client
            .clone()
            .delta_list_recipients(DeltaListRecipientsRequest {
                max_results: 0,
                page_token: String::new(),
            })
            .await?;
        Ok(resp.into_inner().recipients)
    }

    /// # Errors
    /// Returns `DeltaError` if the gRPC call fails.
    pub async fn drop_recipient(&self, name: &str) -> Result<(), DeltaError> {
        self.client
            .clone()
            .delta_drop_recipient(DeltaDropRecipientRequest { name: name.to_string() })
            .await?;
        Ok(())
    }
}
