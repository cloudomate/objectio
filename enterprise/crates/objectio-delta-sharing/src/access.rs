//! Delta Sharing bearer token access control.
//!
//! Delta Sharing uses bearer tokens (not `SigV4`). On each request we look up the
//! recipient by hashing the presented token (SHA-256) and checking that the
//! requested share is in the recipient's share list.

use crate::catalog::DeltaCatalog;
use crate::error::DeltaError;
use axum::http::HeaderMap;
use sha2::{Digest, Sha256};

/// Extract the raw bearer token from the Authorization header.
#[must_use]
pub fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    let header = headers.get("Authorization")?.to_str().ok()?;
    header.strip_prefix("Bearer ").map(|s| s.trim().to_string())
}

/// Compute SHA-256 hex digest of the raw bearer token.
/// This mirrors what `delta_create_recipient` stores in the Meta service.
#[must_use]
pub fn hash_token(raw_token: &str) -> String {
    hex::encode(Sha256::digest(raw_token.as_bytes()))
}

/// Access context for a Delta Sharing request — the authenticated recipient.
#[derive(Clone, Debug)]
pub struct RecipientContext {
    pub name: String,
    pub shares: Vec<String>,
}

impl RecipientContext {
    /// Check that this recipient has access to `share`.
    #[must_use]
    pub fn has_share(&self, share: &str) -> bool {
        self.shares.iter().any(|s| s == share)
    }
}

/// Authenticate a Delta Sharing request and return the recipient context.
///
/// # Errors
/// Returns `DeltaError::forbidden` if the Authorization header is missing,
/// the bearer token is invalid, or the recipient does not have access to
/// the requested share.
pub async fn authenticate_request(
    headers: &HeaderMap,
    catalog: &DeltaCatalog,
    required_share: Option<&str>,
) -> Result<RecipientContext, DeltaError> {
    let raw_token = extract_bearer(headers)
        .ok_or_else(|| DeltaError::forbidden("Missing Authorization: Bearer <token> header"))?;

    let recipient = catalog
        .get_recipient_by_token(&raw_token)
        .await?
        .ok_or_else(|| DeltaError::forbidden("Invalid bearer token"))?;

    let ctx = RecipientContext {
        name: recipient.name,
        shares: recipient.shares,
    };

    if let Some(share) = required_share
        && !ctx.has_share(share)
    {
        return Err(DeltaError::forbidden(format!(
            "Recipient '{}' does not have access to share '{share}'",
            ctx.name
        )));
    }

    Ok(ctx)
}
