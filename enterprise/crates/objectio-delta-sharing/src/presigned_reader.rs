//! `DeltaLogReader` that fetches commits via presigned S3 URLs.
//!
//! Uses `objectio_auth::presign::{presign_get, presign_list_objects_v2}` to
//! sign each request with admin credentials, then plain HTTP via `reqwest`.
//! This works for any tenant's bucket reachable via the gateway endpoint —
//! cross-tenant Delta sharing is supported by design.

use crate::delta_log::{DeltaLogReader, LogListing, is_checkpoint_key, parse_commit_version};
use crate::error::DeltaError;
use objectio_auth::presign::{presign_get, presign_list_objects_v2};
use serde::Deserialize;
use std::time::Duration;

/// Default lifetime of presigned URLs used to fetch `_delta_log/` files.
pub const DEFAULT_LOG_URL_TTL: Duration = Duration::from_secs(900);

/// Cap on how many list-objects pages we follow before bailing — prevents an
/// adversarial bucket with millions of stale commit files from hanging us.
const MAX_LIST_PAGES: usize = 100;

/// Per-page max-keys used when listing `_delta_log/`.
const LIST_PAGE_SIZE: u32 = 1000;

/// Reader that fetches `_delta_log/` files via gateway-presigned URLs.
pub struct PresignedHttpReader<'a> {
    pub endpoint: &'a str,
    pub region: &'a str,
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
    pub url_ttl: Duration,
    pub http: &'a reqwest::Client,
}

impl<'a> PresignedHttpReader<'a> {
    #[must_use]
    pub const fn new(
        endpoint: &'a str,
        region: &'a str,
        access_key_id: &'a str,
        secret_access_key: &'a str,
        url_ttl: Duration,
        http: &'a reqwest::Client,
    ) -> Self {
        Self {
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            url_ttl,
            http,
        }
    }
}

impl DeltaLogReader for PresignedHttpReader<'_> {
    async fn list_log(&self, bucket: &str, table_root: &str) -> Result<LogListing, DeltaError> {
        let prefix = crate::delta_log::log_prefix(table_root);
        let mut commits: Vec<i64> = Vec::new();
        let mut has_checkpoint = false;
        let mut continuation_token: Option<String> = None;

        for _ in 0..MAX_LIST_PAGES {
            let url = presign_list_objects_v2(
                self.endpoint,
                self.region,
                self.access_key_id,
                self.secret_access_key,
                bucket,
                &prefix,
                continuation_token.as_deref(),
                Some(LIST_PAGE_SIZE),
                self.url_ttl,
            );
            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| reqwest_err(&e))?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(DeltaError::internal(format!(
                    "list _delta_log/ failed (HTTP {status}): {body}",
                )));
            }
            let xml = resp.text().await.map_err(|e| reqwest_err(&e))?;
            let parsed: ListBucketResult = quick_xml::de::from_str(&xml)
                .map_err(|e| DeltaError::internal(format!("ListObjectsV2 XML parse error: {e}")))?;

            for content in &parsed.contents {
                if is_checkpoint_key(&content.key) {
                    has_checkpoint = true;
                } else if let Some(v) = parse_commit_version(&content.key) {
                    commits.push(v);
                }
            }

            if parsed.is_truncated && parsed.next_continuation_token.is_some() {
                continuation_token = parsed.next_continuation_token;
            } else {
                commits.sort_unstable();
                commits.dedup();
                return Ok(LogListing {
                    commits,
                    has_checkpoint,
                });
            }
        }

        Err(DeltaError::internal(format!(
            "ListObjectsV2 paginated past {MAX_LIST_PAGES} pages for s3://{bucket}/{prefix}",
        )))
    }

    async fn read_commit(
        &self,
        bucket: &str,
        table_root: &str,
        version: i64,
    ) -> Result<Vec<u8>, DeltaError> {
        let key = crate::delta_log::commit_key(table_root, version);
        let url = presign_get(
            self.endpoint,
            self.region,
            self.access_key_id,
            self.secret_access_key,
            bucket,
            &key,
            self.url_ttl,
        );
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| reqwest_err(&e))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(DeltaError::internal(format!(
                "fetch s3://{bucket}/{key} failed (HTTP {status}): {body}",
            )));
        }
        let bytes = resp.bytes().await.map_err(|e| reqwest_err(&e))?;
        Ok(bytes.to_vec())
    }
}

fn reqwest_err(e: &reqwest::Error) -> DeltaError {
    DeltaError::internal(format!("delta log HTTP error: {e}"))
}

// ---- ListObjectsV2 XML deserialization ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(default)]
    contents: Vec<S3Object>,
    #[serde(default)]
    is_truncated: bool,
    #[serde(default)]
    next_continuation_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct S3Object {
    key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_listbucketresult() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>lake</Name>
  <Prefix>warehouse/events/_delta_log/</Prefix>
  <KeyCount>3</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>warehouse/events/_delta_log/00000000000000000000.json</Key>
    <Size>200</Size>
  </Contents>
  <Contents>
    <Key>warehouse/events/_delta_log/00000000000000000001.json</Key>
    <Size>300</Size>
  </Contents>
  <Contents>
    <Key>warehouse/events/_delta_log/_last_checkpoint</Key>
    <Size>50</Size>
  </Contents>
</ListBucketResult>"#;
        let parsed: ListBucketResult = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(parsed.contents.len(), 3);
        assert!(!parsed.is_truncated);
        assert!(parsed.next_continuation_token.is_none());
        assert_eq!(
            parsed.contents[0].key,
            "warehouse/events/_delta_log/00000000000000000000.json"
        );
    }

    #[test]
    fn deserializes_truncated_listbucketresult() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>lake</Name>
  <KeyCount>1</KeyCount>
  <MaxKeys>1</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>abc/def==</NextContinuationToken>
  <Contents>
    <Key>foo/_delta_log/00000000000000000000.json</Key>
    <Size>100</Size>
  </Contents>
</ListBucketResult>"#;
        let parsed: ListBucketResult = quick_xml::de::from_str(xml).unwrap();
        assert!(parsed.is_truncated);
        assert_eq!(parsed.next_continuation_token.as_deref(), Some("abc/def=="));
    }
}
