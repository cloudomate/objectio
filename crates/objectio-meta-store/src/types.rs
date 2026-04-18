//! Stored types for metadata persistence.
//!
//! These types are serialized to redb via bincode. Proto types embedded
//! in these structs use dedicated serde wrapper modules for encoding.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Serde wrapper for `Vec<StripeMeta>` (prost type in bincode)
mod stripe_meta_vec {
    use prost::Message;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        items: &[objectio_proto::metadata::StripeMeta],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded: Vec<Vec<u8>> = items.iter().map(Message::encode_to_vec).collect();
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Vec<objectio_proto::metadata::StripeMeta>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded: Vec<Vec<u8>> = Vec::<Vec<u8>>::deserialize(deserializer)?;
        encoded
            .into_iter()
            .map(|bytes| {
                objectio_proto::metadata::StripeMeta::decode(bytes.as_slice())
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

/// Serde wrapper for `Option<VolumeQos>` (prost type in bincode)
mod volume_qos_option {
    use prost::Message;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        value: &Option<objectio_proto::block::VolumeQos>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded: Option<Vec<u8>> = value.as_ref().map(Message::encode_to_vec);
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<objectio_proto::block::VolumeQos>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded: Option<Vec<u8>> = Option::<Vec<u8>>::deserialize(deserializer)?;
        encoded
            .map(|bytes| {
                objectio_proto::block::VolumeQos::decode(bytes.as_slice())
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
    }
}

// ---- S3 / Cluster types ----

/// OSD node information for placement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OsdNode {
    pub node_id: [u8; 16],
    pub address: String,
    pub disk_ids: Vec<[u8; 16]>,
    /// Legacy 3-tuple `(region, datacenter, rack)`. Kept for on-disk
    /// back-compat; populated from `topology` below when present.
    pub failure_domain: Option<(String, String, String)>,
    /// Full 5-level topology `(region, zone, datacenter, rack, host)`.
    /// Populated by modern OSDs at registration; `None` on data serialized
    /// before this field existed — in that case the service falls back to
    /// `failure_domain` plus empty zone/host.
    #[serde(default)]
    pub topology: Option<(String, String, String, String, String)>,
    /// Raw capacity per disk in bytes, index-aligned with `disk_ids`. Empty
    /// when the registering OSD predates the field; license capacity checks
    /// treat missing entries as 0 bytes (conservative — under-reports).
    #[serde(default)]
    pub disk_capacity_bytes: Vec<u64>,
    /// Operator-set intent — `In` (default, participating), `Out`
    /// (forced out of placement), or `Draining`. Independent of
    /// heartbeat-derived `NodeStatus` used by the topology; the service
    /// merges the two when rebuilding CRUSH.
    ///
    /// `#[serde(default)]` so existing serialized OSDs load as `In`.
    #[serde(default)]
    pub admin_state: objectio_common::OsdAdminState,
}

/// EC configuration for a storage class
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EcConfig {
    Mds { k: u8, m: u8 },
    Lrc { k: u8, l: u8, g: u8 },
    Replication { count: u8 },
}

impl Default for EcConfig {
    fn default() -> Self {
        Self::Mds { k: 4, m: 2 }
    }
}

/// State for an in-progress multipart upload
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MultipartUploadState {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub content_type: String,
    pub user_metadata: HashMap<String, String>,
    pub initiated: u64,
    pub parts: HashMap<u32, PartState>,
    /// Server-side encryption for this upload. Fixed at CreateMultipartUpload
    /// time; per-UploadPart SSE headers are ignored (AWS semantics). Values
    /// map to `SseAlgorithm` in the proto (0 = none, 1 = AES256, 2 = aws:kms,
    /// 3 = sse-c).
    #[serde(default)]
    pub encryption_algorithm: i32,
    #[serde(default)]
    pub kms_key_id: String,
    #[serde(default)]
    pub encrypted_dek: Vec<u8>,
    /// SSE-C only: base64-encoded MD5 of the customer key provided at
    /// CreateMultipartUpload. Every UploadPart must resupply a matching key;
    /// we never store the raw key bytes.
    #[serde(default)]
    pub customer_key_md5: String,
    /// SSE-KMS only: encryption context supplied at CreateMultipartUpload.
    /// Round-tripped to UploadPart's KmsProvider::decrypt call so the AEAD
    /// binding on the wrapped DEK still validates.
    #[serde(default)]
    pub encryption_context: HashMap<String, String>,
}

/// State for a completed part within a multipart upload
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartState {
    pub part_number: u32,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    #[serde(with = "stripe_meta_vec")]
    pub stripes: Vec<objectio_proto::metadata::StripeMeta>,
}

// ---- IAM types ----

/// Internal user storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredUser {
    pub user_id: String,
    pub display_name: String,
    pub arn: String,
    pub status: i32,
    pub created_at: u64,
    pub email: String,
    #[serde(default)]
    pub tenant: String,
}

/// Internal access key storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredAccessKey {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub user_id: String,
    pub status: i32,
    pub created_at: u64,
    #[serde(default)]
    pub tenant: String,
}

/// Internal group storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredGroup {
    pub group_id: String,
    pub group_name: String,
    pub arn: String,
    pub member_user_ids: Vec<String>,
    pub created_at: u64,
}

// ---- Iceberg data filter types ----

/// Stored data filter for column/row-level security
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredDataFilter {
    pub filter_id: String,
    pub filter_name: String,
    pub namespace_levels: Vec<String>,
    pub table_name: String,
    pub principal_arns: Vec<String>,
    pub allowed_columns: Vec<String>,
    pub excluded_columns: Vec<String>,
    pub row_filter_expression: String,
    pub created_at: u64,
    pub updated_at: u64,
}

// ---- Block storage types ----

/// Stored volume metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredVolume {
    pub volume_id: String,
    pub name: String,
    pub size_bytes: u64,
    pub used_bytes: u64,
    pub pool: String,
    pub state: i32,
    pub created_at: u64,
    pub updated_at: u64,
    pub parent_snapshot_id: String,
    pub chunk_size_bytes: u32,
    pub metadata: HashMap<String, String>,
    #[serde(with = "volume_qos_option")]
    pub qos: Option<objectio_proto::block::VolumeQos>,
}

/// Stored snapshot metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub snapshot_id: String,
    pub volume_id: String,
    pub name: String,
    pub size_bytes: u64,
    pub unique_bytes: u64,
    pub state: i32,
    pub created_at: u64,
    pub metadata: HashMap<String, String>,
    pub chunk_refs: HashMap<u64, StoredChunkRef>,
}

/// Stored chunk reference
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredChunkRef {
    pub chunk_id: u64,
    pub object_key: String,
    pub etag: String,
    pub size_bytes: u64,
}

/// Stored attachment information (ephemeral, not persisted)
#[derive(Clone, Debug)]
pub struct StoredAttachment {
    pub volume_id: String,
    pub target_type: i32,
    pub target_address: String,
    pub initiator: String,
    pub attached_at: u64,
    pub read_only: bool,
}
