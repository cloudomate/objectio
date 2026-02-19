//! OSD Connection Pool for Block Gateway
//!
//! Adapted from bin/objectio-gateway/src/osd_pool.rs.

use objectio_proto::metadata::NodePlacement;
use objectio_proto::storage::storage_service_client::StorageServiceClient;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{error, info, warn};

/// Error type for OSD pool operations
#[derive(Debug, thiserror::Error)]
pub enum OsdPoolError {
    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("no nodes available")]
    #[allow(dead_code)]
    NoNodesAvailable,
}

/// Node identifier (16-byte UUID)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId([u8; 16]);

impl NodeId {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            Some(Self(arr))
        } else {
            None
        }
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl From<[u8; 16]> for NodeId {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

/// Information about a connected OSD node
#[derive(Clone)]
pub struct OsdNode {
    #[allow(dead_code)]
    pub node_id: NodeId,
    #[allow(dead_code)]
    pub address: String,
    pub client: StorageServiceClient<Channel>,
}

/// Pool of OSD connections for multi-node operations
pub struct OsdPool {
    nodes: RwLock<HashMap<NodeId, OsdNode>>,
    address_map: RwLock<HashMap<String, NodeId>>,
}

impl OsdPool {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            address_map: RwLock::new(HashMap::new()),
        }
    }

    pub async fn connect(&self, node_id: NodeId, address: &str) -> Result<(), OsdPoolError> {
        let mut nodes = self.nodes.write().await;
        if nodes.contains_key(&node_id) {
            return Ok(());
        }

        let address_map = self.address_map.read().await;
        if let Some(existing_node_id) = address_map.get(address).cloned() {
            drop(address_map);
            if let Some(existing_node) = nodes.get(&existing_node_id).cloned() {
                let aliased_node = OsdNode {
                    node_id: node_id.clone(),
                    address: address.to_string(),
                    client: existing_node.client,
                };
                nodes.insert(node_id, aliased_node);
                return Ok(());
            }
        } else {
            drop(address_map);
        }

        drop(nodes);

        let max_message_size = 100 * 1024 * 1024;
        let channel = tonic::transport::Endpoint::new(address.to_string())
            .map_err(|e| OsdPoolError::ConnectionFailed(e.to_string()))?
            .connect()
            .await
            .map_err(|e| OsdPoolError::ConnectionFailed(e.to_string()))?;

        let client = StorageServiceClient::new(channel)
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);

        let mut nodes = self.nodes.write().await;
        if nodes.contains_key(&node_id) {
            return Ok(());
        }

        let node = OsdNode {
            node_id: node_id.clone(),
            address: address.to_string(),
            client,
        };
        nodes.insert(node_id.clone(), node);
        drop(nodes);

        self.address_map
            .write()
            .await
            .insert(address.to_string(), node_id);

        info!("Connected to OSD at {}", address);
        Ok(())
    }

    pub async fn get_or_connect(
        &self,
        node_id: &[u8],
        address: &str,
    ) -> Result<StorageServiceClient<Channel>, OsdPoolError> {
        let id = NodeId::from_bytes(node_id)
            .ok_or_else(|| OsdPoolError::NodeNotFound("invalid node ID".to_string()))?;

        if let Some(node) = self.nodes.read().await.get(&id) {
            return Ok(node.client.clone());
        }

        self.connect(id.clone(), address).await?;

        self.nodes
            .read()
            .await
            .get(&id)
            .map(|n| n.client.clone())
            .ok_or_else(|| OsdPoolError::NodeNotFound(id.to_hex()))
    }

    pub async fn get_client_for_placement(
        &self,
        placement: &NodePlacement,
    ) -> Result<StorageServiceClient<Channel>, OsdPoolError> {
        self.get_or_connect(&placement.node_id, &placement.node_address)
            .await
    }
}

impl Default for OsdPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Write a shard to the appropriate OSD
#[allow(clippy::too_many_arguments)]
pub async fn write_shard_to_osd(
    pool: &OsdPool,
    placement: &NodePlacement,
    object_id: &[u8],
    stripe_id: u64,
    position: u32,
    data: Vec<u8>,
    ec_k: u32,
    ec_m: u32,
) -> Result<objectio_proto::storage::BlockLocation, OsdPoolError> {
    use objectio_proto::storage::{Checksum, ShardId, WriteShardRequest};

    let mut client = pool.get_client_for_placement(placement).await?;

    let request = WriteShardRequest {
        shard_id: Some(ShardId {
            object_id: object_id.to_vec(),
            stripe_id,
            position,
        }),
        data: data.clone(),
        ec_k,
        ec_m,
        checksum: Some(Checksum {
            crc32c: crc32c::crc32c(&data),
            xxhash64: 0,
            sha256: vec![],
        }),
    };

    let write_future = client.write_shard(request);
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), write_future)
        .await
        .map_err(|_| {
            error!(
                "Timeout writing shard {} to OSD {}",
                position, placement.node_address
            );
            OsdPoolError::ConnectionFailed("write timeout".to_string())
        })?
        .map_err(|e| {
            error!(
                "Failed to write shard to OSD {}: {}",
                placement.node_address, e
            );
            OsdPoolError::ConnectionFailed(e.to_string())
        })?;

    response
        .into_inner()
        .location
        .ok_or_else(|| OsdPoolError::ConnectionFailed("no location returned".to_string()))
}

/// Read a shard from the appropriate OSD
pub async fn read_shard_from_osd(
    pool: &OsdPool,
    placement: &NodePlacement,
    object_id: &[u8],
    stripe_id: u64,
    position: u32,
) -> Result<Vec<u8>, OsdPoolError> {
    use objectio_proto::storage::{ReadShardRequest, ShardId};

    let mut client = pool.get_client_for_placement(placement).await?;

    let request = ReadShardRequest {
        shard_id: Some(ShardId {
            object_id: object_id.to_vec(),
            stripe_id,
            position,
        }),
        offset: 0,
        length: 0,
    };

    let read_future = client.read_shard(request);
    let response = tokio::time::timeout(std::time::Duration::from_secs(10), read_future)
        .await
        .map_err(|_| {
            error!(
                "Timeout reading shard {} from OSD {}",
                position, placement.node_address
            );
            OsdPoolError::ConnectionFailed("read timeout".to_string())
        })?
        .map_err(|e| {
            warn!(
                "Failed to read shard from OSD {}: {}",
                placement.node_address, e
            );
            OsdPoolError::ConnectionFailed(e.to_string())
        })?;

    Ok(response.into_inner().data)
}

/// Store object metadata on the primary OSD
pub async fn put_object_meta_to_osd(
    pool: &OsdPool,
    primary_placement: &NodePlacement,
    bucket: &str,
    key: &str,
    object_meta: objectio_proto::metadata::ObjectMeta,
) -> Result<(), OsdPoolError> {
    use objectio_proto::storage::PutObjectMetaRequest;

    let mut client = pool.get_client_for_placement(primary_placement).await?;

    let request = PutObjectMetaRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        object: Some(object_meta),
    };

    let put_future = client.put_object_meta(request);
    tokio::time::timeout(std::time::Duration::from_secs(10), put_future)
        .await
        .map_err(|_| {
            error!(
                "Timeout putting object metadata to OSD {}",
                primary_placement.node_address
            );
            OsdPoolError::ConnectionFailed("put_object_meta timeout".to_string())
        })?
        .map_err(|e| {
            error!(
                "Failed to put object metadata to OSD {}: {}",
                primary_placement.node_address, e
            );
            OsdPoolError::ConnectionFailed(e.to_string())
        })?;

    Ok(())
}

/// Get object metadata from the primary OSD
pub async fn get_object_meta_from_osd(
    pool: &OsdPool,
    primary_placement: &NodePlacement,
    bucket: &str,
    key: &str,
) -> Result<Option<objectio_proto::metadata::ObjectMeta>, OsdPoolError> {
    use objectio_proto::storage::GetObjectMetaRequest;

    let mut client = pool.get_client_for_placement(primary_placement).await?;

    let request = GetObjectMetaRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: String::new(),
    };

    let get_future = client.get_object_meta(request);
    let response = tokio::time::timeout(std::time::Duration::from_secs(10), get_future)
        .await
        .map_err(|_| {
            error!(
                "Timeout getting object metadata from OSD {}",
                primary_placement.node_address
            );
            OsdPoolError::ConnectionFailed("get_object_meta timeout".to_string())
        })?
        .map_err(|e| {
            warn!(
                "Failed to get object metadata from OSD {}: {}",
                primary_placement.node_address, e
            );
            OsdPoolError::ConnectionFailed(e.to_string())
        })?;

    let inner = response.into_inner();
    if inner.found {
        Ok(inner.object)
    } else {
        Ok(None)
    }
}

/// Delete object metadata from the primary OSD
#[allow(dead_code)]
pub async fn delete_object_meta_from_osd(
    pool: &OsdPool,
    primary_placement: &NodePlacement,
    bucket: &str,
    key: &str,
) -> Result<(), OsdPoolError> {
    use objectio_proto::storage::DeleteObjectMetaRequest;

    let mut client = pool.get_client_for_placement(primary_placement).await?;

    let request = DeleteObjectMetaRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: String::new(),
    };

    let delete_future = client.delete_object_meta(request);
    tokio::time::timeout(std::time::Duration::from_secs(10), delete_future)
        .await
        .map_err(|_| {
            error!(
                "Timeout deleting object metadata from OSD {}",
                primary_placement.node_address
            );
            OsdPoolError::ConnectionFailed("delete_object_meta timeout".to_string())
        })?
        .map_err(|e| {
            warn!(
                "Failed to delete object metadata from OSD {}: {}",
                primary_placement.node_address, e
            );
            OsdPoolError::ConnectionFailed(e.to_string())
        })?;

    Ok(())
}
