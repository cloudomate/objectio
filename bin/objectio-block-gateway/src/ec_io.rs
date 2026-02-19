//! Erasure-coded chunk I/O
//!
//! Encodes/decodes 4 MB chunks using the same EC path as the S3 gateway:
//!   GetPlacement → WriteShard/ReadShard per shard → PutObjectMeta

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use futures::future::join_all;
use objectio_common::ErasureConfig;
use objectio_erasure::ErasureCodec;
use objectio_proto::metadata::{
    GetPlacementRequest, NodePlacement, ObjectMeta, ShardLocation, StripeMeta,
    metadata_service_client::MetadataServiceClient,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{error, warn};
use uuid::Uuid;

use crate::osd_pool::{
    OsdPool, delete_object_meta_from_osd, get_object_meta_from_osd, put_object_meta_to_osd,
    read_shard_from_osd, write_shard_to_osd,
};

/// Bucket name reserved for all block chunks.
pub const BLOCK_BUCKET: &str = "__block__";

/// Derive the object key for a volume chunk.
pub fn chunk_object_key(volume_id: &str, chunk_id: u64) -> String {
    format!("vol_{volume_id}/chunk_{chunk_id:08x}")
}

/// Write `data` as an EC chunk to the OSD cluster.
///
/// Returns the object key stored in `__block__/<object_key>`.
pub async fn write_chunk(
    meta_client: Arc<Mutex<MetadataServiceClient<Channel>>>,
    osd_pool: &Arc<OsdPool>,
    volume_id: &str,
    chunk_id: u64,
    data: &[u8],
    ec_k: u32,
    ec_m: u32,
) -> Result<String> {
    let object_key = chunk_object_key(volume_id, chunk_id);

    // Get placement for this chunk
    let placement = meta_client
        .lock()
        .await
        .get_placement(GetPlacementRequest {
            bucket: BLOCK_BUCKET.to_string(),
            key: object_key.clone(),
            size: data.len() as u64,
            storage_class: String::new(),
        })
        .await
        .map_err(|e| anyhow!("GetPlacement failed: {e}"))?
        .into_inner();

    if placement.nodes.is_empty() {
        return Err(anyhow!("no placement nodes returned for chunk {chunk_id}"));
    }

    // EC encode
    let codec = ErasureCodec::new(ErasureConfig::new(ec_k as u8, ec_m as u8))
        .map_err(|e| anyhow!("erasure codec init: {e}"))?;

    let shards = codec
        .encode(data)
        .map_err(|e| anyhow!("erasure encode: {e}"))?;

    // Generate a unique object ID for this write
    let object_id = Uuid::new_v4();
    let object_id_bytes: Vec<u8> = object_id.as_bytes().to_vec();

    let total_shards = (ec_k + ec_m) as usize;

    // Write all shards in parallel
    let shard_futs: Vec<_> = shards
        .iter()
        .enumerate()
        .map(|(i, shard_data)| {
            let node_placement = if i < placement.nodes.len() {
                placement.nodes[i].clone()
            } else {
                // Fall back to round-robin if fewer placements than shards
                placement.nodes[i % placement.nodes.len()].clone()
            };
            let oid = object_id_bytes.clone();
            let sdata = shard_data.clone();
            let pool = Arc::clone(osd_pool);
            async move {
                write_shard_to_osd(&pool, &node_placement, &oid, 0, i as u32, sdata, ec_k, ec_m)
                    .await
            }
        })
        .collect();

    let results = join_all(shard_futs).await;

    let mut success_count = 0u32;
    for (i, res) in results.iter().enumerate() {
        match res {
            Ok(_) => success_count += 1,
            Err(e) => error!("Failed to write shard {i} for chunk {chunk_id}: {e}"),
        }
    }

    if success_count < ec_k {
        return Err(anyhow!(
            "only {success_count}/{total_shards} shards written for chunk {chunk_id}, need {ec_k}"
        ));
    }

    // Build shard location list from placement nodes
    let stripe_shards: Vec<ShardLocation> = placement
        .nodes
        .iter()
        .map(|n| ShardLocation {
            position: n.position,
            node_id: n.node_id.clone(),
            disk_id: n.disk_id.clone(),
            offset: 0,
            shard_type: n.shard_type,
            local_group: n.local_group,
        })
        .collect();

    let now = chrono::Utc::now().timestamp_millis() as u64;

    let object_meta = ObjectMeta {
        bucket: BLOCK_BUCKET.to_string(),
        key: object_key.clone(),
        object_id: object_id_bytes.clone(),
        size: data.len() as u64,
        etag: hex::encode(uuid::Uuid::new_v4().as_bytes()), // simple unique etag
        content_type: "application/octet-stream".to_string(),
        created_at: now,
        modified_at: now,
        storage_class: String::new(),
        user_metadata: HashMap::new(),
        version_id: String::new(),
        is_delete_marker: false,
        stripes: vec![StripeMeta {
            stripe_id: 0,
            ec_k,
            ec_m,
            shards: stripe_shards,
            ec_type: 0, // ErasureMds
            ec_local_parity: 0,
            ec_global_parity: 0,
            local_group_size: 0,
            data_size: data.len() as u64,
            object_id: object_id_bytes,
        }],
    };

    // Store object meta on primary OSD (position 0)
    let primary = &placement.nodes[0];
    put_object_meta_to_osd(osd_pool, primary, BLOCK_BUCKET, &object_key, object_meta)
        .await
        .map_err(|e| anyhow!("put_object_meta failed: {e}"))?;

    Ok(object_key)
}

/// Read and reconstruct a chunk from the OSD cluster.
///
/// `object_key` is the value previously returned by `write_chunk`.
pub async fn read_chunk(
    meta_client: Arc<Mutex<MetadataServiceClient<Channel>>>,
    osd_pool: &Arc<OsdPool>,
    object_key: &str,
    ec_k: u32,
    ec_m: u32,
) -> Result<Vec<u8>> {
    // Deterministic placement: same key → same nodes
    let placement = meta_client
        .lock()
        .await
        .get_placement(GetPlacementRequest {
            bucket: BLOCK_BUCKET.to_string(),
            key: object_key.to_string(),
            size: 0,
            storage_class: String::new(),
        })
        .await
        .map_err(|e| anyhow!("GetPlacement failed: {e}"))?
        .into_inner();

    if placement.nodes.is_empty() {
        return Err(anyhow!("no placement nodes for {object_key}"));
    }

    // Build position → node_address map
    let addr_map: HashMap<u32, NodePlacement> = placement
        .nodes
        .iter()
        .map(|n| (n.position, n.clone()))
        .collect();

    // Fetch ObjectMeta from the primary OSD
    let primary = &placement.nodes[0];
    let object_meta = get_object_meta_from_osd(osd_pool, primary, BLOCK_BUCKET, object_key)
        .await
        .map_err(|e| anyhow!("get_object_meta failed: {e}"))?
        .ok_or_else(|| anyhow!("object meta not found for {object_key}"))?;

    let stripe = object_meta
        .stripes
        .first()
        .ok_or_else(|| anyhow!("no stripes in object meta for {object_key}"))?;

    let object_id: &[u8] = if !stripe.object_id.is_empty() {
        &stripe.object_id
    } else {
        &object_meta.object_id
    };

    let original_size = stripe.data_size as usize;
    let total = (ec_k + ec_m) as usize;
    let mut shards: Vec<Option<Vec<u8>>> = vec![None; total];
    let mut read_count = 0usize;

    // Try to read at least ec_k shards (data shards first)
    for shard_loc in &stripe.shards {
        if read_count >= ec_k as usize {
            break;
        }
        let pos = shard_loc.position as usize;
        if pos >= total {
            continue;
        }
        let Some(node) = addr_map.get(&shard_loc.position) else {
            continue;
        };
        let node_placement = NodePlacement {
            position: shard_loc.position,
            node_id: shard_loc.node_id.clone(),
            node_address: node.node_address.clone(),
            disk_id: shard_loc.disk_id.clone(),
            shard_type: shard_loc.shard_type,
            local_group: shard_loc.local_group,
        };
        match read_shard_from_osd(osd_pool, &node_placement, object_id, 0, shard_loc.position).await
        {
            Ok(data) => {
                shards[pos] = Some(data);
                read_count += 1;
            }
            Err(e) => warn!("Failed to read shard {pos} for {object_key}: {e}"),
        }
    }

    if read_count < ec_k as usize {
        return Err(anyhow!(
            "insufficient shards for {object_key}: have {read_count}, need {ec_k}"
        ));
    }

    let codec = ErasureCodec::new(ErasureConfig::new(ec_k as u8, ec_m as u8))
        .map_err(|e| anyhow!("erasure codec init: {e}"))?;

    let decoded = codec
        .decode(&mut shards, original_size)
        .map_err(|e| anyhow!("erasure decode: {e}"))?;

    Ok(decoded)
}

/// Delete a chunk's object metadata from the OSD (for volume deletion).
#[allow(dead_code)]
pub async fn delete_chunk(
    meta_client: Arc<Mutex<MetadataServiceClient<Channel>>>,
    osd_pool: &Arc<OsdPool>,
    object_key: &str,
) -> Result<()> {
    let placement = meta_client
        .lock()
        .await
        .get_placement(GetPlacementRequest {
            bucket: BLOCK_BUCKET.to_string(),
            key: object_key.to_string(),
            size: 0,
            storage_class: String::new(),
        })
        .await
        .map_err(|e| anyhow!("GetPlacement failed: {e}"))?
        .into_inner();

    if let Some(primary) = placement.nodes.first() {
        delete_object_meta_from_osd(osd_pool, primary, BLOCK_BUCKET, object_key)
            .await
            .map_err(|e| anyhow!("delete_object_meta failed: {e}"))?;
    }

    Ok(())
}
