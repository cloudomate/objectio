//! Background flush loop: periodically drains dirty chunks from WriteCache
//! and writes them as EC objects to the OSD cluster.

use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info, warn};

use crate::ec_io::write_chunk;
use crate::service::BlockGatewayState;

/// Flush all dirty chunks for one volume, then persist the chunk refs.
pub async fn flush_volume(vol_id: &str, state: &BlockGatewayState) {
    let chunks = state.cache.get_chunks_to_flush(vol_id);
    if chunks.is_empty() {
        return;
    }

    let mut flushed = Vec::with_capacity(chunks.len());

    for (chunk_id, data) in &chunks {
        match write_chunk(
            Arc::clone(&state.meta_client),
            &state.osd_pool,
            vol_id,
            *chunk_id,
            data,
            state.ec_k,
            state.ec_m,
        )
        .await
        {
            Ok(object_key) => {
                if let Err(e) = state.store.put_chunk(vol_id, *chunk_id, &object_key) {
                    error!("Failed to persist chunk ref vol={vol_id} chunk={chunk_id}: {e}");
                } else {
                    flushed.push(*chunk_id);
                }
            }
            Err(e) => {
                warn!("Failed to flush chunk {chunk_id} for vol {vol_id}: {e}");
            }
        }
    }

    if !flushed.is_empty() {
        state.cache.mark_flushed(vol_id, &flushed);
        info!(
            "Flushed {}/{} chunks for vol {}",
            flushed.len(),
            chunks.len(),
            vol_id
        );
    }
}

/// Force-flush ALL dirty chunks for a volume (used on explicit Flush RPC / DetachVolume).
pub async fn flush_volume_all(vol_id: &str, state: &BlockGatewayState) {
    // flush_volume already drains everything age >= max_dirty_age; repeat until clean.
    // For an explicit flush we drain everything immediately via the inner loop.
    let chunks = state.cache.flush_volume(vol_id);
    if chunks.is_empty() {
        return;
    }

    let mut flushed = Vec::with_capacity(chunks.len());

    for (chunk_id, data) in &chunks {
        match write_chunk(
            Arc::clone(&state.meta_client),
            &state.osd_pool,
            vol_id,
            *chunk_id,
            data,
            state.ec_k,
            state.ec_m,
        )
        .await
        {
            Ok(object_key) => {
                if let Err(e) = state.store.put_chunk(vol_id, *chunk_id, &object_key) {
                    error!("Failed to persist chunk ref vol={vol_id} chunk={chunk_id}: {e}");
                } else {
                    flushed.push(*chunk_id);
                }
            }
            Err(e) => {
                error!("Failed to flush chunk {chunk_id} for vol {vol_id}: {e}");
            }
        }
    }

    info!(
        "Force-flushed {}/{} chunks for vol {}",
        flushed.len(),
        chunks.len(),
        vol_id
    );
}

/// Long-running background task: flush dirty chunks every `interval`.
pub async fn flush_loop(state: Arc<BlockGatewayState>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let volume_ids: Vec<String> = state
            .volume_manager
            .list_volumes()
            .into_iter()
            .map(|v| v.volume_id)
            .collect();

        for vol_id in &volume_ids {
            flush_volume(vol_id, &state).await;
        }
    }
}
