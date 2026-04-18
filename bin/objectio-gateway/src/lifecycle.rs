//! Background lifecycle worker for object expiration and cleanup.

use crate::osd_pool::OsdPool;
use objectio_proto::metadata::{
    GetBucketVersioningRequest, GetListingNodesRequest, ListBucketsRequest, VersioningState,
    metadata_service_client::MetadataServiceClient,
};
use objectio_proto::storage::{
    DeleteObjectMetaRequest, ListObjectsMetaRequest, storage_service_client::StorageServiceClient,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Configuration for the lifecycle worker
pub struct LifecycleWorkerConfig {
    /// How often to run the lifecycle scan (default: 24 hours)
    pub interval: Duration,
}

impl Default for LifecycleWorkerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Start the lifecycle background worker.
/// Runs periodically to expire objects based on lifecycle rules.
pub fn spawn_lifecycle_worker(
    meta_client: MetadataServiceClient<Channel>,
    osd_pool: Arc<OsdPool>,
    config: LifecycleWorkerConfig,
) {
    tokio::spawn(async move {
        info!(
            "Lifecycle worker started (interval={}s)",
            config.interval.as_secs()
        );

        // Initial delay to let the cluster stabilize
        tokio::time::sleep(Duration::from_secs(60)).await;

        let mut interval = tokio::time::interval(config.interval);
        loop {
            interval.tick().await;
            if let Err(e) = run_lifecycle_scan(&meta_client, &osd_pool).await {
                error!("Lifecycle scan failed: {}", e);
            }
        }
    });
}

async fn run_lifecycle_scan(
    meta_client: &MetadataServiceClient<Channel>,
    _osd_pool: &OsdPool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = meta_client.clone();

    // List all buckets
    let buckets = client
        .list_buckets(ListBucketsRequest {
            owner: String::new(),
            tenant: String::new(),
        })
        .await?
        .into_inner()
        .buckets;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut total_expired = 0u64;
    let mut total_markers_cleaned = 0u64;

    for bucket_meta in &buckets {
        let bucket = &bucket_meta.name;

        // Get lifecycle config for this bucket
        let lifecycle = match client
            .get_bucket_lifecycle(objectio_proto::metadata::GetBucketLifecycleRequest {
                bucket: bucket.clone(),
            })
            .await
        {
            Ok(resp) => {
                let inner = resp.into_inner();
                if !inner.found {
                    continue;
                }
                inner.config.unwrap_or_default()
            }
            Err(_) => continue,
        };

        if lifecycle.rules.is_empty() {
            continue;
        }

        // Check versioning state for this bucket
        let versioning_enabled = match client
            .get_bucket_versioning(GetBucketVersioningRequest {
                bucket: bucket.clone(),
            })
            .await
        {
            Ok(resp) => resp.into_inner().state() == VersioningState::VersioningEnabled,
            Err(_) => false,
        };

        // Get all listing nodes for scatter-gather
        let nodes = match client
            .get_listing_nodes(GetListingNodesRequest {
                bucket: bucket.clone(),
                include_all_states: false,
            })
            .await
        {
            Ok(resp) => resp.into_inner().nodes,
            Err(e) => {
                warn!("Failed to get listing nodes for {}: {}", bucket, e);
                continue;
            }
        };

        for rule in &lifecycle.rules {
            if !rule.enabled {
                continue;
            }

            debug!(
                "Processing lifecycle rule '{}' for bucket '{}' (prefix='{}', exp_days={})",
                rule.id, bucket, rule.prefix, rule.expiration_days
            );

            // Process each OSD node
            for node in &nodes {
                let addr = format!("http://{}", node.address);
                let mut osd_client = match StorageServiceClient::connect(addr).await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Failed to connect to OSD {}: {}", node.address, e);
                        continue;
                    }
                };

                // List objects matching the rule prefix
                let objects = match osd_client
                    .list_objects_meta(ListObjectsMetaRequest {
                        bucket: bucket.clone(),
                        prefix: rule.prefix.clone(),
                        start_after: String::new(),
                        max_keys: 10000,
                        continuation_token: String::new(),
                    })
                    .await
                {
                    Ok(resp) => resp.into_inner().objects,
                    Err(e) => {
                        warn!(
                            "Failed to list objects on OSD {} for {}: {}",
                            node.address, bucket, e
                        );
                        continue;
                    }
                };

                for obj in &objects {
                    let age_days = (now.saturating_sub(obj.created_at)) / 86400;

                    // Expiration by days
                    if rule.expiration_days > 0 && age_days >= u64::from(rule.expiration_days) {
                        // Skip delete markers in non-versioned mode
                        if obj.is_delete_marker && !versioning_enabled {
                            continue;
                        }

                        match osd_client
                            .delete_object_meta(DeleteObjectMetaRequest {
                                bucket: bucket.clone(),
                                key: obj.key.clone(),
                                version_id: String::new(),
                            })
                            .await
                        {
                            Ok(_) => {
                                total_expired += 1;
                                debug!(
                                    "Expired object: {}/{} (age={}d, rule={})",
                                    bucket, obj.key, age_days, rule.id
                                );
                            }
                            Err(e) => {
                                warn!("Failed to expire {}/{}: {}", bucket, obj.key, e);
                            }
                        }
                    }

                    // Clean up expired delete markers
                    if rule.expired_object_delete_marker && obj.is_delete_marker {
                        match osd_client
                            .delete_object_meta(DeleteObjectMetaRequest {
                                bucket: bucket.clone(),
                                key: obj.key.clone(),
                                version_id: obj.version_id.clone(),
                            })
                            .await
                        {
                            Ok(_) => {
                                total_markers_cleaned += 1;
                                debug!(
                                    "Cleaned delete marker: {}/{} (version={})",
                                    bucket, obj.key, obj.version_id
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to clean delete marker {}/{}: {}",
                                    bucket, obj.key, e
                                );
                            }
                        }
                    }
                }
            }

            // Abort incomplete multipart uploads
            if rule.abort_incomplete_multipart_upload_days > 0
                && let Ok(resp) = client
                    .list_multipart_uploads(objectio_proto::metadata::ListMultipartUploadsRequest {
                        bucket: bucket.clone(),
                        prefix: rule.prefix.clone(),
                        max_uploads: 1000,
                        key_marker: String::new(),
                        upload_id_marker: String::new(),
                    })
                    .await
            {
                for upload in &resp.into_inner().uploads {
                    let upload_age_days = (now.saturating_sub(upload.initiated)) / 86400;
                    if upload_age_days >= u64::from(rule.abort_incomplete_multipart_upload_days) {
                        let _ = client
                            .abort_multipart_upload(
                                objectio_proto::metadata::AbortMultipartUploadRequest {
                                    bucket: bucket.clone(),
                                    key: upload.key.clone(),
                                    upload_id: upload.upload_id.clone(),
                                },
                            )
                            .await;
                        debug!(
                            "Aborted incomplete upload: {}/{} (upload_id={}, age={}d)",
                            bucket, upload.key, upload.upload_id, upload_age_days
                        );
                    }
                }
            }
        }
    }

    if total_expired > 0 || total_markers_cleaned > 0 {
        info!(
            "Lifecycle scan complete: expired={}, markers_cleaned={}",
            total_expired, total_markers_cleaned
        );
    } else {
        debug!("Lifecycle scan complete: no objects expired");
    }

    Ok(())
}
