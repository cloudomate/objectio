//! Simple test client for OSD gRPC service

use objectio_proto::storage::{
    storage_service_client::StorageServiceClient,
    GetStatusRequest, HealthCheckRequest, ReadShardRequest, ShardId, WriteShardRequest,
    Checksum,
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://192.168.4.225:9002".to_string());

    println!("Connecting to OSD at {}...", endpoint);

    let mut client = StorageServiceClient::connect(endpoint).await?;

    // Test 1: Health check
    println!("\n--- Health Check ---");
    let response = client.health_check(HealthCheckRequest {}).await?;
    let health = response.into_inner();
    println!("Status: {:?}", health.status);
    println!("Message: {}", health.message);

    // Test 2: Get status
    println!("\n--- Node Status ---");
    let response = client.get_status(GetStatusRequest {}).await?;
    let status = response.into_inner();
    println!("Node ID: {}", hex::encode(&status.node_id));
    println!("Node Name: {}", status.node_name);
    println!("Total Capacity: {} GB", status.total_capacity / (1024 * 1024 * 1024));
    println!("Used Capacity: {} bytes", status.used_capacity);
    println!("Shard Count: {}", status.shard_count);
    println!("Uptime: {} seconds", status.uptime_seconds);
    println!("Disks:");
    for disk in &status.disks {
        println!(
            "  - {}: {} ({} GB, status: {})",
            hex::encode(&disk.disk_id),
            disk.path,
            disk.total_capacity / (1024 * 1024 * 1024),
            disk.status
        );
    }

    // Test 3: Write a shard
    println!("\n--- Write Shard ---");
    let object_id = *Uuid::new_v4().as_bytes();
    let test_data = b"Hello from ObjectIO test client! This is test data for the storage service.";

    let write_response = client
        .write_shard(WriteShardRequest {
            shard_id: Some(ShardId {
                object_id: object_id.to_vec(),
                stripe_id: 0,
                position: 0,
            }),
            data: test_data.to_vec(),
            ec_k: 4,
            ec_m: 2,
            checksum: Some(Checksum {
                crc32c: crc32c::crc32c(test_data),
                xxhash64: 0,
                sha256: vec![],
            }),
        })
        .await?;

    let write_result = write_response.into_inner();
    println!("Write successful!");
    println!("Object ID: {}", hex::encode(&object_id));
    if let Some(loc) = &write_result.location {
        println!("Location: disk={}, offset={}", hex::encode(&loc.disk_id), loc.offset);
    }

    // Test 4: Read the shard back
    println!("\n--- Read Shard ---");
    let read_response = client
        .read_shard(ReadShardRequest {
            shard_id: Some(ShardId {
                object_id: object_id.to_vec(),
                stripe_id: 0,
                position: 0,
            }),
            offset: 0,
            length: 0,
        })
        .await?;

    let read_result = read_response.into_inner();
    let read_data = &read_result.data;
    println!("Read {} bytes", read_data.len());
    println!("Data matches: {}", read_data == test_data);
    println!("Content: {}", String::from_utf8_lossy(read_data));

    // Test 5: Get status again to see updated shard count
    println!("\n--- Updated Status ---");
    let response = client.get_status(GetStatusRequest {}).await?;
    let status = response.into_inner();
    println!("Shard Count: {}", status.shard_count);

    println!("\n=== All tests passed! ===");

    Ok(())
}
