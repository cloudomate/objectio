//! ObjectIO CLI - Admin Command Line Interface
//!
//! This binary provides administrative commands for ObjectIO.

use anyhow::Result;
use clap::{Parser, Subcommand};
use objectio_proto::block::{
    CloneVolumeRequest, CreateSnapshotRequest, CreateVolumeRequest, DeleteSnapshotRequest,
    DeleteVolumeRequest, GetSnapshotRequest, GetVolumeRequest, ListSnapshotsRequest,
    ListVolumesRequest, ResizeVolumeRequest, block_service_client::BlockServiceClient,
};
use objectio_proto::metadata::{
    CreateAccessKeyRequest, CreateUserRequest, DeleteAccessKeyRequest, DeleteUserRequest,
    ListAccessKeysRequest, ListUsersRequest, metadata_service_client::MetadataServiceClient,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "objectio-cli")]
#[command(about = "ObjectIO Admin CLI")]
#[command(version)]
struct Args {
    /// Metadata service endpoint
    #[arg(short, long, default_value = "http://localhost:9100")]
    endpoint: String,

    /// Log level
    #[arg(long, default_value = "warn")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Cluster operations
    Cluster {
        #[command(subcommand)]
        action: ClusterCommands,
    },
    /// Node operations
    Node {
        #[command(subcommand)]
        action: NodeCommands,
    },
    /// Disk operations
    Disk {
        #[command(subcommand)]
        action: DiskCommands,
    },
    /// Bucket operations
    Bucket {
        #[command(subcommand)]
        action: BucketCommands,
    },
    /// User operations (IAM)
    User {
        #[command(subcommand)]
        action: UserCommands,
    },
    /// Access key operations (IAM)
    Key {
        #[command(subcommand)]
        action: KeyCommands,
    },
    /// Block volume operations
    Volume {
        #[command(subcommand)]
        action: VolumeCommands,
    },
    /// Block snapshot operations
    Snapshot {
        #[command(subcommand)]
        action: SnapshotCommands,
    },
}

#[derive(Subcommand, Debug)]
enum ClusterCommands {
    /// Show cluster status
    Status,
    /// Show cluster topology
    Topology,
}

#[derive(Subcommand, Debug)]
enum NodeCommands {
    /// List all nodes
    List,
    /// Show node details
    Show {
        /// Node ID
        node_id: String,
    },
    /// Drain a node (stop accepting new data)
    Drain {
        /// Node ID
        node_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum DiskCommands {
    /// List all disks
    List,
    /// Show disk details
    Show {
        /// Disk ID
        disk_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum BucketCommands {
    /// List all buckets
    List,
    /// Show bucket details
    Show {
        /// Bucket name
        name: String,
    },
}

#[derive(Subcommand, Debug)]
enum UserCommands {
    /// List all users
    List,
    /// Create a new user
    Create {
        /// Display name for the user
        display_name: String,
        /// Optional email address
        #[arg(short, long, default_value = "")]
        email: String,
    },
    /// Delete a user
    Delete {
        /// User ID to delete
        user_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum KeyCommands {
    /// List access keys for a user
    List {
        /// User ID
        user_id: String,
    },
    /// Create a new access key for a user
    Create {
        /// User ID
        user_id: String,
    },
    /// Delete an access key
    Delete {
        /// Access key ID to delete
        access_key_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum VolumeCommands {
    /// List all volumes
    List {
        /// Filter by storage pool
        #[arg(short, long, default_value = "")]
        pool: String,
    },
    /// Create a new volume
    Create {
        /// Volume name
        name: String,
        /// Volume size (e.g. 10G, 1T, 500M)
        #[arg(short, long)]
        size: String,
        /// Storage pool
        #[arg(short, long, default_value = "")]
        pool: String,
    },
    /// Show volume details
    Show {
        /// Volume ID
        volume_id: String,
    },
    /// Resize a volume (grow only)
    Resize {
        /// Volume ID
        volume_id: String,
        /// New size (e.g. 20G, 2T)
        #[arg(short, long)]
        size: String,
    },
    /// Delete a volume
    Delete {
        /// Volume ID
        volume_id: String,
        /// Force delete even if attached
        #[arg(short, long)]
        force: bool,
    },
}

#[derive(Subcommand, Debug)]
enum SnapshotCommands {
    /// List snapshots for a volume
    List {
        /// Volume ID
        volume_id: String,
    },
    /// Create a snapshot
    Create {
        /// Volume ID
        volume_id: String,
        /// Snapshot name
        #[arg(short, long)]
        name: String,
    },
    /// Show snapshot details
    Show {
        /// Snapshot ID
        snapshot_id: String,
    },
    /// Delete a snapshot
    Delete {
        /// Snapshot ID
        snapshot_id: String,
    },
    /// Clone a volume from a snapshot
    Clone {
        /// Snapshot ID to clone from
        snapshot_id: String,
        /// Name for the new volume
        #[arg(short, long)]
        name: String,
    },
}

/// Parse a human-readable size string (e.g. "10G", "1T", "500M") into bytes.
fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let (num, multiplier) = if let Some(n) = s.strip_suffix('T') {
        (n, 1024 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('G') {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('M') {
        (n, 1024 * 1024)
    } else {
        // Assume bytes if no suffix
        (s, 1)
    };
    let value: u64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid size: '{s}'"))?;
    Ok(value * multiplier)
}

/// Format bytes as a human-readable size string.
fn format_size(bytes: u64) -> String {
    const TIB: u64 = 1024 * 1024 * 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    if bytes >= TIB && bytes.is_multiple_of(TIB) {
        format!("{} TiB", bytes / TIB)
    } else if bytes >= GIB && bytes.is_multiple_of(GIB) {
        format!("{} GiB", bytes / GIB)
    } else if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn format_volume_state(state: i32) -> &'static str {
    match state {
        0 => "Unknown",
        1 => "Creating",
        2 => "Available",
        3 => "Attached",
        4 => "Error",
        5 => "Deleting",
        _ => "Unknown",
    }
}

fn format_snapshot_state(state: i32) -> &'static str {
    match state {
        0 => "Unknown",
        1 => "Creating",
        2 => "Available",
        3 => "Deleting",
        4 => "Error",
        _ => "Unknown",
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    match args.command {
        Commands::Cluster { action } => match action {
            ClusterCommands::Status => {
                println!("Cluster Status");
                println!("==============");
                println!("Status: Healthy (placeholder)");
                println!("Nodes: 0");
                println!("Disks: 0");
            }
            ClusterCommands::Topology => {
                println!("Cluster Topology");
                println!("================");
                println!("(placeholder)");
            }
        },
        Commands::Node { action } => match action {
            NodeCommands::List => {
                println!("Nodes");
                println!("=====");
                println!("(placeholder)");
            }
            NodeCommands::Show { node_id } => {
                println!("Node: {node_id}");
                println!("(placeholder)");
            }
            NodeCommands::Drain { node_id } => {
                println!("Draining node: {node_id}");
                println!("(placeholder)");
            }
        },
        Commands::Disk { action } => match action {
            DiskCommands::List => {
                println!("Disks");
                println!("=====");
                println!("(placeholder)");
            }
            DiskCommands::Show { disk_id } => {
                println!("Disk: {disk_id}");
                println!("(placeholder)");
            }
        },
        Commands::Bucket { action } => match action {
            BucketCommands::List => {
                println!("Buckets");
                println!("=======");
                println!("(placeholder)");
            }
            BucketCommands::Show { name } => {
                println!("Bucket: {name}");
                println!("(placeholder)");
            }
        },
        Commands::User { action } => {
            let mut client = MetadataServiceClient::connect(args.endpoint.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to metadata service: {}", e))?;

            match action {
                UserCommands::List => {
                    let response = client
                        .list_users(ListUsersRequest {
                            max_results: 1000,
                            marker: String::new(),
                        })
                        .await?;

                    let resp = response.into_inner();
                    println!("Users");
                    println!("=====");
                    if resp.users.is_empty() {
                        println!("No users found");
                    } else {
                        println!("{:<40} {:<30} {:<30}", "USER ID", "DISPLAY NAME", "EMAIL");
                        println!("{}", "-".repeat(100));
                        for user in resp.users {
                            println!(
                                "{:<40} {:<30} {:<30}",
                                user.user_id,
                                user.display_name,
                                if user.email.is_empty() {
                                    "-"
                                } else {
                                    &user.email
                                }
                            );
                        }
                    }
                }
                UserCommands::Create {
                    display_name,
                    email,
                } => {
                    let response = client
                        .create_user(CreateUserRequest {
                            display_name: display_name.clone(),
                            email,
                        })
                        .await?;

                    let user = response.into_inner().user.unwrap();
                    println!("User created successfully!");
                    println!();
                    println!("User ID:      {}", user.user_id);
                    println!("Display Name: {}", user.display_name);
                    println!("ARN:          {}", user.arn);
                    if !user.email.is_empty() {
                        println!("Email:        {}", user.email);
                    }
                    println!();
                    println!("Next, create an access key:");
                    println!(
                        "  objectio-cli -e {} key create {}",
                        args.endpoint, user.user_id
                    );
                }
                UserCommands::Delete { user_id } => {
                    client
                        .delete_user(DeleteUserRequest {
                            user_id: user_id.clone(),
                        })
                        .await?;

                    println!("User '{}' deleted successfully", user_id);
                }
            }
        }
        Commands::Key { action } => {
            let mut client = MetadataServiceClient::connect(args.endpoint.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to metadata service: {}", e))?;

            match action {
                KeyCommands::List { user_id } => {
                    let response = client
                        .list_access_keys(ListAccessKeysRequest {
                            user_id: user_id.clone(),
                        })
                        .await?;

                    let resp = response.into_inner();
                    println!("Access Keys for user: {}", user_id);
                    println!("====================");
                    if resp.access_keys.is_empty() {
                        println!("No access keys found");
                    } else {
                        println!("{:<25} {:<15} {:<20}", "ACCESS KEY ID", "STATUS", "CREATED");
                        println!("{}", "-".repeat(60));
                        for key in resp.access_keys {
                            let status = match key.status {
                                0 => "Active",
                                1 => "Inactive",
                                _ => "Unknown",
                            };
                            println!(
                                "{:<25} {:<15} {:<20}",
                                key.access_key_id, status, key.created_at
                            );
                        }
                    }
                }
                KeyCommands::Create { user_id } => {
                    let response = client
                        .create_access_key(CreateAccessKeyRequest {
                            user_id: user_id.clone(),
                        })
                        .await?;

                    let key = response.into_inner().access_key.unwrap();
                    println!("Access key created successfully!");
                    println!();
                    println!("Access Key ID:     {}", key.access_key_id);
                    println!("Secret Access Key: {}", key.secret_access_key);
                    println!();
                    println!("IMPORTANT: Save the secret access key now.");
                    println!("           It will not be shown again!");
                    println!();
                    println!("Configure AWS CLI:");
                    println!("  export AWS_ACCESS_KEY_ID={}", key.access_key_id);
                    println!("  export AWS_SECRET_ACCESS_KEY={}", key.secret_access_key);
                }
                KeyCommands::Delete { access_key_id } => {
                    client
                        .delete_access_key(DeleteAccessKeyRequest {
                            access_key_id: access_key_id.clone(),
                        })
                        .await?;

                    println!("Access key '{}' deleted successfully", access_key_id);
                }
            }
        }
        Commands::Volume { action } => {
            let mut client = BlockServiceClient::connect(args.endpoint.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to block service: {}", e))?;

            match action {
                VolumeCommands::List { pool } => {
                    let response = client
                        .list_volumes(ListVolumesRequest {
                            pool,
                            max_results: 1000,
                            marker: String::new(),
                        })
                        .await?;

                    let resp = response.into_inner();
                    println!("Volumes");
                    println!("=======");
                    if resp.volumes.is_empty() {
                        println!("No volumes found");
                    } else {
                        println!(
                            "{:<40} {:<20} {:<12} {:<12} {:<12}",
                            "VOLUME ID", "NAME", "SIZE", "USED", "STATE"
                        );
                        println!("{}", "-".repeat(96));
                        for vol in resp.volumes {
                            println!(
                                "{:<40} {:<20} {:<12} {:<12} {:<12}",
                                vol.volume_id,
                                vol.name,
                                format_size(vol.size_bytes),
                                format_size(vol.used_bytes),
                                format_volume_state(vol.state),
                            );
                        }
                    }
                }
                VolumeCommands::Create { name, size, pool } => {
                    let size_bytes = parse_size(&size)?;
                    let response = client
                        .create_volume(CreateVolumeRequest {
                            name: name.clone(),
                            size_bytes,
                            pool,
                            chunk_size_bytes: 0,
                            metadata: Default::default(),
                            qos: None,
                        })
                        .await?;

                    let vol = response.into_inner().volume.unwrap();
                    println!("Volume created successfully!");
                    println!();
                    println!("Volume ID: {}", vol.volume_id);
                    println!("Name:      {}", vol.name);
                    println!("Size:      {}", format_size(vol.size_bytes));
                    println!(
                        "Pool:      {}",
                        if vol.pool.is_empty() { "-" } else { &vol.pool }
                    );
                    println!("State:     {}", format_volume_state(vol.state));
                }
                VolumeCommands::Show { volume_id } => {
                    let response = client
                        .get_volume(GetVolumeRequest {
                            volume_id: volume_id.clone(),
                        })
                        .await?;

                    let vol = response.into_inner().volume.unwrap();
                    println!("Volume: {}", vol.volume_id);
                    println!("========{}", "=".repeat(vol.volume_id.len()));
                    println!("Name:              {}", vol.name);
                    println!("Size:              {}", format_size(vol.size_bytes));
                    println!("Used:              {}", format_size(vol.used_bytes));
                    println!(
                        "Pool:              {}",
                        if vol.pool.is_empty() { "-" } else { &vol.pool }
                    );
                    println!("State:             {}", format_volume_state(vol.state));
                    println!(
                        "Chunk Size:        {}",
                        format_size(u64::from(vol.chunk_size_bytes))
                    );
                    if !vol.parent_snapshot_id.is_empty() {
                        println!("Parent Snapshot:   {}", vol.parent_snapshot_id);
                    }
                    println!("Created At:        {}", vol.created_at);
                    println!("Updated At:        {}", vol.updated_at);
                    if let Some(qos) = vol.qos {
                        println!();
                        println!("QoS Configuration:");
                        if qos.max_iops > 0 {
                            println!("  Max IOPS:        {}", qos.max_iops);
                        }
                        if qos.min_iops > 0 {
                            println!("  Min IOPS:        {}", qos.min_iops);
                        }
                        if qos.max_bandwidth_bps > 0 {
                            println!(
                                "  Max Bandwidth:   {}/s",
                                format_size(qos.max_bandwidth_bps)
                            );
                        }
                        if qos.burst_iops > 0 {
                            println!(
                                "  Burst IOPS:      {} ({}s)",
                                qos.burst_iops, qos.burst_seconds
                            );
                        }
                    }
                }
                VolumeCommands::Resize { volume_id, size } => {
                    let new_size_bytes = parse_size(&size)?;
                    let response = client
                        .resize_volume(ResizeVolumeRequest {
                            volume_id: volume_id.clone(),
                            new_size_bytes,
                        })
                        .await?;

                    let vol = response.into_inner().volume.unwrap();
                    println!("Volume resized successfully!");
                    println!();
                    println!("Volume ID: {}", vol.volume_id);
                    println!("New Size:  {}", format_size(vol.size_bytes));
                }
                VolumeCommands::Delete { volume_id, force } => {
                    client
                        .delete_volume(DeleteVolumeRequest {
                            volume_id: volume_id.clone(),
                            force,
                        })
                        .await?;

                    println!("Volume '{}' deleted successfully", volume_id);
                }
            }
        }
        Commands::Snapshot { action } => {
            let mut client = BlockServiceClient::connect(args.endpoint.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to block service: {}", e))?;

            match action {
                SnapshotCommands::List { volume_id } => {
                    let response = client
                        .list_snapshots(ListSnapshotsRequest {
                            volume_id: volume_id.clone(),
                            max_results: 1000,
                            marker: String::new(),
                        })
                        .await?;

                    let resp = response.into_inner();
                    println!("Snapshots for volume: {}", volume_id);
                    println!("====================");
                    if resp.snapshots.is_empty() {
                        println!("No snapshots found");
                    } else {
                        println!(
                            "{:<40} {:<20} {:<12} {:<12} {:<12}",
                            "SNAPSHOT ID", "NAME", "SIZE", "UNIQUE", "STATE"
                        );
                        println!("{}", "-".repeat(96));
                        for snap in resp.snapshots {
                            println!(
                                "{:<40} {:<20} {:<12} {:<12} {:<12}",
                                snap.snapshot_id,
                                snap.name,
                                format_size(snap.size_bytes),
                                format_size(snap.unique_bytes),
                                format_snapshot_state(snap.state),
                            );
                        }
                    }
                }
                SnapshotCommands::Create { volume_id, name } => {
                    let response = client
                        .create_snapshot(CreateSnapshotRequest {
                            volume_id: volume_id.clone(),
                            name: name.clone(),
                            metadata: Default::default(),
                        })
                        .await?;

                    let snap = response.into_inner().snapshot.unwrap();
                    println!("Snapshot created successfully!");
                    println!();
                    println!("Snapshot ID: {}", snap.snapshot_id);
                    println!("Volume ID:   {}", snap.volume_id);
                    println!("Name:        {}", snap.name);
                    println!("Size:        {}", format_size(snap.size_bytes));
                    println!("State:       {}", format_snapshot_state(snap.state));
                }
                SnapshotCommands::Show { snapshot_id } => {
                    let response = client
                        .get_snapshot(GetSnapshotRequest {
                            snapshot_id: snapshot_id.clone(),
                        })
                        .await?;

                    let snap = response.into_inner().snapshot.unwrap();
                    println!("Snapshot: {}", snap.snapshot_id);
                    println!("=========={}", "=".repeat(snap.snapshot_id.len()));
                    println!("Volume ID:   {}", snap.volume_id);
                    println!("Name:        {}", snap.name);
                    println!("Size:        {}", format_size(snap.size_bytes));
                    println!("Unique:      {}", format_size(snap.unique_bytes));
                    println!("State:       {}", format_snapshot_state(snap.state));
                    println!("Created At:  {}", snap.created_at);
                }
                SnapshotCommands::Delete { snapshot_id } => {
                    client
                        .delete_snapshot(DeleteSnapshotRequest {
                            snapshot_id: snapshot_id.clone(),
                        })
                        .await?;

                    println!("Snapshot '{}' deleted successfully", snapshot_id);
                }
                SnapshotCommands::Clone { snapshot_id, name } => {
                    let response = client
                        .clone_volume(CloneVolumeRequest {
                            snapshot_id: snapshot_id.clone(),
                            name: name.clone(),
                            metadata: Default::default(),
                        })
                        .await?;

                    let vol = response.into_inner().volume.unwrap();
                    println!("Volume cloned from snapshot successfully!");
                    println!();
                    println!("Volume ID:       {}", vol.volume_id);
                    println!("Name:            {}", vol.name);
                    println!("Size:            {}", format_size(vol.size_bytes));
                    println!("Parent Snapshot: {}", vol.parent_snapshot_id);
                    println!("State:           {}", format_volume_state(vol.state));
                }
            }
        }
    }

    Ok(())
}
