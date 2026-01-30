//! ObjectIO CLI - Admin Command Line Interface
//!
//! This binary provides administrative commands for ObjectIO.

use anyhow::Result;
use clap::{Parser, Subcommand};
use objectio_proto::metadata::{
    metadata_service_client::MetadataServiceClient,
    CreateAccessKeyRequest, CreateUserRequest, DeleteAccessKeyRequest, DeleteUserRequest,
    ListAccessKeysRequest, ListUsersRequest,
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
                                if user.email.is_empty() { "-" } else { &user.email }
                            );
                        }
                    }
                }
                UserCommands::Create { display_name, email } => {
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
                    println!("  objectio-cli -e {} key create {}", args.endpoint, user.user_id);
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
                                key.access_key_id,
                                status,
                                key.created_at
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
    }

    Ok(())
}
