//! ObjectIO Installation and Deployment Tool
//!
//! This binary provides automated deployment and configuration for ObjectIO clusters.
//!
//! Usage:
//!   objectio-install init --role all --disks /dev/sdb,/dev/sdc
//!   objectio-install disk list
//!   objectio-install disk prepare /dev/sdb

mod config;
mod disk;
mod systemd;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "objectio-install")]
#[command(about = "ObjectIO installation and deployment tool")]
#[command(version)]
struct Args {
    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize this node with specified role(s)
    Init {
        /// Role(s) to initialize: all, gateway, meta, osd
        #[arg(long, value_delimiter = ',')]
        role: Vec<String>,

        /// Disk paths to use for OSD (comma-separated)
        #[arg(long, value_delimiter = ',')]
        disks: Option<Vec<String>>,

        /// Metadata service endpoint (for gateway/osd roles)
        #[arg(long, default_value = "http://localhost:9001")]
        meta_endpoint: String,

        /// Raft peers for metadata cluster (comma-separated addresses)
        #[arg(long, value_delimiter = ',')]
        raft_peers: Option<Vec<String>>,

        /// Erasure coding profile: "k+m" format
        /// Common profiles:
        ///   2+1: 3 disks, 66% efficiency, tolerates 1 failure
        ///   4+2: 6 disks, 66% efficiency, tolerates 2 failures (default)
        ///   8+4: 12 disks, 66% efficiency, tolerates 4 failures
        ///   1+2: 3-way replication, 33% efficiency
        #[arg(long, default_value = "4+2")]
        ec_profile: String,

        /// Disable authentication (for development)
        #[arg(long)]
        no_auth: bool,

        /// Configuration output directory
        #[arg(long, default_value = "/etc/objectio")]
        config_dir: String,

        /// Install systemd service files (requires root)
        #[arg(long)]
        systemd: bool,

        /// Skip disk preparation (just generate config files)
        #[arg(long)]
        skip_prepare: bool,

        /// Block size in MB for disk initialization (default: 4)
        /// Larger block sizes support larger objects without multipart upload.
        #[arg(long, default_value = "4")]
        block_size_mb: u32,
    },

    /// Join an existing cluster
    Join {
        /// Metadata service endpoint of existing cluster
        #[arg(long)]
        meta_endpoint: String,

        /// Disk paths to use for OSD (comma-separated)
        #[arg(long, value_delimiter = ',')]
        disks: Option<Vec<String>>,

        /// Block size in MB for disk initialization (default: 4)
        #[arg(long, default_value = "4")]
        block_size_mb: u32,
    },

    /// Disk management commands
    Disk {
        #[command(subcommand)]
        command: DiskCommands,
    },

    /// Show current configuration
    Status {
        /// Configuration directory
        #[arg(long, default_value = "/etc/objectio")]
        config_dir: String,
    },
}

#[derive(Subcommand, Debug)]
enum DiskCommands {
    /// List available disks
    List {
        /// Include disks that are already in use
        #[arg(long)]
        all: bool,
    },

    /// Prepare a disk for ObjectIO
    Prepare {
        /// Path to the disk device
        path: String,

        /// Force preparation even if disk has data
        #[arg(long)]
        force: bool,

        /// Block size in MB for disk initialization (default: 4)
        #[arg(long, default_value = "4")]
        block_size_mb: u32,
    },

    /// Check disk status
    Check {
        /// Path to the disk device
        path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
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
        Commands::Init {
            role,
            disks,
            meta_endpoint,
            raft_peers,
            ec_profile,
            no_auth,
            config_dir,
            systemd,
            skip_prepare,
            block_size_mb,
        } => {
            init_node(
                role,
                disks,
                meta_endpoint,
                raft_peers,
                ec_profile,
                no_auth,
                config_dir,
                systemd,
                skip_prepare,
                block_size_mb,
            )
            .await?;
        }
        Commands::Join {
            meta_endpoint,
            disks,
            block_size_mb,
        } => {
            join_cluster(meta_endpoint, disks, block_size_mb).await?;
        }
        Commands::Disk { command } => {
            handle_disk_command(command).await?;
        }
        Commands::Status { config_dir } => {
            show_status(&config_dir)?;
        }
    }

    Ok(())
}

/// Initialize node with specified role(s)
#[allow(clippy::too_many_arguments)]
async fn init_node(
    roles: Vec<String>,
    disks: Option<Vec<String>>,
    meta_endpoint: String,
    raft_peers: Option<Vec<String>>,
    ec_profile_str: String,
    no_auth: bool,
    config_dir: String,
    install_systemd_units: bool,
    skip_prepare: bool,
    block_size_mb: u32,
) -> Result<()> {
    info!("Initializing ObjectIO node");
    info!("Roles: {:?}", roles);

    // Parse EC profile
    let ec_profile = config::EcProfile::from_str(&ec_profile_str).ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid EC profile format '{}'. Use 'k+m' format like '4+2'",
            ec_profile_str
        )
    })?;

    info!(
        "Erasure coding profile: {}+{} (min {} disks)",
        ec_profile.k,
        ec_profile.m,
        ec_profile.min_disks()
    );

    // Create config directory
    std::fs::create_dir_all(&config_dir)?;

    let roles_normalized: Vec<String> = roles.iter().map(|r| r.to_lowercase()).collect();
    let is_all = roles_normalized.contains(&"all".to_string());

    // Initialize gateway role
    if is_all || roles_normalized.contains(&"gateway".to_string()) {
        info!("Configuring Gateway role");
        let gateway_config = config::generate_gateway_config_full(&config::GatewayConfig {
            meta_endpoint: meta_endpoint.clone(),
            ec_profile: ec_profile.clone(),
            auth_enabled: !no_auth,
            ..Default::default()
        });
        let gateway_path = format!("{}/gateway.toml", config_dir);
        std::fs::write(&gateway_path, &gateway_config)?;
        info!("Wrote gateway config to {}", gateway_path);

        if install_systemd_units {
            let unit = systemd::generate_gateway_unit();
            install_systemd_unit("objectio-gateway", &unit)?;
        }
    }

    // Initialize metadata role
    if is_all || roles_normalized.contains(&"meta".to_string()) {
        info!("Configuring Metadata role");
        let peers = raft_peers.unwrap_or_default();
        let meta_config = config::generate_meta_config(&peers);
        let meta_path = format!("{}/meta.toml", config_dir);
        std::fs::write(&meta_path, &meta_config)?;
        info!("Wrote metadata config to {}", meta_path);

        if install_systemd_units {
            let unit = systemd::generate_meta_unit();
            install_systemd_unit("objectio-meta", &unit)?;
        }
    }

    // Initialize OSD role
    if is_all || roles_normalized.contains(&"osd".to_string()) {
        info!("Configuring OSD role");

        let disk_paths = match disks {
            Some(d) => d,
            None => {
                info!("No disks specified, detecting available disks...");
                disk::detect_available_disks()?
            }
        };

        if disk_paths.is_empty() {
            anyhow::bail!("No disks available for OSD. Use --disks to specify disk paths.");
        }

        // Validate disk count against EC profile
        let min_disks = ec_profile.min_disks() as usize;
        if disk_paths.len() < min_disks {
            info!(
                "WARNING: {} disks provided, but EC profile {}+{} requires at least {} disks",
                disk_paths.len(),
                ec_profile.k,
                ec_profile.m,
                min_disks
            );
            info!(
                "         Consider using a smaller EC profile like '2+1' (3 disks) or providing more disks."
            );
        }

        info!("Using {} disks: {:?}", disk_paths.len(), disk_paths);

        // Prepare each disk (unless skip_prepare is set)
        let block_size_bytes = block_size_mb * 1024 * 1024;
        if skip_prepare {
            info!("Skipping disk preparation (--skip-prepare specified)");
        } else {
            info!(
                "Using block size: {} MB ({} bytes)",
                block_size_mb, block_size_bytes
            );
            for disk_path in &disk_paths {
                info!("Preparing disk: {}", disk_path);
                match disk::prepare_disk(disk_path, false, Some(block_size_bytes)) {
                    Ok(disk_id) => info!("Disk {} prepared with ID: {}", disk_path, disk_id),
                    Err(e) => {
                        if e.to_string().contains("already initialized") {
                            info!("Disk {} is already an ObjectIO disk", disk_path);
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        let osd_config = config::generate_osd_config_with_block_size(
            &disk_paths,
            &meta_endpoint,
            block_size_bytes,
        );
        let osd_path = format!("{}/osd.toml", config_dir);
        std::fs::write(&osd_path, &osd_config)?;
        info!("Wrote OSD config to {}", osd_path);

        if install_systemd_units {
            let unit = systemd::generate_osd_unit();
            install_systemd_unit("objectio-osd", &unit)?;
        }
    }

    info!("============================================");
    info!("ObjectIO node initialized successfully!");
    info!("");
    info!("Configuration:");
    info!("  Directory: {}", config_dir);
    info!(
        "  Erasure Coding: {}+{} ({} data + {} parity shards)",
        ec_profile.k, ec_profile.m, ec_profile.k, ec_profile.m
    );
    info!(
        "  Storage Efficiency: {:.0}%",
        (ec_profile.k as f64 / (ec_profile.k + ec_profile.m) as f64) * 100.0
    );
    info!("  Fault Tolerance: {} disk failures", ec_profile.m);
    info!(
        "  Authentication: {}",
        if no_auth { "DISABLED" } else { "ENABLED" }
    );
    info!("");
    info!("To start services:");
    if install_systemd_units {
        info!("  sudo systemctl start objectio-meta");
        info!("  sudo systemctl start objectio-osd");
        info!("  sudo systemctl start objectio-gateway");
    } else {
        info!("  objectio-meta --config {}/meta.toml", config_dir);
        info!("  objectio-osd --config {}/osd.toml", config_dir);
        info!("  objectio-gateway --config {}/gateway.toml", config_dir);
    }
    info!("============================================");

    Ok(())
}

/// Join an existing cluster
async fn join_cluster(
    meta_endpoint: String,
    disks: Option<Vec<String>>,
    block_size_mb: u32,
) -> Result<()> {
    info!("Joining existing ObjectIO cluster at {}", meta_endpoint);

    // TODO: Connect to metadata service and register this node
    // For now, just set up as an OSD node

    let disk_paths = match disks {
        Some(d) => d,
        None => {
            info!("No disks specified, detecting available disks...");
            disk::detect_available_disks()?
        }
    };

    if disk_paths.is_empty() {
        anyhow::bail!("No disks available. Use --disks to specify disk paths.");
    }

    let block_size_bytes = block_size_mb * 1024 * 1024;
    info!(
        "Using block size: {} MB ({} bytes)",
        block_size_mb, block_size_bytes
    );

    for disk_path in &disk_paths {
        info!("Preparing disk: {}", disk_path);
        match disk::prepare_disk(disk_path, false, Some(block_size_bytes)) {
            Ok(disk_id) => info!("Disk {} prepared with ID: {}", disk_path, disk_id),
            Err(e) => {
                if e.to_string().contains("already initialized") {
                    info!("Disk {} is already an ObjectIO disk", disk_path);
                } else {
                    return Err(e);
                }
            }
        }
    }

    let config_dir = "/etc/objectio";
    std::fs::create_dir_all(config_dir)?;

    let osd_config =
        config::generate_osd_config_with_block_size(&disk_paths, &meta_endpoint, block_size_bytes);
    let osd_path = format!("{}/osd.toml", config_dir);
    std::fs::write(&osd_path, &osd_config)?;

    let unit = systemd::generate_osd_unit();
    install_systemd_unit("objectio-osd", &unit)?;

    info!("============================================");
    info!("Joined cluster successfully!");
    info!("Start the OSD service: systemctl start objectio-osd");
    info!("============================================");

    Ok(())
}

/// Handle disk management commands
async fn handle_disk_command(command: DiskCommands) -> Result<()> {
    match command {
        DiskCommands::List { all } => {
            let disks = if all {
                disk::list_all_disks()?
            } else {
                disk::detect_available_disks()?
            };

            if disks.is_empty() {
                println!("No disks found.");
            } else {
                println!("Available disks:");
                for disk_path in disks {
                    let status = disk::check_disk(&disk_path)?;
                    println!("  {} - {}", disk_path, status);
                }
            }
        }
        DiskCommands::Prepare {
            path,
            force,
            block_size_mb,
        } => {
            let block_size_bytes = block_size_mb * 1024 * 1024;
            info!(
                "Preparing disk: {} (block_size: {} MB)",
                path, block_size_mb
            );
            let disk_id = disk::prepare_disk(&path, force, Some(block_size_bytes))?;
            info!("Disk prepared successfully!");
            info!("Disk ID: {}", disk_id);
        }
        DiskCommands::Check { path } => {
            let status = disk::check_disk(&path)?;
            println!("Disk: {}", path);
            println!("Status: {}", status);
        }
    }

    Ok(())
}

/// Show current configuration status
fn show_status(config_dir: &str) -> Result<()> {
    println!("ObjectIO Configuration Status");
    println!("==============================");
    println!("Config directory: {}", config_dir);
    println!();

    let configs = ["gateway.toml", "meta.toml", "osd.toml"];
    for config_name in configs {
        let path = format!("{}/{}", config_dir, config_name);
        if std::path::Path::new(&path).exists() {
            println!("[✓] {} - found", config_name);
        } else {
            println!("[ ] {} - not configured", config_name);
        }
    }

    println!();
    println!("Systemd Services:");

    let services = ["objectio-gateway", "objectio-meta", "objectio-osd"];
    for service in services {
        let unit_path = format!("/etc/systemd/system/{}.service", service);
        if std::path::Path::new(&unit_path).exists() {
            println!("[✓] {} - installed", service);
        } else {
            println!("[ ] {} - not installed", service);
        }
    }

    Ok(())
}

/// Install a systemd unit file
fn install_systemd_unit(name: &str, content: &str) -> Result<()> {
    let unit_path = format!("/etc/systemd/system/{}.service", name);

    // Check if we can write to /etc/systemd/system
    if std::fs::metadata("/etc/systemd/system").is_err() {
        info!("Cannot access /etc/systemd/system (not running as root?)");
        info!("Systemd unit for {} would be:", name);
        println!("{}", content);
        return Ok(());
    }

    std::fs::write(&unit_path, content)?;
    info!("Installed systemd unit: {}", unit_path);

    // Try to reload systemd
    let _ = std::process::Command::new("systemctl")
        .args(["daemon-reload"])
        .status();

    Ok(())
}
