//! ObjectIO Gateway - S3 API Gateway
//!
//! This binary provides the S3-compatible HTTP API.
//! Credentials are managed by the metadata service for persistence.

mod admin;
mod auth_middleware;
mod chunked_decode;
mod console_auth;
mod host_provider;
mod iceberg_auth;
mod kms;
mod license_gate;
mod lifecycle;
mod metrics_middleware;
mod osd_pool;
mod s3;
mod scatter_gather;

use anyhow::Result;
use auth_middleware::{AuthState, auth_layer, optional_auth_layer};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::{delete, get, head, post, put},
};
use clap::Parser;
use objectio_auth::policy::PolicyEvaluator;
use objectio_delta_sharing::{
    DeltaSharingConfig, admin_router as delta_admin_router, router as delta_router,
};
use objectio_proto::metadata::metadata_service_client::MetadataServiceClient;
use objectio_s3::{ProtectionConfig, s3_metrics};
use osd_pool::OsdPool;
use s3::AppState;
use scatter_gather::ScatterGatherEngine;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Resolve the initial license at startup.
///
/// Order of precedence:
/// 1. explicit `--license` flag path
/// 2. `OBJECTIO_LICENSE` env var — either a path or inline JSON
/// 3. meta config at `license/active` (what the console writes)
///
/// Any failure (missing file, bad signature, expired) logs a warning and
/// degrades to Community tier. Startup never hard-fails on the license.
async fn load_initial_license(
    cli_path: Option<&str>,
    meta_client: MetadataServiceClient<tonic::transport::Channel>,
) -> objectio_license::License {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // 1. CLI flag
    if let Some(path) = cli_path {
        match std::fs::read(path) {
            Ok(bytes) => match objectio_license::License::load_from_bytes(&bytes, now) {
                Ok(l) => return l,
                Err(e) => warn!("--license {} rejected: {} — falling back", path, e),
            },
            Err(e) => warn!("--license {} unreadable: {} — falling back", path, e),
        }
    }

    // 2. Environment variable — path or inline JSON
    if let Ok(val) = std::env::var("OBJECTIO_LICENSE")
        && !val.is_empty()
    {
        let bytes = if val.trim_start().starts_with('{') {
            Some(val.into_bytes())
        } else {
            std::fs::read(&val).ok()
        };
        if let Some(bytes) = bytes {
            match objectio_license::License::load_from_bytes(&bytes, now) {
                Ok(l) => return l,
                Err(e) => warn!("OBJECTIO_LICENSE rejected: {} — falling back", e),
            }
        }
    }

    // 3. Meta config
    let mut client = meta_client;
    if let Ok(resp) = client
        .get_config(objectio_proto::metadata::GetConfigRequest {
            key: "license/active".to_string(),
        })
        .await
    {
        let inner = resp.into_inner();
        if inner.found
            && let Some(entry) = inner.entry
            && !entry.value.is_empty()
        {
            match objectio_license::License::load_from_bytes(&entry.value, now) {
                Ok(l) => return l,
                Err(e) => warn!("license in meta config rejected: {} — falling back", e),
            }
        }
    }

    objectio_license::License::community()
}

/// Prometheus metrics endpoint handler
async fn metrics_handler() -> impl IntoResponse {
    let metrics = s3_metrics().export_prometheus();
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics,
    )
}

#[derive(Parser, Debug)]
#[command(name = "objectio-gateway")]
#[command(about = "ObjectIO S3 API Gateway")]
#[command(version)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "/etc/objectio/gateway.toml")]
    config: String,

    /// Listen address for S3 API
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    listen: String,

    /// Metadata service endpoint
    #[arg(long, default_value = "http://localhost:9001")]
    meta_endpoint: String,

    /// OSD endpoint (initial OSD, more discovered via metadata service)
    #[arg(long, default_value = "http://localhost:9002")]
    osd_endpoint: String,

    /// Erasure coding data shards (k)
    #[arg(long, default_value = "4")]
    ec_k: u32,

    /// Erasure coding parity shards (m)
    #[arg(long, default_value = "2")]
    ec_m: u32,

    /// Protection scheme: ec (MDS erasure coding), lrc (locally repairable codes), replication
    #[arg(long, default_value = "ec")]
    protection: String,

    /// LRC local parity shards (only used when --protection=lrc)
    #[arg(long, default_value = "0")]
    lrc_local_parity: u32,

    /// LRC global parity shards (only used when --protection=lrc)
    #[arg(long, default_value = "0")]
    lrc_global_parity: u32,

    /// Number of replicas (only used when --protection=replication)
    #[arg(long, default_value = "3")]
    replicas: u32,

    /// Disable authentication (for development)
    #[arg(long, default_value_t = false)]
    no_auth: bool,

    /// Name of the env var holding the base64-encoded 32-byte SSE master key.
    /// If the env var is set, SSE-S3 is enabled — PUT to buckets with
    /// ServerSideEncryptionConfiguration will encrypt at rest. If missing,
    /// SSE-S3 requests on PUT will be rejected; plaintext writes/reads are
    /// unaffected. In `--no-auth` mode only, a random key is generated
    /// when the env var is missing (dev convenience, objects are lost on restart).
    #[arg(long, default_value = "OBJECTIO_MASTER_KEY")]
    master_key_env: String,

    /// KMS backend used for SSE-KMS operations: `local` (keys stored in meta,
    /// wrapped by the service master key) or `vault` (HashiCorp Vault Transit
    /// engine). Vault reads config from `VAULT_ADDR` / `VAULT_TOKEN` /
    /// `VAULT_TRANSIT_PATH` env vars. SSE-S3 works regardless of this flag
    /// and always uses the service master key.
    #[arg(long, default_value = "local")]
    kms_backend: String,

    /// AWS region for SigV4 verification
    #[arg(long, default_value = "us-east-1")]
    region: String,

    /// Iceberg REST Catalog warehouse location (S3 URL prefix for table data)
    #[arg(long, default_value = "s3://objectio-warehouse")]
    warehouse_location: String,

    /// Public endpoint URL for presigned URLs (e.g. https://objectio.example.com).
    /// Used by Delta Sharing to generate presigned S3 URLs for Parquet files.
    /// Defaults to http://<listen> if not set.
    #[arg(long, default_value = "")]
    external_endpoint: String,

    /// Admin access key ID for Delta Sharing presigned URL generation.
    /// Leave empty to disable Delta Sharing presigned URL support.
    #[arg(long, default_value = "")]
    delta_access_key_id: String,

    /// Admin secret access key for Delta Sharing presigned URL generation.
    #[arg(long, default_value = "")]
    delta_secret_key: String,

    /// OIDC issuer URL for Iceberg JWT authentication (e.g., https://keycloak.example.com/realms/myrealm)
    #[arg(long)]
    oidc_issuer_url: Option<String>,

    /// OIDC client ID (registered in the OIDC provider)
    #[arg(long)]
    oidc_client_id: Option<String>,

    /// OIDC client secret (for client_credentials grant at /iceberg/v1/oauth/tokens)
    #[arg(long)]
    oidc_client_secret: Option<String>,

    /// OIDC audience for JWT validation (defaults to client_id if not set)
    #[arg(long)]
    oidc_audience: Option<String>,

    /// OIDC claim name for user groups/roles (default: "groups";
    /// Keycloak uses "roles", Azure AD uses "roles", Okta uses "groups")
    #[arg(long, default_value = "groups")]
    oidc_groups_claim: String,

    /// OIDC claim name for user role (default: "role")
    #[arg(long, default_value = "role")]
    oidc_role_claim: String,

    /// OIDC scopes to request (default: "openid profile email")
    #[arg(long, default_value = "openid profile email")]
    oidc_scopes: String,

    /// OIDC groups/roles that grant Iceberg catalog admin access.
    /// Comma-separated. These are matched as ARNs: arn:obio:iam::oidc:group/<value>
    #[arg(long, value_delimiter = ',')]
    oidc_admin_roles: Vec<String>,

    /// Path to an Enterprise license file. Falls back to `$OBJECTIO_LICENSE`,
    /// then to the `license/active` key in meta config, then to Community
    /// tier (no Enterprise features).
    #[arg(long)]
    license: Option<String>,

    /// Gateway's own topology position — used by locality-aware read routing
    /// to prefer shards on nearby OSDs (Phase 2). Empty string at any level
    /// means "inherit / unknown". Also configurable via
    /// OBJECTIO_TOPOLOGY_{REGION,ZONE,DATACENTER,RACK,HOST} env vars.
    #[arg(long, env = "OBJECTIO_TOPOLOGY_REGION", default_value = "")]
    topology_region: String,
    #[arg(long, env = "OBJECTIO_TOPOLOGY_ZONE", default_value = "")]
    topology_zone: String,
    #[arg(long, env = "OBJECTIO_TOPOLOGY_DATACENTER", default_value = "")]
    topology_datacenter: String,
    #[arg(long, env = "OBJECTIO_TOPOLOGY_RACK", default_value = "")]
    topology_rack: String,
    #[arg(long, env = "OBJECTIO_TOPOLOGY_HOST", default_value = "")]
    topology_host: String,

    /// Host lifecycle backend for /_admin/hosts and /_admin/osds/*/reboot.
    /// `noop` (default) refuses those actions; `k8s` talks to the
    /// in-cluster Kubernetes API (requires a ServiceAccount with
    /// patch-scale on the OSD StatefulSet + delete-pod). Future values:
    /// `linux-ssh`, `appliance`.
    #[arg(long, env = "OBJECTIO_HOST_PROVIDER", default_value = "noop")]
    host_provider: String,

    /// Namespace the OSD StatefulSet lives in. Only read when
    /// `--host-provider=k8s`. Defaults to POD_NAMESPACE (downward API
    /// set by the helm chart) or "default" if unset.
    #[arg(long, env = "POD_NAMESPACE", default_value = "default")]
    host_provider_namespace: String,

    /// Name of the OSD StatefulSet — the chart's `objectio.osd.fullname`
    /// helper renders this as `{release}-osd` (e.g. `objectio-osd`).
    /// Override if you've customised the release name.
    #[arg(
        long,
        env = "OBJECTIO_HOST_PROVIDER_OSD_STS",
        default_value = "objectio-osd"
    )]
    host_provider_osd_sts: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,
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

    info!("Starting ObjectIO Gateway");
    info!("Metadata endpoint: {}", args.meta_endpoint);
    info!("OSD endpoint: {}", args.osd_endpoint);

    // Register protection config for Prometheus metrics
    let protection_config = match args.protection.as_str() {
        "lrc" => {
            let total = args.ec_k + args.lrc_local_parity + args.lrc_global_parity;
            info!(
                "Protection: LRC k={} l={} g={} (total={}, efficiency={:.1}%)",
                args.ec_k,
                args.lrc_local_parity,
                args.lrc_global_parity,
                total,
                (f64::from(args.ec_k) / f64::from(total)) * 100.0
            );
            ProtectionConfig {
                scheme: "lrc".to_string(),
                data_shards: args.ec_k,
                parity_shards: args.lrc_local_parity + args.lrc_global_parity,
                total_shards: total,
                efficiency: f64::from(args.ec_k) / f64::from(total),
                lrc_local_parity: args.lrc_local_parity,
                lrc_global_parity: args.lrc_global_parity,
            }
        }
        "replication" => {
            info!(
                "Protection: Replication replicas={} (efficiency={:.1}%)",
                args.replicas,
                (1.0 / f64::from(args.replicas)) * 100.0
            );
            ProtectionConfig {
                scheme: "replication".to_string(),
                data_shards: 1,
                parity_shards: args.replicas - 1,
                total_shards: args.replicas,
                efficiency: 1.0 / f64::from(args.replicas),
                lrc_local_parity: 0,
                lrc_global_parity: 0,
            }
        }
        _ => {
            // Default: MDS erasure coding
            let total = args.ec_k + args.ec_m;
            info!(
                "Protection: EC (MDS) k={} m={} (total={}, efficiency={:.1}%)",
                args.ec_k,
                args.ec_m,
                total,
                (f64::from(args.ec_k) / f64::from(total)) * 100.0
            );
            ProtectionConfig {
                scheme: "ec".to_string(),
                data_shards: args.ec_k,
                parity_shards: args.ec_m,
                total_shards: total,
                efficiency: f64::from(args.ec_k) / f64::from(total),
                lrc_local_parity: 0,
                lrc_global_parity: 0,
            }
        }
    };
    s3_metrics().set_protection_config(protection_config);

    // Connect to metadata service
    let meta_client = MetadataServiceClient::connect(args.meta_endpoint.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to metadata service: {}", e))?;

    info!("Connected to metadata service");
    info!("Credentials are managed by the metadata service");

    // Create OSD connection pool
    let osd_pool = Arc::new(OsdPool::new());

    // Connect to initial OSD (more will be discovered via placement)
    // Generate a temporary node ID for the initial OSD
    let initial_node_id = uuid::Uuid::new_v4();
    if let Err(e) = osd_pool
        .connect(
            osd_pool::NodeId::from(*initial_node_id.as_bytes()),
            &args.osd_endpoint,
        )
        .await
    {
        tracing::warn!(
            "Failed to connect to initial OSD: {}. Will connect on demand.",
            e
        );
    } else {
        info!("Connected to initial OSD at {}", args.osd_endpoint);
    }

    // STS provider for vended Iceberg credentials + S3 temporary auth
    let sts_signing_key = format!("objectio-sts-{}", args.region);
    let sts_provider = objectio_auth::sts::StsProvider::new(sts_signing_key.as_bytes());

    // Create auth state using metadata service for credential lookup
    let auth_state =
        Arc::new(AuthState::new(meta_client.clone(), &args.region).with_sts(sts_provider.clone()));

    // Create scatter-gather engine with a signing key derived from region
    let signing_key = format!("objectio-scatter-gather-{}", args.region);
    let scatter_gather = ScatterGatherEngine::new(osd_pool.clone(), signing_key.as_bytes());

    // Build admin principals list from OIDC admin roles config. Used by
    // the Iceberg catalog router.
    let admin_principals: Vec<String> = args
        .oidc_admin_roles
        .iter()
        .map(|role| format!("arn:obio:iam::oidc:group/{role}"))
        .collect();

    // S3 endpoint for vended Iceberg credentials.
    let s3_endpoint = if args.external_endpoint.is_empty() {
        format!(
            "http://{}:{}",
            args.listen.split(':').next().unwrap_or("localhost"),
            args.listen.split(':').nth(1).unwrap_or("9000")
        )
    } else {
        args.external_endpoint.clone()
    };

    // OIDC provider — used by both the Iceberg auth layer (Enterprise) and
    // the console OIDC login flow (all tiers). Defined here so it stays in
    // scope for the Community build even when the Iceberg block below is
    // compiled out.
    let oidc_provider = if let (Some(issuer_url), Some(client_id)) =
        (&args.oidc_issuer_url, &args.oidc_client_id)
    {
        let audience = args
            .oidc_audience
            .as_deref()
            .unwrap_or(client_id)
            .to_string();
        let oidc_config = objectio_auth::OidcConfig {
            issuer_url: issuer_url.clone(),
            client_id: client_id.clone(),
            client_secret: args.oidc_client_secret.clone().unwrap_or_default(),
            audience,
            jwks_uri: None,
            token_endpoint: None,
            groups_claim: args.oidc_groups_claim.clone(),
            role_claim: args.oidc_role_claim.clone(),
            scopes: args.oidc_scopes.clone(),
        };
        info!(
            "OIDC configured: issuer={} client={}",
            issuer_url, client_id
        );
        Some(std::sync::Arc::new(objectio_auth::OidcProvider::new(
            oidc_config,
        )))
    } else {
        None
    };

    // Build Iceberg REST Catalog router and Delta Sharing router. Both are
    // Enterprise features, gated at runtime by `feature_gate` middleware —
    // without a valid Enterprise license the routers reject with 403.
    let iceberg_router = {
        let router = objectio_iceberg::router(
            meta_client.clone(),
            args.warehouse_location.clone(),
            PolicyEvaluator::new(),
            admin_principals,
            Some(sts_provider),
            s3_endpoint,
        );
        info!(
            "Iceberg REST Catalog at /iceberg/v1/* ({})",
            if oidc_provider.is_some() {
                "SigV4 + OIDC + session cookie"
            } else {
                "SigV4 + session cookie"
            }
        );
        let iceberg_auth_state = Arc::new(iceberg_auth::IcebergAuthState {
            sigv4_state: Arc::clone(&auth_state),
            oidc_provider: oidc_provider.clone(),
        });
        if let Some(ref oidc) = oidc_provider {
            let oauth_router = Router::new()
                .route(
                    "/v1/oauth/tokens",
                    axum::routing::post(iceberg_auth::oauth_tokens),
                )
                .with_state(Arc::clone(oidc));
            router
                .layer(middleware::from_fn_with_state(
                    iceberg_auth_state,
                    iceberg_auth::iceberg_unified_auth_layer,
                ))
                .merge(oauth_router)
        } else {
            router.layer(middleware::from_fn_with_state(
                iceberg_auth_state,
                iceberg_auth::iceberg_unified_auth_layer,
            ))
        }
    };

    // Warehouse prefix rewrite layer — harmless when the license gate
    // rejects Iceberg: rewritten requests simply short-circuit with 403.
    let warehouse_rewrite =
        tower::util::MapRequestLayer::new(|mut req: axum::http::Request<axum::body::Body>| {
            let path = req.uri().path().to_string();
            if let Some(rest) = path.strip_prefix("/iceberg/v1/ws/")
                && let Some(slash_pos) = rest.find('/')
            {
                let warehouse = &rest[..slash_pos];
                let remaining = &rest[slash_pos..];
                let new_path = format!("/iceberg/v1{remaining}");
                let existing_query = req.uri().query().unwrap_or("");
                let new_query = if existing_query.is_empty() {
                    format!("warehouse={warehouse}")
                } else {
                    format!("{existing_query}&warehouse={warehouse}")
                };
                if let Ok(uri) = format!("{new_path}?{new_query}").parse() {
                    *req.uri_mut() = uri;
                }
            }
            req
        });

    let (delta_sharing_router, delta_sharing_admin_router) = {
        let external_endpoint = if args.external_endpoint.is_empty() {
            format!("http://{}", args.listen)
        } else {
            args.external_endpoint.clone()
        };
        let delta_config = DeltaSharingConfig {
            endpoint: external_endpoint.clone(),
            region: args.region.clone(),
            access_key_id: args.delta_access_key_id.clone(),
            secret_access_key: args.delta_secret_key.clone(),
        };
        let delta_admin_config = DeltaSharingConfig {
            endpoint: external_endpoint,
            region: args.region.clone(),
            access_key_id: args.delta_access_key_id.clone(),
            secret_access_key: args.delta_secret_key.clone(),
        };
        info!("Delta Sharing protocol enabled at /delta-sharing/v1/*");
        info!("Delta Sharing admin API enabled at /_admin/delta-sharing/*");
        (
            delta_router(meta_client.clone(), delta_config),
            delta_admin_router(meta_client.clone(), delta_admin_config),
        )
    };

    // Start lifecycle background worker
    lifecycle::spawn_lifecycle_worker(
        meta_client.clone(),
        Arc::clone(&osd_pool),
        lifecycle::LifecycleWorkerConfig::default(),
    );

    // Clone meta_client for OIDC auto-provisioning before it's moved into AppState
    let meta_client_for_oidc = meta_client.clone();

    // Load the SSE master key. Order of preference: env var, dev fallback,
    // otherwise leave disabled. When a bucket has default encryption set
    // but `master_key` is None, PUT will fail — that's the intended gate.
    let master_key = match objectio_kms::MasterKey::from_env(&args.master_key_env) {
        Ok(k) => {
            info!(
                "SSE master key loaded from env var {} — SSE-S3 enabled",
                args.master_key_env
            );
            Some(k)
        }
        Err(e) if args.no_auth => {
            let k = objectio_kms::MasterKey::generate_random();
            warn!(
                "SSE master key env var not usable ({}); generated a random key for dev mode. \
                 Objects encrypted with this key will be UNREADABLE after gateway restart. \
                 Set {}=<base64 32 bytes> to persist across restarts.",
                e, args.master_key_env,
            );
            Some(k)
        }
        Err(e) => {
            warn!(
                "SSE master key not available ({}); SSE-S3 is disabled. Plaintext writes/reads \
                 are unaffected. To enable SSE, set {}=<base64 32 bytes>.",
                e, args.master_key_env,
            );
            None
        }
    };

    // Resolve the initial KMS backend. Meta's `kms/config` entry wins if
    // present (so the console can reconfigure without a gateway restart);
    // otherwise fall back to `--kms-backend` + env. Admin PUTs at runtime
    // replace the providers in-place via `AppState::set_kms`.
    let kms_config = match kms::load_backend_config_from_meta(meta_client.clone()).await {
        Some(cfg) => {
            info!("SSE-KMS backend from meta: {}", cfg.label());
            cfg
        }
        None => {
            let cfg = kms::KmsBackendConfig::from_cli(&args.kms_backend);
            info!("SSE-KMS backend from CLI/env: {}", cfg.label());
            cfg
        }
    };
    let (kms_local, kms) =
        kms::build_kms_provider(meta_client.clone(), master_key.as_ref(), &kms_config);

    // Load license. Order of precedence: --license flag, OBJECTIO_LICENSE env,
    // meta config at `license/active`. No license → Community tier (all
    // Enterprise features remain gated). Parse/verify failures log a warning
    // and fall back to Community — never hard-fail startup on a broken license.
    let license =
        load_initial_license(args.license.as_deref(), meta_client.clone()).await;
    match license.tier {
        objectio_license::Tier::Enterprise => info!(
            "License: Enterprise — licensee='{}' expires_at={}",
            license.licensee, license.expires_at
        ),
        objectio_license::Tier::Community => {
            info!("License: Community — Enterprise features gated")
        }
    }

    // Build this gateway's self-topology from CLI flags / env so read
    // routing can prefer locally-adjacent OSDs.
    let self_topology = objectio_placement::FailureDomainInfo::new_full(
        &args.topology_region,
        &args.topology_zone,
        &args.topology_datacenter,
        &args.topology_rack,
        &args.topology_host,
    );
    if !self_topology.region.is_empty() {
        info!(
            "Gateway topology: region={} zone={} datacenter={} rack={} host={}",
            self_topology.region,
            self_topology.zone,
            self_topology.datacenter,
            self_topology.rack,
            self_topology.host
        );
    } else {
        info!("Gateway topology: unconfigured — locality-aware read routing disabled");
    }

    // Host provider selection. `noop` means /_admin/hosts and Reboot
    // return 501 — safe default for dev clusters with no platform to
    // talk to. `k8s` connects to the in-cluster API; startup fails
    // fast if the ServiceAccount isn't wired.
    let host_provider: Arc<dyn host_provider::HostProvider> =
        match args.host_provider.as_str() {
            "noop" => Arc::new(host_provider::NoopHostProvider),
            "k8s" => {
                info!(
                    "Host provider: k8s (namespace={}, osd_sts={})",
                    args.host_provider_namespace, args.host_provider_osd_sts
                );
                match host_provider::K8sHostProvider::try_new(
                    args.host_provider_namespace.clone(),
                    args.host_provider_osd_sts.clone(),
                )
                .await
                {
                    Ok(p) => Arc::new(p),
                    Err(e) => {
                        // Don't hard-fail boot — if the k8s API is
                        // briefly unreachable (CNI still starting, RBAC
                        // propagation) we'd rather serve S3 + return 503
                        // on host-action endpoints than refuse traffic
                        // entirely. Startup logs will flag the config
                        // for ops to check.
                        warn!("k8s host provider init failed, falling back to noop: {e}");
                        Arc::new(host_provider::NoopHostProvider)
                    }
                }
            }
            other => {
                warn!(
                    "unknown --host-provider={:?}; falling back to noop. Accepted: noop, k8s",
                    other
                );
                Arc::new(host_provider::NoopHostProvider)
            }
        };

    // Create application state. KMS fields are held behind RwLocks so
    // `PUT /_admin/kms/config` can hot-swap the backend at runtime; we seed
    // them here with whatever the CLI flag + env / meta config resolved to.
    let state = Arc::new(AppState {
        meta_client,
        osd_pool,
        ec_k: args.ec_k,
        ec_m: args.ec_m,
        policy_evaluator: PolicyEvaluator::new(),
        scatter_gather,
        master_key,
        kms: parking_lot::RwLock::new(kms),
        kms_local: parking_lot::RwLock::new(kms_local),
        license: parking_lot::RwLock::new(Arc::new(license)),
        self_topology,
        host_provider,
    });

    // Build router
    // Allow up to 100MB for single-part uploads (larger objects need multipart)
    let body_limit = DefaultBodyLimit::max(100 * 1024 * 1024);
    info!("Max single-part upload size: 100 MB");

    // Build S3 routes (behind SigV4 auth when enabled)
    let s3_routes = Router::new()
        // Metrics and health routes FIRST (no auth, must come before wildcards)
        .route("/metrics", get(metrics_handler))
        .route("/health", get(s3::health_check))
        // Service endpoint (list buckets)
        .route("/", get(s3::list_buckets))
        // Bucket operations (including ?policy and ?uploads query params)
        .route("/{bucket}", put(s3::create_bucket))
        .route("/{bucket}", delete(s3::delete_bucket))
        .route("/{bucket}", head(s3::head_bucket))
        .route("/{bucket}", get(s3::list_objects))
        // POST /{bucket}?delete - batch delete objects
        .route("/{bucket}", post(s3::post_bucket))
        // Bucket with trailing slash (s3fs compatibility)
        .route("/{bucket}/", head(s3::head_bucket_trailing))
        .route("/{bucket}/", get(s3::list_objects_trailing))
        // Object operations (with multipart upload support via query params)
        .route("/{bucket}/{*key}", put(s3::put_object_with_params))
        .route("/{bucket}/{*key}", get(s3::get_object_with_params))
        .route("/{bucket}/{*key}", head(s3::head_object))
        .route("/{bucket}/{*key}", delete(s3::delete_object_with_params))
        .route("/{bucket}/{*key}", post(s3::post_object))
        .with_state(Arc::clone(&state));

    // Admin API routes — outside SigV4, protected by session cookie
    let admin_routes = Router::new()
        .route("/_admin/users", get(s3::admin_list_users))
        .route("/_admin/users", post(s3::admin_create_user))
        .route("/_admin/users/{user_id}", delete(s3::admin_delete_user))
        .route(
            "/_admin/users/{user_id}/access-keys",
            get(s3::admin_list_access_keys),
        )
        .route(
            "/_admin/users/{user_id}/access-keys",
            post(s3::admin_create_access_key),
        )
        .route(
            "/_admin/access-keys/{access_key_id}",
            delete(s3::admin_delete_access_key),
        )
        .route("/_admin/config", get(admin::admin_list_config))
        .route("/_admin/config/{*section}", get(admin::admin_get_config))
        .route("/_admin/config/{*section}", put(admin::admin_set_config))
        .route(
            "/_admin/config/{*section}",
            delete(admin::admin_delete_config),
        )
        .route("/_admin/pools", get(admin::admin_list_pools))
        .route("/_admin/pools", post(admin::admin_create_pool))
        .route("/_admin/pools/{name}", get(admin::admin_get_pool))
        .route("/_admin/pools/{name}", put(admin::admin_update_pool))
        .route("/_admin/pools/{name}", delete(admin::admin_delete_pool))
        .route("/_admin/tenants", get(admin::admin_list_tenants))
        .route("/_admin/tenants", post(admin::admin_create_tenant))
        .route("/_admin/tenants/{name}", get(admin::admin_get_tenant))
        .route("/_admin/tenants/{name}", put(admin::admin_update_tenant))
        .route("/_admin/tenants/{name}", delete(admin::admin_delete_tenant))
        .route(
            "/_admin/tenants/{name}/admins",
            post(admin::admin_add_tenant_admin),
        )
        .route(
            "/_admin/tenants/{name}/admins/{user}",
            delete(admin::admin_remove_tenant_admin),
        )
        .route("/_admin/nodes", get(admin::admin_list_nodes))
        .route(
            "/_admin/osds/{node_id}/admin-state",
            put(admin::admin_set_osd_state),
        )
        .route(
            "/_admin/osds/{node_id}/reboot",
            post(admin::admin_reboot_osd),
        )
        .route("/_admin/hosts", post(admin::admin_add_hosts))
        .route("/_admin/host-provider", get(admin::admin_host_provider_info))
        .route("/_admin/drain-status", get(admin::admin_drain_status))
        .route(
            "/_admin/rebalance-status",
            get(admin::admin_rebalance_status),
        )
        .route(
            "/_admin/rebalance/pause",
            post(admin::admin_rebalance_pause),
        )
        .route(
            "/_admin/rebalance/resume",
            post(admin::admin_rebalance_resume),
        )
        .route("/_admin/cluster-info", get(admin::admin_cluster_info))
        .route("/_admin/topology", get(admin::admin_get_topology))
        .route(
            "/_admin/placement/validate",
            get(admin::admin_validate_placement),
        )
        // IAM Policies
        .route("/_admin/policies", get(admin::admin_list_policies))
        .route("/_admin/policies", post(admin::admin_create_policy))
        .route(
            "/_admin/policies/{name}",
            delete(admin::admin_delete_policy),
        )
        .route("/_admin/policies/attach", post(admin::admin_attach_policy))
        .route("/_admin/policies/detach", post(admin::admin_detach_policy))
        .route(
            "/_admin/policies/attached",
            get(admin::admin_list_attached_policies),
        )
        // Tenant-aware Table Sharing admin
        .route("/_admin/shares", get(admin::admin_list_shares_tenant))
        .route("/_admin/shares", post(admin::admin_create_share_tenant))
        .route(
            "/_admin/recipients",
            get(admin::admin_list_recipients_tenant),
        )
        .route("/_admin/warehouses", get(admin::admin_list_warehouses))
        .route("/_admin/warehouses", post(admin::admin_create_warehouse))
        .route(
            "/_admin/warehouses/{name}",
            delete(admin::admin_delete_warehouse),
        )
        .route("/_admin/buckets", get(admin::admin_list_buckets))
        .route("/_admin/buckets", post(admin::admin_create_bucket))
        .route("/_admin/buckets/{name}", delete(admin::admin_delete_bucket))
        .route(
            "/_admin/buckets/{name}/policy",
            get(admin::admin_get_bucket_policy),
        )
        .route(
            "/_admin/buckets/{name}/policy",
            put(admin::admin_put_bucket_policy),
        )
        .route(
            "/_admin/buckets/{name}/policy",
            delete(admin::admin_delete_bucket_policy),
        )
        .route(
            "/_admin/buckets/{name}/objects",
            get(admin::admin_list_objects),
        )
        // KMS admin API
        .route("/_admin/kms/status", get(kms::admin_kms_status))
        .route("/_admin/kms/version", get(kms::admin_kms_version))
        .route("/_admin/kms/api", get(kms::admin_kms_api))
        .route("/_admin/kms/keys", get(kms::admin_list_kms_keys))
        .route("/_admin/kms/keys", post(kms::admin_create_kms_key))
        .route("/_admin/kms/keys/{key_id}", get(kms::admin_get_kms_key))
        .route("/_admin/kms/keys/{key_id}", delete(kms::admin_delete_kms_key))
        // Dynamic backend config: console-driven runtime reconfiguration
        .route("/_admin/kms/config", get(kms::admin_kms_get_config))
        .route("/_admin/kms/config", put(kms::admin_kms_put_config))
        .route("/_admin/kms/config", delete(kms::admin_kms_delete_config))
        .route("/_admin/kms/test", post(kms::admin_kms_test))
        // License management
        .route("/_admin/license", get(admin::admin_get_license))
        .route("/_admin/license", put(admin::admin_put_license))
        .route("/_admin/license", delete(admin::admin_delete_license))
        .with_state(Arc::clone(&state))
        // Layer SigV4 verification that is optional — if a request carries
        // `Authorization: AWS4-HMAC-SHA256 ...`, verify it and inject
        // `Extension<AuthResult>`. Cookie-only console requests pass through
        // untouched. This lets handlers like `check_kms_policy` observe both
        // SigV4 callers (for IAM policy eval) and session users (admin).
        .layer(middleware::from_fn_with_state(
            Arc::clone(&auth_state),
            optional_auth_layer,
        ));

    // Console auth API routes (login/logout/session — no auth required)
    let console_oidc_state = Arc::new(console_auth::ConsoleOidcState {
        oidc_provider: oidc_provider.clone(),
        external_endpoint: args.external_endpoint.clone(),
        meta_client: meta_client_for_oidc,
    });

    let console_api_routes = Router::new()
        .route("/_console/api/login", post(console_auth::console_login))
        .route("/_console/api/session", get(console_auth::console_session))
        .route("/_console/api/logout", post(console_auth::console_logout))
        // Self-service key management (any authenticated user)
        .route("/_console/api/me/keys", get(console_auth::my_list_keys))
        .route("/_console/api/me/keys", post(console_auth::my_create_key))
        .route(
            "/_console/api/me/keys/{key_id}",
            delete(console_auth::my_delete_key),
        )
        .with_state(Arc::clone(&state));

    let console_oidc_routes = Router::new()
        .route(
            "/_console/api/oidc/enabled",
            get(console_auth::oidc_enabled),
        )
        .route(
            "/_console/api/oidc/authorize",
            get(console_auth::oidc_authorize),
        )
        .route(
            "/_console/api/oidc/callback",
            get(console_auth::oidc_callback),
        )
        .with_state(console_oidc_state);

    // Merge S3 routes with shared layers. Iceberg and Delta Sharing are nested
    // OUTSIDE the SigV4 auth layer — they use their own auth (OAuth/bearer tokens)
    // so standard clients (PyIceberg, Spark, Trino) can connect without SigV4.
    let app = if !args.no_auth {
        info!("Authentication is ENABLED (credentials from metadata service)");
        info!("Admin API is ENABLED (requires 'admin' user credentials)");
        info!("Metrics endpoint: /metrics (no auth)");
        info!("Iceberg REST Catalog: /iceberg/v1/* (no SigV4, use OAuth/bearer)");
        // Protected routes (SigV4 auth applied) — S3 API only
        let protected = Router::new()
            .merge(s3_routes)
            .layer(middleware::from_fn(chunked_decode::s3_chunked_decode_layer))
            .layer(body_limit)
            .layer(middleware::from_fn_with_state(auth_state, auth_layer));
        // Unprotected: Iceberg + Delta Sharing use their own auth
        // Console: static SPA served from /_console/
        let console_service = tower_http::services::ServeDir::new(
            std::env::var("OBJECTIO_CONSOLE_DIR")
                .unwrap_or_else(|_| "/usr/share/objectio/console".to_string()),
        )
        .fallback(tower_http::services::ServeFile::new(format!(
            "{}/index.html",
            std::env::var("OBJECTIO_CONSOLE_DIR")
                .unwrap_or_else(|_| "/usr/share/objectio/console".to_string())
        )));

        let iceberg_gated = iceberg_router.layer(middleware::from_fn_with_state(
            (Arc::clone(&state), objectio_license::Feature::Iceberg),
            license_gate::feature_gate,
        ));
        let delta_gated = delta_sharing_router.layer(middleware::from_fn_with_state(
            (Arc::clone(&state), objectio_license::Feature::DeltaSharing),
            license_gate::feature_gate,
        ));
        let delta_admin_gated =
            delta_sharing_admin_router.layer(middleware::from_fn_with_state(
                (Arc::clone(&state), objectio_license::Feature::DeltaSharing),
                license_gate::feature_gate,
            ));
        Router::new()
            .merge(protected)
            .merge(admin_routes)
            .merge(console_api_routes)
            .merge(console_oidc_routes)
            .nest("/iceberg", iceberg_gated)
            .nest("/delta-sharing", delta_gated)
            .nest("/_admin/delta-sharing", delta_admin_gated)
            .nest_service("/_console", console_service)
            .layer(middleware::from_fn(metrics_middleware::metrics_layer))
            .layer(TraceLayer::new_for_http())
    } else {
        info!("Authentication is DISABLED (development mode)");
        info!("Admin API is ENABLED (no auth required in dev mode)");
        info!("Metrics endpoint: /metrics");
        let console_service_noauth = tower_http::services::ServeDir::new(
            std::env::var("OBJECTIO_CONSOLE_DIR")
                .unwrap_or_else(|_| "/usr/share/objectio/console".to_string()),
        )
        .fallback(tower_http::services::ServeFile::new(format!(
            "{}/index.html",
            std::env::var("OBJECTIO_CONSOLE_DIR")
                .unwrap_or_else(|_| "/usr/share/objectio/console".to_string())
        )));

        let iceberg_gated = iceberg_router.layer(middleware::from_fn_with_state(
            (Arc::clone(&state), objectio_license::Feature::Iceberg),
            license_gate::feature_gate,
        ));
        let delta_gated = delta_sharing_router.layer(middleware::from_fn_with_state(
            (Arc::clone(&state), objectio_license::Feature::DeltaSharing),
            license_gate::feature_gate,
        ));
        let delta_admin_gated =
            delta_sharing_admin_router.layer(middleware::from_fn_with_state(
                (Arc::clone(&state), objectio_license::Feature::DeltaSharing),
                license_gate::feature_gate,
            ));
        Router::new()
            .merge(s3_routes)
            .merge(admin_routes)
            .merge(console_api_routes)
            .merge(console_oidc_routes)
            .nest("/iceberg", iceberg_gated)
            .nest("/delta-sharing", delta_gated)
            .nest("/_admin/delta-sharing", delta_admin_gated)
            .nest_service("/_console", console_service_noauth)
            .layer(middleware::from_fn(chunked_decode::s3_chunked_decode_layer))
            .layer(body_limit)
            .layer(middleware::from_fn(metrics_middleware::metrics_layer))
            .layer(TraceLayer::new_for_http())
    };

    // Parse listen address
    let addr: SocketAddr = args
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid listen address {}: {}", args.listen, e))?;

    info!("Starting S3 API server on {}", addr);

    // Start server
    let listener = TcpListener::bind(addr).await?;

    // Apply warehouse prefix rewrite as a tower service wrapper (runs before Axum routing)
    let app = tower::ServiceBuilder::new()
        .layer(warehouse_rewrite)
        .service(app);

    axum::serve(listener, tower::make::Shared::new(app))
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
        })
        .await?;

    info!("Gateway shut down gracefully");

    Ok(())
}
