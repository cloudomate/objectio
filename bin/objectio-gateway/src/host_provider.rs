//! Host lifecycle operations (add / reboot / remove) for the running
//! cluster. These sit one level below the cluster-layer admin_state
//! (which only says "don't place on this OSD") and know about the
//! concrete platform underneath — k8s, a Linux package on SSH-reachable
//! machines, or the sealed appliance OS.
//!
//! ## Shape
//!
//! Every provider implements [`HostProvider`]. The gateway selects one
//! at startup via `--host-provider` and wires it into admin handlers.
//! The trait stays platform-agnostic so the console calls the same
//! endpoints regardless of where the cluster runs.
//!
//! ## Phases
//!
//! Phase 2 (this): [`K8sHostProvider`]. Scales `StatefulSet/objectio-osd`
//! for Add Host, deletes the pod for Reboot. Requires an RBAC-granted
//! ServiceAccount on the gateway pod.
//!
//! Later phases add `LinuxHostProvider` (SSH + systemd, .deb/.rpm) and
//! `ApplianceHostProvider` (talks to a node-local agent for ostree A/B
//! upgrades). They'll live next to this file.

use async_trait::async_trait;

/// Errors returned by a host provider.
///
/// Distinct from `objectio_common::Error` because the K8s client's
/// error types are opaque; surfacing them as strings with a category
/// keeps the admin HTTP layer simple (400 vs 503 vs 500).
#[derive(Debug, thiserror::Error)]
pub enum HostProviderError {
    /// The provider intentionally doesn't support this operation.
    /// Example: the `NoopHostProvider` used in unit tests returns this
    /// for every call; a K8s provider returns this if it's asked to do
    /// something that requires a platform-specific hook that doesn't
    /// apply (e.g., "reboot the physical host" on k8s).
    #[error("host action not supported on this platform: {0}")]
    Unsupported(&'static str),

    /// The caller asked for an invalid thing (bad count, unknown host
    /// name). Maps to HTTP 400.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Transient — the underlying platform refused temporarily
    /// (k8s API server busy, SSH target unreachable). Maps to 503.
    #[error("temporary failure: {0}")]
    Unavailable(String),

    /// Anything else. Maps to 500. Message is surfaced to admin UI.
    #[error("provider error: {0}")]
    Other(String),
}

/// Result of adding one or more hosts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AddHostOutcome {
    /// Replica count before the scale.
    pub previous_replicas: i32,
    /// Replica count after the scale.
    pub new_replicas: i32,
    /// Best-effort list of pod names that were added. May be empty if
    /// the provider doesn't track pods individually (future SSH provider).
    pub pods_added: Vec<String>,
}

/// Result of a reboot — thin for now; kept as a struct so later
/// phases can add progress / completion-time fields without changing
/// the trait.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RebootOutcome {
    pub pod: String,
    /// True iff the platform acknowledged the reboot (e.g., k8s
    /// accepted the delete-pod). The actual restart is async.
    pub requested: bool,
}

/// Platform-agnostic host lifecycle.
#[async_trait]
pub trait HostProvider: Send + Sync {
    /// Identifier for logs + the admin API's `GET /_admin/host-provider`
    /// endpoint.
    fn name(&self) -> &'static str;

    /// Provision `count` additional OSD hosts. The provider decides
    /// *where* (k8s: extend the StatefulSet; SSH: ssh into next
    /// inventory entry; appliance: PXE boot a new machine). Returns
    /// the old/new replica counts so the caller can show a diff.
    async fn add_hosts(&self, count: i32) -> Result<AddHostOutcome, HostProviderError>;

    /// Reboot the pod / machine running an OSD, identified by its
    /// pod / host name (not the OSD `node_id`). K8s: `delete pod`;
    /// SSH: `systemctl restart objectio-osd`; appliance: agent reboot.
    async fn reboot(&self, host: &str) -> Result<RebootOutcome, HostProviderError>;
}

/// Default provider: refuses every action with `Unsupported`. Used when
/// no `--host-provider` is configured (bare-metal dev, no k8s cluster
/// to call into). Console sees 501 on the Reboot / Add Host buttons
/// and shows a hint that a platform provider needs to be configured.
pub struct NoopHostProvider;

#[async_trait]
impl HostProvider for NoopHostProvider {
    fn name(&self) -> &'static str {
        "noop"
    }
    async fn add_hosts(&self, _count: i32) -> Result<AddHostOutcome, HostProviderError> {
        Err(HostProviderError::Unsupported(
            "no host provider configured; pass --host-provider k8s (or similar) to the gateway",
        ))
    }
    async fn reboot(&self, _host: &str) -> Result<RebootOutcome, HostProviderError> {
        Err(HostProviderError::Unsupported(
            "no host provider configured; pass --host-provider k8s (or similar) to the gateway",
        ))
    }
}

// ---------------------------------------------------------------------
// K8s implementation
// ---------------------------------------------------------------------

/// Talks to the Kubernetes API inside the cluster. Uses the pod's
/// mounted ServiceAccount token (`InClusterConfig`) — the Helm chart
/// grants this SA the RBAC below.
///
/// Permissions required:
///   - patch `statefulsets/scale` on `objectio-osd` (Add Host)
///   - delete `pods` matching the OSD label selector (Reboot)
pub struct K8sHostProvider {
    client: kube::Client,
    /// Namespace the cluster runs in; helm renders this into the
    /// gateway env as `POD_NAMESPACE` (downward API).
    namespace: String,
    /// Name of the OSD StatefulSet — typically `{release}-osd`, e.g.
    /// `objectio-osd`. Gateway args surface this so a renamed helm
    /// release still addresses the right sts.
    sts_name: String,
}

impl K8sHostProvider {
    /// Connect to the in-cluster API server. Fails if the pod isn't
    /// running with a mounted ServiceAccount or the K8s API is
    /// unreachable — both of which would surface on startup, not on
    /// first admin call.
    pub async fn try_new(
        namespace: String,
        sts_name: String,
    ) -> Result<Self, HostProviderError> {
        let client = kube::Client::try_default()
            .await
            .map_err(|e| HostProviderError::Unavailable(format!("kube client: {e}")))?;
        Ok(Self {
            client,
            namespace,
            sts_name,
        })
    }
}

#[async_trait]
impl HostProvider for K8sHostProvider {
    fn name(&self) -> &'static str {
        "k8s"
    }

    async fn add_hosts(&self, count: i32) -> Result<AddHostOutcome, HostProviderError> {
        if count <= 0 {
            return Err(HostProviderError::InvalidRequest(
                "count must be >= 1".into(),
            ));
        }

        use k8s_openapi::api::apps::v1::StatefulSet;
        use kube::api::{Api, Patch, PatchParams};

        let sts_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);

        // Read current replicas. If the sts doesn't exist, the operator
        // has misnamed the release — return 400 rather than silently
        // creating one.
        let current = sts_api
            .get(&self.sts_name)
            .await
            .map_err(|e| HostProviderError::InvalidRequest(format!(
                "StatefulSet {}/{} not found: {e}",
                self.namespace, self.sts_name
            )))?;
        let previous_replicas = current
            .spec
            .as_ref()
            .and_then(|s| s.replicas)
            .unwrap_or(0);
        let new_replicas = previous_replicas.saturating_add(count);

        // scale subresource via merge patch is the canonical way to
        // bump a StatefulSet replica count without re-rendering the
        // whole spec.
        let patch = serde_json::json!({ "spec": { "replicas": new_replicas } });
        sts_api
            .patch_scale(
                &self.sts_name,
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
            .map_err(|e| HostProviderError::Other(format!("patch replicas: {e}")))?;

        // The newly-scheduled pods follow the sts naming pattern
        // `{sts_name}-{ordinal}`; list them so the caller can display
        // "added: objectio-osd-5, objectio-osd-6".
        let pods_added: Vec<String> = (previous_replicas..new_replicas)
            .map(|i| format!("{}-{}", self.sts_name, i))
            .collect();

        Ok(AddHostOutcome {
            previous_replicas,
            new_replicas,
            pods_added,
        })
    }

    async fn reboot(&self, host: &str) -> Result<RebootOutcome, HostProviderError> {
        // Sanity-check: host must be a pod inside our sts (prefix match).
        // Prevents a stray call from deleting unrelated pods in the
        // same namespace.
        if !host.starts_with(&format!("{}-", self.sts_name)) {
            return Err(HostProviderError::InvalidRequest(format!(
                "host {host:?} is not a member of StatefulSet {}",
                self.sts_name
            )));
        }

        use k8s_openapi::api::core::v1::Pod;
        use kube::api::{Api, DeleteParams};

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        // Default delete — graceful termination; k8s respawns the pod
        // via the StatefulSet immediately. Skip grace if the caller
        // explicitly asks for a hard reboot in a future iteration.
        pods.delete(host, &DeleteParams::default())
            .await
            .map_err(|e| HostProviderError::Other(format!("delete pod {host}: {e}")))?;

        Ok(RebootOutcome {
            pod: host.to_string(),
            requested: true,
        })
    }
}
