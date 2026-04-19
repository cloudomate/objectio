// API client for ObjectIO gateway
// Uses SigV4 signing for /_admin/* endpoints and plain fetch for /iceberg/*

const BASE = "";

export interface ConfigEntry {
  key: string;
  value: Record<string, unknown>;
  updated_at: number;
  updated_by: string;
  version: number;
}

export interface User {
  user_id: string;
  username: string;
  status: string;
  created_at: number;
}

export interface AccessKey {
  access_key_id: string;
  status: string;
  created_at: number;
}

export interface Bucket {
  name: string;
  creation_date: string;
}

export interface IcebergNamespace {
  namespace: string[];
  properties?: Record<string, string>;
}

export interface IcebergTable {
  namespace: string[];
  name: string;
}

// Simple fetch wrapper with error handling. `body` is JSON.stringify'd
// for objects/arrays; raw strings are sent verbatim so balancer config
// writes can pass a plain `0.95` or `true` without a JSON-object wrap.
export async function request<T>(
  method: string,
  path: string,
  body?: unknown
): Promise<T> {
  const opts: RequestInit = {
    method,
    headers: { "Content-Type": "application/json" },
  };
  if (body !== undefined && body !== null) {
    opts.body = typeof body === "string" ? body : JSON.stringify(body);
  }

  const resp = await fetch(`${BASE}${path}`, opts);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status}: ${text}`);
  }
  if (resp.status === 204) return {} as T;
  return resp.json();
}

// Config API
export const config = {
  list: (prefix = "") =>
    request<ConfigEntry[]>(
      "GET",
      `/_admin/config${prefix ? `?prefix=${encodeURIComponent(prefix)}` : ""}`
    ),
  get: (key: string) => request<ConfigEntry>("GET", `/_admin/config/${key}`),
  set: (key: string, value: unknown) =>
    request<ConfigEntry>("PUT", `/_admin/config/${key}`, value),
  delete: (key: string) =>
    request<void>("DELETE", `/_admin/config/${key}`),
};

// Users API
export const users = {
  list: () => request<{ users: User[] }>("GET", "/_admin/users"),
  create: (username: string) =>
    request<{ user_id: string; access_key_id: string; secret_access_key: string }>(
      "POST",
      "/_admin/users",
      { username }
    ),
  delete: (userId: string) =>
    request<void>("DELETE", `/_admin/users/${userId}`),
  listKeys: (userId: string) =>
    request<{ access_keys: AccessKey[] }>(
      "GET",
      `/_admin/users/${userId}/access-keys`
    ),
  createKey: (userId: string) =>
    request<{ access_key_id: string; secret_access_key: string }>(
      "POST",
      `/_admin/users/${userId}/access-keys`
    ),
  deleteKey: (keyId: string) =>
    request<void>("DELETE", `/_admin/access-keys/${keyId}`),
};

// Buckets API (S3)
export const buckets = {
  list: () =>
    request<Bucket[]>("GET", "/").catch(() => [] as Bucket[]),
};

// Iceberg API
function whq(warehouse?: string) {
  return warehouse ? `?warehouse=${encodeURIComponent(warehouse)}` : "";
}

export const iceberg = {
  getConfig: () =>
    request<{ defaults: Record<string, string> }>("GET", "/iceberg/v1/config"),
  listNamespaces: (warehouse?: string) =>
    request<{ namespaces: string[][] }>(
      "GET",
      `/iceberg/v1/namespaces${whq(warehouse)}`
    ),
  getNamespace: (ns: string, warehouse?: string) =>
    request<{ namespace: string[]; properties: Record<string, string> }>(
      "GET",
      `/iceberg/v1/namespaces/${ns}${whq(warehouse)}`
    ),
  createNamespace: (
    ns: string[],
    properties: Record<string, string> = {},
    warehouse?: string
  ) =>
    request<IcebergNamespace>(
      "POST",
      `/iceberg/v1/namespaces${whq(warehouse)}`,
      { namespace: ns, properties }
    ),
  deleteNamespace: (ns: string, warehouse?: string) =>
    request<void>("DELETE", `/iceberg/v1/namespaces/${ns}${whq(warehouse)}`),
  listTables: (ns: string, warehouse?: string) =>
    request<{ identifiers: IcebergTable[] }>(
      "GET",
      `/iceberg/v1/namespaces/${ns}/tables${whq(warehouse)}`
    ),
  getTable: (ns: string, table: string, warehouse?: string) =>
    request<Record<string, unknown>>(
      "GET",
      `/iceberg/v1/namespaces/${ns}/tables/${table}${whq(warehouse)}`
    ),
  deleteTable: (ns: string, table: string, warehouse?: string) =>
    request<void>(
      "DELETE",
      `/iceberg/v1/namespaces/${ns}/tables/${table}?purgeRequested=false${warehouse ? `&warehouse=${encodeURIComponent(warehouse)}` : ""}`
    ),
};

// Health/metrics
export const cluster = {
  health: () => fetch(`${BASE}/health`).then((r) => r.ok),
  metrics: () => fetch(`${BASE}/metrics`).then((r) => r.text()),
};

// Nodes / Drives
export interface DiskInfo {
  disk_id: string;
  path: string;
  total_capacity: number;
  used_capacity: number;
  status: string;
  shard_count: number;
}

/// Operator-declared state for an OSD. Separate from `online`, which is
/// derived from heartbeats. `in` is the default; `out` / `draining`
/// keep the OSD out of placement.
export type OsdAdminState = "in" | "out" | "draining";

export interface NodeInfo {
  node_id: string;
  node_name: string;
  address: string;
  total_capacity: number;
  used_capacity: number;
  shard_count: number;
  uptime_seconds: number;
  disks: DiskInfo[];
  online: boolean;
  kubernetes_node: string;
  pod_name: string;
  hostname: string;
  os_info: string;
  cpu_cores: number;
  memory_bytes: number;
  version: string;
  /// Optional — present on meta servers that expose admin_state
  /// (anything past the host-actions phase 1 rollout). Older meta
  /// omits the field; treat missing as "in".
  admin_state?: OsdAdminState;
}

export const nodes = {
  list: (): Promise<{ nodes: NodeInfo[] }> =>
    request("GET", "/_admin/nodes"),
  setAdminState: (nodeId: string, state: OsdAdminState) =>
    request<{ found: boolean; changed: boolean; state: OsdAdminState }>(
      "PUT",
      `/_admin/osds/${nodeId}/admin-state`,
      { state },
    ),
  reboot: (nodeId: string) =>
    request<{ provider: string; pod: string; requested: boolean }>(
      "POST",
      `/_admin/osds/${nodeId}/reboot`,
    ),
};

/// Host-lifecycle provider that the gateway is wired to. `provider` is
/// one of "noop" (default — button renders disabled with a hint),
/// "k8s", "linux-ssh", "appliance".
export interface HostProviderInfo {
  provider: string;
  supports_add_host: boolean;
  supports_reboot: boolean;
}

export const hostProvider = {
  info: (): Promise<HostProviderInfo> =>
    request("GET", "/_admin/host-provider"),
  addHosts: (count: number) =>
    request<{
      provider: string;
      previous_replicas: number;
      new_replicas: number;
      pods_added: string[];
    }>("POST", "/_admin/hosts", { count }),
};

/// One entry per currently-Draining OSD. `initial_shards` is captured
/// the first time this leader observed the OSD in Draining state, so
/// progress = migrated / initial. `last_error` carries any transient
/// failure from the latest sweep (empty on success).
export interface DrainStatus {
  node_id: string;
  shards_remaining: number;
  initial_shards: number;
  shards_migrated: number;
  updated_at: number;
  last_error: string;
}

export const drain = {
  status: (): Promise<{ drains: DrainStatus[] }> =>
    request("GET", "/_admin/drain-status"),
};

export interface RebalanceStatus {
  started: boolean;
  paused: boolean;
  last_sweep_at: number;
  scanned_this_pass: number;
  drifts_seen_this_pass: number;
  shards_rebalanced_total: number;
  last_error: string;
  // PG balancer counters. Optional so older gateways (pre-4.7) that
  // don't include them still type-check; the UI falls back to 0.
  pgs_moved_total?: number;
  pg_candidates_last_tick?: number;
  pgs_scanned_last_tick?: number;
}

export const rebalance = {
  status: (): Promise<RebalanceStatus> =>
    request("GET", "/_admin/rebalance-status"),
  pause: () => request<{ paused: boolean }>("POST", "/_admin/rebalance/pause"),
  resume: () => request<{ paused: boolean }>("POST", "/_admin/rebalance/resume"),
};

// ---------------------------------------------------------------------
// Placement groups
// ---------------------------------------------------------------------

export interface PlacementGroup {
  pool: string;
  pg_id: number;
  osd_ids: string[]; // hex-encoded 16-byte UUIDs
  version: number;
  updated_at: number;
  migrating_to_osd_ids: string[];
}

export const placementGroups = {
  list: (
    pool: string,
    opts?: { start_after?: number; max?: number },
  ): Promise<{ pgs: PlacementGroup[]; next_pg_id: number }> => {
    const q = new URLSearchParams();
    if (opts?.start_after) q.set("start_after", String(opts.start_after));
    if (opts?.max) q.set("max", String(opts.max));
    const suffix = q.toString() ? `?${q}` : "";
    return request(
      "GET",
      `/_admin/pools/${encodeURIComponent(pool)}/placement-groups${suffix}`,
    );
  },
};
