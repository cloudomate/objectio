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

// Simple fetch wrapper with error handling
async function request<T>(
  method: string,
  path: string,
  body?: unknown
): Promise<T> {
  const opts: RequestInit = {
    method,
    headers: { "Content-Type": "application/json" },
  };
  if (body) opts.body = JSON.stringify(body);

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
};
