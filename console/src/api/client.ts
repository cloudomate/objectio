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
  /// Server returns `display_name`. Older code referenced `username` — keep
  /// that as a deprecated alias so existing call sites stay compiling while
  /// callers migrate.
  display_name: string;
  username?: string;
  arn?: string;
  email?: string;
  tenant?: string;
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

// IAM Groups API. Mirrors `users` — same `/_admin/...` shape, same auth gate.
// The meta service stores group ARNs as `arn:objectio:iam::group/<name>`;
// attaching a policy to a group is the same call as attaching to a user but
// with `group_id` in the body instead of `user_id`.
export interface Group {
  group_id: string;
  group_name: string;
  arn: string;
  member_user_ids: string[];
  created_at: number;
}

export const groups = {
  list: () => request<{ groups: Group[] }>("GET", "/_admin/groups"),
  create: (group_name: string) =>
    request<Group>("POST", "/_admin/groups", { group_name }),
  delete: (groupId: string) =>
    request<void>("DELETE", `/_admin/groups/${groupId}`),
  addMember: (groupId: string, userId: string) =>
    request<void>("POST", `/_admin/groups/${groupId}/members`, {
      user_id: userId,
    }),
  removeMember: (groupId: string, userId: string) =>
    request<void>(
      "DELETE",
      `/_admin/groups/${groupId}/members/${userId}`,
    ),
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

// Unity Catalog API — Databricks-style three-level namespace
// (catalog.schema.table) over /api/2.1/unity-catalog/*.
export interface UnityCatalogInfo {
  name: string;
  comment?: string;
  owner?: string;
  created_at: number;
  updated_at: number;
  properties?: Record<string, string>;
}

export interface UnitySchemaInfo {
  name: string;
  catalog_name: string;
  full_name?: string;
  comment?: string;
  owner?: string;
  created_at: number;
  updated_at: number;
  properties?: Record<string, string>;
}

export interface UnityColumnInfo {
  name: string;
  type_text: string;
  type_name?: string;
  type_json?: string;
  position: number;
  nullable?: boolean;
  comment?: string;
  partition_index?: number | null;
}

export interface UnityRowFilter {
  function_full_name: string;
  input_column_names?: string[];
}

export interface UnityColumnMask {
  function_full_name: string;
  using_column_names?: string[];
}

export interface UnityTableInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  table_type: string; // "MANAGED" | "EXTERNAL"
  data_source_format: string; // "DELTA" | "ICEBERG" | "PARQUET" | ...
  storage_location: string;
  columns?: UnityColumnInfo[];
  owner?: string;
  created_at: number;
  updated_at: number;
  properties?: Record<string, string>;
  table_id?: string;
  row_filter?: UnityRowFilter | null;
  column_masks?: Record<string, UnityColumnMask>;
}

export interface UnityFunctionInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  routine_definition?: string; // Source text (the SQL/code)
  routine_body: string; // "SQL" | "EXTERNAL"
  external_language?: string; // Runtime for EXTERNAL bodies (e.g. "PYTHON")
  data_type?: string;
  full_data_type?: string;
  parameter_style?: string;
  is_deterministic?: boolean;
  sql_data_access?: string;
  is_null_call?: boolean;
  security_type?: string;
  specific_name?: string;
  comment?: string;
  owner?: string;
  created_at: number;
  updated_at: number;
  properties?: Record<string, string>;
  function_id?: string;
}

export interface UnityVolumeInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  volume_type: string; // "MANAGED" | "EXTERNAL"
  storage_location: string;
  comment?: string;
  owner?: string;
  created_at: number;
  updated_at: number;
  properties?: Record<string, string>;
  volume_id?: string;
}

export interface UnityModelInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  full_name?: string;
  storage_location: string;
  comment?: string;
  owner?: string;
  created_at: number;
  updated_at: number;
  properties?: Record<string, string>;
  model_id?: string;
}

export interface UnityModelVersionInfo {
  model_name: string;
  catalog_name: string;
  schema_name: string;
  version: number;
  source: string;
  run_id?: string;
  status: string; // "PENDING_REGISTRATION" | "READY" | "FAILED_REGISTRATION"
  description?: string;
  created_at: number;
  updated_at: number;
  properties?: Record<string, string>;
  version_id?: string;
}

const UC = "/api/2.1/unity-catalog";

export const unity = {
  // Catalogs
  listCatalogs: () =>
    request<{ catalogs: UnityCatalogInfo[] }>("GET", `${UC}/catalogs`),
  createCatalog: (name: string, comment = "") =>
    request<UnityCatalogInfo>("POST", `${UC}/catalogs`, { name, comment }),
  getCatalog: (name: string) =>
    request<UnityCatalogInfo>("GET", `${UC}/catalogs/${encodeURIComponent(name)}`),
  deleteCatalog: (name: string, force = false) =>
    request<void>(
      "DELETE",
      `${UC}/catalogs/${encodeURIComponent(name)}${force ? "?force=true" : ""}`,
    ),

  // Schemas — addressed via "{catalog}.{schema}".
  listSchemas: (catalog: string) =>
    request<{ schemas: UnitySchemaInfo[] }>(
      "GET",
      `${UC}/schemas?catalog_name=${encodeURIComponent(catalog)}`,
    ),
  createSchema: (catalog: string, name: string, comment = "") =>
    request<UnitySchemaInfo>("POST", `${UC}/schemas`, {
      name,
      catalog_name: catalog,
      comment,
    }),
  getSchema: (catalog: string, schema: string) =>
    request<UnitySchemaInfo>(
      "GET",
      `${UC}/schemas/${encodeURIComponent(`${catalog}.${schema}`)}`,
    ),
  deleteSchema: (catalog: string, schema: string) =>
    request<void>(
      "DELETE",
      `${UC}/schemas/${encodeURIComponent(`${catalog}.${schema}`)}`,
    ),

  // Tables — addressed via "{catalog}.{schema}.{table}".
  listTables: (catalog: string, schema: string) =>
    request<{ tables: UnityTableInfo[] }>(
      "GET",
      `${UC}/tables?catalog_name=${encodeURIComponent(catalog)}&schema_name=${encodeURIComponent(schema)}`,
    ),
  createTable: (req: {
    name: string;
    catalog_name: string;
    schema_name: string;
    table_type?: string;
    data_source_format?: string;
    storage_location?: string;
    columns?: UnityColumnInfo[];
  }) => request<UnityTableInfo>("POST", `${UC}/tables`, req),
  getTable: (catalog: string, schema: string, table: string) =>
    request<UnityTableInfo>(
      "GET",
      `${UC}/tables/${encodeURIComponent(`${catalog}.${schema}.${table}`)}`,
    ),
  deleteTable: (catalog: string, schema: string, table: string) =>
    request<void>(
      "DELETE",
      `${UC}/tables/${encodeURIComponent(`${catalog}.${schema}.${table}`)}`,
    ),

  // Functions — addressed via "{catalog}.{schema}.{function}".
  listFunctions: (catalog: string, schema: string) =>
    request<{ functions: UnityFunctionInfo[] }>(
      "GET",
      `${UC}/functions?catalog_name=${encodeURIComponent(catalog)}&schema_name=${encodeURIComponent(schema)}`,
    ),
  createFunction: (req: {
    name: string;
    catalog_name: string;
    schema_name: string;
    routine_definition: string;
    routine_body?: string; // "SQL" | "EXTERNAL", defaults to SQL
    external_language?: string;
    data_type?: string;
    comment?: string;
  }) =>
    // Wrap in `function_info` envelope per Databricks spec — the same
    // shape the official Databricks SDK posts.
    request<UnityFunctionInfo>("POST", `${UC}/functions`, {
      function_info: req,
    }),
  getFunction: (catalog: string, schema: string, name: string) =>
    request<UnityFunctionInfo>(
      "GET",
      `${UC}/functions/${encodeURIComponent(`${catalog}.${schema}.${name}`)}`,
    ),
  deleteFunction: (catalog: string, schema: string, name: string) =>
    request<void>(
      "DELETE",
      `${UC}/functions/${encodeURIComponent(`${catalog}.${schema}.${name}`)}`,
    ),

  // Volumes — addressed via "{catalog}.{schema}.{volume}".
  listVolumes: (catalog: string, schema: string) =>
    request<{ volumes: UnityVolumeInfo[] }>(
      "GET",
      `${UC}/volumes?catalog_name=${encodeURIComponent(catalog)}&schema_name=${encodeURIComponent(schema)}`,
    ),
  createVolume: (req: {
    name: string;
    catalog_name: string;
    schema_name: string;
    volume_type?: string;
    storage_location?: string;
    comment?: string;
  }) => request<UnityVolumeInfo>("POST", `${UC}/volumes`, req),
  getVolume: (catalog: string, schema: string, name: string) =>
    request<UnityVolumeInfo>(
      "GET",
      `${UC}/volumes/${encodeURIComponent(`${catalog}.${schema}.${name}`)}`,
    ),
  deleteVolume: (catalog: string, schema: string, name: string) =>
    request<void>(
      "DELETE",
      `${UC}/volumes/${encodeURIComponent(`${catalog}.${schema}.${name}`)}`,
    ),

  // Models — addressed via "{catalog}.{schema}.{model}".
  listModels: (catalog: string, schema: string) =>
    request<{ registered_models: UnityModelInfo[] }>(
      "GET",
      `${UC}/models?catalog_name=${encodeURIComponent(catalog)}&schema_name=${encodeURIComponent(schema)}`,
    ),
  createModel: (req: {
    name: string;
    catalog_name: string;
    schema_name: string;
    storage_location?: string;
    comment?: string;
  }) => request<UnityModelInfo>("POST", `${UC}/models`, req),
  getModel: (catalog: string, schema: string, name: string) =>
    request<UnityModelInfo>(
      "GET",
      `${UC}/models/${encodeURIComponent(`${catalog}.${schema}.${name}`)}`,
    ),
  deleteModel: (catalog: string, schema: string, name: string) =>
    request<void>(
      "DELETE",
      `${UC}/models/${encodeURIComponent(`${catalog}.${schema}.${name}`)}`,
    ),

  // Model Versions
  listModelVersions: (catalog: string, schema: string, model: string) =>
    request<{ model_versions: UnityModelVersionInfo[] }>(
      "GET",
      `${UC}/models/${encodeURIComponent(`${catalog}.${schema}.${model}`)}/versions`,
    ),
  createModelVersion: (
    catalog: string,
    schema: string,
    model: string,
    req: { source: string; run_id?: string; description?: string },
  ) =>
    request<UnityModelVersionInfo>(
      "POST",
      `${UC}/models/${encodeURIComponent(`${catalog}.${schema}.${model}`)}/versions`,
      { ...req, catalog_name: catalog, schema_name: schema, model_name: model },
    ),
  updateModelVersionStatus: (
    catalog: string,
    schema: string,
    model: string,
    version: number,
    status: string,
  ) =>
    request<UnityModelVersionInfo>(
      "PATCH",
      `${UC}/models/${encodeURIComponent(`${catalog}.${schema}.${model}`)}/versions/${version}`,
      { status },
    ),
  deleteModelVersion: (
    catalog: string,
    schema: string,
    model: string,
    version: number,
  ) =>
    request<void>(
      "DELETE",
      `${UC}/models/${encodeURIComponent(`${catalog}.${schema}.${model}`)}/versions/${version}`,
    ),

  // Entity-level policies (admin only). These sit alongside the IAM policies
  // managed in /policies — Unity evaluates IAM-attached first, then walks
  // catalog → schema → table policies. Body shape: { policy: <JSON> }.
  getCatalogPolicy: (name: string) =>
    request<{ policy: unknown }>(
      "GET",
      `${UC}/catalogs/${encodeURIComponent(name)}/policy`,
    ),
  setCatalogPolicy: (name: string, policy: unknown) =>
    request<void>(
      "PUT",
      `${UC}/catalogs/${encodeURIComponent(name)}/policy`,
      { policy },
    ),
  getSchemaPolicy: (catalog: string, schema: string) =>
    request<{ policy: unknown }>(
      "GET",
      `${UC}/schemas/${encodeURIComponent(`${catalog}.${schema}`)}/policy`,
    ),
  setSchemaPolicy: (catalog: string, schema: string, policy: unknown) =>
    request<void>(
      "PUT",
      `${UC}/schemas/${encodeURIComponent(`${catalog}.${schema}`)}/policy`,
      { policy },
    ),
  getTablePolicy: (catalog: string, schema: string, table: string) =>
    request<{ policy: unknown }>(
      "GET",
      `${UC}/tables/${encodeURIComponent(`${catalog}.${schema}.${table}`)}/policy`,
    ),
  setTablePolicy: (
    catalog: string,
    schema: string,
    table: string,
    policy: unknown,
  ) =>
    request<void>(
      "PUT",
      `${UC}/tables/${encodeURIComponent(`${catalog}.${schema}.${table}`)}/policy`,
      { policy },
    ),

  // Bind (or clear) a row filter and/or column masks on a table.
  // Pass `row_filter: null` and an empty `column_masks` object to clear.
  setTableSecurity: (
    catalog: string,
    schema: string,
    table: string,
    body: {
      row_filter?: UnityRowFilter | null;
      column_masks?: Record<string, UnityColumnMask>;
    },
  ) =>
    request<UnityTableInfo>(
      "PUT",
      `${UC}/tables/${encodeURIComponent(`${catalog}.${schema}.${table}`)}/security`,
      body,
    ),

  // Caller identity — engines use this for current_user() / is_member().
  whoami: () =>
    request<{
      user_name: string;
      user_id: string;
      user_arn: string;
      groups: string[];
      group_arns: string[];
      tenant: string;
      is_admin: boolean;
    }>("GET", `${UC}/me`),
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
