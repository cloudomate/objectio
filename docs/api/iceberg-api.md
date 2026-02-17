# Iceberg REST Catalog API

ObjectIO includes an Apache Iceberg REST Catalog API, enabling query engines like Spark, Trino, and Flink to discover and manage Iceberg tables.

## Base URL

All Iceberg endpoints are served under `/iceberg/v1/`.

```
http://gateway:9000/iceberg/v1/
```

## Authentication

Iceberg endpoints use the same SigV4 authentication as S3. When `--no-auth` is set, all Iceberg endpoints are accessible without credentials.

---

## Configuration

### GetConfig

Get catalog configuration.

```
GET /iceberg/v1/config
```

**Response:**

```json
{
  "defaults": {},
  "overrides": {
    "warehouse": "s3://objectio-warehouse"
  }
}
```

---

## Namespace Operations

### ListNamespaces

List all namespaces, optionally filtered by parent.

```
GET /iceberg/v1/namespaces
GET /iceberg/v1/namespaces?parent=parent_ns
```

**Response:**

```json
{
  "namespaces": [
    ["analytics"],
    ["production"]
  ]
}
```

---

### CreateNamespace

Create a new namespace.

```
POST /iceberg/v1/namespaces
```

**Request Body:**

```json
{
  "namespace": ["analytics"],
  "properties": {
    "owner": "alice",
    "description": "Analytics data"
  }
}
```

**Response:**

```json
{
  "namespace": ["analytics"],
  "properties": {
    "owner": "alice",
    "description": "Analytics data"
  }
}
```

---

### LoadNamespace

Get namespace details.

```
GET /iceberg/v1/namespaces/{namespace}
```

**Response:**

```json
{
  "namespace": ["analytics"],
  "properties": {
    "owner": "alice"
  }
}
```

---

### NamespaceExists

Check if a namespace exists.

```
HEAD /iceberg/v1/namespaces/{namespace}
```

**Response:** 204 No Content (exists) or 404 Not Found

---

### DropNamespace

Delete a namespace. Must be empty (no tables).

```
DELETE /iceberg/v1/namespaces/{namespace}
```

**Response:** 204 No Content

---

### UpdateNamespaceProperties

Update namespace properties.

```
POST /iceberg/v1/namespaces/{namespace}/properties
```

**Request Body:**

```json
{
  "updates": {
    "description": "Updated description"
  },
  "removals": ["old_property"]
}
```

**Response:**

```json
{
  "updated": ["description"],
  "removed": ["old_property"],
  "missing": []
}
```

---

## Table Operations

### ListTables

List tables in a namespace.

```
GET /iceberg/v1/namespaces/{namespace}/tables
```

**Response:**

```json
{
  "identifiers": [
    {
      "namespace": ["analytics"],
      "name": "events"
    }
  ]
}
```

---

### CreateTable

Create a new table.

```
POST /iceberg/v1/namespaces/{namespace}/tables
```

**Request Body:**

```json
{
  "name": "events",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "id", "type": "long", "required": true},
      {"id": 2, "name": "data", "type": "string", "required": false}
    ]
  },
  "properties": {}
}
```

**Response:**

```json
{
  "metadata": {
    "format-version": 2,
    "table-uuid": "...",
    "location": "s3://objectio-warehouse/analytics/events",
    "schema": { ... }
  },
  "metadata-location": "s3://objectio-warehouse/analytics/events/metadata/v1.metadata.json"
}
```

---

### LoadTable

Get table metadata.

```
GET /iceberg/v1/namespaces/{namespace}/tables/{table}
```

**Response:** Same format as CreateTable response.

---

### TableExists

Check if a table exists.

```
HEAD /iceberg/v1/namespaces/{namespace}/tables/{table}
```

**Response:** 204 No Content (exists) or 404 Not Found

---

### UpdateTable

Update table metadata (schema evolution, property changes).

```
POST /iceberg/v1/namespaces/{namespace}/tables/{table}
```

---

### DropTable

Delete a table.

```
DELETE /iceberg/v1/namespaces/{namespace}/tables/{table}
```

**Response:** 204 No Content

---

### RenameTable

Rename a table (optionally across namespaces).

```
POST /iceberg/v1/tables/rename
```

**Request Body:**

```json
{
  "source": {
    "namespace": ["analytics"],
    "name": "events"
  },
  "destination": {
    "namespace": ["analytics"],
    "name": "events_v2"
  }
}
```

**Response:** 204 No Content

---

## Access Control

### Namespace Policy

Set a policy on a namespace (admin only).

```
PUT /iceberg/v1/namespaces/{namespace}/policy
```

**Request Body:**

```json
{
  "policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"OBIO\":[\"arn:objectio:iam::user/alice\"]},\"Action\":[\"iceberg:*\"],\"Resource\":[\"arn:obio:iceberg:::analytics/*\"]}]}"
}
```

### Table Policy

Set a policy on a table (admin only).

```
PUT /iceberg/v1/namespaces/{namespace}/tables/{table}/policy
```

**Request Body:** Same format as namespace policy.

### Policy Evaluation

Policies follow the same semantics as S3 bucket policies:
- **Explicit Deny** always wins
- **Explicit Allow** grants access
- **Implicit Deny** (no matching statement) allows access (owner has implicit access)
- **No policy** allows access

### Iceberg Actions

| Action | Description |
|--------|-------------|
| `iceberg:ListNamespaces` | List namespaces |
| `iceberg:CreateNamespace` | Create a namespace |
| `iceberg:LoadNamespace` | Load namespace details |
| `iceberg:DropNamespace` | Delete a namespace |
| `iceberg:UpdateNamespaceProperties` | Update namespace properties |
| `iceberg:ListTables` | List tables in a namespace |
| `iceberg:CreateTable` | Create a table |
| `iceberg:LoadTable` | Load table metadata |
| `iceberg:UpdateTable` | Update table metadata |
| `iceberg:DropTable` | Delete a table |
| `iceberg:RenameTable` | Rename a table |

### Resource ARNs

- Namespace: `arn:obio:iceberg:::{namespace}`
- Table: `arn:obio:iceberg:::{namespace}/{table}`

---

## Client Configuration

### Apache Spark

```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.objectio", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.objectio.type", "rest") \
    .config("spark.sql.catalog.objectio.uri", "http://gateway:9000/iceberg/v1") \
    .config("spark.sql.catalog.objectio.warehouse", "s3://objectio-warehouse") \
    .getOrCreate()

# Use the catalog
spark.sql("CREATE NAMESPACE objectio.analytics")
spark.sql("CREATE TABLE objectio.analytics.events (id BIGINT, data STRING)")
```

### Trino

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://gateway:9000/iceberg/v1
iceberg.rest-catalog.warehouse=s3://objectio-warehouse
```

---

## Error Responses

Iceberg errors follow the Iceberg REST API error format:

```json
{
  "error": {
    "message": "Namespace does not exist: analytics",
    "type": "NoSuchNamespaceException",
    "code": 404
  }
}
```

### Error Types

| Type | HTTP Status | Description |
|------|-------------|-------------|
| `NoSuchNamespaceException` | 404 | Namespace not found |
| `NamespaceAlreadyExistsException` | 409 | Namespace already exists |
| `NoSuchTableException` | 404 | Table not found |
| `AlreadyExistsException` | 409 | Table already exists |
| `ForbiddenException` | 403 | Access denied by policy |
| `BadRequestException` | 400 | Invalid request |
