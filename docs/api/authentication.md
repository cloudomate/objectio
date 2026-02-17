# Authentication

ObjectIO supports AWS Signature Version 4 (SigV4) authentication for S3 API compatibility.

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        S3 Client                                 │
│  (aws-cli, boto3, aws-sdk-rust, etc.)                           │
└─────────────────────────┬───────────────────────────────────────┘
                          │ Authorization: AWS4-HMAC-SHA256 ...
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ObjectIO Gateway                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                  Auth Middleware                           │  │
│  │  1. Parse Authorization header                             │  │
│  │  2. Lookup credentials from Metadata Service (with cache)  │  │
│  │  3. Compute canonical request                              │  │
│  │  4. Verify signature                                       │  │
│  │  5. Evaluate bucket policy                                 │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Credential Storage

Credentials are centrally managed by the **Metadata Service**, not the gateway. This ensures:

- **Persistence**: Credentials survive gateway restarts
- **Consistency**: All gateways see the same credentials
- **Centralized management**: Single source of truth for IAM

```
┌─────────────┐      gRPC       ┌─────────────────┐
│   Gateway   │ ◄────────────► │  Metadata Svc   │
│             │ GetAccessKey   │                 │
│  (caches    │ ForAuth        │  - Users        │
│   5 min)    │                │  - Access Keys  │
└─────────────┘                └─────────────────┘
```

### Initial Admin User

On first startup, the metadata service creates a default admin user:

```bash
# Admin credentials are logged on meta service startup:
INFO objectio_meta: ============================================
INFO objectio_meta: Admin credentials (save these!):
INFO objectio_meta:   Access Key ID:     AKIA...
INFO objectio_meta:   Secret Access Key: wJalr...
INFO objectio_meta: ============================================
```

**Important**: Save these credentials! They are only shown once at startup.

---

## AWS Signature Version 4

ObjectIO implements AWS SigV4, the standard authentication method for AWS services.

### Authorization Header Format

```
Authorization: AWS4-HMAC-SHA256
Credential={access_key}/{date}/{region}/s3/aws4_request,
SignedHeaders={signed_headers},
Signature={signature}
```

**Example:**

```
Authorization: AWS4-HMAC-SHA256
Credential=AKIAIOSFODNN7EXAMPLE/20240115/us-east-1/s3/aws4_request,
SignedHeaders=host;x-amz-content-sha256;x-amz-date,
Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024
```

### Required Headers

| Header | Description |
|--------|-------------|
| `Host` | Bucket endpoint |
| `x-amz-date` | Request timestamp (ISO 8601) |
| `x-amz-content-sha256` | SHA-256 hash of request body |

---

## User Management

### Using the CLI

Users are managed through the ObjectIO CLI (connects directly to the metadata service):

```bash
# List all users
objectio-cli user list

# Create a new user
objectio-cli user create alice --email alice@example.com

# Delete a user
objectio-cli user delete <user_id>
```

### Managing Access Keys (CLI)

```bash
# List access keys for a user
objectio-cli key list <user_id>

# Create additional access key for a user
objectio-cli key create <user_id>

# Delete an access key
objectio-cli key delete <access_key_id>
```

---

## Admin API

The Admin API provides HTTP endpoints for user and access key management. It uses the same SigV4 authentication as the S3 API, but **only the admin user** can access these endpoints.

### Admin API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/_admin/users` | GET | List all users |
| `/_admin/users` | POST | Create a new user |
| `/_admin/users/{user_id}` | DELETE | Delete a user |
| `/_admin/users/{user_id}/access-keys` | GET | List user's access keys |
| `/_admin/users/{user_id}/access-keys` | POST | Create access key for user |
| `/_admin/access-keys/{access_key_id}` | DELETE | Delete an access key |

### Authentication

The Admin API requires SigV4 authentication with **admin credentials**. Non-admin users receive a `403 Forbidden` response.

```bash
# Configure admin credentials
export AWS_ACCESS_KEY_ID=AKIA...      # From meta service startup logs
export AWS_SECRET_ACCESS_KEY=...
```

### List Users

```bash
curl -X GET https://s3.example.com/_admin/users \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

Response:

```json
{
  "users": [
    {
      "user_id": "550e8400-e29b-41d4-a716-446655440000",
      "display_name": "admin",
      "arn": "arn:objectio:iam::user/admin",
      "status": "UserActive",
      "created_at": 1705320000,
      "email": ""
    }
  ],
  "is_truncated": false,
  "next_marker": null
}
```

### Create User

```bash
curl -X POST https://s3.example.com/_admin/users \
  -H "Content-Type: application/json" \
  -d '{"display_name": "alice", "email": "alice@example.com"}' \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

Response:

```json
{
  "user_id": "660e8400-e29b-41d4-a716-446655440001",
  "display_name": "alice",
  "arn": "arn:objectio:iam::user/alice",
  "status": "UserActive",
  "created_at": 1705320100,
  "email": "alice@example.com"
}
```

### Create Access Key

```bash
curl -X POST https://s3.example.com/_admin/users/{user_id}/access-keys \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

Response:

```json
{
  "access_key_id": "AKIAIOSFODNN7EXAMPLE",
  "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "user_id": "660e8400-e29b-41d4-a716-446655440001",
  "status": "KeyActive",
  "created_at": 1705320200
}
```

**Important:** The `secret_access_key` is only returned when creating a key. Save it immediately!

### Delete User

```bash
curl -X DELETE https://s3.example.com/_admin/users/{user_id} \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

Returns `204 No Content` on success.

### Delete Access Key

```bash
curl -X DELETE https://s3.example.com/_admin/access-keys/{access_key_id} \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY"
```

Returns `204 No Content` on success.

### Error Responses

| Status | Description |
|--------|-------------|
| `401 Unauthorized` | Authentication required (no-auth mode) |
| `403 Forbidden` | Non-admin user attempted access |
| `404 Not Found` | User or access key not found |
| `500 Internal Server Error` | Server error |

---

## Bucket Policies

Bucket policies control access to buckets and objects using IAM-style JSON policies.

### Policy Structure

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicRead",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::my-bucket/*"
        }
    ]
}
```

### Policy Elements

| Element | Description |
|---------|-------------|
| `Version` | Policy version (always "2012-10-17") |
| `Statement` | Array of permission statements |
| `Sid` | Statement ID (optional) |
| `Effect` | "Allow" or "Deny" |
| `Principal` | Who the statement applies to |
| `Action` | S3 actions (e.g., "s3:GetObject") |
| `Resource` | ARN of bucket/objects |
| `Condition` | Optional conditions |

### Supported Actions

**S3 Object Actions:**
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`
- `s3:GetObjectVersion`

**S3 Bucket Actions:**
- `s3:ListBucket`
- `s3:GetBucketLocation`
- `s3:GetBucketPolicy`
- `s3:PutBucketPolicy`
- `s3:DeleteBucketPolicy`

**Iceberg Namespace Actions:**
- `iceberg:ListNamespaces`
- `iceberg:CreateNamespace`
- `iceberg:LoadNamespace`
- `iceberg:DropNamespace`
- `iceberg:UpdateNamespaceProperties`

**Iceberg Table Actions:**
- `iceberg:ListTables`
- `iceberg:CreateTable`
- `iceberg:LoadTable`
- `iceberg:UpdateTable`
- `iceberg:DropTable`
- `iceberg:RenameTable`

### Example Policies

**Public Read Access:**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::public-bucket/*"
        }
    ]
}
```

**Cross-Account Access:**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:user/external-user"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::shared-bucket/*"
        }
    ]
}
```

**IP-Based Restriction:**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::secure-bucket",
                "arn:aws:s3:::secure-bucket/*"
            ],
            "Condition": {
                "NotIpAddress": {
                    "aws:SourceIp": "192.168.1.0/24"
                }
            }
        }
    ]
}
```

### Setting Bucket Policy

```bash
# Using AWS CLI
aws s3api put-bucket-policy \
  --endpoint-url http://localhost:9000 \
  --bucket my-bucket \
  --policy file://policy.json

# Get current policy
aws s3api get-bucket-policy \
  --endpoint-url http://localhost:9000 \
  --bucket my-bucket

# Delete policy
aws s3api delete-bucket-policy \
  --endpoint-url http://localhost:9000 \
  --bucket my-bucket
```

---

## Condition Keys

Supported condition keys for policy evaluation:

| Key | Description |
|-----|-------------|
| `aws:SourceIp` | Client IP address |
| `aws:CurrentTime` | Request timestamp |
| `aws:SecureTransport` | Whether HTTPS was used |
| `s3:prefix` | Object key prefix |
| `s3:max-keys` | Maximum keys in list |

---

## Anonymous Access

When authentication is disabled (`--no-auth`), all requests are treated as anonymous:

```bash
# Start gateway without authentication
objectio-gateway --no-auth

# Anonymous requests work
curl http://localhost:9000/my-bucket/file.txt
```

**Warning:** Only use `--no-auth` for testing or internal networks.

---

## Client Configuration

### AWS CLI

```bash
# Configure credentials
aws configure
# Enter: Access Key ID, Secret Access Key, Region, Output format

# Or use environment variables
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=wJalr...
export AWS_DEFAULT_REGION=us-east-1

# Use with ObjectIO
aws s3 --endpoint-url http://localhost:9000 ls
```

### Boto3 (Python)

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='AKIA...',
    aws_secret_access_key='wJalr...',
    region_name='us-east-1'
)

# List buckets
response = s3.list_buckets()
```

### AWS SDK for Rust

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;

let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("http://localhost:9000")
    .load()
    .await;

let client = Client::new(&config);

let buckets = client.list_buckets().send().await?;
```

---

## Security Best Practices

1. **Use HTTPS in Production**
   - Configure TLS certificates
   - Redirect HTTP to HTTPS

2. **Rotate Access Keys**
   - Create new keys periodically
   - Delete old keys after transition

3. **Use Least Privilege**
   - Grant minimum required permissions
   - Use specific resource ARNs

4. **Enable Logging**
   - Track authentication failures
   - Monitor for suspicious activity

5. **Secure Credential Storage**
   - Never hardcode credentials
   - Use environment variables or secrets managers
