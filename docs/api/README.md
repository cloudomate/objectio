# API Reference

ObjectIO provides an S3-compatible REST API.

## Contents

- [S3 Operations](s3-operations.md) - Supported S3 API operations
- [Iceberg REST Catalog](iceberg-api.md) - Iceberg namespace and table API
- [Authentication](authentication.md) - AWS SigV4 and security

## Quick Start

### Using AWS CLI

```bash
# Configure endpoint
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Create bucket
aws s3 mb s3://my-bucket

# Upload object
aws s3 cp file.txt s3://my-bucket/

# List objects
aws s3 ls s3://my-bucket/

# Download object
aws s3 cp s3://my-bucket/file.txt ./downloaded.txt

# Delete object
aws s3 rm s3://my-bucket/file.txt
```

### Using Python (boto3)

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
)

# Create bucket
s3.create_bucket(Bucket='my-bucket')

# Upload object
s3.put_object(Bucket='my-bucket', Key='hello.txt', Body=b'Hello World')

# Download object
response = s3.get_object(Bucket='my-bucket', Key='hello.txt')
data = response['Body'].read()

# List objects
response = s3.list_objects_v2(Bucket='my-bucket')
for obj in response.get('Contents', []):
    print(obj['Key'])
```

### Using cURL

```bash
# List buckets (no auth)
curl http://localhost:9000/

# Create bucket (no auth mode)
curl -X PUT http://localhost:9000/my-bucket

# Upload object
curl -X PUT -d "Hello World" http://localhost:9000/my-bucket/hello.txt

# Download object
curl http://localhost:9000/my-bucket/hello.txt
```

## API Endpoints

### S3 API

| Operation | Method | Path |
|-----------|--------|------|
| List Buckets | GET | `/` |
| Create Bucket | PUT | `/{bucket}` |
| Delete Bucket | DELETE | `/{bucket}` |
| Head Bucket | HEAD | `/{bucket}` |
| List Objects | GET | `/{bucket}` |
| Put Object | PUT | `/{bucket}/{key}` |
| Get Object | GET | `/{bucket}/{key}` |
| Delete Object | DELETE | `/{bucket}/{key}` |
| Head Object | HEAD | `/{bucket}/{key}` |

See [S3 Operations](s3-operations.md) for the complete reference.

### Iceberg REST Catalog API

| Operation | Method | Path |
|-----------|--------|------|
| Get Config | GET | `/iceberg/v1/config` |
| List Namespaces | GET | `/iceberg/v1/namespaces` |
| Create Namespace | POST | `/iceberg/v1/namespaces` |
| Load Namespace | GET | `/iceberg/v1/namespaces/{ns}` |
| Drop Namespace | DELETE | `/iceberg/v1/namespaces/{ns}` |
| List Tables | GET | `/iceberg/v1/namespaces/{ns}/tables` |
| Create Table | POST | `/iceberg/v1/namespaces/{ns}/tables` |
| Load Table | GET | `/iceberg/v1/namespaces/{ns}/tables/{table}` |
| Update Table | POST | `/iceberg/v1/namespaces/{ns}/tables/{table}` |
| Drop Table | DELETE | `/iceberg/v1/namespaces/{ns}/tables/{table}` |
| Rename Table | POST | `/iceberg/v1/tables/rename` |

See [Iceberg REST Catalog](iceberg-api.md) for the complete reference.

## Health Check

```bash
curl http://localhost:9000/health
# {"status":"healthy"}
```

## Error Responses

ObjectIO returns standard S3 error responses:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchBucket</Code>
    <Message>The specified bucket does not exist</Message>
    <BucketName>nonexistent-bucket</BucketName>
    <RequestId>abc123</RequestId>
</Error>
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `AccessDenied` | 403 | Access denied |
| `BucketAlreadyExists` | 409 | Bucket exists |
| `BucketNotEmpty` | 409 | Bucket not empty |
| `NoSuchBucket` | 404 | Bucket not found |
| `NoSuchKey` | 404 | Object not found |
| `InvalidAccessKeyId` | 403 | Invalid credentials |
| `SignatureDoesNotMatch` | 403 | Signature mismatch |
