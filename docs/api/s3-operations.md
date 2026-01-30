# S3 API Operations

Supported S3 API operations in ObjectIO.

## Bucket Operations

### ListBuckets

List all buckets owned by the authenticated user.

```
GET /
```

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
    <Owner>
        <ID>user-id</ID>
        <DisplayName>User Name</DisplayName>
    </Owner>
    <Buckets>
        <Bucket>
            <Name>my-bucket</Name>
            <CreationDate>2024-01-15T10:30:00.000Z</CreationDate>
        </Bucket>
    </Buckets>
</ListAllMyBucketsResult>
```

---

### CreateBucket

Create a new bucket.

```
PUT /{bucket}
```

**Headers:**
- `x-amz-acl`: Canned ACL (optional)

**Response:** 200 OK

---

### DeleteBucket

Delete an empty bucket.

```
DELETE /{bucket}
```

**Response:** 204 No Content

**Errors:**
- `BucketNotEmpty`: Bucket contains objects

---

### HeadBucket

Check if a bucket exists and you have access.

```
HEAD /{bucket}
```

**Response:** 200 OK (bucket exists) or 404 Not Found

---

### GetBucketLocation

Get the region of a bucket.

```
GET /{bucket}?location
```

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint>us-east-1</LocationConstraint>
```

---

## Object Operations

### PutObject

Upload an object.

```
PUT /{bucket}/{key}
```

**Headers:**
- `Content-Type`: MIME type
- `Content-Length`: Object size
- `x-amz-meta-*`: User metadata

**Response:**

```
HTTP/1.1 200 OK
ETag: "d41d8cd98f00b204e9800998ecf8427e"
```

**Example:**

```bash
curl -X PUT \
  -H "Content-Type: text/plain" \
  -d "Hello World" \
  http://localhost:9000/my-bucket/hello.txt
```

---

### GetObject

Download an object.

```
GET /{bucket}/{key}
```

**Headers:**
- `Range`: Byte range (optional)

**Response Headers:**
- `Content-Type`: Object MIME type
- `Content-Length`: Object size
- `ETag`: Object hash
- `Last-Modified`: Last modification time

**Range Request:**

```bash
curl -H "Range: bytes=0-99" http://localhost:9000/my-bucket/file.txt
```

---

### HeadObject

Get object metadata without body.

```
HEAD /{bucket}/{key}
```

**Response Headers:**
- `Content-Type`: Object MIME type
- `Content-Length`: Object size
- `ETag`: Object hash
- `x-amz-meta-*`: User metadata

---

### DeleteObject

Delete an object.

```
DELETE /{bucket}/{key}
```

**Response:** 204 No Content

---

### CopyObject

Copy an object.

```
PUT /{bucket}/{key}
x-amz-copy-source: /source-bucket/source-key
```

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
    <LastModified>2024-01-15T10:30:00.000Z</LastModified>
    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
</CopyObjectResult>
```

---

### ListObjectsV2

List objects in a bucket.

```
GET /{bucket}?list-type=2
```

**Parameters:**
- `prefix`: Filter by prefix
- `delimiter`: Group by delimiter
- `max-keys`: Max results (default: 1000)
- `continuation-token`: Pagination token
- `start-after`: Start listing after this key

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
    <Name>my-bucket</Name>
    <Prefix></Prefix>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>file1.txt</Key>
        <LastModified>2024-01-15T10:30:00.000Z</LastModified>
        <ETag>"abc123"</ETag>
        <Size>1024</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
</ListBucketResult>
```

---

## Multipart Upload

Multipart upload allows uploading large objects in parts, which is required for files larger than 5GB and recommended for files larger than 100MB.

### Limits and Behavior

| Parameter | Value | Notes |
|-----------|-------|-------|
| Minimum part size | 5 MB | Except last part |
| Maximum part size | 5 GB | |
| Maximum parts | 10,000 | |
| Maximum object size | 5 TB | 10,000 × 5GB |
| Default AWS CLI part size | 8 MB | Configurable via `--multipart-chunk-size` |

### Multi-Stripe Chunking

ObjectIO uses a 4MB internal block size. When a part exceeds 4MB, it is automatically split into multiple stripes:

```
8 MB Part Upload:
┌─────────────────────────────────────────────────────┐
│                   8 MB Part Data                     │
└─────────────────────────────────────────────────────┘
                         │
              Split into ~4MB stripes
                         │
         ┌───────────────┴───────────────┐
         ▼                               ▼
    ┌─────────┐                     ┌─────────┐
    │ Stripe 0│ (4 MB)              │ Stripe 1│ (4 MB)
    └────┬────┘                     └────┬────┘
         │                               │
    EC Encode (4+2)                 EC Encode (4+2)
         │                               │
    6 shards                        6 shards
```

Each stripe is independently erasure-coded and distributed across storage nodes.

### CreateMultipartUpload

Initiate a multipart upload.

```
POST /{bucket}/{key}?uploads
```

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
    <Bucket>my-bucket</Bucket>
    <Key>large-file.zip</Key>
    <UploadId>abc123</UploadId>
</InitiateMultipartUploadResult>
```

---

### UploadPart

Upload a part. Parts can be uploaded in any order and in parallel.

```
PUT /{bucket}/{key}?partNumber={n}&uploadId={id}
```

**Parameters:**

- `partNumber`: 1 to 10,000
- `uploadId`: From CreateMultipartUpload response

**Response:**

```
HTTP/1.1 200 OK
ETag: "part-etag"
```

**Example with curl:**

```bash
# Upload part 1
curl -X PUT \
  "http://localhost:9000/my-bucket/large-file?partNumber=1&uploadId=abc123" \
  --data-binary @part1.bin
```

---

### CompleteMultipartUpload

Complete the upload by providing the list of parts in order.

```
POST /{bucket}/{key}?uploadId={id}
```

**Request Body:**

```xml
<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>"part1-etag"</ETag>
    </Part>
    <Part>
        <PartNumber>2</PartNumber>
        <ETag>"part2-etag"</ETag>
    </Part>
</CompleteMultipartUpload>
```

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult>
    <Location>http://localhost:9000/my-bucket/large-file</Location>
    <Bucket>my-bucket</Bucket>
    <Key>large-file</Key>
    <ETag>"abc123-2"</ETag>
</CompleteMultipartUploadResult>
```

The final ETag format is `"<md5-of-part-md5s>-<part-count>"`.

---

### AbortMultipartUpload

Abort an in-progress upload and delete uploaded parts.

```
DELETE /{bucket}/{key}?uploadId={id}
```

**Response:** 204 No Content

---

### ListParts

List uploaded parts for an in-progress multipart upload.

```
GET /{bucket}/{key}?uploadId={id}
```

**Parameters:**

- `max-parts`: Maximum parts to return (default: 1000)
- `part-number-marker`: Start listing after this part number

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult>
    <Bucket>my-bucket</Bucket>
    <Key>large-file</Key>
    <UploadId>abc123</UploadId>
    <MaxParts>1000</MaxParts>
    <IsTruncated>false</IsTruncated>
    <Part>
        <PartNumber>1</PartNumber>
        <LastModified>2024-01-15T10:30:00.000Z</LastModified>
        <ETag>"part1-etag"</ETag>
        <Size>8388608</Size>
    </Part>
</ListPartsResult>
```

---

### ListMultipartUploads

List in-progress multipart uploads for a bucket.

```
GET /{bucket}?uploads
```

**Parameters:**

- `prefix`: Filter by key prefix
- `max-uploads`: Maximum uploads to return (default: 1000)

**Response:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult>
    <Bucket>my-bucket</Bucket>
    <MaxUploads>1000</MaxUploads>
    <IsTruncated>false</IsTruncated>
    <Upload>
        <Key>large-file</Key>
        <UploadId>abc123</UploadId>
        <Initiated>2024-01-15T10:30:00.000Z</Initiated>
    </Upload>
</ListMultipartUploadsResult>
```

---

### Example: Upload Large File with AWS CLI

```bash
# AWS CLI automatically uses multipart upload for files > 8MB
aws --endpoint-url http://localhost:9000 s3 cp large-file.zip s3://my-bucket/

# Configure part size (minimum 5MB)
aws configure set default.s3.multipart_chunksize 16MB
aws --endpoint-url http://localhost:9000 s3 cp large-file.zip s3://my-bucket/

# Manual multipart upload
aws --endpoint-url http://localhost:9000 s3api create-multipart-upload \
  --bucket my-bucket --key large-file.zip

# Upload parts
aws --endpoint-url http://localhost:9000 s3api upload-part \
  --bucket my-bucket --key large-file.zip \
  --part-number 1 --upload-id $UPLOAD_ID --body part1.bin

# Complete upload
aws --endpoint-url http://localhost:9000 s3api complete-multipart-upload \
  --bucket my-bucket --key large-file.zip \
  --upload-id $UPLOAD_ID \
  --multipart-upload file://parts.json
```

---

## Bucket Policy

### PutBucketPolicy

Set bucket policy.

```
PUT /{bucket}?policy
```

**Request Body:**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::my-bucket/*"
        }
    ]
}
```

---

### GetBucketPolicy

Get bucket policy.

```
GET /{bucket}?policy
```

---

### DeleteBucketPolicy

Delete bucket policy.

```
DELETE /{bucket}?policy
```

---

## Versioning

### PutBucketVersioning

Enable/suspend versioning.

```
PUT /{bucket}?versioning
```

**Request Body:**

```xml
<VersioningConfiguration>
    <Status>Enabled</Status>
</VersioningConfiguration>
```

---

### GetBucketVersioning

Get versioning status.

```
GET /{bucket}?versioning
```

---

### ListObjectVersions

List object versions.

```
GET /{bucket}?versions
```

---

## Not Yet Implemented

The following operations are planned but not yet implemented:

- S3 Select
- Object Lock
- Inventory
- Analytics
- Replication Configuration
- Lifecycle Configuration
- Website Configuration
- CORS Configuration
- Logging Configuration
- Tagging
- Accelerate Configuration
- Request Payment
