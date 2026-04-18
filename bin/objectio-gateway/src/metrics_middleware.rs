//! Metrics middleware for automatic S3 operation tracking
//!
//! Intercepts all requests and records metrics based on HTTP method and path patterns.

use axum::{body::Body, extract::Request, http::Method, middleware::Next, response::Response};
use objectio_s3::{IcebergOperation, S3Operation, s3_metrics};
use std::time::Instant;

/// Extract S3 operation type from HTTP method and path
fn extract_operation(method: &Method, path: &str) -> Option<S3Operation> {
    // Remove query string
    let path = path.split('?').next().unwrap_or(path);
    let path = path.trim_start_matches('/');

    // Split path into segments
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match (method, segments.as_slice()) {
        // Service level (GET /)
        (m, []) if m == Method::GET => Some(S3Operation::ListBuckets),

        // Bucket operations (GET/PUT/DELETE/HEAD /{bucket})
        (m, [_bucket]) if m == Method::GET => Some(S3Operation::ListObjects),
        (m, [_bucket]) if m == Method::PUT => Some(S3Operation::CreateBucket),
        (m, [_bucket]) if m == Method::DELETE => Some(S3Operation::DeleteBucket),
        (m, [_bucket]) if m == Method::HEAD => Some(S3Operation::HeadBucket),
        (m, [_bucket]) if m == Method::POST => {
            // POST /{bucket}?delete is batch delete - treat as DeleteObjects
            Some(S3Operation::DeleteObjects)
        }

        // Object operations (GET/PUT/DELETE/HEAD/POST /{bucket}/{key...})
        (m, [_bucket, ..]) if m == Method::GET => Some(S3Operation::GetObject),
        (m, [_bucket, ..]) if m == Method::PUT => Some(S3Operation::PutObject),
        (m, [_bucket, ..]) if m == Method::DELETE => Some(S3Operation::DeleteObject),
        (m, [_bucket, ..]) if m == Method::HEAD => Some(S3Operation::HeadObject),
        (m, [_bucket, ..]) if m == Method::POST => {
            // POST on object path could be multipart initiate/complete
            Some(S3Operation::InitiateMultipartUpload)
        }

        // Skip admin and metrics endpoints
        _ => None,
    }
}

/// Refine operation type based on query parameters
fn refine_operation(op: S3Operation, query: Option<&str>) -> S3Operation {
    let query = match query {
        Some(q) if !q.is_empty() => q,
        _ => return op,
    };

    match op {
        S3Operation::PutObject if query.contains("uploadId") && query.contains("partNumber") => {
            S3Operation::UploadPart
        }
        S3Operation::GetObject if query.contains("uploadId") => S3Operation::ListParts,
        S3Operation::DeleteObject if query.contains("uploadId") => {
            S3Operation::AbortMultipartUpload
        }
        S3Operation::InitiateMultipartUpload if query.contains("uploads") => {
            S3Operation::InitiateMultipartUpload
        }
        S3Operation::InitiateMultipartUpload if query.contains("uploadId") => {
            S3Operation::CompleteMultipartUpload
        }
        _ => op,
    }
}

/// Extract Iceberg operation type from HTTP method and path (without /iceberg prefix).
fn extract_iceberg_operation(method: &Method, path: &str) -> Option<IcebergOperation> {
    let path = path.split('?').next().unwrap_or(path);
    let path = path.trim_start_matches('/');
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match (method, segments.as_slice()) {
        // GET /v1/config
        (m, ["v1", "config"]) if m == Method::GET => Some(IcebergOperation::GetConfig),
        // GET /v1/namespaces
        (m, ["v1", "namespaces"]) if m == Method::GET => Some(IcebergOperation::ListNamespaces),
        // POST /v1/namespaces
        (m, ["v1", "namespaces"]) if m == Method::POST => Some(IcebergOperation::CreateNamespace),
        // POST /v1/tables/rename
        (m, ["v1", "tables", "rename"]) if m == Method::POST => Some(IcebergOperation::RenameTable),
        // POST /v1/namespaces/{ns}/properties
        (m, ["v1", "namespaces", _ns, "properties"]) if m == Method::POST => {
            Some(IcebergOperation::UpdateNamespaceProperties)
        }
        // GET /v1/namespaces/{ns}/tables
        (m, ["v1", "namespaces", _ns, "tables"]) if m == Method::GET => {
            Some(IcebergOperation::ListTables)
        }
        // POST /v1/namespaces/{ns}/tables
        (m, ["v1", "namespaces", _ns, "tables"]) if m == Method::POST => {
            Some(IcebergOperation::CreateTable)
        }
        // GET /v1/namespaces/{ns}/tables/{table}
        (m, ["v1", "namespaces", _ns, "tables", _table]) if m == Method::GET => {
            Some(IcebergOperation::LoadTable)
        }
        // POST /v1/namespaces/{ns}/tables/{table}
        (m, ["v1", "namespaces", _ns, "tables", _table]) if m == Method::POST => {
            Some(IcebergOperation::UpdateTable)
        }
        // HEAD /v1/namespaces/{ns}/tables/{table}
        (m, ["v1", "namespaces", _ns, "tables", _table]) if m == Method::HEAD => {
            Some(IcebergOperation::TableExists)
        }
        // DELETE /v1/namespaces/{ns}/tables/{table}
        (m, ["v1", "namespaces", _ns, "tables", _table]) if m == Method::DELETE => {
            Some(IcebergOperation::DropTable)
        }
        // GET /v1/namespaces/{ns}
        (m, ["v1", "namespaces", _ns]) if m == Method::GET => Some(IcebergOperation::LoadNamespace),
        // HEAD /v1/namespaces/{ns}
        (m, ["v1", "namespaces", _ns]) if m == Method::HEAD => {
            Some(IcebergOperation::NamespaceExists)
        }
        // DELETE /v1/namespaces/{ns}
        (m, ["v1", "namespaces", _ns]) if m == Method::DELETE => {
            Some(IcebergOperation::DropNamespace)
        }
        _ => None,
    }
}

/// Metrics middleware that records S3 operation metrics
pub async fn metrics_layer(request: Request<Body>, next: Next) -> Response {
    let start = Instant::now();

    // Extract operation type from request
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path();
    let query = uri.query();

    // Skip metrics and health endpoints
    if path == "/metrics" || path == "/health" || path.starts_with("/_admin") {
        return next.run(request).await;
    }

    // Determine S3 operation type
    let s3_operation = extract_operation(&method, path).map(|op| refine_operation(op, query));

    // Try Iceberg operation if not an S3 operation
    let iceberg_operation = if s3_operation.is_none() {
        path.strip_prefix("/iceberg")
            .and_then(|iceberg_path| extract_iceberg_operation(&method, iceberg_path))
    } else {
        None
    };

    // Get request body size from Content-Length header
    let request_bytes = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Run the handler
    let response = next.run(request).await;

    let status_code = response.status().as_u16();
    let latency_us = start.elapsed().as_micros() as u64;

    // Record metrics for S3 or Iceberg operation
    if let Some(op) = s3_operation {
        let response_bytes = response
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        s3_metrics().record_operation(op, status_code, request_bytes, response_bytes, latency_us);
    } else if let Some(op) = iceberg_operation {
        s3_metrics().record_iceberg_operation(op, status_code, latency_us);
    }

    response
}
