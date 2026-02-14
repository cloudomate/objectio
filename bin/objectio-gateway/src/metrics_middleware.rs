//! Metrics middleware for automatic S3 operation tracking
//!
//! Intercepts all requests and records metrics based on HTTP method and path patterns.

use axum::{body::Body, extract::Request, http::Method, middleware::Next, response::Response};
use objectio_s3::{S3Operation, s3_metrics};
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
    let operation = extract_operation(&method, path).map(|op| refine_operation(op, query));

    // Get request body size from Content-Length header
    let request_bytes = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Run the handler
    let response = next.run(request).await;

    // Record metrics if this is an S3 operation
    if let Some(op) = operation {
        let status_code = response.status().as_u16();
        let latency_us = start.elapsed().as_micros() as u64;

        // Get response body size from Content-Length header
        let response_bytes = response
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        s3_metrics().record_operation(op, status_code, request_bytes, response_bytes, latency_us);
    }

    response
}
