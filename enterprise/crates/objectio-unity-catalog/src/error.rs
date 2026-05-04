//! Unity Catalog REST API error types.
//!
//! Error responses follow the Databricks Unity Catalog wire format:
//! `{"error_code": "RESOURCE_ALREADY_EXISTS", "message": "...", "details": []}`
//!
//! `error_code` strings match the Databricks API error code enum so existing
//! Unity-aware clients (`PySpark`, Trino Unity connector, Daft) recognize them.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug)]
pub struct UnityError {
    pub status: StatusCode,
    pub error_code: &'static str,
    pub message: String,
}

#[derive(Serialize)]
struct UnityErrorBody {
    error_code: &'static str,
    message: String,
}

impl UnityError {
    #[must_use]
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_code: "RESOURCE_DOES_NOT_EXIST",
            message: msg.into(),
        }
    }

    #[must_use]
    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            error_code: "RESOURCE_ALREADY_EXISTS",
            message: msg.into(),
        }
    }

    #[must_use]
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error_code: "INVALID_PARAMETER_VALUE",
            message: msg.into(),
        }
    }

    #[must_use]
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            error_code: "PERMISSION_DENIED",
            message: msg.into(),
        }
    }

    #[must_use]
    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            error_code: "UNAUTHENTICATED",
            message: msg.into(),
        }
    }

    #[must_use]
    pub fn not_empty(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            error_code: "RESOURCE_CONFLICT",
            message: msg.into(),
        }
    }

    #[must_use]
    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_code: "INTERNAL_ERROR",
            message: msg.into(),
        }
    }
}

impl IntoResponse for UnityError {
    fn into_response(self) -> Response {
        let body = UnityErrorBody {
            error_code: self.error_code,
            message: self.message,
        };
        (self.status, axum::Json(body)).into_response()
    }
}

impl From<tonic::Status> for UnityError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => Self::not_found(status.message()),
            tonic::Code::AlreadyExists => Self::already_exists(status.message()),
            tonic::Code::InvalidArgument => Self::bad_request(status.message()),
            tonic::Code::FailedPrecondition => Self::not_empty(status.message()),
            tonic::Code::PermissionDenied => Self::forbidden(status.message()),
            tonic::Code::Unauthenticated => Self::unauthorized(status.message()),
            _ => Self::internal(status.message()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_serializes_unity_wire_shape() {
        let err = UnityError::not_found("catalog 'foo' not found");
        let body = serde_json::to_string(&UnityErrorBody {
            error_code: err.error_code,
            message: err.message,
        })
        .unwrap();
        // Unity clients parse exactly these two top-level keys.
        assert!(body.contains(r#""error_code":"RESOURCE_DOES_NOT_EXIST""#));
        assert!(body.contains(r#""message":"catalog 'foo' not found""#));
    }

    #[test]
    fn maps_grpc_codes_to_unity_codes() {
        let cases = [
            (
                tonic::Code::NotFound,
                "RESOURCE_DOES_NOT_EXIST",
                StatusCode::NOT_FOUND,
            ),
            (
                tonic::Code::AlreadyExists,
                "RESOURCE_ALREADY_EXISTS",
                StatusCode::CONFLICT,
            ),
            (
                tonic::Code::InvalidArgument,
                "INVALID_PARAMETER_VALUE",
                StatusCode::BAD_REQUEST,
            ),
            (
                tonic::Code::FailedPrecondition,
                "RESOURCE_CONFLICT",
                StatusCode::CONFLICT,
            ),
            (
                tonic::Code::PermissionDenied,
                "PERMISSION_DENIED",
                StatusCode::FORBIDDEN,
            ),
            (
                tonic::Code::Unauthenticated,
                "UNAUTHENTICATED",
                StatusCode::UNAUTHORIZED,
            ),
            (
                tonic::Code::Internal,
                "INTERNAL_ERROR",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (grpc, expected_code, expected_status) in cases {
            let err: UnityError = tonic::Status::new(grpc, "msg").into();
            assert_eq!(err.error_code, expected_code, "grpc={grpc:?}");
            assert_eq!(err.status, expected_status, "grpc={grpc:?}");
        }
    }
}
