//! Delta Sharing error type.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Delta Sharing protocol error.
#[derive(Debug)]
pub struct DeltaError {
    pub status: StatusCode,
    pub message: String,
}

impl DeltaError {
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: msg.into(),
        }
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: msg.into(),
        }
    }
}

impl IntoResponse for DeltaError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "errorCode": self.status.as_u16().to_string(),
            "message": self.message,
        });
        (self.status, axum::Json(body)).into_response()
    }
}

impl From<tonic::Status> for DeltaError {
    fn from(s: tonic::Status) -> Self {
        match s.code() {
            tonic::Code::NotFound => Self::not_found(s.message()),
            tonic::Code::PermissionDenied => Self::forbidden(s.message()),
            tonic::Code::AlreadyExists => Self::conflict(s.message()),
            tonic::Code::InvalidArgument => Self::bad_request(s.message()),
            _ => Self::internal(s.message()),
        }
    }
}
