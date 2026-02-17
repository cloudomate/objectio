//! Iceberg REST API error types.
//!
//! Error responses follow the Iceberg REST spec format:
//! `{"error": {"message": "...", "type": "...", "code": N}}`

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Iceberg REST API error.
#[derive(Debug)]
pub struct IcebergError {
    pub code: StatusCode,
    pub error_type: String,
    pub message: String,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
    r#type: String,
    code: u16,
}

impl IcebergError {
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: StatusCode::NOT_FOUND,
            error_type: "NoSuchNamespaceException".to_string(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self {
            code: StatusCode::CONFLICT,
            error_type: "AlreadyExistsException".to_string(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            code: StatusCode::BAD_REQUEST,
            error_type: "BadRequestException".to_string(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn not_empty(message: impl Into<String>) -> Self {
        Self {
            code: StatusCode::CONFLICT,
            error_type: "NamespaceNotEmptyException".to_string(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn commit_failed(message: impl Into<String>) -> Self {
        Self {
            code: StatusCode::CONFLICT,
            error_type: "CommitFailedException".to_string(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            code: StatusCode::FORBIDDEN,
            error_type: "ForbiddenException".to_string(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "InternalServerError".to_string(),
            message: message.into(),
        }
    }
}

impl IntoResponse for IcebergError {
    fn into_response(self) -> Response {
        let body = ErrorEnvelope {
            error: ErrorBody {
                message: self.message,
                r#type: self.error_type,
                code: self.code.as_u16(),
            },
        };
        (self.code, axum::Json(body)).into_response()
    }
}

impl From<tonic::Status> for IcebergError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => Self::not_found(status.message()),
            tonic::Code::AlreadyExists => Self::already_exists(status.message()),
            tonic::Code::InvalidArgument => Self::bad_request(status.message()),
            tonic::Code::FailedPrecondition => {
                let msg = status.message();
                if msg.contains("not empty") {
                    Self::not_empty(msg)
                } else {
                    Self::commit_failed(msg)
                }
            }
            _ => Self::internal(status.message()),
        }
    }
}
