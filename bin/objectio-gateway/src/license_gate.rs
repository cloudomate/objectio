//! License gates for Enterprise-only features.
//!
//! Each of the Enterprise subsystems (Iceberg catalog, Delta Sharing, SSE-KMS,
//! multi-tenancy, OIDC, LRC) gets a corresponding middleware returned by
//! [`feature_gate`]. When no valid Enterprise license is installed the gate
//! short-circuits with a structured 403 response; handlers downstream never
//! run. Because the license lives in `AppState` behind a `RwLock`, installing
//! a license via `PUT /_admin/license` unlocks gated routes without a
//! restart.
//!
//! Handlers that aren't behind a gated router (for example, admin endpoints
//! shared across tiers) should call [`require_feature`] inline instead of
//! wiring in a middleware.

use axum::{
    Json,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use objectio_license::Feature;
use std::sync::Arc;

use crate::s3::AppState;

/// Axum middleware factory. Returns a function that can be used with
/// `middleware::from_fn_with_state` to gate a whole router sub-tree.
/// Used to wrap Iceberg + Delta-Sharing routers — the middleware
/// short-circuits with a structured 403 when no valid Enterprise
/// license is installed.
pub async fn feature_gate(
    State((state, feature)): State<(Arc<AppState>, Feature)>,
    request: Request,
    next: Next,
) -> Response {
    if state.has_feature(feature) {
        next.run(request).await
    } else {
        enterprise_required(feature)
    }
}

/// Inline check for handlers that aren't behind a gated router.
/// Returns `Ok(())` if licensed, else an error response ready to return.
#[allow(clippy::result_large_err)]
pub fn require_feature(state: &AppState, feature: Feature) -> Result<(), Response> {
    if state.has_feature(feature) {
        Ok(())
    } else {
        Err(enterprise_required(feature))
    }
}

/// Build the 403 body that all gates share. Machine-readable so CLI/console
/// can render a consistent upsell.
pub fn enterprise_required(feature: Feature) -> Response {
    let body = serde_json::json!({
        "error": "EnterpriseLicenseRequired",
        "feature": feature.as_str(),
        "tier_required": "free-or-enterprise",
        "message": format!(
            "The '{}' feature requires a Free or Enterprise license. \
             Get a Free license (single-host cap, no cost) or an \
             Enterprise license (commercial, multi-host), then \
             install via PUT /_admin/license or set $OBJECTIO_LICENSE.",
            feature.as_str()
        ),
    });
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}
