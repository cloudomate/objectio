//! Iceberg namespace and table access control.
//!
//! Reuses the existing `BucketPolicy` / `PolicyEvaluator` / `RequestContext`
//! from `objectio-auth::policy` with `iceberg:` action prefixes and
//! `arn:obio:iceberg:::` resource ARNs.

use crate::error::IcebergError;
use objectio_auth::policy::{BucketPolicy, PolicyDecision, PolicyEvaluator, RequestContext};

/// Build an Iceberg resource ARN.
///
/// - Namespace: `arn:obio:iceberg:::{ns1/ns2}`
/// - Table: `arn:obio:iceberg:::{ns1/ns2}/{table}`
#[must_use]
pub fn build_iceberg_arn(ns_levels: &[String], table: Option<&str>) -> String {
    let ns_path = ns_levels.join("/");
    table.map_or_else(
        || format!("arn:obio:iceberg:::{ns_path}"),
        |t| format!("arn:obio:iceberg:::{ns_path}/{t}"),
    )
}

/// Check an Iceberg policy (namespace or table level).
///
/// Returns `Ok(())` if access is allowed (explicit Allow, implicit deny with no
/// policy, or no policy at all — owner has implicit access, same as S3).
/// Returns `Err(IcebergError)` with 403 on explicit Deny.
///
/// # Errors
/// Returns `IcebergError` with `FORBIDDEN` status on explicit policy deny.
pub fn check_iceberg_policy(
    evaluator: &PolicyEvaluator,
    policy_json: Option<&str>,
    user_arn: &str,
    action: &str,
    resource_arn: &str,
) -> Result<(), IcebergError> {
    let Some(json) = policy_json else {
        return Ok(());
    };
    if json.is_empty() {
        return Ok(());
    }

    let policy = match BucketPolicy::from_json(json) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Failed to parse iceberg policy: {e}");
            // Invalid policy — don't block (same as S3)
            return Ok(());
        }
    };

    let context = RequestContext::new(user_arn, action, resource_arn);
    match evaluator.evaluate(&policy, &context) {
        PolicyDecision::Deny => Err(IcebergError::forbidden("Access denied by iceberg policy")),
        PolicyDecision::Allow | PolicyDecision::ImplicitDeny => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_arn_namespace() {
        let arn = build_iceberg_arn(&["db1".into(), "schema1".into()], None);
        assert_eq!(arn, "arn:obio:iceberg:::db1/schema1");
    }

    #[test]
    fn test_build_arn_table() {
        let arn = build_iceberg_arn(&["db1".into()], Some("events"));
        assert_eq!(arn, "arn:obio:iceberg:::db1/events");
    }

    #[test]
    fn test_no_policy_allows() {
        let evaluator = PolicyEvaluator::new();
        let result = check_iceberg_policy(
            &evaluator,
            None,
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_policy_allows() {
        let evaluator = PolicyEvaluator::new();
        let result = check_iceberg_policy(
            &evaluator,
            Some(""),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_explicit_deny() {
        let evaluator = PolicyEvaluator::new();
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["iceberg:DropTable"],
                "Resource": ["arn:obio:iceberg:::db1/*"]
            }]
        }"#;

        let result = check_iceberg_policy(
            &evaluator,
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/events",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_explicit_allow() {
        let evaluator = PolicyEvaluator::new();
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["iceberg:LoadTable"],
                "Resource": ["arn:obio:iceberg:::db1/*"]
            }]
        }"#;

        let result = check_iceberg_policy(
            &evaluator,
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_implicit_deny_allows() {
        let evaluator = PolicyEvaluator::new();
        // Policy only allows bob, but alice is requesting — implicit deny
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/bob"]},
                "Action": ["iceberg:LoadTable"],
                "Resource": ["arn:obio:iceberg:::db1/*"]
            }]
        }"#;

        let result = check_iceberg_policy(
            &evaluator,
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );
        // Implicit deny = allow (owner has implicit access)
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_policy_json_allows() {
        let evaluator = PolicyEvaluator::new();
        let result = check_iceberg_policy(
            &evaluator,
            Some("not-valid-json"),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );
        // Invalid policy — don't block
        assert!(result.is_ok());
    }
}
