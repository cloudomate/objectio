//! Unity Catalog access control — three-level policy hierarchy
//! (catalog → schema → table) with `unity:` action prefix and
//! `arn:obio:unity:::{catalog}/{schema}/{table}` resource ARNs.
//!
//! Reuses `BucketPolicy` / `PolicyEvaluator` / `RequestContext` from
//! `objectio_auth::policy`. Differs from the Iceberg variant only in:
//! - Fixed three-level walk (catalog → schema → table) rather than
//!   walking N namespace prefixes.
//! - `unity:` action prefix and Unity-specific condition keys
//!   (`unity:catalog`, `unity:schema`, `unity:tableName`,
//!   `unity:operation`).

use crate::error::UnityError;
use objectio_auth::policy::{BucketPolicy, PolicyDecision, PolicyEvaluator, RequestContext};

/// Build a Unity resource ARN for any of the three levels.
///
/// - Catalog: `arn:obio:unity:::{catalog}`
/// - Schema: `arn:obio:unity:::{catalog}/{schema}`
/// - Table: `arn:obio:unity:::{catalog}/{schema}/{table}`
#[must_use]
pub fn build_unity_arn(catalog: &str, schema: Option<&str>, table: Option<&str>) -> String {
    match (schema, table) {
        (None, _) => format!("arn:obio:unity:::{catalog}"),
        (Some(s), None) => format!("arn:obio:unity:::{catalog}/{s}"),
        (Some(s), Some(t)) => format!("arn:obio:unity:::{catalog}/{s}/{t}"),
    }
}

/// One level of policy in the catalog → schema → table walk.
pub struct PolicyLevel<'a> {
    /// Label printed in tracing decisions ("catalog" / "schema:foo" / "table:foo.bar").
    pub label: &'a str,
    /// Raw policy JSON (UTF-8). `None` skips the level.
    pub policy_json: Option<&'a str>,
}

/// Parameters passed to a single policy evaluation.
pub struct UnityPolicyCheck<'a> {
    pub evaluator: &'a PolicyEvaluator,
    pub policy_json: Option<&'a str>,
    pub user_arn: &'a str,
    pub action: &'a str,
    pub resource_arn: &'a str,
    pub catalog: &'a str,
    pub schema: Option<&'a str>,
    pub table: Option<&'a str>,
    pub policy_source: &'a str,
}

/// Evaluate a single Unity policy.
///
/// Returns `Ok(())` for explicit Allow, implicit deny (no matching
/// statement), and missing/empty/invalid policy (owner-implicit-allow,
/// matches S3 and Iceberg). Returns `Err` only on explicit Deny.
///
/// # Errors
/// Returns `UnityError::forbidden` on explicit Deny.
pub fn check_unity_policy(check: &UnityPolicyCheck<'_>) -> Result<(), UnityError> {
    let UnityPolicyCheck {
        evaluator,
        policy_json,
        user_arn,
        action,
        resource_arn,
        catalog,
        schema,
        table,
        policy_source,
    } = check;
    let Some(json) = policy_json else {
        tracing::debug!(
            catalog = %catalog,
            schema = schema.unwrap_or(""),
            table = table.unwrap_or(""),
            user = %user_arn,
            action = %action,
            decision = "allow",
            policy_source = %policy_source,
            "unity.policy.decision: no policy"
        );
        return Ok(());
    };
    if json.is_empty() {
        return Ok(());
    }

    let policy = match BucketPolicy::from_json(json) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Failed to parse unity policy at {policy_source}: {e}");
            // Same as Iceberg/S3: invalid policy doesn't block — admin already
            // owns implicit access via /policy PUT.
            return Ok(());
        }
    };

    let mut context = RequestContext::new(*user_arn, *action, *resource_arn);
    context
        .variables
        .insert("unity:catalog".to_string(), (*catalog).to_string());
    if let Some(s) = schema {
        context
            .variables
            .insert("unity:schema".to_string(), (*s).to_string());
    }
    if let Some(t) = table {
        context
            .variables
            .insert("unity:tableName".to_string(), (*t).to_string());
    }
    context
        .variables
        .insert("unity:operation".to_string(), (*action).to_string());

    match evaluator.evaluate(&policy, &context) {
        PolicyDecision::Deny => {
            tracing::warn!(
                catalog = %catalog,
                schema = schema.unwrap_or(""),
                table = table.unwrap_or(""),
                user = %user_arn,
                action = %action,
                decision = "deny",
                policy_source = %policy_source,
                "unity.policy.decision"
            );
            Err(UnityError::forbidden("Access denied by unity policy"))
        }
        PolicyDecision::Allow | PolicyDecision::ImplicitDeny => Ok(()),
    }
}

/// Walk catalog → schema → table policies in order. Any explicit Deny short-circuits.
///
/// `levels` should be ordered most-general → most-specific. The walk
/// returns `Ok(())` if every level allows (or is silent).
///
/// # Errors
/// Returns `UnityError::forbidden` on the first explicit Deny.
#[allow(clippy::too_many_arguments)]
pub fn check_three_level(
    evaluator: &PolicyEvaluator,
    user_arn: &str,
    action: &str,
    resource_arn: &str,
    catalog: &str,
    schema: Option<&str>,
    table: Option<&str>,
    levels: &[PolicyLevel<'_>],
) -> Result<(), UnityError> {
    for level in levels {
        check_unity_policy(&UnityPolicyCheck {
            evaluator,
            policy_json: level.policy_json,
            user_arn,
            action,
            resource_arn,
            catalog,
            schema,
            table,
            policy_source: level.label,
        })?;
    }
    Ok(())
}

/// Validate a policy JSON string is shaped correctly for Unity:
/// - Parses as `BucketPolicy`
/// - All actions use the `unity:` prefix (or `*`)
/// - All resources use `arn:obio:unity:::` prefix (or `*`)
///
/// # Errors
/// Returns a descriptive `String` if validation fails. Used by the
/// admin policy PUT endpoint to reject misrouted Iceberg/S3 policies.
pub fn validate_unity_policy(policy_json: &str) -> Result<(), String> {
    let policy =
        BucketPolicy::from_json(policy_json).map_err(|e| format!("invalid policy JSON: {e}"))?;
    for (i, stmt) in policy.statements.iter().enumerate() {
        for action in &stmt.action.0 {
            if action != "*" && !action.starts_with("unity:") {
                return Err(format!(
                    "statement {i}: action \"{action}\" must use the \"unity:\" prefix"
                ));
            }
        }
        for resource in &stmt.resource.0 {
            if resource != "*" && !resource.starts_with("arn:obio:unity:::") {
                return Err(format!(
                    "statement {i}: resource \"{resource}\" must start with \"arn:obio:unity:::\""
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(
        policy_json: Option<&str>,
        user_arn: &str,
        action: &str,
        resource_arn: &str,
        catalog: &str,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<(), UnityError> {
        let evaluator = PolicyEvaluator::new();
        check_unity_policy(&UnityPolicyCheck {
            evaluator: &evaluator,
            policy_json,
            user_arn,
            action,
            resource_arn,
            catalog,
            schema,
            table,
            policy_source: "test",
        })
    }

    #[test]
    fn build_arn_three_levels() {
        assert_eq!(build_unity_arn("prod", None, None), "arn:obio:unity:::prod");
        assert_eq!(
            build_unity_arn("prod", Some("sales"), None),
            "arn:obio:unity:::prod/sales"
        );
        assert_eq!(
            build_unity_arn("prod", Some("sales"), Some("events")),
            "arn:obio:unity:::prod/sales/events"
        );
    }

    #[test]
    fn no_policy_allows() {
        let r = check(
            None,
            "arn:objectio:iam::user/alice",
            "unity:LoadTable",
            "arn:obio:unity:::prod/sales/events",
            "prod",
            Some("sales"),
            Some("events"),
        );
        assert!(r.is_ok());
    }

    #[test]
    fn explicit_deny_blocks() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["unity:DeleteTable"],
                "Resource": ["arn:obio:unity:::prod/*"]
            }]
        }"#;
        let r = check(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "unity:DeleteTable",
            "arn:obio:unity:::prod/sales/events",
            "prod",
            Some("sales"),
            Some("events"),
        );
        assert!(r.is_err());
    }

    #[test]
    fn implicit_deny_allows() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/bob"]},
                "Action": ["unity:LoadTable"],
                "Resource": ["arn:obio:unity:::prod/*"]
            }]
        }"#;
        let r = check(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "unity:LoadTable",
            "arn:obio:unity:::prod/sales/events",
            "prod",
            Some("sales"),
            Some("events"),
        );
        // Implicit deny → owner-implicit-allow per shared semantics.
        assert!(r.is_ok());
    }

    #[test]
    fn condition_key_unity_schema_deny() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/intern"]},
                "Action": ["unity:*"],
                "Resource": ["arn:obio:unity:::*"],
                "Condition": {
                    "StringEquals": {"unity:schema": "production"}
                }
            }]
        }"#;
        // Hits production schema → deny.
        let r = check(
            Some(policy),
            "arn:objectio:iam::user/intern",
            "unity:LoadTable",
            "arn:obio:unity:::prod/production/events",
            "prod",
            Some("production"),
            Some("events"),
        );
        assert!(r.is_err());
        // Different schema → allowed.
        let r2 = check(
            Some(policy),
            "arn:objectio:iam::user/intern",
            "unity:LoadTable",
            "arn:obio:unity:::prod/dev/events",
            "prod",
            Some("dev"),
            Some("events"),
        );
        assert!(r2.is_ok());
    }

    #[test]
    fn three_level_table_deny_blocks_despite_catalog_allow() {
        let evaluator = PolicyEvaluator::new();
        let catalog_allow = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["unity:*"],
                "Resource": ["arn:obio:unity:::*"]
            }]
        }"#;
        let table_deny = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["unity:DeleteTable"],
                "Resource": ["arn:obio:unity:::prod/sales/events"]
            }]
        }"#;
        let levels = vec![
            PolicyLevel {
                label: "catalog",
                policy_json: Some(catalog_allow),
            },
            PolicyLevel {
                label: "schema:sales",
                policy_json: None,
            },
            PolicyLevel {
                label: "table:sales.events",
                policy_json: Some(table_deny),
            },
        ];
        let r = check_three_level(
            &evaluator,
            "arn:objectio:iam::user/alice",
            "unity:DeleteTable",
            "arn:obio:unity:::prod/sales/events",
            "prod",
            Some("sales"),
            Some("events"),
            &levels,
        );
        assert!(r.is_err());
    }

    #[test]
    fn three_level_no_policies_allows() {
        let evaluator = PolicyEvaluator::new();
        let levels = vec![
            PolicyLevel {
                label: "catalog",
                policy_json: None,
            },
            PolicyLevel {
                label: "schema",
                policy_json: None,
            },
            PolicyLevel {
                label: "table",
                policy_json: None,
            },
        ];
        let r = check_three_level(
            &evaluator,
            "arn:objectio:iam::user/alice",
            "unity:LoadTable",
            "arn:obio:unity:::prod/sales/events",
            "prod",
            Some("sales"),
            Some("events"),
            &levels,
        );
        assert!(r.is_ok());
    }

    #[test]
    fn validate_rejects_iceberg_action() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["iceberg:LoadTable"],
                "Resource": ["arn:obio:unity:::*"]
            }]
        }"#;
        let err = validate_unity_policy(policy).unwrap_err();
        assert!(err.contains("iceberg:LoadTable"));
        assert!(err.contains("unity:"));
    }

    #[test]
    fn validate_rejects_s3_resource_arn() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["unity:LoadTable"],
                "Resource": ["arn:aws:s3:::my-bucket"]
            }]
        }"#;
        let err = validate_unity_policy(policy).unwrap_err();
        assert!(err.contains("arn:obio:unity:::"));
    }

    #[test]
    fn validate_accepts_wildcard_action_and_resource() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["*"],
                "Resource": ["*"]
            }]
        }"#;
        validate_unity_policy(policy).unwrap();
    }
}
