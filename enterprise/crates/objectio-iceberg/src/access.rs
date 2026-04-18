//! Iceberg namespace and table access control.
//!
//! Reuses the existing `BucketPolicy` / `PolicyEvaluator` / `RequestContext`
//! from `objectio-auth::policy` with `iceberg:` action prefixes and
//! `arn:obio:iceberg:::` resource ARNs.
//!
//! Populates `RequestContext.variables` with Iceberg-specific condition keys:
//! - `iceberg:namespace` — dot-joined namespace (e.g. `"analytics"`, `"prod.sales"`)
//! - `iceberg:tableName` — table name if applicable
//! - `iceberg:operation` — the action being performed (e.g. `"iceberg:CreateTable"`)

use crate::error::IcebergError;
use objectio_auth::policy::{BucketPolicy, PolicyDecision, PolicyEvaluator, RequestContext};
use std::collections::HashMap;

/// Prefix used for storing tags as namespace properties.
pub const TAG_PREFIX: &str = "__tag:";

/// Extract resource tags from namespace properties (keys with `__tag:` prefix).
#[must_use]
pub fn extract_tags<S: std::hash::BuildHasher>(
    properties: &HashMap<String, String, S>,
) -> HashMap<String, String> {
    properties
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(TAG_PREFIX)
                .map(|tag_key| (tag_key.to_string(), v.clone()))
        })
        .collect()
}

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

/// Parameters for an Iceberg policy check.
pub struct IcebergPolicyCheck<'a> {
    pub evaluator: &'a PolicyEvaluator,
    pub policy_json: Option<&'a str>,
    pub user_arn: &'a str,
    pub action: &'a str,
    pub resource_arn: &'a str,
    pub ns_levels: &'a [String],
    pub table_name: Option<&'a str>,
    pub policy_source: &'a str,
    /// Resource tags for `obio:ResourceTag/{key}` condition keys.
    pub resource_tags: &'a HashMap<String, String>,
}

/// Check an Iceberg policy (namespace or table level).
///
/// Returns `Ok(())` if access is allowed (explicit Allow, implicit deny with no
/// policy, or no policy at all — owner has implicit access, same as S3).
/// Returns `Err(IcebergError)` with 403 on explicit Deny.
///
/// Populates `RequestContext.variables` with Iceberg-specific condition keys
/// so policies can use `Condition` blocks referencing `iceberg:namespace`,
/// `iceberg:tableName`, and `iceberg:operation`.
///
/// # Errors
/// Returns `IcebergError` with `FORBIDDEN` status on explicit policy deny.
pub fn check_iceberg_policy(check: &IcebergPolicyCheck<'_>) -> Result<(), IcebergError> {
    let IcebergPolicyCheck {
        evaluator,
        policy_json,
        user_arn,
        action,
        resource_arn,
        ns_levels,
        table_name,
        policy_source,
        resource_tags,
    } = check;
    let Some(json) = policy_json else {
        tracing::debug!(
            namespace = %ns_levels.join("."),
            table = table_name.unwrap_or(""),
            user = %user_arn,
            action = %action,
            decision = "allow",
            policy_source = %policy_source,
            "iceberg.policy.decision: no policy set"
        );
        return Ok(());
    };
    if json.is_empty() {
        tracing::debug!(
            namespace = %ns_levels.join("."),
            table = table_name.unwrap_or(""),
            user = %user_arn,
            action = %action,
            decision = "allow",
            policy_source = %policy_source,
            "iceberg.policy.decision: empty policy"
        );
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

    // Build context with Iceberg-specific condition keys
    let mut context = RequestContext::new(*user_arn, *action, *resource_arn);
    context
        .variables
        .insert("iceberg:namespace".to_string(), ns_levels.join("."));
    if let Some(t) = table_name {
        context
            .variables
            .insert("iceberg:tableName".to_string(), (*t).to_string());
    }
    context
        .variables
        .insert("iceberg:operation".to_string(), (*action).to_string());

    // Populate obio:ResourceTag/{key} condition variables from resource tags
    for (key, value) in *resource_tags {
        context
            .variables
            .insert(format!("obio:ResourceTag/{key}"), value.clone());
    }

    let decision = evaluator.evaluate(&policy, &context);

    match decision {
        PolicyDecision::Deny => {
            tracing::warn!(
                namespace = %ns_levels.join("."),
                table = table_name.unwrap_or(""),
                user = %user_arn,
                action = %action,
                decision = "deny",
                policy_source = %policy_source,
                "iceberg.policy.decision"
            );
            Err(IcebergError::forbidden("Access denied by iceberg policy"))
        }
        PolicyDecision::Allow => {
            tracing::info!(
                namespace = %ns_levels.join("."),
                table = table_name.unwrap_or(""),
                user = %user_arn,
                action = %action,
                decision = "allow",
                policy_source = %policy_source,
                "iceberg.policy.decision"
            );
            Ok(())
        }
        PolicyDecision::ImplicitDeny => {
            tracing::debug!(
                namespace = %ns_levels.join("."),
                table = table_name.unwrap_or(""),
                user = %user_arn,
                action = %action,
                decision = "implicit_deny",
                policy_source = %policy_source,
                "iceberg.policy.decision"
            );
            Ok(())
        }
    }
}

/// Reserved namespace name for storing the catalog-level default policy.
pub const CATALOG_POLICY_NAMESPACE: &str = "__catalog";

/// Parameters for a hierarchical Iceberg policy check.
pub struct HierarchicalPolicyCheck<'a> {
    pub evaluator: &'a PolicyEvaluator,
    pub catalog_policy_json: Option<&'a str>,
    pub ancestor_policies: &'a [(String, Option<String>)],
    pub user_arn: &'a str,
    pub action: &'a str,
    pub resource_arn: &'a str,
    pub ns_levels: &'a [String],
    pub table_name: Option<&'a str>,
    /// Merged resource tags from the namespace hierarchy.
    pub resource_tags: &'a HashMap<String, String>,
}

/// Check hierarchical policies for an Iceberg request.
///
/// Walks the full policy chain in order:
/// 1. Catalog-level default policy (from `__catalog` namespace `__policy` property)
/// 2. Each ancestor namespace policy (for multi-level namespaces)
/// 3. The target namespace policy
///
/// Any explicit Deny at any level immediately blocks the request.
/// This function returns `Err` on the first Deny encountered.
///
/// # Errors
/// Returns `IcebergError` with `FORBIDDEN` status on explicit policy deny.
pub fn check_hierarchical_policies(
    check: &HierarchicalPolicyCheck<'_>,
) -> Result<(), IcebergError> {
    let HierarchicalPolicyCheck {
        evaluator,
        catalog_policy_json,
        ancestor_policies,
        user_arn,
        action,
        resource_arn,
        ns_levels,
        table_name,
        resource_tags,
    } = check;
    // 1. Check catalog-level policy
    check_iceberg_policy(&IcebergPolicyCheck {
        evaluator,
        policy_json: *catalog_policy_json,
        user_arn,
        action,
        resource_arn,
        ns_levels,
        table_name: *table_name,
        policy_source: "catalog",
        resource_tags,
    })?;

    // 2. Check each ancestor/target namespace policy
    for (ns_label, policy_json) in *ancestor_policies {
        check_iceberg_policy(&IcebergPolicyCheck {
            evaluator,
            policy_json: policy_json.as_deref(),
            user_arn,
            action,
            resource_arn,
            ns_levels,
            table_name: *table_name,
            policy_source: ns_label,
            resource_tags,
        })?;
    }

    Ok(())
}

/// Validate that a policy JSON string is a valid Iceberg policy.
///
/// Checks:
/// - Valid JSON that parses as `BucketPolicy`
/// - All actions use the `iceberg:` prefix (not `s3:`)
/// - All resources use `arn:obio:iceberg:::` prefix
///
/// # Errors
/// Returns a descriptive error message if validation fails.
pub fn validate_iceberg_policy(policy_json: &str) -> Result<(), String> {
    let policy =
        BucketPolicy::from_json(policy_json).map_err(|e| format!("Invalid policy JSON: {e}"))?;

    for (i, stmt) in policy.statements.iter().enumerate() {
        // Validate action prefixes
        for action in &stmt.action.0 {
            if action != "*" && !action.starts_with("iceberg:") {
                return Err(format!(
                    "Statement {i}: action \"{action}\" must use the \"iceberg:\" prefix"
                ));
            }
        }

        // Validate resource ARN prefixes
        for resource in &stmt.resource.0 {
            if resource != "*" && !resource.starts_with("arn:obio:iceberg:::") {
                return Err(format!(
                    "Statement {i}: resource \"{resource}\" must start with \"arn:obio:iceberg:::\""
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
        ns_levels: &[String],
        table_name: Option<&str>,
        policy_source: &str,
    ) -> Result<(), IcebergError> {
        check_with_tags(
            policy_json,
            user_arn,
            action,
            resource_arn,
            ns_levels,
            table_name,
            policy_source,
            &HashMap::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn check_with_tags(
        policy_json: Option<&str>,
        user_arn: &str,
        action: &str,
        resource_arn: &str,
        ns_levels: &[String],
        table_name: Option<&str>,
        policy_source: &str,
        resource_tags: &HashMap<String, String>,
    ) -> Result<(), IcebergError> {
        let evaluator = PolicyEvaluator::new();
        check_iceberg_policy(&IcebergPolicyCheck {
            evaluator: &evaluator,
            policy_json,
            user_arn,
            action,
            resource_arn,
            ns_levels,
            table_name,
            policy_source,
            resource_tags,
        })
    }

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
        let result = check(
            None,
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
            &["db1".into()],
            Some("events"),
            "table",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_policy_allows() {
        let result = check(
            Some(""),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
            &["db1".into()],
            Some("events"),
            "table",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_explicit_deny() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["iceberg:DropTable"],
                "Resource": ["arn:obio:iceberg:::db1/*"]
            }]
        }"#;

        let result = check(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/events",
            &["db1".into()],
            Some("events"),
            "namespace",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_explicit_allow() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["iceberg:LoadTable"],
                "Resource": ["arn:obio:iceberg:::db1/*"]
            }]
        }"#;

        let result = check(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
            &["db1".into()],
            Some("events"),
            "namespace",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_implicit_deny_allows() {
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

        let result = check(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
            &["db1".into()],
            Some("events"),
            "namespace",
        );
        // Implicit deny = allow (owner has implicit access)
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_policy_json_allows() {
        let result = check(
            Some("not-valid-json"),
            "arn:objectio:iam::user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
            &["db1".into()],
            Some("events"),
            "table",
        );
        // Invalid policy — don't block
        assert!(result.is_ok());
    }

    #[test]
    fn test_condition_key_namespace_deny() {
        // Deny intern from production namespace using condition key
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/intern"]},
                "Action": ["iceberg:*"],
                "Resource": ["arn:obio:iceberg:::*"],
                "Condition": {
                    "StringEquals": { "iceberg:namespace": "production" }
                }
            }]
        }"#;

        let result = check(
            Some(policy),
            "arn:objectio:iam::user/intern",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::production/events",
            &["production".into()],
            Some("events"),
            "namespace",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_condition_key_namespace_allow_different_ns() {
        // Deny intern from production — but requesting from staging, should pass
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/intern"]},
                "Action": ["iceberg:*"],
                "Resource": ["arn:obio:iceberg:::*"],
                "Condition": {
                    "StringEquals": { "iceberg:namespace": "production" }
                }
            }]
        }"#;

        let result = check(
            Some(policy),
            "arn:objectio:iam::user/intern",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::staging/events",
            &["staging".into()],
            Some("events"),
            "namespace",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_condition_key_table_name() {
        // Deny drops on specific table using condition key
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["iceberg:DropTable"],
                "Resource": ["arn:obio:iceberg:::*"],
                "Condition": {
                    "StringEquals": { "iceberg:tableName": "critical_data" }
                }
            }]
        }"#;

        let result = check(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/critical_data",
            &["db1".into()],
            Some("critical_data"),
            "namespace",
        );
        assert!(result.is_err());

        // Different table should be allowed
        let result = check(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/temp_data",
            &["db1".into()],
            Some("temp_data"),
            "namespace",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_iceberg_policy_valid() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:LoadTable", "iceberg:ListTables"],
                "Resource": ["arn:obio:iceberg:::db1/*"]
            }]
        }"#;
        assert!(validate_iceberg_policy(policy).is_ok());
    }

    #[test]
    fn test_validate_iceberg_policy_invalid_json() {
        assert!(validate_iceberg_policy("not json").is_err());
    }

    #[test]
    fn test_validate_iceberg_policy_s3_action() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": ["arn:obio:iceberg:::db1/*"]
            }]
        }"#;
        let err = validate_iceberg_policy(policy).unwrap_err();
        assert!(err.contains("iceberg:"));
    }

    #[test]
    fn test_validate_iceberg_policy_wrong_resource() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:LoadTable"],
                "Resource": ["arn:obio:s3:::mybucket/*"]
            }]
        }"#;
        let err = validate_iceberg_policy(policy).unwrap_err();
        assert!(err.contains("arn:obio:iceberg:::"));
    }

    #[test]
    fn test_validate_iceberg_policy_wildcard_action_and_resource() {
        // Wildcard actions and resources should be allowed
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["*"],
                "Resource": ["*"]
            }]
        }"#;
        assert!(validate_iceberg_policy(policy).is_ok());
    }

    #[test]
    fn test_hierarchical_catalog_deny_blocks() {
        // Catalog policy denies alice from all iceberg actions
        let catalog_policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"OBIO": ["arn:objectio:iam::user/alice"]},
                "Action": ["iceberg:*"],
                "Resource": ["arn:obio:iceberg:::*"]
            }]
        }"#;

        let ns_levels = vec!["analytics".to_string()];
        let result = check_hierarchical_policies(&HierarchicalPolicyCheck {
            evaluator: &PolicyEvaluator::new(),
            catalog_policy_json: Some(catalog_policy),
            ancestor_policies: &[],
            user_arn: "arn:objectio:iam::user/alice",
            action: "iceberg:LoadTable",
            resource_arn: "arn:obio:iceberg:::analytics/events",
            ns_levels: &ns_levels,
            table_name: Some("events"),
            resource_tags: &HashMap::new(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_hierarchical_ancestor_deny_blocks() {
        // No catalog policy, but parent "prod" namespace denies DropTable
        let prod_policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["iceberg:DropTable"],
                "Resource": ["arn:obio:iceberg:::*"]
            }]
        }"#;

        let ns_levels = vec!["prod".to_string(), "sales".to_string()];
        let ancestor_policies = vec![
            ("namespace:prod".to_string(), Some(prod_policy.to_string())),
            ("namespace:prod.sales".to_string(), None),
        ];

        let result = check_hierarchical_policies(&HierarchicalPolicyCheck {
            evaluator: &PolicyEvaluator::new(),
            catalog_policy_json: None,
            ancestor_policies: &ancestor_policies,
            user_arn: "arn:objectio:iam::user/alice",
            action: "iceberg:DropTable",
            resource_arn: "arn:obio:iceberg:::prod/sales/events",
            ns_levels: &ns_levels,
            table_name: Some("events"),
            resource_tags: &HashMap::new(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_hierarchical_no_deny_allows() {
        // Catalog allows, parent namespace allows, should pass
        let catalog_policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:*"],
                "Resource": ["arn:obio:iceberg:::*"]
            }]
        }"#;

        let ns_levels = vec!["analytics".to_string()];
        let ancestor_policies = vec![("namespace:analytics".to_string(), None)];

        let result = check_hierarchical_policies(&HierarchicalPolicyCheck {
            evaluator: &PolicyEvaluator::new(),
            catalog_policy_json: Some(catalog_policy),
            ancestor_policies: &ancestor_policies,
            user_arn: "arn:objectio:iam::user/alice",
            action: "iceberg:LoadTable",
            resource_arn: "arn:obio:iceberg:::analytics/events",
            ns_levels: &ns_levels,
            table_name: Some("events"),
            resource_tags: &HashMap::new(),
        });
        assert!(result.is_ok());
    }

    #[test]
    fn test_resource_tag_condition_deny() {
        // Deny all writes on PII-tagged resources
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["iceberg:DropTable", "iceberg:UpdateTable"],
                "Resource": ["arn:obio:iceberg:::*"],
                "Condition": {
                    "StringEquals": { "obio:ResourceTag/classification": "pii" }
                }
            }]
        }"#;

        let tags = HashMap::from([("classification".to_string(), "pii".to_string())]);
        let result = check_with_tags(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/users",
            &["db1".into()],
            Some("users"),
            "namespace",
            &tags,
        );
        assert!(result.is_err());

        // Non-PII table should pass
        let tags = HashMap::from([("classification".to_string(), "public".to_string())]);
        let result = check_with_tags(
            Some(policy),
            "arn:objectio:iam::user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/users",
            &["db1".into()],
            Some("users"),
            "namespace",
            &tags,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_tags() {
        let props = HashMap::from([
            ("__tag:env".to_string(), "production".to_string()),
            ("__tag:team".to_string(), "data".to_string()),
            ("__policy".to_string(), "{}".to_string()),
            ("owner".to_string(), "alice".to_string()),
        ]);
        let tags = extract_tags(&props);
        assert_eq!(tags.len(), 2);
        assert_eq!(tags["env"], "production");
        assert_eq!(tags["team"], "data");
    }
}
