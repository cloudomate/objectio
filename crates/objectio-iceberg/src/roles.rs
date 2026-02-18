//! Built-in Iceberg role templates.
//!
//! Pre-defined roles that expand to IAM-style policy documents. These can be
//! bound to users or groups via the role-binding endpoint.

use serde_json::json;

/// Built-in Iceberg role names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergRole {
    /// Full admin: all `iceberg:*` actions on all resources, plus policy management.
    CatalogAdmin,
    /// Owner of a specific namespace: all actions within `ns/*`.
    NamespaceOwner,
    /// Writer: `LoadTable`, `UpdateTable`, `CreateTable`, `ListTables` on `ns/*`.
    TableWriter,
    /// Reader: `LoadTable`, `ListTables`, `TableExists`, `ListNamespaces` on `ns/*`.
    TableReader,
}

impl IcebergRole {
    /// Parse a role name string into an `IcebergRole`.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "iceberg:CatalogAdmin" | "CatalogAdmin" => Some(Self::CatalogAdmin),
            "iceberg:NamespaceOwner" | "NamespaceOwner" => Some(Self::NamespaceOwner),
            "iceberg:TableWriter" | "TableWriter" => Some(Self::TableWriter),
            "iceberg:TableReader" | "TableReader" => Some(Self::TableReader),
            _ => None,
        }
    }

    /// Return the role name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CatalogAdmin => "iceberg:CatalogAdmin",
            Self::NamespaceOwner => "iceberg:NamespaceOwner",
            Self::TableWriter => "iceberg:TableWriter",
            Self::TableReader => "iceberg:TableReader",
        }
    }

    /// Generate the policy JSON for this role, scoped to the given principal
    /// ARN(s) and namespace.
    ///
    /// For `CatalogAdmin` the `namespace` parameter is ignored (applies to all resources).
    /// For other roles, `namespace` is used to scope the resource ARN.
    ///
    /// # Panics
    /// Panics if internal policy serialization fails (should never happen).
    #[must_use]
    pub fn to_policy_json(self, principal_arns: &[String], namespace: &str) -> String {
        let principals = json!(principal_arns);
        let policy = match self {
            Self::CatalogAdmin => json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "CatalogAdminAccess",
                    "Effect": "Allow",
                    "Principal": { "OBIO": principals },
                    "Action": ["iceberg:*"],
                    "Resource": ["arn:obio:iceberg:::*"]
                }]
            }),
            Self::NamespaceOwner => json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "NamespaceOwnerAccess",
                    "Effect": "Allow",
                    "Principal": { "OBIO": principals },
                    "Action": ["iceberg:*"],
                    "Resource": [
                        format!("arn:obio:iceberg:::{namespace}"),
                        format!("arn:obio:iceberg:::{namespace}/*")
                    ]
                }]
            }),
            Self::TableWriter => json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "TableWriterAccess",
                    "Effect": "Allow",
                    "Principal": { "OBIO": principals },
                    "Action": [
                        "iceberg:LoadTable",
                        "iceberg:UpdateTable",
                        "iceberg:CreateTable",
                        "iceberg:ListTables",
                        "iceberg:TableExists",
                        "iceberg:ListNamespaces",
                        "iceberg:LoadNamespace",
                        "iceberg:NamespaceExists"
                    ],
                    "Resource": [
                        format!("arn:obio:iceberg:::{namespace}"),
                        format!("arn:obio:iceberg:::{namespace}/*")
                    ]
                }]
            }),
            Self::TableReader => json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "TableReaderAccess",
                    "Effect": "Allow",
                    "Principal": { "OBIO": principals },
                    "Action": [
                        "iceberg:LoadTable",
                        "iceberg:ListTables",
                        "iceberg:TableExists",
                        "iceberg:ListNamespaces",
                        "iceberg:LoadNamespace",
                        "iceberg:NamespaceExists"
                    ],
                    "Resource": [
                        format!("arn:obio:iceberg:::{namespace}"),
                        format!("arn:obio:iceberg:::{namespace}/*")
                    ]
                }]
            }),
        };
        serde_json::to_string(&policy).expect("role policy serialization should not fail")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use objectio_auth::policy::BucketPolicy;

    #[test]
    fn test_role_from_name() {
        assert_eq!(
            IcebergRole::from_name("CatalogAdmin"),
            Some(IcebergRole::CatalogAdmin)
        );
        assert_eq!(
            IcebergRole::from_name("iceberg:TableReader"),
            Some(IcebergRole::TableReader)
        );
        assert_eq!(IcebergRole::from_name("unknown"), None);
    }

    #[test]
    fn test_catalog_admin_policy() {
        let arns = vec!["arn:obio:iam::objectio:user/admin".to_string()];
        let json = IcebergRole::CatalogAdmin.to_policy_json(&arns, "ignored");
        let policy = BucketPolicy::from_json(&json).unwrap();
        assert_eq!(policy.statements.len(), 1);
        assert_eq!(policy.statements[0].action.0, vec!["iceberg:*"]);
    }

    #[test]
    fn test_table_reader_policy() {
        let arns = vec!["arn:obio:iam::objectio:group/data-team".to_string()];
        let json = IcebergRole::TableReader.to_policy_json(&arns, "analytics");
        let policy = BucketPolicy::from_json(&json).unwrap();
        assert_eq!(policy.statements.len(), 1);
        // Reader should have 6 read-only actions
        assert_eq!(policy.statements[0].action.0.len(), 6);
        assert!(policy.statements[0]
            .resource
            .0
            .iter()
            .any(|r| r.contains("analytics")));
    }

    #[test]
    fn test_namespace_owner_policy() {
        let arns = vec!["arn:obio:iam::objectio:user/alice".to_string()];
        let json = IcebergRole::NamespaceOwner.to_policy_json(&arns, "prod");
        let policy = BucketPolicy::from_json(&json).unwrap();
        assert_eq!(policy.statements[0].action.0, vec!["iceberg:*"]);
        assert!(policy.statements[0].resource.0.contains(&"arn:obio:iceberg:::prod/*".to_string()));
    }
}
