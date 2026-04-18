//! Bucket policy structures and evaluation
//!
//! Implements S3-compatible bucket policies with IAM-like permissions.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;

/// A bucket policy document
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BucketPolicy {
    /// Policy version (typically "2012-10-17")
    #[serde(default = "default_version")]
    pub version: String,
    /// Policy ID (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Policy statements
    #[serde(rename = "Statement")]
    pub statements: Vec<PolicyStatement>,
}

fn default_version() -> String {
    "2012-10-17".to_string()
}

impl Default for BucketPolicy {
    fn default() -> Self {
        Self {
            version: default_version(),
            id: None,
            statements: Vec::new(),
        }
    }
}

impl BucketPolicy {
    /// Create a new empty policy
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a policy from JSON
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Add a statement to the policy
    pub fn add_statement(&mut self, statement: PolicyStatement) {
        self.statements.push(statement);
    }
}

/// A policy statement
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PolicyStatement {
    /// Statement ID (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    /// Effect: Allow or Deny
    pub effect: Effect,
    /// Principal: who this statement applies to
    pub principal: Principal,
    /// Actions this statement covers
    pub action: ActionList,
    /// Resources this statement covers
    pub resource: ResourceList,
    /// Conditions for this statement (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<Conditions>,
}

impl PolicyStatement {
    /// Create a new Allow statement
    pub fn allow() -> PolicyStatementBuilder {
        PolicyStatementBuilder::new(Effect::Allow)
    }

    /// Create a new Deny statement
    pub fn deny() -> PolicyStatementBuilder {
        PolicyStatementBuilder::new(Effect::Deny)
    }
}

/// Builder for policy statements
pub struct PolicyStatementBuilder {
    effect: Effect,
    sid: Option<String>,
    principal: Option<Principal>,
    actions: Vec<String>,
    resources: Vec<String>,
    conditions: Option<Conditions>,
}

impl PolicyStatementBuilder {
    fn new(effect: Effect) -> Self {
        Self {
            effect,
            sid: None,
            principal: None,
            actions: Vec::new(),
            resources: Vec::new(),
            conditions: None,
        }
    }

    pub fn sid(mut self, sid: impl Into<String>) -> Self {
        self.sid = Some(sid.into());
        self
    }

    pub fn principal_any(mut self) -> Self {
        self.principal = Some(Principal::Wildcard);
        self
    }

    pub fn principal_obio(mut self, arns: Vec<String>) -> Self {
        self.principal = Some(Principal::OBIO(arns));
        self
    }

    pub fn action(mut self, action: impl Into<String>) -> Self {
        self.actions.push(action.into());
        self
    }

    pub fn actions(mut self, actions: Vec<String>) -> Self {
        self.actions.extend(actions);
        self
    }

    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resources.push(resource.into());
        self
    }

    pub fn resources(mut self, resources: Vec<String>) -> Self {
        self.resources.extend(resources);
        self
    }

    pub fn condition(mut self, conditions: Conditions) -> Self {
        self.conditions = Some(conditions);
        self
    }

    pub fn build(self) -> PolicyStatement {
        PolicyStatement {
            sid: self.sid,
            effect: self.effect,
            principal: self.principal.unwrap_or(Principal::Wildcard),
            action: ActionList(self.actions),
            resource: ResourceList(self.resources),
            condition: self.conditions,
        }
    }
}

/// Policy effect
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    Allow,
    Deny,
}

/// Principal specification
#[derive(Debug, Clone)]
pub enum Principal {
    /// Wildcard ("*") - applies to everyone
    Wildcard,
    /// Specific OBIO principals (user/role ARNs)
    OBIO(Vec<String>),
}

impl Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Principal::Wildcard => serializer.serialize_str("*"),
            Principal::OBIO(arns) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("OBIO", arns)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Principal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};

        struct PrincipalVisitor;

        impl<'de> Visitor<'de> for PrincipalVisitor {
            type Value = Principal;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("\"*\" or {\"OBIO\": [...]}")
            }

            fn visit_str<E>(self, value: &str) -> Result<Principal, E>
            where
                E: de::Error,
            {
                if value == "*" {
                    Ok(Principal::Wildcard)
                } else {
                    Err(de::Error::custom(format!(
                        "invalid principal string: expected \"*\", got \"{}\"",
                        value
                    )))
                }
            }

            fn visit_map<M>(self, mut map: M) -> Result<Principal, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut obio_principals: Option<Vec<String>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    if key == "OBIO" {
                        // OBIO can be "*", a single string, or an array
                        let value: serde_json::Value = map.next_value()?;
                        match value {
                            serde_json::Value::String(s) if s == "*" => {
                                return Ok(Principal::Wildcard);
                            }
                            serde_json::Value::String(s) => {
                                obio_principals = Some(vec![s]);
                            }
                            serde_json::Value::Array(arr) => {
                                let arns: Result<Vec<String>, _> = arr
                                    .into_iter()
                                    .map(|v| {
                                        v.as_str().map(|s| s.to_string()).ok_or_else(|| {
                                            de::Error::custom("expected string in OBIO array")
                                        })
                                    })
                                    .collect();
                                obio_principals = Some(arns?);
                            }
                            _ => {
                                return Err(de::Error::custom(
                                    "OBIO must be \"*\", string, or array",
                                ));
                            }
                        }
                    } else {
                        // Skip unknown keys
                        let _: serde_json::Value = map.next_value()?;
                    }
                }

                obio_principals
                    .map(Principal::OBIO)
                    .ok_or_else(|| de::Error::custom("missing OBIO key in principal"))
            }
        }

        deserializer.deserialize_any(PrincipalVisitor)
    }
}

/// Helper to serialize single value or array
#[derive(Debug, Clone)]
pub struct ActionList(pub Vec<String>);

impl From<Vec<String>> for ActionList {
    fn from(v: Vec<String>) -> Self {
        Self(v)
    }
}

impl From<&str> for ActionList {
    fn from(s: &str) -> Self {
        Self(vec![s.to_string()])
    }
}

impl Serialize for ActionList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.0.len() == 1 {
            self.0[0].serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ActionList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(s) => Ok(ActionList(vec![s])),
            serde_json::Value::Array(arr) => {
                let strings: Result<Vec<String>, _> = arr
                    .into_iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| serde::de::Error::custom("expected string in array"))
                    })
                    .collect();
                Ok(ActionList(strings?))
            }
            _ => Err(serde::de::Error::custom(
                "expected string or array of strings",
            )),
        }
    }
}

/// Helper to serialize single value or array
#[derive(Debug, Clone)]
pub struct ResourceList(pub Vec<String>);

impl From<Vec<String>> for ResourceList {
    fn from(v: Vec<String>) -> Self {
        Self(v)
    }
}

impl From<&str> for ResourceList {
    fn from(s: &str) -> Self {
        Self(vec![s.to_string()])
    }
}

impl Serialize for ResourceList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.0.len() == 1 {
            self.0[0].serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ResourceList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(s) => Ok(ResourceList(vec![s])),
            serde_json::Value::Array(arr) => {
                let strings: Result<Vec<String>, _> = arr
                    .into_iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| serde::de::Error::custom("expected string in array"))
                    })
                    .collect();
                Ok(ResourceList(strings?))
            }
            _ => Err(serde::de::Error::custom(
                "expected string or array of strings",
            )),
        }
    }
}

/// Policy conditions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Conditions {
    /// String equals conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_equals: Option<HashMap<String, StringOrList>>,
    /// String not equals conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_not_equals: Option<HashMap<String, StringOrList>>,
    /// String like (wildcard) conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_like: Option<HashMap<String, StringOrList>>,
    /// IP address conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<HashMap<String, StringOrList>>,
    /// Not IP address conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_ip_address: Option<HashMap<String, StringOrList>>,
    /// Date greater than conditions (ISO 8601)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_greater_than: Option<HashMap<String, StringOrList>>,
    /// Date less than conditions (ISO 8601)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_less_than: Option<HashMap<String, StringOrList>>,
}

/// String or list of strings (for conditions)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrList {
    Single(String),
    List(Vec<String>),
}

impl StringOrList {
    pub fn as_vec(&self) -> Vec<&str> {
        match self {
            StringOrList::Single(s) => vec![s.as_str()],
            StringOrList::List(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

/// Policy evaluation result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    /// Explicitly allowed
    Allow,
    /// Explicitly denied
    Deny,
    /// No matching statement (implicit deny)
    ImplicitDeny,
}

/// Detailed policy evaluation result with explanation.
#[derive(Debug, Clone)]
pub struct PolicyExplanation {
    /// The decision: Allow, Deny, or ImplicitDeny
    pub decision: PolicyDecision,
    /// The statement that caused the decision (None for ImplicitDeny)
    pub matched_statement: Option<MatchedStatement>,
}

/// Information about the statement that matched.
#[derive(Debug, Clone)]
pub struct MatchedStatement {
    /// Statement ID (if present in the policy)
    pub sid: Option<String>,
    /// Effect of the matched statement
    pub effect: Effect,
    /// The policy source (e.g., "catalog", "namespace:prod", "table:events")
    pub source: String,
}

/// Context for policy evaluation
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// User ARN making the request
    pub user_arn: String,
    /// Action being performed (e.g., "s3:GetObject")
    pub action: String,
    /// Resource ARN (e.g., "arn:obio:s3:::bucket/key")
    pub resource: String,
    /// Source IP address
    pub source_ip: Option<IpAddr>,
    /// Additional context variables (single-valued)
    pub variables: HashMap<String, String>,
    /// Multi-valued context variables (e.g., `obio:PrincipalGroup`)
    pub multi_variables: HashMap<String, Vec<String>>,
}

impl RequestContext {
    /// Create a new request context
    pub fn new(
        user_arn: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
    ) -> Self {
        Self {
            user_arn: user_arn.into(),
            action: action.into(),
            resource: resource.into(),
            source_ip: None,
            variables: HashMap::new(),
            multi_variables: HashMap::new(),
        }
    }

    /// Set source IP
    pub fn with_source_ip(mut self, ip: IpAddr) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Add a context variable
    pub fn with_variable(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.variables.insert(key.into(), value.into());
        self
    }

    /// Add a multi-valued context variable (e.g., `obio:PrincipalGroup`)
    pub fn with_multi_variable(mut self, key: impl Into<String>, values: Vec<String>) -> Self {
        self.multi_variables.insert(key.into(), values);
        self
    }
}

/// Policy evaluator
pub struct PolicyEvaluator;

impl Default for PolicyEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyEvaluator {
    /// Create a new policy evaluator
    pub fn new() -> Self {
        Self
    }

    /// Evaluate a policy against a request context
    pub fn evaluate(&self, policy: &BucketPolicy, context: &RequestContext) -> PolicyDecision {
        let mut explicit_deny = false;
        let mut explicit_allow = false;

        for statement in &policy.statements {
            // Check if statement applies to this request
            if !self.matches_principal(&statement.principal, &context.user_arn) {
                continue;
            }
            if !self.matches_action(&statement.action, &context.action) {
                continue;
            }
            if !self.matches_resource(&statement.resource, &context.resource) {
                continue;
            }
            if !self.matches_conditions(&statement.condition, context) {
                continue;
            }

            // Statement matches - record effect
            match statement.effect {
                Effect::Deny => explicit_deny = true,
                Effect::Allow => explicit_allow = true,
            }
        }

        // Deny takes precedence over Allow
        if explicit_deny {
            PolicyDecision::Deny
        } else if explicit_allow {
            PolicyDecision::Allow
        } else {
            PolicyDecision::ImplicitDeny
        }
    }

    /// Evaluate a policy against a request context with detailed explanation.
    ///
    /// Returns the decision and the statement that caused it.
    pub fn evaluate_with_explanation(
        &self,
        policy: &BucketPolicy,
        context: &RequestContext,
        source: &str,
    ) -> PolicyExplanation {
        let mut deny_stmt: Option<&PolicyStatement> = None;
        let mut allow_stmt: Option<&PolicyStatement> = None;

        for statement in &policy.statements {
            if !self.matches_principal(&statement.principal, &context.user_arn) {
                continue;
            }
            if !self.matches_action(&statement.action, &context.action) {
                continue;
            }
            if !self.matches_resource(&statement.resource, &context.resource) {
                continue;
            }
            if !self.matches_conditions(&statement.condition, context) {
                continue;
            }

            match statement.effect {
                Effect::Deny => {
                    if deny_stmt.is_none() {
                        deny_stmt = Some(statement);
                    }
                }
                Effect::Allow => {
                    if allow_stmt.is_none() {
                        allow_stmt = Some(statement);
                    }
                }
            }
        }

        if let Some(stmt) = deny_stmt {
            PolicyExplanation {
                decision: PolicyDecision::Deny,
                matched_statement: Some(MatchedStatement {
                    sid: stmt.sid.clone(),
                    effect: Effect::Deny,
                    source: source.to_string(),
                }),
            }
        } else if let Some(stmt) = allow_stmt {
            PolicyExplanation {
                decision: PolicyDecision::Allow,
                matched_statement: Some(MatchedStatement {
                    sid: stmt.sid.clone(),
                    effect: Effect::Allow,
                    source: source.to_string(),
                }),
            }
        } else {
            PolicyExplanation {
                decision: PolicyDecision::ImplicitDeny,
                matched_statement: None,
            }
        }
    }

    /// Check if principal matches
    fn matches_principal(&self, principal: &Principal, user_arn: &str) -> bool {
        match principal {
            Principal::Wildcard => true,
            Principal::OBIO(arns) => arns.iter().any(|arn| {
                if arn == "*" {
                    true
                } else {
                    self.matches_pattern(arn, user_arn)
                }
            }),
        }
    }

    /// Check if action matches
    fn matches_action(&self, actions: &ActionList, request_action: &str) -> bool {
        actions.0.iter().any(|action| {
            if action == "*" || action == "s3:*" || action == "iceberg:*" {
                true
            } else {
                self.matches_pattern(action, request_action)
            }
        })
    }

    /// Check if resource matches
    fn matches_resource(&self, resources: &ResourceList, request_resource: &str) -> bool {
        resources
            .0
            .iter()
            .any(|resource| self.matches_pattern(resource, request_resource))
    }

    /// Check if conditions match
    fn matches_conditions(
        &self,
        conditions: &Option<Conditions>,
        context: &RequestContext,
    ) -> bool {
        let conditions = match conditions {
            Some(c) => c,
            None => return true, // No conditions means match
        };

        // Check StringEquals
        if let Some(ref string_equals) = conditions.string_equals {
            for (key, expected) in string_equals {
                let actuals = self.get_condition_values(key, context);
                if actuals.is_empty()
                    || !expected
                        .as_vec()
                        .iter()
                        .any(|e| actuals.iter().any(|a| a == e))
                {
                    return false;
                }
            }
        }

        // Check StringNotEquals
        if let Some(ref string_not_equals) = conditions.string_not_equals {
            for (key, not_expected) in string_not_equals {
                let actuals = self.get_condition_values(key, context);
                if not_expected
                    .as_vec()
                    .iter()
                    .any(|e| actuals.iter().any(|a| a == e))
                {
                    return false;
                }
            }
        }

        // Check StringLike (with wildcards)
        if let Some(ref string_like) = conditions.string_like {
            for (key, patterns) in string_like {
                let actuals = self.get_condition_values(key, context);
                if actuals.is_empty() {
                    return false;
                }
                if !patterns
                    .as_vec()
                    .iter()
                    .any(|p| actuals.iter().any(|a| self.matches_pattern(p, a)))
                {
                    return false;
                }
            }
        }

        // Check IpAddress
        if let (Some(ip_conditions), Some(source_ip)) = (&conditions.ip_address, context.source_ip)
        {
            for cidrs in ip_conditions.values() {
                if !cidrs
                    .as_vec()
                    .iter()
                    .any(|cidr| self.ip_matches_cidr(&source_ip, cidr))
                {
                    return false;
                }
            }
        }

        // Check NotIpAddress
        if let (Some(not_ip_conditions), Some(source_ip)) =
            (&conditions.not_ip_address, context.source_ip)
        {
            for cidrs in not_ip_conditions.values() {
                if cidrs
                    .as_vec()
                    .iter()
                    .any(|cidr| self.ip_matches_cidr(&source_ip, cidr))
                {
                    return false;
                }
            }
        }

        // Check DateGreaterThan
        if let Some(ref date_gt) = conditions.date_greater_than {
            for (key, thresholds) in date_gt {
                let actual = match self.get_condition_value(key, context) {
                    Some(v) => v,
                    None => return false,
                };
                let actual_dt = match chrono::DateTime::parse_from_rfc3339(&actual) {
                    Ok(dt) => dt,
                    Err(_) => return false,
                };
                // All threshold values must be less than actual (actual > threshold)
                if !thresholds.as_vec().iter().any(|t| {
                    chrono::DateTime::parse_from_rfc3339(t)
                        .is_ok_and(|threshold_dt| actual_dt > threshold_dt)
                }) {
                    return false;
                }
            }
        }

        // Check DateLessThan
        if let Some(ref date_lt) = conditions.date_less_than {
            for (key, thresholds) in date_lt {
                let actual = match self.get_condition_value(key, context) {
                    Some(v) => v,
                    None => return false,
                };
                let actual_dt = match chrono::DateTime::parse_from_rfc3339(&actual) {
                    Ok(dt) => dt,
                    Err(_) => return false,
                };
                // All threshold values must be greater than actual (actual < threshold)
                if !thresholds.as_vec().iter().any(|t| {
                    chrono::DateTime::parse_from_rfc3339(t)
                        .is_ok_and(|threshold_dt| actual_dt < threshold_dt)
                }) {
                    return false;
                }
            }
        }

        true
    }

    /// Get condition value from context (single-valued).
    fn get_condition_value(&self, key: &str, context: &RequestContext) -> Option<String> {
        match key {
            "aws:SourceIp" => context.source_ip.map(|ip| ip.to_string()),
            "aws:username" => Some(context.user_arn.clone()),
            "s3:prefix" => context.variables.get("prefix").cloned(),
            "obio:CurrentTime" => Some(chrono::Utc::now().to_rfc3339()),
            _ => context.variables.get(key).cloned(),
        }
    }

    /// Get condition values from context (multi-valued).
    ///
    /// For keys like `obio:PrincipalGroup`, returns all values from
    /// `context.multi_variables`. For single-valued keys, wraps the
    /// result of `get_condition_value` in a `Vec`.
    fn get_condition_values(&self, key: &str, context: &RequestContext) -> Vec<String> {
        // Check multi_variables first (e.g., obio:PrincipalGroup)
        if let Some(values) = context.multi_variables.get(key) {
            return values.clone();
        }
        // Fall back to single-valued lookup
        self.get_condition_value(key, context).into_iter().collect()
    }

    /// Match a pattern with wildcards (* and ?)
    fn matches_pattern(&self, pattern: &str, value: &str) -> bool {
        // Convert S3/IAM wildcard pattern to regex
        let regex_pattern = pattern
            .replace('.', r"\.")
            .replace('*', ".*")
            .replace('?', ".");

        let regex_pattern = format!("^{}$", regex_pattern);

        match Regex::new(&regex_pattern) {
            Ok(re) => re.is_match(value),
            Err(_) => pattern == value,
        }
    }

    /// Check if IP matches a CIDR range (e.g., `10.0.0.0/8`, `192.168.1.0/24`).
    ///
    /// Supports both IPv4 and IPv6 CIDR notation. Falls back to exact IP match
    /// if no prefix length is specified.
    fn ip_matches_cidr(&self, ip: &IpAddr, cidr: &str) -> bool {
        if let Some((network_str, prefix_str)) = cidr.split_once('/')
            && let Ok(prefix_len) = prefix_str.parse::<u32>()
            && let Ok(network_ip) = network_str.parse::<IpAddr>()
        {
            return match (ip, &network_ip) {
                (IpAddr::V4(addr), IpAddr::V4(net)) => {
                    if prefix_len == 0 {
                        return true;
                    }
                    if prefix_len > 32 {
                        return false;
                    }
                    let mask = u32::MAX.checked_shl(32 - prefix_len).unwrap_or(0);
                    u32::from(*addr) & mask == u32::from(*net) & mask
                }
                (IpAddr::V6(addr), IpAddr::V6(net)) => {
                    if prefix_len == 0 {
                        return true;
                    }
                    if prefix_len > 128 {
                        return false;
                    }
                    let mask = u128::MAX.checked_shl(128 - prefix_len).unwrap_or(0);
                    u128::from(*addr) & mask == u128::from(*net) & mask
                }
                _ => false, // IPv4 vs IPv6 mismatch
            };
        }

        // No prefix — try exact IP match
        cidr.parse::<IpAddr>().is_ok_and(|cidr_ip| ip == &cidr_ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_parsing() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:obio:s3:::mybucket/*"]
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        assert_eq!(policy.statements.len(), 1);
        assert_eq!(policy.statements[0].effect, Effect::Allow);
    }

    #[test]
    fn test_policy_evaluation_allow() {
        let mut policy = BucketPolicy::new();
        policy.add_statement(
            PolicyStatement::allow()
                .principal_any()
                .action("s3:GetObject")
                .resource("arn:obio:s3:::mybucket/*")
                .build(),
        );

        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/testuser",
            "s3:GetObject",
            "arn:obio:s3:::mybucket/mykey",
        );

        let evaluator = PolicyEvaluator::new();
        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Allow);
    }

    #[test]
    fn test_policy_evaluation_deny_takes_precedence() {
        let mut policy = BucketPolicy::new();
        policy.add_statement(
            PolicyStatement::allow()
                .principal_any()
                .action("s3:*")
                .resource("arn:obio:s3:::mybucket/*")
                .build(),
        );
        policy.add_statement(
            PolicyStatement::deny()
                .principal_any()
                .action("s3:DeleteObject")
                .resource("arn:obio:s3:::mybucket/*")
                .build(),
        );

        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/testuser",
            "s3:DeleteObject",
            "arn:obio:s3:::mybucket/mykey",
        );

        let evaluator = PolicyEvaluator::new();
        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Deny);
    }

    #[test]
    fn test_policy_evaluation_implicit_deny() {
        let policy = BucketPolicy::new(); // Empty policy

        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/testuser",
            "s3:GetObject",
            "arn:obio:s3:::mybucket/mykey",
        );

        let evaluator = PolicyEvaluator::new();
        assert_eq!(
            evaluator.evaluate(&policy, &context),
            PolicyDecision::ImplicitDeny
        );
    }

    #[test]
    fn test_wildcard_matching() {
        let evaluator = PolicyEvaluator::new();

        assert!(evaluator.matches_pattern("arn:obio:s3:::bucket/*", "arn:obio:s3:::bucket/key"));
        assert!(
            evaluator.matches_pattern("arn:obio:s3:::bucket/*", "arn:obio:s3:::bucket/prefix/key")
        );
        assert!(!evaluator.matches_pattern("arn:obio:s3:::bucket/*", "arn:obio:s3:::other/key"));
        assert!(evaluator.matches_pattern("s3:*", "s3:GetObject"));
        assert!(evaluator.matches_pattern("*", "anything"));
    }

    #[test]
    fn test_builder() {
        let statement = PolicyStatement::allow()
            .sid("TestStatement")
            .principal_obio(vec!["arn:obio:iam::objectio:user/admin".to_string()])
            .actions(vec!["s3:GetObject".to_string(), "s3:PutObject".to_string()])
            .resource("arn:obio:s3:::mybucket/*")
            .build();

        assert_eq!(statement.sid, Some("TestStatement".to_string()));
        assert_eq!(statement.effect, Effect::Allow);
        assert_eq!(statement.action.0.len(), 2);
    }

    #[test]
    fn test_date_greater_than_condition() {
        let evaluator = PolicyEvaluator::new();

        // Policy: Allow only after 2020-01-01
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:*"],
                "Resource": ["*"],
                "Condition": {
                    "DateGreaterThan": {
                        "obio:CurrentTime": "2020-01-01T00:00:00+00:00"
                    }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );

        // Current time is after 2020, so this should match
        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Allow);
    }

    #[test]
    fn test_date_less_than_condition() {
        let evaluator = PolicyEvaluator::new();

        // Policy: Allow only before 2020-01-01 (this will be expired)
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:*"],
                "Resource": ["*"],
                "Condition": {
                    "DateLessThan": {
                        "obio:CurrentTime": "2020-01-01T00:00:00+00:00"
                    }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );

        // Current time is after 2020, so DateLessThan won't match -> implicit deny
        assert_eq!(
            evaluator.evaluate(&policy, &context),
            PolicyDecision::ImplicitDeny
        );
    }

    #[test]
    fn test_date_range_condition() {
        let evaluator = PolicyEvaluator::new();

        // Policy: Allow between 2020-01-01 and 2030-01-01
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:*"],
                "Resource": ["*"],
                "Condition": {
                    "DateGreaterThan": {
                        "obio:CurrentTime": "2020-01-01T00:00:00+00:00"
                    },
                    "DateLessThan": {
                        "obio:CurrentTime": "2030-01-01T00:00:00+00:00"
                    }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );

        // Current time should be between 2020 and 2030
        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Allow);
    }

    #[test]
    fn test_principal_group_string_equals() {
        let evaluator = PolicyEvaluator::new();

        // Policy: Allow only members of data-engineers group
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:*"],
                "Resource": ["*"],
                "Condition": {
                    "StringEquals": {
                        "obio:PrincipalGroup": "arn:obio:iam::objectio:group/data-engineers"
                    }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        // User in the data-engineers group — should match
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        )
        .with_multi_variable(
            "obio:PrincipalGroup",
            vec![
                "arn:obio:iam::objectio:group/data-engineers".to_string(),
                "arn:obio:iam::objectio:group/analysts".to_string(),
            ],
        );
        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Allow);

        // User NOT in the data-engineers group — should not match
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/bob",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        )
        .with_multi_variable(
            "obio:PrincipalGroup",
            vec!["arn:obio:iam::objectio:group/marketing".to_string()],
        );
        assert_eq!(
            evaluator.evaluate(&policy, &context),
            PolicyDecision::ImplicitDeny
        );

        // User with no groups — should not match
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/carol",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );
        assert_eq!(
            evaluator.evaluate(&policy, &context),
            PolicyDecision::ImplicitDeny
        );
    }

    #[test]
    fn test_principal_group_string_like() {
        let evaluator = PolicyEvaluator::new();

        // Policy: Deny users in any group matching "data-*"
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["iceberg:DropTable"],
                "Resource": ["*"],
                "Condition": {
                    "StringLike": {
                        "obio:PrincipalGroup": "arn:obio:iam::objectio:group/data-*"
                    }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        // User in data-engineers group — should match the deny
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/events",
        )
        .with_multi_variable(
            "obio:PrincipalGroup",
            vec!["arn:obio:iam::objectio:group/data-engineers".to_string()],
        );
        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Deny);

        // User in marketing group — should not match the deny
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/bob",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/events",
        )
        .with_multi_variable(
            "obio:PrincipalGroup",
            vec!["arn:obio:iam::objectio:group/marketing".to_string()],
        );
        assert_eq!(
            evaluator.evaluate(&policy, &context),
            PolicyDecision::ImplicitDeny
        );
    }

    #[test]
    fn test_principal_group_string_not_equals() {
        let evaluator = PolicyEvaluator::new();

        // Policy: Allow only if user is NOT in the interns group
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:*"],
                "Resource": ["*"],
                "Condition": {
                    "StringNotEquals": {
                        "obio:PrincipalGroup": "arn:obio:iam::objectio:group/interns"
                    }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        // User in interns group — StringNotEquals fails, no allow
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/intern1",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        )
        .with_multi_variable(
            "obio:PrincipalGroup",
            vec!["arn:obio:iam::objectio:group/interns".to_string()],
        );
        assert_eq!(
            evaluator.evaluate(&policy, &context),
            PolicyDecision::ImplicitDeny
        );

        // User NOT in interns group — StringNotEquals passes, allow
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        )
        .with_multi_variable(
            "obio:PrincipalGroup",
            vec!["arn:obio:iam::objectio:group/engineers".to_string()],
        );
        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Allow);
    }

    #[test]
    fn test_custom_variable_conditions() {
        let evaluator = PolicyEvaluator::new();

        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["iceberg:*"],
                "Resource": ["*"],
                "Condition": {
                    "StringEquals": {
                        "iceberg:namespace": "production"
                    }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        // With production namespace variable — should deny
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::production/events",
        )
        .with_variable("iceberg:namespace", "production");

        assert_eq!(evaluator.evaluate(&policy, &context), PolicyDecision::Deny);

        // With staging namespace variable — should not match the deny
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::staging/events",
        )
        .with_variable("iceberg:namespace", "staging");

        assert_eq!(
            evaluator.evaluate(&policy, &context),
            PolicyDecision::ImplicitDeny
        );
    }

    #[test]
    fn test_evaluate_with_explanation_deny() {
        let evaluator = PolicyEvaluator::new();
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "DenyDrops",
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["iceberg:DropTable"],
                "Resource": ["arn:obio:iceberg:::*"]
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:DropTable",
            "arn:obio:iceberg:::db1/events",
        );

        let result = evaluator.evaluate_with_explanation(&policy, &context, "namespace:db1");
        assert_eq!(result.decision, PolicyDecision::Deny);
        let ms = result.matched_statement.unwrap();
        assert_eq!(ms.sid, Some("DenyDrops".to_string()));
        assert_eq!(ms.effect, Effect::Deny);
        assert_eq!(ms.source, "namespace:db1");
    }

    #[test]
    fn test_evaluate_with_explanation_allow() {
        let evaluator = PolicyEvaluator::new();
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "AllowReads",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["iceberg:LoadTable"],
                "Resource": ["*"]
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );

        let result = evaluator.evaluate_with_explanation(&policy, &context, "catalog");
        assert_eq!(result.decision, PolicyDecision::Allow);
        let ms = result.matched_statement.unwrap();
        assert_eq!(ms.sid, Some("AllowReads".to_string()));
        assert_eq!(ms.effect, Effect::Allow);
        assert_eq!(ms.source, "catalog");
    }

    #[test]
    fn test_evaluate_with_explanation_implicit_deny() {
        let evaluator = PolicyEvaluator::new();
        let policy = BucketPolicy::new(); // Empty policy
        let context = RequestContext::new(
            "arn:obio:iam::objectio:user/alice",
            "iceberg:LoadTable",
            "arn:obio:iceberg:::db1/events",
        );

        let result = evaluator.evaluate_with_explanation(&policy, &context, "catalog");
        assert_eq!(result.decision, PolicyDecision::ImplicitDeny);
        assert!(result.matched_statement.is_none());
    }

    #[test]
    fn test_cidr_ipv4_matching() {
        let evaluator = PolicyEvaluator::new();

        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:*"],
                "Resource": ["*"],
                "Condition": {
                    "IpAddress": { "aws:SourceIp": "10.0.0.0/8" }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        // IP inside 10.0.0.0/8
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("10.1.2.3".parse().unwrap());
        assert_eq!(evaluator.evaluate(&policy, &ctx), PolicyDecision::Allow);

        // IP outside 10.0.0.0/8
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("192.168.1.1".parse().unwrap());
        assert_eq!(
            evaluator.evaluate(&policy, &ctx),
            PolicyDecision::ImplicitDeny
        );

        // Exact boundary: 10.255.255.255 is in /8
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("10.255.255.255".parse().unwrap());
        assert_eq!(evaluator.evaluate(&policy, &ctx), PolicyDecision::Allow);

        // 11.0.0.0 is NOT in 10.0.0.0/8
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("11.0.0.0".parse().unwrap());
        assert_eq!(
            evaluator.evaluate(&policy, &ctx),
            PolicyDecision::ImplicitDeny
        );
    }

    #[test]
    fn test_cidr_ipv4_slash24() {
        let evaluator = PolicyEvaluator::new();

        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["s3:*"],
                "Resource": ["*"],
                "Condition": {
                    "NotIpAddress": { "aws:SourceIp": "192.168.1.0/24" }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        // IP inside 192.168.1.0/24 — NotIpAddress does NOT match → no deny
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("192.168.1.50".parse().unwrap());
        assert_eq!(
            evaluator.evaluate(&policy, &ctx),
            PolicyDecision::ImplicitDeny
        );

        // IP outside 192.168.1.0/24 — NotIpAddress matches → deny
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("192.168.2.1".parse().unwrap());
        assert_eq!(evaluator.evaluate(&policy, &ctx), PolicyDecision::Deny);
    }

    #[test]
    fn test_cidr_ipv6_matching() {
        let evaluator = PolicyEvaluator::new();

        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:*"],
                "Resource": ["*"],
                "Condition": {
                    "IpAddress": { "aws:SourceIp": "fd00::/8" }
                }
            }]
        }"#;
        let policy = BucketPolicy::from_json(json).unwrap();

        // fd12::1 is in fd00::/8
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("fd12::1".parse().unwrap());
        assert_eq!(evaluator.evaluate(&policy, &ctx), PolicyDecision::Allow);

        // fe80::1 is NOT in fd00::/8
        let ctx = RequestContext::new(
            "arn:obio:iam::objectio:user/a",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )
        .with_source_ip("fe80::1".parse().unwrap());
        assert_eq!(
            evaluator.evaluate(&policy, &ctx),
            PolicyDecision::ImplicitDeny
        );
    }

    #[test]
    fn test_cidr_wildcard_ranges() {
        let evaluator = PolicyEvaluator::new();
        // 0.0.0.0/0 matches any IPv4
        assert!(evaluator.ip_matches_cidr(&"1.2.3.4".parse().unwrap(), "0.0.0.0/0"));
        // ::/0 matches any IPv6
        assert!(evaluator.ip_matches_cidr(&"::1".parse().unwrap(), "::/0"));
        // Exact IP match (no prefix)
        assert!(evaluator.ip_matches_cidr(&"10.0.0.1".parse().unwrap(), "10.0.0.1"));
        assert!(!evaluator.ip_matches_cidr(&"10.0.0.2".parse().unwrap(), "10.0.0.1"));
    }
}
