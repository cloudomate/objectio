//! Native Delta Lake `_delta_log/` reader.
//!
//! Hand-rolled JSON parser for Delta Lake commit files (`_delta_log/*.json`).
//! Reads the actions of each commit (protocol, metaData, add, remove) and
//! folds them into a `DeltaSnapshot` representing the latest table state.
//!
//! v1 limitation: checkpoint Parquet files are NOT supported. Tables that
//! have written `_delta_log/*.checkpoint.parquet` files (or a
//! `_delta_log/_last_checkpoint` hint) return an error suggesting shorter
//! retention. v2 will swap this implementation for the `deltalake` crate.
//!
//! Reference: <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>

use crate::error::DeltaError;
use serde::Deserialize;
use std::collections::HashMap;

/// Resolved snapshot of a Delta table — the latest protocol + metadata plus
/// every live `add` action (paths in subsequent `remove` actions are dropped).
#[derive(Debug, Clone)]
pub struct DeltaSnapshot {
    pub version: i64,
    pub protocol: ProtocolAction,
    pub metadata: MetadataAction,
    pub adds: Vec<AddAction>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProtocolAction {
    #[serde(rename = "minReaderVersion")]
    pub min_reader_version: i32,
    #[serde(rename = "minWriterVersion", default)]
    pub min_writer_version: i32,
}

impl Default for ProtocolAction {
    fn default() -> Self {
        Self {
            min_reader_version: 1,
            min_writer_version: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetadataAction {
    #[serde(default)]
    pub id: String,
    #[serde(rename = "schemaString", default)]
    pub schema_string: String,
    #[serde(rename = "partitionColumns", default)]
    pub partition_columns: Vec<String>,
    #[serde(default)]
    pub configuration: HashMap<String, String>,
    #[serde(rename = "createdTime")]
    pub created_time: Option<i64>,
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub format: Option<MetadataFormat>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetadataFormat {
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AddAction {
    pub path: String,
    #[serde(rename = "partitionValues", default)]
    pub partition_values: HashMap<String, Option<String>>,
    #[serde(default)]
    pub size: i64,
    #[serde(rename = "modificationTime", default)]
    pub modification_time: i64,
    pub stats: Option<String>,
    #[serde(rename = "dataChange", default)]
    pub data_change: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoveAction {
    pub path: String,
}

/// One Delta action as it appears on a single line of a `_delta_log/*.json`.
///
/// Each line is a JSON object with exactly one key naming the action type — we
/// dispatch by inspecting that key, since serde's `other` fallback doesn't
/// accept a payload on externally-tagged enums.
#[derive(Debug, Clone)]
pub enum DeltaAction {
    Protocol(ProtocolAction),
    Metadata(MetadataAction),
    Add(AddAction),
    Remove(RemoveAction),
    /// Any other action type (`commitInfo`, `txn`, `cdc`, etc.) we don't need.
    Other,
}

/// What was found in `_delta_log/` when listing.
pub struct LogListing {
    /// Sorted ascending. Each entry is the version number of a `*.json` commit.
    pub commits: Vec<i64>,
    /// True if any `_delta_log/*.checkpoint.parquet` or `_last_checkpoint` was
    /// found. v1 cannot read checkpoints; presence triggers an error.
    pub has_checkpoint: bool,
}

/// Read `_delta_log/` of a Delta table from S3.
///
/// Implementations are responsible for listing JSON commits and fetching each
/// commit's NDJSON body. The reader does not need to interpret the contents.
pub trait DeltaLogReader {
    /// List `_delta_log/*.json` commits and detect checkpoint presence.
    fn list_log(
        &self,
        bucket: &str,
        table_root: &str,
    ) -> impl std::future::Future<Output = Result<LogListing, DeltaError>> + Send;

    /// Fetch the raw bytes of `_delta_log/{version:020}.json`.
    fn read_commit(
        &self,
        bucket: &str,
        table_root: &str,
        version: i64,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, DeltaError>> + Send;
}

/// Build a snapshot by walking every commit JSON in version order and folding
/// actions into the live state. Errors out on checkpoint Parquet (v1 limit).
///
/// # Errors
/// Returns `DeltaError::bad_request` if checkpoints are present, `not_found`
/// if there are no commits, or `internal` on parse failures.
pub async fn build_snapshot<R: DeltaLogReader + Sync>(
    reader: &R,
    bucket: &str,
    table_root: &str,
) -> Result<DeltaSnapshot, DeltaError> {
    let listing = reader.list_log(bucket, table_root).await?;

    if listing.has_checkpoint {
        return Err(DeltaError::bad_request(
            "Delta table has a checkpoint (.checkpoint.parquet or _last_checkpoint); \
             checkpoint reading is not supported in this version. Either reduce the \
             table's log retention so all commits remain as JSON, or wait for \
             checkpoint Parquet support in a future release.",
        ));
    }

    if listing.commits.is_empty() {
        return Err(DeltaError::not_found(format!(
            "no Delta commits found at s3://{bucket}/{}_delta_log/",
            normalize_root(table_root),
        )));
    }

    let mut protocol: Option<ProtocolAction> = None;
    let mut metadata: Option<MetadataAction> = None;
    let mut live_adds: HashMap<String, AddAction> = HashMap::new();
    let mut latest_version = 0i64;

    for version in &listing.commits {
        let bytes = reader.read_commit(bucket, table_root, *version).await?;
        for action in parse_commit(&bytes)? {
            match action {
                DeltaAction::Protocol(p) => protocol = Some(p),
                DeltaAction::Metadata(m) => metadata = Some(m),
                DeltaAction::Add(a) => {
                    live_adds.insert(a.path.clone(), a);
                }
                DeltaAction::Remove(r) => {
                    live_adds.remove(&r.path);
                }
                DeltaAction::Other => {}
            }
        }
        latest_version = *version;
    }

    Ok(DeltaSnapshot {
        version: latest_version,
        protocol: protocol.unwrap_or_default(),
        metadata: metadata.ok_or_else(|| {
            DeltaError::internal(format!(
                "Delta log at s3://{bucket}/{}_delta_log/ contains no metaData action",
                normalize_root(table_root),
            ))
        })?,
        adds: live_adds.into_values().collect(),
    })
}

/// Parse a `_delta_log/*.json` commit file (NDJSON: one action JSON object per line).
///
/// Each line is an externally-tagged action wrapper like `{"add": {...}}`.
/// Unknown action types (`commitInfo`, `txn`, `cdc`, ...) are skipped.
///
/// # Errors
/// Returns `DeltaError::internal` if a line is invalid JSON, isn't an object,
/// or fails to deserialize into a known action's payload.
pub fn parse_commit(bytes: &[u8]) -> Result<Vec<DeltaAction>, DeltaError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| DeltaError::internal(format!("delta commit not valid utf-8: {e}")))?;

    let mut out = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            DeltaError::internal(format!("delta commit line {} parse error: {e}", idx + 1))
        })?;
        let obj = value.as_object().ok_or_else(|| {
            DeltaError::internal(format!(
                "delta commit line {} is not a JSON object",
                idx + 1
            ))
        })?;

        let mut iter = obj.iter();
        let Some((key, payload)) = iter.next() else {
            continue;
        };

        let action = match key.as_str() {
            "protocol" => DeltaAction::Protocol(
                serde_json::from_value(payload.clone())
                    .map_err(|e| action_err(idx, "protocol", &e))?,
            ),
            "metaData" => DeltaAction::Metadata(
                serde_json::from_value(payload.clone())
                    .map_err(|e| action_err(idx, "metaData", &e))?,
            ),
            "add" => DeltaAction::Add(
                serde_json::from_value(payload.clone()).map_err(|e| action_err(idx, "add", &e))?,
            ),
            "remove" => DeltaAction::Remove(
                serde_json::from_value(payload.clone())
                    .map_err(|e| action_err(idx, "remove", &e))?,
            ),
            _ => DeltaAction::Other,
        };
        out.push(action);
    }
    Ok(out)
}

fn action_err(idx: usize, kind: &str, e: &serde_json::Error) -> DeltaError {
    DeltaError::internal(format!(
        "delta commit line {} {kind} parse error: {e}",
        idx + 1
    ))
}

/// Build the `_delta_log/{version:020}.json` key under `table_root`.
#[must_use]
pub fn commit_key(table_root: &str, version: i64) -> String {
    format!(
        "{}_delta_log/{:020}.json",
        normalize_root(table_root),
        version,
    )
}

/// Build the `_delta_log/` prefix under `table_root` for `ListObjectsV2`.
#[must_use]
pub fn log_prefix(table_root: &str) -> String {
    format!("{}_delta_log/", normalize_root(table_root))
}

/// Ensure `table_root` ends with `/` (or is empty).
fn normalize_root(table_root: &str) -> String {
    if table_root.is_empty() || table_root.ends_with('/') {
        table_root.to_string()
    } else {
        format!("{table_root}/")
    }
}

/// Parse a `_delta_log/*.json` key (or any path) into the commit version,
/// returning `None` for non-commit files.
#[must_use]
pub fn parse_commit_version(key: &str) -> Option<i64> {
    let filename = key.rsplit('/').next()?;
    let stem = filename.strip_suffix(".json")?;
    if stem.len() != 20 {
        return None;
    }
    stem.parse::<i64>().ok()
}

/// Detect whether a `_delta_log/` key represents a checkpoint Parquet or
/// `_last_checkpoint` hint file.
#[must_use]
pub fn is_checkpoint_key(key: &str) -> bool {
    let Some(filename) = key.rsplit('/').next() else {
        return false;
    };
    filename == "_last_checkpoint" || filename.ends_with(".checkpoint.parquet")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubReader {
        commits: Vec<i64>,
        has_checkpoint: bool,
        bodies: HashMap<i64, Vec<u8>>,
    }

    impl DeltaLogReader for StubReader {
        async fn list_log(
            &self,
            _bucket: &str,
            _table_root: &str,
        ) -> Result<LogListing, DeltaError> {
            Ok(LogListing {
                commits: self.commits.clone(),
                has_checkpoint: self.has_checkpoint,
            })
        }

        async fn read_commit(
            &self,
            _bucket: &str,
            _table_root: &str,
            version: i64,
        ) -> Result<Vec<u8>, DeltaError> {
            self.bodies
                .get(&version)
                .cloned()
                .ok_or_else(|| DeltaError::internal(format!("missing commit {version}")))
        }
    }

    fn metadata_line() -> String {
        r#"{"metaData":{"id":"abc","schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":["country"],"configuration":{},"createdTime":1700000000000,"format":{"provider":"parquet"}}}"#.into()
    }

    fn protocol_line() -> String {
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.into()
    }

    fn add_line(path: &str, size: i64) -> String {
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{{"country":"US"}},"size":{size},"modificationTime":1700000000000,"dataChange":true,"stats":"{{\"numRecords\":42}}"}}}}"#
        )
    }

    fn remove_line(path: &str) -> String {
        format!(
            r#"{{"remove":{{"path":"{path}","deletionTimestamp":1700000005000,"dataChange":true,"extendedFileMetadata":true}}}}"#
        )
    }

    #[test]
    fn parse_commit_extracts_actions() {
        let body = format!(
            "{}\n{}\n{}\n",
            protocol_line(),
            metadata_line(),
            add_line("part-00000.parquet", 1234),
        );
        let actions = parse_commit(body.as_bytes()).unwrap();
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], DeltaAction::Protocol(_)));
        assert!(matches!(actions[1], DeltaAction::Metadata(_)));
        let DeltaAction::Add(a) = &actions[2] else {
            panic!("expected Add")
        };
        assert_eq!(a.path, "part-00000.parquet");
        assert_eq!(a.size, 1234);
        assert_eq!(
            a.partition_values.get("country").and_then(Option::as_deref),
            Some("US"),
        );
    }

    #[test]
    fn parse_commit_skips_unknown_actions() {
        let body = format!(
            "{{\"commitInfo\":{{\"engineInfo\":\"spark\"}}}}\n{}\n",
            protocol_line()
        );
        let actions = parse_commit(body.as_bytes()).unwrap();
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], DeltaAction::Other));
    }

    #[test]
    fn parse_commit_skips_blank_lines() {
        let body = format!("\n{}\n\n", protocol_line());
        let actions = parse_commit(body.as_bytes()).unwrap();
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn parse_commit_returns_error_on_invalid_json() {
        let err = parse_commit(b"not json\n").unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.message.contains("line 1"));
    }

    #[tokio::test]
    async fn build_snapshot_walks_versions_and_folds_removes() {
        let mut bodies = HashMap::new();
        bodies.insert(
            0,
            format!(
                "{}\n{}\n{}\n{}\n",
                protocol_line(),
                metadata_line(),
                add_line("a.parquet", 100),
                add_line("b.parquet", 200),
            )
            .into_bytes(),
        );
        bodies.insert(
            1,
            format!(
                "{}\n{}\n",
                add_line("c.parquet", 300),
                remove_line("a.parquet"),
            )
            .into_bytes(),
        );
        let reader = StubReader {
            commits: vec![0, 1],
            has_checkpoint: false,
            bodies,
        };

        let snap = build_snapshot(&reader, "lake", "tbl/").await.unwrap();
        assert_eq!(snap.version, 1);
        assert_eq!(snap.adds.len(), 2);
        let paths: std::collections::HashSet<_> =
            snap.adds.iter().map(|a| a.path.clone()).collect();
        assert!(paths.contains("b.parquet"));
        assert!(paths.contains("c.parquet"));
        assert!(!paths.contains("a.parquet"));
        assert_eq!(snap.metadata.id, "abc");
        assert_eq!(snap.metadata.partition_columns, vec!["country".to_string()]);
        assert_eq!(snap.protocol.min_reader_version, 1);
    }

    #[tokio::test]
    async fn build_snapshot_uses_latest_metadata() {
        let mut bodies = HashMap::new();
        bodies.insert(
            0,
            format!("{}\n{}\n", protocol_line(), metadata_line()).into_bytes(),
        );
        let mut later = MetadataAction {
            id: "abc".into(),
            schema_string: "{\"new\":true}".into(),
            ..MetadataAction::default()
        };
        later.partition_columns = vec!["region".into()];
        let later_line = serde_json::to_string(&serde_json::json!({ "metaData": {
            "id": later.id,
            "schemaString": later.schema_string,
            "partitionColumns": later.partition_columns,
            "configuration": {},
            "format": { "provider": "parquet" },
        }}))
        .unwrap();
        bodies.insert(1, format!("{later_line}\n").into_bytes());
        let reader = StubReader {
            commits: vec![0, 1],
            has_checkpoint: false,
            bodies,
        };

        let snap = build_snapshot(&reader, "lake", "tbl").await.unwrap();
        assert_eq!(snap.metadata.schema_string, "{\"new\":true}");
        assert_eq!(snap.metadata.partition_columns, vec!["region".to_string()]);
    }

    #[tokio::test]
    async fn build_snapshot_rejects_checkpoint() {
        let reader = StubReader {
            commits: vec![10],
            has_checkpoint: true,
            bodies: HashMap::new(),
        };
        let err = build_snapshot(&reader, "lake", "tbl").await.unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("checkpoint"));
    }

    #[tokio::test]
    async fn build_snapshot_errors_on_empty_log() {
        let reader = StubReader {
            commits: vec![],
            has_checkpoint: false,
            bodies: HashMap::new(),
        };
        let err = build_snapshot(&reader, "lake", "tbl/").await.unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn build_snapshot_errors_when_no_metadata_action() {
        let mut bodies = HashMap::new();
        bodies.insert(0, format!("{}\n", protocol_line()).into_bytes());
        let reader = StubReader {
            commits: vec![0],
            has_checkpoint: false,
            bodies,
        };
        let err = build_snapshot(&reader, "lake", "tbl").await.unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.message.contains("metaData"));
    }

    #[test]
    fn commit_key_pads_version() {
        assert_eq!(
            commit_key("warehouse/sales/events/", 7),
            "warehouse/sales/events/_delta_log/00000000000000000007.json",
        );
        assert_eq!(
            commit_key("warehouse/sales/events", 7),
            "warehouse/sales/events/_delta_log/00000000000000000007.json",
        );
    }

    #[test]
    fn log_prefix_normalizes_trailing_slash() {
        assert_eq!(log_prefix("a/b"), "a/b/_delta_log/");
        assert_eq!(log_prefix("a/b/"), "a/b/_delta_log/");
        assert_eq!(log_prefix(""), "_delta_log/");
    }

    #[test]
    fn parse_commit_version_recognizes_padded_files() {
        assert_eq!(
            parse_commit_version("foo/_delta_log/00000000000000000003.json"),
            Some(3),
        );
        assert_eq!(parse_commit_version("00000000000000000010.json"), Some(10),);
        assert_eq!(parse_commit_version("foo/_delta_log/3.json"), None);
        assert_eq!(
            parse_commit_version("foo/_delta_log/00000000000000000010.checkpoint.parquet"),
            None,
        );
    }

    #[test]
    fn is_checkpoint_key_identifies_checkpoints_and_hint() {
        assert!(is_checkpoint_key(
            "foo/_delta_log/00000000000000000010.checkpoint.parquet"
        ));
        assert!(is_checkpoint_key("foo/_delta_log/_last_checkpoint"));
        assert!(!is_checkpoint_key(
            "foo/_delta_log/00000000000000000003.json"
        ));
    }
}
