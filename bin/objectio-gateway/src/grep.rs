//! Gateway-side grep for S3 objects.
//!
//! Lets a client push a regex / fixed-string pattern to the gateway
//! and receive back only the matching lines, with enough byte-offset
//! metadata to do follow-up `Range:` reads. Designed for agent / LLM
//! tool workloads where a 10 GB file × 100 users shouldn't mean 1 TB
//! of download traffic.
//!
//! ## Wire
//!
//! ```
//! POST /{bucket}/{key}?grep
//! Content-Type: application/json
//!
//! {
//!   "pattern":          "ERROR.*timeout",
//!   "literal":          false,
//!   "case_insensitive": false,
//!   "max_matches":      100,
//!   "content_max_bytes": 2048,
//!   "invert":           false,
//!   "line_numbers":     true
//! }
//! ```
//!
//! Response is `application/x-ndjson` — one JSON value per line:
//!
//! ```ignore
//! {"type":"start","file_size":N,"etag":"…","pattern":"…"}
//! {"type":"match","line_number":42,"offset":12345,"line_length":80,
//!  "match_offset":12350,"match_length":5,"content":"…"}
//! ...
//! {"type":"end","matches":2,"bytes_scanned":10485760,"truncated":false,"elapsed_ms":1240}
//! ```
//!
//! `offset` / `line_length` / `match_offset` / `match_length` are in
//! *bytes*, absolute in the source object — agents can feed them
//! straight into a `Range: bytes=offset-…` follow-up fetch.
//!
//! ## Implementation
//!
//! * Works for `.txt` / `.md` / `.jsonl` (line-oriented text). For
//!   pretty-printed JSON the line shape is a best-effort match, but
//!   the byte-offset semantics are still sound and an agent can
//!   fetch around the match with Range.
//! * Memory-bounded: tokio `BufReader` streams the object body, one
//!   line at a time. No accumulation. 10 GB file scans with a fixed
//!   ~64 KiB buffer.
//! * Streaming response — matches arrive at the client as the scan
//!   progresses. Client disconnect propagates back up the stream via
//!   tokio cancellation and the gateway stops reading.

use std::time::Instant;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::ReceiverStream;

/// Agent-side request body for prefix grep (multi-object scan).
///
/// Lets an agent ask "grep these lines across every key under this
/// prefix" in one request. The server fans out key-by-key and
/// streams `Object` / `Match` / `ObjectEnd` frames as each key
/// completes. Global `max_matches` caps the overall match stream;
/// `max_matches_per_object` protects against one noisy key eating
/// the whole budget.
#[derive(Debug, Deserialize)]
pub struct PrefixGrepRequest {
    /// Regex or literal pattern; same semantics as `GrepRequest::pattern`.
    pub pattern: String,

    /// Prefix that candidate keys must start with. Empty string →
    /// entire bucket.
    #[serde(default)]
    pub prefix: String,

    /// Cap on the number of keys scanned. `0` → 100.
    #[serde(default)]
    pub max_keys: u32,

    /// Global cap on matches returned. `0` → 1000.
    #[serde(default)]
    pub max_matches: u32,

    /// Per-object cap. `0` → 100.
    #[serde(default)]
    pub max_matches_per_object: u32,

    #[serde(default)]
    pub literal: bool,
    #[serde(default)]
    pub case_insensitive: bool,
    #[serde(default)]
    pub content_max_bytes: u32,
    #[serde(default)]
    pub invert: bool,
}

impl PrefixGrepRequest {
    /// Turn the prefix request into a per-object `GrepRequest` plus
    /// the three multi-object caps (global/per-object/max_keys).
    pub fn to_per_object(&self) -> (GrepRequest, PrefixCaps) {
        let per_object = GrepRequest {
            pattern: self.pattern.clone(),
            literal: self.literal,
            case_insensitive: self.case_insensitive,
            max_matches: if self.max_matches_per_object == 0 {
                100
            } else {
                self.max_matches_per_object
            },
            content_max_bytes: self.content_max_bytes,
            invert: self.invert,
        };
        let caps = PrefixCaps {
            prefix: self.prefix.clone(),
            max_keys: if self.max_keys == 0 { 100 } else { self.max_keys },
            max_matches_global: if self.max_matches == 0 {
                1000
            } else {
                self.max_matches
            },
        };
        (per_object, caps)
    }
}

/// Extracted caps to keep the driver from re-reading the request.
#[derive(Debug, Clone)]
pub struct PrefixCaps {
    pub prefix: String,
    pub max_keys: u32,
    pub max_matches_global: u32,
}

/// Agent-side request body for single-object grep.
#[derive(Debug, Deserialize)]
pub struct GrepRequest {
    /// Pattern to search for. Regex by default; set `literal: true`
    /// for fixed-string matching (equivalent to `grep -F`).
    pub pattern: String,

    /// If true, treat `pattern` as a fixed string (no regex
    /// metachars). Default false.
    #[serde(default)]
    pub literal: bool,

    /// Case-insensitive matching. Default false.
    #[serde(default)]
    pub case_insensitive: bool,

    /// Stop after this many matches. `0` or absent → 1000.
    #[serde(default)]
    pub max_matches: u32,

    /// Truncate each match's `content` field at N bytes. Keeps
    /// pathological long lines from blowing past an LLM context
    /// window. `0` or absent → 8 KiB. Agent can still use `offset` /
    /// `line_length` to fetch the full line via Range.
    #[serde(default)]
    pub content_max_bytes: u32,

    /// Return lines that do *not* match the pattern (grep -v).
    #[serde(default)]
    pub invert: bool,
}

/// One frame emitted in the NDJSON response stream. Events are a
/// superset covering both single-object grep (`Start` / `Match` /
/// `End`) and multi-object prefix grep (additional `Object` /
/// `ObjectEnd` bracketing per key).
///
/// Backwards compatibility: existing single-object callers will only
/// see `Start`, `Match`, `End`, `Error` — they can ignore the
/// multi-object variants.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GrepEvent {
    /// Emitted once at the start of a single-object scan.
    Start {
        file_size: u64,
        etag: String,
        pattern: String,
    },
    /// Emitted once per key in a prefix scan, before that key's
    /// matches stream. Gives agents the size + etag up-front so
    /// they can decide "this file is tiny, fetch the whole thing
    /// and skip grep" without an extra HEAD.
    Object {
        bucket: String,
        key: String,
        file_size: u64,
        etag: String,
    },
    Match {
        /// Bucket / key populated only in prefix-grep responses.
        /// Single-object responses omit these fields.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bucket: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
        /// 1-indexed line number in the source file.
        line_number: u64,
        /// Byte offset of the start of the line in the source file.
        /// Feed into `Range: bytes=offset-…` to reread from this point.
        offset: u64,
        /// Line length in bytes, including trailing newline if any.
        line_length: u64,
        /// Byte offset of the matched substring within the source
        /// file. Only meaningful for the first match on the line.
        match_offset: u64,
        /// Length of the matched substring in bytes.
        match_length: u64,
        /// The line content (possibly truncated to
        /// `content_max_bytes`). Trailing newline stripped.
        content: String,
        /// True if `content` was truncated.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        truncated: bool,
    },
    /// End of the current object's matches in a prefix scan. Omitted
    /// in single-object mode.
    ObjectEnd {
        bucket: String,
        key: String,
        matches_in_object: u64,
        /// True if this object's scan stopped at
        /// `max_matches_per_object`.
        per_object_truncated: bool,
    },
    End {
        matches: u64,
        bytes_scanned: u64,
        truncated: bool,
        elapsed_ms: u64,
        /// Populated in prefix scans; omitted in single-object.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        objects_scanned: Option<u64>,
    },
    Error {
        message: String,
    },
}

/// Parameters derived from the request — kept separate so the pure
/// scan function doesn't need serde.
#[derive(Debug, Clone)]
struct ScanOpts {
    max_matches: u64,
    content_max_bytes: usize,
    invert: bool,
}

const DEFAULT_MAX_MATCHES: u64 = 1000;
const DEFAULT_CONTENT_MAX_BYTES: usize = 8192;
const NDJSON: &str = "application/x-ndjson";

impl GrepRequest {
    fn validate(&self) -> Result<(regex::Regex, ScanOpts), GrepError> {
        if self.pattern.is_empty() {
            return Err(GrepError::bad_request("pattern is required"));
        }
        // Arbitrary soft cap — pathological patterns would be caught
        // by the regex compiler anyway, but fail fast on obvious
        // abuse.
        if self.pattern.len() > 64 * 1024 {
            return Err(GrepError::bad_request("pattern exceeds 64 KiB"));
        }
        let pattern = if self.literal {
            regex::escape(&self.pattern)
        } else {
            self.pattern.clone()
        };
        let re = RegexBuilder::new(&pattern)
            .case_insensitive(self.case_insensitive)
            // Cap to 10 MiB of regex bytecode — pathological regexes
            // would otherwise DoS the gateway.
            .size_limit(10 * 1024 * 1024)
            .build()
            .map_err(|e| GrepError::bad_request(format!("invalid regex: {e}")))?;
        let opts = ScanOpts {
            max_matches: if self.max_matches == 0 {
                DEFAULT_MAX_MATCHES
            } else {
                u64::from(self.max_matches)
            },
            content_max_bytes: if self.content_max_bytes == 0 {
                DEFAULT_CONTENT_MAX_BYTES
            } else {
                self.content_max_bytes as usize
            },
            invert: self.invert,
        };
        Ok((re, opts))
    }
}

/// Error shape used internally before we materialise a Response.
/// The outer handler maps `BadRequest` to 400 and `Upstream` to 502.
#[derive(Debug)]
pub enum GrepError {
    BadRequest(String),
    #[allow(dead_code)]
    Upstream(String),
}

impl GrepError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }
}

impl GrepError {
    pub fn into_response(self) -> Response {
        let (status, msg) = match self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::Upstream(m) => (StatusCode::BAD_GATEWAY, m),
        };
        let body = serde_json::json!({
            "error": match &status {
                &StatusCode::BAD_REQUEST => "BadRequest",
                _ => "Upstream",
            },
            "message": msg,
        });
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap_or_else(|_| Response::new(Body::from(msg)))
    }
}

/// Stream a scan over any tokio `AsyncRead`. Returns a receiver whose
/// items are pre-serialised NDJSON frames (newline-terminated).
///
/// The reader is consumed on a dedicated task so the returned stream
/// can emit frames as matches are found — client doesn't wait for
/// the full scan to finish.
fn scan_stream<R>(
    reader: R,
    regex: regex::Regex,
    opts: ScanOpts,
    file_size: u64,
    etag: String,
    pattern_echo: String,
) -> ReceiverStream<Result<Bytes, std::io::Error>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);

    tokio::spawn(async move {
        let start_time = Instant::now();

        // Emit the start frame.
        let start = GrepEvent::Start {
            file_size,
            etag,
            pattern: pattern_echo,
        };
        if tx
            .send(Ok(frame(&start)))
            .await
            .is_err()
        {
            return; // client hung up before we started
        }

        let mut reader = BufReader::with_capacity(64 * 1024, reader);
        let mut buf = Vec::with_capacity(4096);
        let mut line_no: u64 = 0;
        let mut cursor: u64 = 0;
        let mut matches: u64 = 0;
        let mut truncated = false;

        loop {
            buf.clear();
            let n = match reader.read_until(b'\n', &mut buf).await {
                Ok(0) => break, // EOF
                Ok(n) => n,
                Err(e) => {
                    let _ = tx
                        .send(Ok(frame(&GrepEvent::Error {
                            message: format!("read failed at byte {cursor}: {e}"),
                        })))
                        .await;
                    return;
                }
            };
            line_no += 1;
            let line_offset = cursor;
            let line_length = n as u64;
            cursor += line_length;

            // Strip trailing CR/LF for matching + display; preserve
            // line_length for Range math.
            let bytes = trim_trailing_newline(&buf);
            let text = std::str::from_utf8(bytes).ok();

            // Regex needs &str; non-UTF-8 bytes won't match regex,
            // skip them. Agents targetting text files won't hit this;
            // binary would.
            let Some(text) = text else { continue };
            let hit = regex.find(text);
            let should_emit = match (hit.is_some(), opts.invert) {
                (true, false) | (false, true) => true,
                _ => false,
            };
            if !should_emit {
                continue;
            }

            let (match_offset, match_length) = match hit {
                Some(m) => (
                    line_offset + m.start() as u64,
                    (m.end() - m.start()) as u64,
                ),
                None => (line_offset, 0), // invert case — no substring match
            };

            let (content, content_truncated) = truncate_utf8(text, opts.content_max_bytes);
            let ev = GrepEvent::Match {
                bucket: None,
                key: None,
                line_number: line_no,
                offset: line_offset,
                line_length,
                match_offset,
                match_length,
                content: content.to_string(),
                truncated: content_truncated,
            };
            if tx.send(Ok(frame(&ev))).await.is_err() {
                return; // client hung up
            }

            matches += 1;
            if matches >= opts.max_matches {
                truncated = true;
                break;
            }
        }

        let elapsed_ms = start_time.elapsed().as_millis() as u64;
        let end = GrepEvent::End {
            matches,
            bytes_scanned: cursor,
            truncated,
            elapsed_ms,
            objects_scanned: None,
        };
        let _ = tx.send(Ok(frame(&end))).await;
    });

    ReceiverStream::new(rx)
}

/// Convert the full request flow into a streaming axum `Response`.
/// Caller provides a byte stream of the object body + its size/etag.
pub fn respond(
    req: GrepRequest,
    body: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    file_size: u64,
    etag: String,
) -> Response {
    let (regex, opts) = match req.validate() {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let stream = scan_stream(body, regex, opts, file_size, etag, req.pattern);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, NDJSON)
        .header("X-Object-Size", file_size.to_string())
        // Tell clients this is a chunked, streaming response.
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| Response::new(Body::from("internal error building response")))
}

/// Parse a prefix-grep body. Same JSON-only content-type contract as
/// [`parse_body`]; returns either the request or a ready-to-send 400.
pub fn parse_prefix_body(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<PrefixGrepRequest, Response> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct.contains("application/json") {
        return Err(GrepError::bad_request(
            "Content-Type must be application/json",
        )
        .into_response());
    }
    serde_json::from_slice::<PrefixGrepRequest>(body).map_err(|e| {
        GrepError::bad_request(format!("invalid request body: {e}")).into_response()
    })
}

/// Run a scan over a single in-memory object buffer, sending events
/// into a caller-provided channel. Bucket/key are tagged onto every
/// emitted `Match`. Returns `(matches_emitted, per_object_truncated)`.
///
/// Caller handles object fetch, channel wiring, and driving the
/// global `max_matches` cap.
pub async fn scan_object_into_channel(
    bucket: String,
    key: String,
    body: Vec<u8>,
    req: GrepRequest,
    max_matches: u64,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> (u64, bool) {
    let (regex, mut opts) = match req.validate() {
        Ok(v) => v,
        Err(e) => {
            let _ = tx
                .send(Ok(frame(&GrepEvent::Error {
                    message: format!("{bucket}/{key}: {e:?}"),
                })))
                .await;
            return (0, false);
        }
    };
    // Clamp per-object cap to the remaining global budget so we
    // never exceed the caller's global limit.
    opts.max_matches = opts.max_matches.min(max_matches);
    if opts.max_matches == 0 {
        return (0, false);
    }

    let reader = tokio::io::BufReader::new(std::io::Cursor::new(body));
    let mut buf = Vec::with_capacity(4096);
    let mut line_no: u64 = 0;
    let mut cursor: u64 = 0;
    let mut matches: u64 = 0;
    let mut truncated = false;
    use tokio::io::AsyncBufReadExt;
    let mut reader = reader;
    loop {
        buf.clear();
        let n = match reader.read_until(b'\n', &mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                let _ = tx
                    .send(Ok(frame(&GrepEvent::Error {
                        message: format!("{bucket}/{key}: read at {cursor}: {e}"),
                    })))
                    .await;
                break;
            }
        };
        line_no += 1;
        let line_offset = cursor;
        let line_length = n as u64;
        cursor += line_length;
        let bytes = trim_trailing_newline(&buf);
        let Some(text) = std::str::from_utf8(bytes).ok() else {
            continue;
        };
        let hit = regex.find(text);
        let should_emit = match (hit.is_some(), opts.invert) {
            (true, false) | (false, true) => true,
            _ => false,
        };
        if !should_emit {
            continue;
        }
        let (match_offset, match_length) = match hit {
            Some(m) => (
                line_offset + m.start() as u64,
                (m.end() - m.start()) as u64,
            ),
            None => (line_offset, 0),
        };
        let (content, content_truncated) = truncate_utf8(text, opts.content_max_bytes);
        let ev = GrepEvent::Match {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            line_number: line_no,
            offset: line_offset,
            line_length,
            match_offset,
            match_length,
            content: content.to_string(),
            truncated: content_truncated,
        };
        if tx.send(Ok(frame(&ev))).await.is_err() {
            return (matches, truncated);
        }
        matches += 1;
        if matches >= opts.max_matches {
            truncated = true;
            break;
        }
    }
    (matches, truncated)
}

/// Emit the Start frame for a prefix scan. file_size/etag zeroed —
/// they're per-object concepts.
pub async fn emit_prefix_start(
    pattern: &str,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), ()> {
    let ev = GrepEvent::Start {
        file_size: 0,
        etag: String::new(),
        pattern: pattern.to_string(),
    };
    tx.send(Ok(frame(&ev))).await.map_err(|_| ())
}

/// Emit an `Object` frame to the channel.
pub async fn emit_object_start(
    bucket: &str,
    key: &str,
    file_size: u64,
    etag: &str,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), ()> {
    let ev = GrepEvent::Object {
        bucket: bucket.to_string(),
        key: key.to_string(),
        file_size,
        etag: etag.to_string(),
    };
    tx.send(Ok(frame(&ev))).await.map_err(|_| ())
}

/// Emit an `ObjectEnd` frame.
pub async fn emit_object_end(
    bucket: &str,
    key: &str,
    matches_in_object: u64,
    per_object_truncated: bool,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), ()> {
    let ev = GrepEvent::ObjectEnd {
        bucket: bucket.to_string(),
        key: key.to_string(),
        matches_in_object,
        per_object_truncated,
    };
    tx.send(Ok(frame(&ev))).await.map_err(|_| ())
}

/// Emit the final `End` frame for a prefix scan.
pub async fn emit_prefix_end(
    matches: u64,
    bytes_scanned: u64,
    truncated: bool,
    elapsed_ms: u64,
    objects_scanned: u64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), ()> {
    let ev = GrepEvent::End {
        matches,
        bytes_scanned,
        truncated,
        elapsed_ms,
        objects_scanned: Some(objects_scanned),
    };
    tx.send(Ok(frame(&ev))).await.map_err(|_| ())
}

/// Build a streaming NDJSON response from a caller-owned channel.
/// The caller spawns a task that sends events (via the helper emit_*
/// functions above); this wraps the receiver in an axum body.
pub fn respond_from_channel(
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>,
    object_size_hint: Option<u64>,
) -> Response {
    let stream = ReceiverStream::new(rx);
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, NDJSON)
        .header(header::TRANSFER_ENCODING, "chunked");
    if let Some(sz) = object_size_hint {
        builder = builder.header("X-Object-Size", sz.to_string());
    }
    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| Response::new(Body::from("internal error building response")))
}

/// Parse the request body once, returning the validated request or a
/// 400 response ready to send.
pub fn parse_body(headers: &HeaderMap, body: &[u8]) -> Result<GrepRequest, Response> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct.contains("application/json") {
        return Err(GrepError::bad_request(
            "Content-Type must be application/json",
        )
        .into_response());
    }
    serde_json::from_slice::<GrepRequest>(body).map_err(|e| {
        GrepError::bad_request(format!("invalid request body: {e}")).into_response()
    })
}

// ------- helpers -------

fn frame(ev: &GrepEvent) -> Bytes {
    let mut s = serde_json::to_string(ev).unwrap_or_else(|_| "{\"type\":\"error\"}".into());
    s.push('\n');
    Bytes::from(s)
}

fn trim_trailing_newline(b: &[u8]) -> &[u8] {
    let mut end = b.len();
    while end > 0 && matches!(b[end - 1], b'\n' | b'\r') {
        end -= 1;
    }
    &b[..end]
}

/// Truncate `s` to at most `max_bytes` bytes while preserving UTF-8
/// char boundaries. Returns (prefix, was_truncated).
fn truncate_utf8(s: &str, max_bytes: usize) -> (&str, bool) {
    if s.len() <= max_bytes {
        return (s, false);
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

// ------- tests -------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::io::Cursor;

    async fn collect(stream: ReceiverStream<Result<Bytes, std::io::Error>>) -> Vec<GrepEvent> {
        let mut out = Vec::new();
        let bytes: Vec<Bytes> = stream.map(|r| r.unwrap()).collect().await;
        for b in bytes {
            for line in b.split(|&c| c == b'\n') {
                if line.is_empty() {
                    continue;
                }
                out.push(serde_json::from_slice::<GrepEvent>(line).unwrap());
            }
        }
        out
    }

    fn run(body: &str, req: GrepRequest) -> Vec<GrepEvent> {
        // Multi-thread because scan_stream internally tokio::spawns;
        // current-thread runtime would panic on spawn without LocalSet.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let body_owned = body.to_string();
        rt.block_on(async move {
            let (re, opts) = req.validate().unwrap();
            let file_size = body_owned.len() as u64;
            let rdr = Cursor::new(body_owned.into_bytes());
            let rdr = tokio::io::BufReader::new(rdr);
            let stream = scan_stream(
                rdr,
                re,
                opts,
                file_size,
                "\"etag\"".into(),
                req.pattern.clone(),
            );
            collect(stream).await
        })
    }

    #[test]
    fn basic_regex_match() {
        let body = "alpha\nerror here\nbeta\nanother error\ngamma\n";
        let req = GrepRequest {
            pattern: "error".into(),
            literal: false,
            case_insensitive: false,
            max_matches: 0,
            content_max_bytes: 0,
            invert: false,
        };
        let events = run(body, req);
        // start + 2 match + end
        assert_eq!(events.len(), 4);
        assert!(matches!(events[0], GrepEvent::Start { .. }));
        let mut matches = 0;
        for e in &events[1..3] {
            match e {
                GrepEvent::Match {
                    line_number,
                    offset,
                    content,
                    ..
                } => {
                    matches += 1;
                    assert!(content.contains("error"));
                    // Verify offset lands on the start of the line in the source.
                    let line = &body[*offset as usize..];
                    assert!(line.starts_with(content.as_str()));
                    let _ = line_number; // just exercised
                }
                _ => panic!("expected Match"),
            }
        }
        assert_eq!(matches, 2);
        if let GrepEvent::End {
            matches,
            truncated,
            bytes_scanned,
            ..
        } = &events[3]
        {
            assert_eq!(*matches, 2);
            assert!(!truncated);
            assert_eq!(*bytes_scanned, body.len() as u64);
        } else {
            panic!("expected End");
        }
    }

    #[test]
    fn literal_escapes_regex_metachars() {
        // Without literal, "a.b" (regex) matches "a.b", "a+b", "a/b".
        // With literal it matches only "a.b".
        let body = "a.b\na+b\na/b\n";
        let events = run(
            body,
            GrepRequest {
                pattern: "a.b".into(),
                literal: true,
                case_insensitive: false,
                max_matches: 0,
                content_max_bytes: 0,
                invert: false,
            },
        );
        // Only "a.b" matches under literal mode; "a+b" and "a/b" do not.
        let ms: Vec<_> = events
            .iter()
            .filter_map(|e| {
                if let GrepEvent::Match { content, .. } = e {
                    Some(content.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(ms, vec!["a.b"]);
    }

    #[test]
    fn invert_returns_non_matches() {
        let body = "keep\ndrop\nkeep\ndrop\n";
        let events = run(
            body,
            GrepRequest {
                pattern: "drop".into(),
                literal: false,
                case_insensitive: false,
                max_matches: 0,
                content_max_bytes: 0,
                invert: true,
            },
        );
        let ms: Vec<_> = events
            .iter()
            .filter_map(|e| {
                if let GrepEvent::Match { content, .. } = e {
                    Some(content.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(ms, vec!["keep", "keep"]);
    }

    #[test]
    fn max_matches_caps_output() {
        let body = "x\n".repeat(100);
        let events = run(
            &body,
            GrepRequest {
                pattern: "x".into(),
                literal: false,
                case_insensitive: false,
                max_matches: 5,
                content_max_bytes: 0,
                invert: false,
            },
        );
        let n = events
            .iter()
            .filter(|e| matches!(e, GrepEvent::Match { .. }))
            .count();
        assert_eq!(n, 5);
        if let GrepEvent::End { truncated, .. } = events.last().unwrap() {
            assert!(*truncated);
        }
    }

    #[test]
    fn offset_is_absolute_in_file() {
        // Mixed-length lines; verify offset math is byte-accurate.
        let body = "a\nhello world\nfoo\nmatch_here: boom\nend\n";
        let events = run(
            body,
            GrepRequest {
                pattern: "boom".into(),
                literal: false,
                case_insensitive: false,
                max_matches: 0,
                content_max_bytes: 0,
                invert: false,
            },
        );
        let m = events
            .iter()
            .find_map(|e| if let GrepEvent::Match { offset, match_offset, content, .. } = e {
                Some((*offset, *match_offset, content.clone()))
            } else { None })
            .unwrap();
        let (line_offset, match_offset, content) = m;
        // Line "match_here: boom" starts at byte offset 2+12+4 = 18
        assert_eq!(&body[line_offset as usize..line_offset as usize + content.len()], content);
        // Match is inside that line at column 12.
        assert_eq!(match_offset, line_offset + 12);
        assert_eq!(&body[match_offset as usize..match_offset as usize + 4], "boom");
    }

    #[test]
    fn content_truncation_respects_utf8() {
        // Multi-byte characters at the truncation boundary should not split.
        let body = format!("{}\n", "日本語日本語".repeat(100));
        let events = run(
            &body,
            GrepRequest {
                pattern: "日".into(),
                literal: false,
                case_insensitive: false,
                max_matches: 0,
                content_max_bytes: 10, // odd number within multi-byte chars
                invert: false,
            },
        );
        for e in &events {
            if let GrepEvent::Match { content, truncated, .. } = e {
                assert!(*truncated);
                // Must be valid UTF-8 (no splits).
                let _ = std::str::from_utf8(content.as_bytes()).unwrap();
                assert!(content.len() <= 10);
            }
        }
    }
}
