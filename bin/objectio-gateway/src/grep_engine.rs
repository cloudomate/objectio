//! Pluggable pattern-matching engines for gateway-side grep.
//!
//! The default engine is the pure-Rust `regex` crate — fast, safe,
//! always available. It handles every pattern ObjectIO grep tests
//! cover (it's RE2-based: linear-time, no catastrophic backtracking,
//! no backrefs, no lookaround).
//!
//! Two opt-in alternatives handle cases the default can't or is slow on:
//!
//! * **PCRE2** (feature `pcre2`) — Perl-compatible regex. Covers
//!   backrefs (`(\w+)\s+\1`), lookaround (`foo(?=bar)`), atomic
//!   groups, recursive patterns, named captures. JIT compile for
//!   competitive speed on simple patterns. Useful for customers
//!   migrating grep/awk/Python-regex scripts that rely on these
//!   features.
//!
//! * **Hyperscan / Vectorscan** (feature `hyperscan`) — Intel's
//!   SIMD-accelerated multi-pattern matcher. Dramatically faster on
//!   simple anchor-free patterns over large streams. Doesn't support
//!   backrefs or full lookaround. Linux-primary (ARM via Vectorscan
//!   fork). Useful when grep is the bottleneck across terabytes of
//!   logs.
//!
//! All three engines expose the same minimal interface: compile a
//! pattern string, then report the first match in a line. That's
//! enough for the grep use case — we don't do multi-match per line,
//! multiline spanning, or capture-group extraction (yet). The wire
//! format carries byte offsets, so agents can always Range-fetch the
//! surrounding context.
//!
//! Compile-time shape:
//!
//! | Feature | Default build | `--features pcre2` | `--features hyperscan` |
//! |---|---|---|---|
//! | engine=regex    | ✓ | ✓ | ✓ |
//! | engine=pcre2    | 400 error | ✓ | — |
//! | engine=hyperscan| 400 error | — | ✓ |
//!
//! The gateway still ships a working grep with just the default.
//! Opting in enables the matching engine at startup.

use crate::grep::GrepRequest;
use std::fmt;

/// A single-match result in line-byte coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hit {
    /// Byte offset of the match start within the line.
    pub start: usize,
    /// Byte offset just past the match end.
    pub end: usize,
}

/// Descriptive error for compile or runtime match failures.
#[derive(Debug)]
pub enum EngineError {
    /// The selected engine isn't compiled into this binary.
    NotCompiledIn(&'static str),
    /// Pattern compile failed (bad regex, too large, unsupported
    /// construct).
    CompileFailed(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCompiledIn(name) => write!(
                f,
                "engine '{name}' is not compiled into this gateway; \
                 rebuild with `--features {name}` or select another engine"
            ),
            Self::CompileFailed(msg) => write!(f, "pattern compile failed: {msg}"),
        }
    }
}

impl std::error::Error for EngineError {}

/// The compiled pattern. One variant per backend. `match_line`
/// returns either the first hit or `None`.
#[derive(Debug)]
pub enum CompiledPattern {
    Regex(regex::Regex),

    #[cfg(feature = "pcre2")]
    Pcre2(pcre2::bytes::Regex),

    #[cfg(feature = "hyperscan")]
    Hyperscan(hyperscan::BlockDatabase),
}

impl CompiledPattern {
    /// Look for the first match in `text`. Text is expected to be
    /// valid UTF-8 for the regex engine; Hyperscan operates on raw
    /// bytes so we pass through.
    pub fn match_line(&self, text: &str) -> Option<Hit> {
        match self {
            Self::Regex(re) => re.find(text).map(|m| Hit {
                start: m.start(),
                end: m.end(),
            }),
            #[cfg(feature = "pcre2")]
            Self::Pcre2(re) => {
                // pcre2 returns an Option<Result<_>>. Both a miss and
                // a runtime error collapse to "no match" — the
                // scanner logs and moves on.
                re.find(text.as_bytes())
                    .ok()
                    .flatten()
                    .map(|m| Hit {
                        start: m.start(),
                        end: m.end(),
                    })
            }
            #[cfg(feature = "hyperscan")]
            Self::Hyperscan(db) => hyperscan_first_match(db, text),
        }
    }
}

/// Which backend to compile against. Parsed from the request's
/// `engine` field (string). Defaults to `Regex` when absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Regex,
    Pcre2,
    Hyperscan,
}

impl Engine {
    pub fn parse(s: &str) -> Result<Self, EngineError> {
        match s {
            "" | "regex" => Ok(Self::Regex),
            "pcre2" => Ok(Self::Pcre2),
            "hyperscan" | "vectorscan" => Ok(Self::Hyperscan),
            other => Err(EngineError::CompileFailed(format!(
                "unknown engine {other:?}; expected regex | pcre2 | hyperscan"
            ))),
        }
    }
}

/// Build a [`CompiledPattern`] from a [`GrepRequest`]. Applies the
/// literal / case-insensitive flags; selects the backend based on
/// `engine`.
pub fn compile(req: &GrepRequest) -> Result<CompiledPattern, EngineError> {
    let engine = Engine::parse(&req.engine)?;
    let pattern = if req.literal {
        // regex::escape works for regex + pcre2 (same metachar set on
        // the common subset we escape). Hyperscan has its own syntax
        // — still passes through regex::escape because that output is
        // a valid PCRE-syntax literal string.
        regex::escape(&req.pattern)
    } else {
        req.pattern.clone()
    };

    match engine {
        Engine::Regex => {
            let re = regex::RegexBuilder::new(&pattern)
                .case_insensitive(req.case_insensitive)
                .size_limit(10 * 1024 * 1024)
                .build()
                .map_err(|e| EngineError::CompileFailed(e.to_string()))?;
            Ok(CompiledPattern::Regex(re))
        }
        Engine::Pcre2 => {
            #[cfg(feature = "pcre2")]
            {
                let re = pcre2::bytes::RegexBuilder::new()
                    .caseless(req.case_insensitive)
                    .jit(true)
                    .build(&pattern)
                    .map_err(|e| EngineError::CompileFailed(e.to_string()))?;
                Ok(CompiledPattern::Pcre2(re))
            }
            #[cfg(not(feature = "pcre2"))]
            Err(EngineError::NotCompiledIn("pcre2"))
        }
        Engine::Hyperscan => {
            #[cfg(feature = "hyperscan")]
            {
                use hyperscan::prelude::*;
                // SOM_LEFTMOST is required for the `from` offset to
                // be populated in the match callback — without it,
                // Hyperscan only reports end-of-match.
                let mut flags = Flags::SOM_LEFTMOST;
                if req.case_insensitive {
                    flags |= Flags::CASELESS;
                }
                let pat = Pattern::with_flags(pattern, flags)
                    .map_err(|e| EngineError::CompileFailed(format!("hyperscan pattern: {e:?}")))?;
                let db: BlockDatabase = pat
                    .build()
                    .map_err(|e| EngineError::CompileFailed(format!("hyperscan compile: {e:?}")))?;
                Ok(CompiledPattern::Hyperscan(db))
            }
            #[cfg(not(feature = "hyperscan"))]
            Err(EngineError::NotCompiledIn("hyperscan"))
        }
    }
}

// Hyperscan uses a callback-driven scan model. We want the first
// match only, so the callback signals a halt as soon as it fires.
#[cfg(feature = "hyperscan")]
fn hyperscan_first_match(db: &hyperscan::BlockDatabase, text: &str) -> Option<Hit> {
    use hyperscan::prelude::*;
    use std::cell::Cell;

    let scratch = match db.alloc_scratch() {
        Ok(s) => s,
        Err(_) => return None,
    };
    let hit: Cell<Option<Hit>> = Cell::new(None);
    let halt = |_id: u32, from: u64, to: u64, _flags: u32| -> Matching {
        hit.set(Some(Hit {
            start: from as usize,
            end: to as usize,
        }));
        Matching::Terminate
    };
    let _ = db.scan(text.as_bytes(), &scratch, halt);
    hit.into_inner()
}

// --------------------------------------------------------------
// Tests — exercise every engine that's compiled in.
// --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn req(pattern: &str, engine: &str, literal: bool, ci: bool) -> GrepRequest {
        GrepRequest {
            pattern: pattern.into(),
            literal,
            case_insensitive: ci,
            max_matches: 0,
            content_max_bytes: 0,
            invert: false,
            engine: engine.into(),
        }
    }

    #[test]
    fn default_engine_is_regex() {
        let r = req("foo", "", false, false);
        assert_eq!(Engine::parse(&r.engine).unwrap(), Engine::Regex);
    }

    #[test]
    fn regex_engine_basic_match() {
        let c = compile(&req("ERROR.*timeout", "regex", false, false)).unwrap();
        let h = c.match_line("2026 ERROR tcp timeout happened").unwrap();
        assert_eq!(h.start, 5);
        assert_eq!(h.end, 22);
    }

    #[test]
    fn regex_engine_literal_escapes() {
        // "a.b" as literal must match "a.b" only, not "a+b"
        let c = compile(&req("a.b", "regex", true, false)).unwrap();
        assert!(c.match_line("a.b").is_some());
        assert!(c.match_line("a+b").is_none());
    }

    #[test]
    fn regex_engine_case_insensitive() {
        let c = compile(&req("error", "regex", false, true)).unwrap();
        assert!(c.match_line("This is an ERROR").is_some());
    }

    #[test]
    fn unknown_engine_is_bad_request() {
        let err = compile(&req("foo", "hopscotch", false, false)).unwrap_err();
        assert!(matches!(err, EngineError::CompileFailed(_)));
    }

    #[cfg(not(feature = "pcre2"))]
    #[test]
    fn pcre2_not_compiled_reports_clearly() {
        let err = compile(&req("foo", "pcre2", false, false)).unwrap_err();
        match err {
            EngineError::NotCompiledIn("pcre2") => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[cfg(feature = "pcre2")]
    #[test]
    fn pcre2_lookahead_works() {
        // Positive lookahead — unsupported by the regex crate.
        let c = compile(&req(r"foo(?=bar)", "pcre2", false, false)).unwrap();
        assert!(c.match_line("xyz foobar abc").is_some());
        assert!(c.match_line("foobaz").is_none());
    }

    #[cfg(feature = "pcre2")]
    #[test]
    fn pcre2_backref_works() {
        // Named backref — also unsupported by the regex crate.
        let c = compile(&req(r"(\w+) \1", "pcre2", false, false)).unwrap();
        assert!(c.match_line("hello hello").is_some());
        assert!(c.match_line("hello world").is_none());
    }

    #[cfg(feature = "hyperscan")]
    #[test]
    fn hyperscan_basic_match() {
        let c = compile(&req("timeout", "hyperscan", false, false)).unwrap();
        assert!(c.match_line("2026 ERROR timeout").is_some());
    }
}
