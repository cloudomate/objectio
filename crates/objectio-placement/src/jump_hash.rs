//! Jump consistent hash — Lamping & Veach (Google, 2014).
//!
//! Maps a 64-bit key into `[0, num_buckets)` with the property that
//! growing `num_buckets` by one remaps ≈ `old/new` of keys and nothing
//! else. No ring structure to maintain; just a tight loop of integer
//! arithmetic. Cost is O(log N) multiplications per call — ≈ 100 ns
//! at our typical N, well under a microsecond.
//!
//! Reference: "A Fast, Minimal Memory, Consistent Hash Algorithm",
//! arXiv:1406.2294.
//!
//! Used in ObjectIO's placement engine to route `object_id → pg_id`.
//! The PG → OSD hop is an explicit Meta lookup, not another hash.

/// Pick a bucket in `[0, num_buckets)` for the given 64-bit key.
///
/// Returns 0 if `num_buckets <= 0` — callers must pre-validate that
/// `num_buckets >= 1`. We don't panic on the zero case because
/// placement is on the hot path and returning a sentinel avoids a
/// branch per call for callers that already enforce the invariant.
#[must_use]
pub fn jump_consistent_hash(key: u64, num_buckets: i32) -> i32 {
    if num_buckets <= 0 {
        return 0;
    }
    // Signed arithmetic is the reference implementation from the
    // paper — matches the reference C and Go code exactly. The
    // wrapping operations are deliberate (paper uses unsigned mul,
    // i64 wrapping is the clean Rust rendering).
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    let mut k: i64 = key as i64;
    while j < num_buckets as i64 {
        b = j;
        // Linear congruential step used by the paper.
        k = k.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        // Compute next jump: j = (b + 1) * (2^31 / ((k >> 33) + 1))
        // Re-derived using the double-precision form for clarity.
        let shifted = ((k >> 33) & 0x7fff_ffff) + 1;
        let ratio = (1_i64 << 31) as f64 / shifted as f64;
        j = ((b + 1) as f64 * ratio) as i64;
    }
    b as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_buckets_returns_zero() {
        assert_eq!(jump_consistent_hash(0x12345, 0), 0);
        assert_eq!(jump_consistent_hash(0x12345, -1), 0);
    }

    #[test]
    fn single_bucket_always_returns_zero() {
        for key in [0_u64, 1, 42, 1 << 63, u64::MAX] {
            assert_eq!(jump_consistent_hash(key, 1), 0);
        }
    }

    #[test]
    fn distribution_is_roughly_uniform() {
        // 100_000 keys across 64 buckets. Each bucket should see
        // avg ≈ 1562. ±30% is a generous bound that still catches a
        // regression where the implementation collapses onto a
        // handful of buckets.
        let mut counts = [0usize; 64];
        for i in 0..100_000_u64 {
            // Spread inputs so sequential keys don't all land together.
            let k = i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let b = jump_consistent_hash(k, 64) as usize;
            counts[b] += 1;
        }
        let avg = 100_000.0 / 64.0;
        let low = (avg * 0.7) as usize;
        let high = (avg * 1.3) as usize;
        for (i, &c) in counts.iter().enumerate() {
            assert!(
                c >= low && c <= high,
                "bucket {i}: {c} outside [{low}, {high}]"
            );
        }
    }

    #[test]
    fn growth_minimally_remaps() {
        // Minimal-remap property from the paper: growing num_buckets
        // from N to N+1 should move ≈ 1 / (N+1) fraction of keys, and
        // every moving key must move INTO the new bucket N.
        let n = 32_i32;
        let mut moved = 0;
        let mut non_final_target = 0;
        for i in 0..50_000_u64 {
            let k = i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let before = jump_consistent_hash(k, n);
            let after = jump_consistent_hash(k, n + 1);
            if before != after {
                moved += 1;
                if after != n {
                    non_final_target += 1;
                }
            }
        }
        assert_eq!(
            non_final_target, 0,
            "keys that moved must land on the new bucket only"
        );
        let expected = 50_000.0 / (n as f64 + 1.0);
        let tolerance = expected * 0.2;
        assert!(
            (moved as f64 - expected).abs() < tolerance,
            "moved={moved}, expected ≈ {expected}"
        );
    }

    #[test]
    fn deterministic() {
        // Same input → same bucket. Sanity check against accidentally
        // introducing randomness (e.g. SipHash with a random seed).
        for key in [1_u64, 1 << 20, 0xdead_beef_cafe_babe_u64, u64::MAX] {
            let a = jump_consistent_hash(key, 1024);
            let b = jump_consistent_hash(key, 1024);
            assert_eq!(a, b);
        }
    }
}
