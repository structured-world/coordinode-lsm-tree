// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod block;
pub mod ribbon;

use ribbon::burr::{BurrBuilder, BurrParams};
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

/// Hasher type embedded in `BuRR` filters. Only its type identity is used —
/// the construction + probe paths in this crate go through
/// `BurrBuilder::build_from_hashes` / `BurrFilter::contains_hash` /
/// `BurrFilterReader::contains_hash`, all of which take pre-computed u64
/// hashes (xxh3 via `crate::hash::hash64`) and never invoke the
/// `BuildHasher`. The type slot exists only to satisfy the vendored
/// ribbon-filter's generic `S: BuildHasher` bound.
type FilterHasher = BuildHasherDefault<DefaultHasher>;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BloomConstructionPolicy {
    BitsPerKey(f32),
    FalsePositiveRate(f32),
}

impl Default for BloomConstructionPolicy {
    fn default() -> Self {
        Self::BitsPerKey(10.0)
    }
}

impl BloomConstructionPolicy {
    /// Returns `true` if this policy can produce a valid filter
    /// (`burr_params` would return `Some` for any non-zero `n`). False
    /// means the writer should skip filter construction entirely
    /// instead of buffering hashes that will later be dropped.
    #[must_use]
    pub fn is_active(&self) -> bool {
        // Delegate to `burr_params` so this method is exact-equivalent
        // to "would this policy produce a non-empty filter for n=1?".
        // Anything stricter (e.g. fpr too small → r > 64) is captured
        // by the params constructor's own validation.
        self.burr_params(1).is_some()
    }

    /// Build `BurrParams` for the given key count under this policy.
    ///
    /// Returns `None` if `n == 0` or the policy translates to an invalid
    /// `BurrParams` (e.g. `bpk > 64` or `fpr` outside `(0,1)`). Callers
    /// should treat `None` as "skip filter construction for this block".
    pub(crate) fn burr_params(self, n: usize) -> Option<BurrParams> {
        if n == 0 {
            return None;
        }
        match self {
            Self::BitsPerKey(bpk) => BurrParams::with_bpk(n, bpk).ok(),
            Self::FalsePositiveRate(fpr) => BurrParams::with_fp_rate(n, fpr).ok(),
        }
    }

    /// Returns the estimated filter size in bytes.
    ///
    /// Returns `0` if the policy is inactive for the given `n`
    /// (`burr_params` would return `None`). Otherwise estimates the
    /// `BuRR` body size as `n * r * 1.05 / 8` — `r` is the fingerprint
    /// width chosen by the params constructor, `1.05` is a flat 5%
    /// overhead for layer thresholds + last-layer enlargement.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "estimation, precision loss is acceptable"
    )]
    pub fn estimated_filter_size(&self, n: usize) -> usize {
        // Delegate to burr_params so the estimate is 0 exactly when the
        // builder would also return empty — keeps memory accounting in
        // sync with build behavior.
        let Some(params) = self.burr_params(n) else {
            return 0;
        };
        let r_bits = f32::from(params.r);
        ((n as f32) * r_bits * 1.05 / 8.0) as usize
    }
}

/// Build a `BuRR` filter block payload from pre-hashed keys under the given
/// policy. Returns the serialized wire bytes the
/// [`block::FilterBlock`] reader can parse.
///
/// Returns an empty `Vec` if `hashes` is empty or the policy parameters
/// are invalid for `n = hashes.len()` — callers should treat that as
/// "no filter for this block".
///
/// Consumes `hashes` so the writer's accumulated `bloom_hash_buffer` can
/// be `mem::take`n straight in without a `to_vec()` copy at the boundary.
pub(crate) fn build_burr_filter_bytes(
    policy: BloomConstructionPolicy,
    hashes: Vec<u64>,
) -> crate::Result<Vec<u8>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    let Some(params) = policy.burr_params(hashes.len()) else {
        return Ok(Vec::new());
    };
    let build_hasher = FilterHasher::default();
    let builder = BurrBuilder::new(params, build_hasher).map_err(|e| {
        log::error!("BuRR builder init failed: {e:?}");
        crate::Error::Unrecoverable
    })?;
    let filter = builder.build_from_hashes_owned(hashes).map_err(|e| {
        log::error!("BuRR build_from_hashes failed: {e:?}");
        crate::Error::Unrecoverable
    })?;
    Ok(filter.to_wire_bytes())
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn burr_estimated_size_bpk() {
        let policy = BloomConstructionPolicy::BitsPerKey(10.0);
        let n = 1_000_000;
        let estimated_size = policy.estimated_filter_size(n);
        // 10 bits/key × 1M keys × 1.05 overhead / 8 ≈ 1.31 MB
        assert!(estimated_size > 1_200_000);
        assert!(estimated_size < 1_400_000);
    }

    #[test]
    fn burr_estimated_size_fpr() {
        let policy = BloomConstructionPolicy::FalsePositiveRate(0.01);
        let n = 1_000_000;
        let estimated_size = policy.estimated_filter_size(n);
        // ceil(-log2(0.01)) = 7 bits/key → 7M bits × 1.05 / 8 ≈ 918 KB
        assert!(estimated_size > 800_000);
        assert!(estimated_size < 1_000_000);
    }

    #[test]
    fn build_burr_filter_bytes_empty_returns_empty() {
        let policy = BloomConstructionPolicy::BitsPerKey(10.0);
        let bytes = build_burr_filter_bytes(policy, Vec::new()).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn build_burr_filter_bytes_round_trips_via_reader() {
        use crate::table::filter::ribbon::burr::BurrFilterReader;
        let policy = BloomConstructionPolicy::FalsePositiveRate(0.01);
        let hashes: Vec<u64> = (0..1_000_u64)
            .map(|i| crate::hash::hash64(&i.to_le_bytes()))
            .collect();
        let bytes = build_burr_filter_bytes(policy, hashes.clone()).unwrap();
        assert!(!bytes.is_empty());
        let reader = BurrFilterReader::new(&bytes).expect("reader");
        for h in &hashes {
            assert!(reader.contains_hash(*h), "inserted hash {h} not found");
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
#[expect(clippy::unwrap_used, reason = "test code")]
mod extra_tests {
    use super::*;
    use test_log::test;

    #[test]
    fn policy_default_is_bits_per_key_10() {
        let policy = BloomConstructionPolicy::default();
        assert_eq!(policy, BloomConstructionPolicy::BitsPerKey(10.0));
    }

    #[test]
    fn is_active_false_for_bpk_below_one() {
        assert!(!BloomConstructionPolicy::BitsPerKey(0.5).is_active());
        assert!(!BloomConstructionPolicy::BitsPerKey(0.0).is_active());
    }

    #[test]
    fn is_active_false_for_bpk_above_64() {
        assert!(!BloomConstructionPolicy::BitsPerKey(70.0).is_active());
    }

    #[test]
    fn is_active_true_for_valid_bpk() {
        assert!(BloomConstructionPolicy::BitsPerKey(10.0).is_active());
        assert!(BloomConstructionPolicy::BitsPerKey(1.0).is_active());
        assert!(BloomConstructionPolicy::BitsPerKey(64.0).is_active());
    }

    #[test]
    fn is_active_false_for_fpr_out_of_range() {
        assert!(!BloomConstructionPolicy::FalsePositiveRate(0.0).is_active());
        assert!(!BloomConstructionPolicy::FalsePositiveRate(-0.1).is_active());
        assert!(!BloomConstructionPolicy::FalsePositiveRate(1.0).is_active());
        assert!(!BloomConstructionPolicy::FalsePositiveRate(1.5).is_active());
        // Too tight — would map to r > 64.
        assert!(!BloomConstructionPolicy::FalsePositiveRate(1.0e-25_f32).is_active());
    }

    #[test]
    fn is_active_true_for_valid_fpr() {
        assert!(BloomConstructionPolicy::FalsePositiveRate(0.01).is_active());
        assert!(BloomConstructionPolicy::FalsePositiveRate(0.0001).is_active());
        assert!(BloomConstructionPolicy::FalsePositiveRate(0.5).is_active());
    }

    #[test]
    fn estimated_size_zero_n_returns_zero() {
        let policy = BloomConstructionPolicy::BitsPerKey(10.0);
        assert_eq!(policy.estimated_filter_size(0), 0);
        let policy_fpr = BloomConstructionPolicy::FalsePositiveRate(0.01);
        assert_eq!(policy_fpr.estimated_filter_size(0), 0);
    }

    #[test]
    fn burr_params_returns_none_for_n_zero() {
        let policy = BloomConstructionPolicy::BitsPerKey(10.0);
        assert!(policy.burr_params(0).is_none());
    }

    #[test]
    fn burr_params_returns_some_for_valid_inputs() {
        let policy = BloomConstructionPolicy::BitsPerKey(10.0);
        let params = policy.burr_params(100).expect("valid");
        assert_eq!(params.n, 100);
        assert_eq!(params.r, 10);
    }

    #[test]
    fn burr_params_fpr_variant() {
        let policy = BloomConstructionPolicy::FalsePositiveRate(0.01);
        let params = policy.burr_params(100).expect("valid");
        assert_eq!(params.n, 100);
        // r = ceil(-log2(0.01)) = 7
        assert_eq!(params.r, 7);
    }

    #[test]
    fn build_burr_filter_bytes_invalid_policy_returns_empty() {
        // Policy too tight → burr_params returns None → empty bytes.
        let policy = BloomConstructionPolicy::FalsePositiveRate(1.0e-25_f32);
        let hashes: Vec<u64> = (0..10)
            .map(|i: u64| crate::hash::hash64(&i.to_le_bytes()))
            .collect();
        let bytes = build_burr_filter_bytes(policy, hashes).unwrap();
        assert!(bytes.is_empty());
    }
}
