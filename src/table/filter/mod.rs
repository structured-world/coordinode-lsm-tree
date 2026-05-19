// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

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
    #[must_use]
    pub fn is_active(&self) -> bool {
        match self {
            Self::BitsPerKey(bpk) => *bpk > 0.0,
            Self::FalsePositiveRate(fpr) => *fpr > 0.0,
        }
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
    /// `BuRR`'s storage is essentially `r` bits per key (the fingerprint
    /// width) plus per-block threshold bytes (~1% overhead). For BPK
    /// policy, `r ≈ bpk`. For FPR policy, `r = ceil(-log2(fpr))`.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "estimation, precision loss is acceptable"
    )]
    pub fn estimated_filter_size(&self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }

        let r_bits: f32 = match self {
            Self::BitsPerKey(bpk) => bpk.clamp(1.0, 64.0),
            Self::FalsePositiveRate(fpr) => (-fpr.log2()).ceil().clamp(1.0, 64.0),
        };

        // ribbon body ≈ n * r bits; +5% layer overhead absorbs threshold
        // metadata and last-layer enlargement. Divide by 8 for bytes.
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
pub(crate) fn build_burr_filter_bytes(
    policy: BloomConstructionPolicy,
    hashes: &[u64],
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
    let filter = builder.build_from_hashes(hashes).map_err(|e| {
        log::error!("BuRR build_from_hashes failed: {e:?}");
        crate::Error::Unrecoverable
    })?;
    Ok(filter.to_wire_bytes())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, reason = "test code")]
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
        let bytes = build_burr_filter_bytes(policy, &[]).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn build_burr_filter_bytes_round_trips_via_reader() {
        use crate::table::filter::ribbon::burr::BurrFilterReader;
        let policy = BloomConstructionPolicy::FalsePositiveRate(0.01);
        let hashes: Vec<u64> = (0..1_000_u64)
            .map(|i| crate::hash::hash64(&i.to_le_bytes()))
            .collect();
        let bytes = build_burr_filter_bytes(policy, &hashes).unwrap();
        assert!(!bytes.is_empty());
        let reader = BurrFilterReader::new(&bytes).expect("reader");
        for h in &hashes {
            assert!(reader.contains_hash(*h), "inserted hash {h} not found");
        }
    }
}
