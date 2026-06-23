// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

/// Extracts prefixes from keys for prefix bloom filter indexing.
///
/// When a `PrefixExtractor` is configured on a tree, the bloom filter indexes
/// not only full keys but also the prefixes returned by [`PrefixExtractor::prefixes`].
/// This allows prefix scans to skip entire segments that contain no keys with a
/// matching prefix, dramatically reducing I/O for prefix-heavy workloads (e.g.,
/// graph adjacency lists, time-series buckets).
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync + UnwindSafe + RefUnwindSafe`.
/// The extractor is shared across flush, compaction, and read threads via `Arc`,
/// and may be accessed across panic boundaries (e.g., `catch_unwind` in tests).
///
/// # Example
///
/// ```
/// use lsm_tree::PrefixExtractor;
///
/// /// Extracts prefixes at each ':' separator boundary.
/// ///
/// /// For key `adj:out:42:KNOWS`, yields:
/// ///   `adj:`, `adj:out:`, `adj:out:42:`
/// struct ColonSeparatedPrefix;
///
/// impl PrefixExtractor for ColonSeparatedPrefix {
///     fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         Box::new(
///             key.iter()
///                 .enumerate()
///                 .filter(|(_, b)| **b == b':')
///                 .map(move |(i, _)| &key[..=i]),
///         )
///     }
/// }
/// ```
pub trait PrefixExtractor:
    Send + Sync + core::panic::UnwindSafe + core::panic::RefUnwindSafe
{
    /// Returns an iterator of prefixes to index for the given key.
    ///
    /// Each yielded prefix will be hashed and inserted into the segment's
    /// bloom filter. During a prefix scan, the scan prefix is hashed and
    /// checked against the bloom — segments without a match are skipped.
    ///
    /// Implementations should return prefixes from shortest to longest.
    /// The full key itself is always indexed separately by the standard bloom
    /// path; including it in the returned prefixes is allowed but redundant
    /// and generally unnecessary.
    ///
    /// The returned iterator must be finite and yield a small number of
    /// prefixes per key (typically 1–5). It is called on the write path
    /// for every key during flush and compaction.
    ///
    /// # Performance note
    ///
    /// Returns `Box<dyn Iterator>` for object safety (`Arc<dyn PrefixExtractor>`).
    /// Most extractors yield 1–5 prefixes per key, so the allocation is negligible
    /// compared to the bloom hash + I/O cost. A callback-based `for_each_prefix`
    /// alternative could avoid this allocation but would expand the trait API
    /// surface; consider adding it if profiling shows measurable overhead.
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a>;

    // NOTE: Renamed from `is_valid_prefix_boundary` (added in PR #43, never
    // released). No deprecated shim needed — no downstream consumers exist.

    /// Returns `true` if `prefix` is a valid scan boundary for this extractor.
    ///
    /// A scan boundary is valid when **every key** that the tree would consider
    /// a match for this prefix in a prefix scan had `prefix` indexed via
    /// [`prefixes`](Self::prefixes) at write time. This is the contract that
    /// makes bloom-based table skipping safe: if the bloom filter says "no
    /// match", we can skip the table because every matching key would have
    /// produced the prefix hash during flush/compaction.
    ///
    /// # Default implementation
    ///
    /// Checks whether `prefixes(prefix)` emits `prefix` itself — i.e.,
    /// whether the extractor considers this byte sequence a boundary.
    /// This is correct for well-behaved extractors whose `prefixes()` returns
    /// sub-slices of the input key.
    ///
    /// # When to override
    ///
    /// Override this method when the default self-referential check is either:
    /// - **Too expensive** — e.g., the extractor can check a sentinel byte in
    ///   O(1) instead of iterating all prefixes.
    /// - **Incorrect** — e.g., the extractor produces prefixes that are *not*
    ///   sub-slices of the input, so the default `any(|p| p == prefix)` check
    ///   would never match even for valid boundaries.
    fn is_valid_scan_boundary(&self, prefix: &[u8]) -> bool {
        !prefix.is_empty() && self.prefixes(prefix).any(|p| p == prefix)
    }
}

/// Computes the prefix hash for bloom-filter-based table skipping.
///
/// Returns `Some(hash)` only when the scan prefix is non-empty and is a valid
/// boundary for the configured extractor. Returns `None` otherwise (no bloom
/// skip will be attempted).
///
/// Used by both `Tree::create_prefix` and `BlobTree::prefix` to avoid
/// duplicating the boundary-check + hashing logic.
pub fn compute_prefix_hash(
    extractor: Option<&alloc::sync::Arc<dyn PrefixExtractor>>,
    prefix_bytes: &[u8],
) -> Option<u64> {
    if prefix_bytes.is_empty() {
        return None;
    }

    extractor
        .filter(|e| e.is_valid_scan_boundary(prefix_bytes))
        .map(|_| crate::hash::hash64(prefix_bytes))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests;
