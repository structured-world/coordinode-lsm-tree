// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
use alloc::sync::Arc;

/// Trait for custom user key comparison.
///
/// Comparators must be safe across unwind boundaries since they are stored
/// in tree structures that may be referenced inside `catch_unwind` blocks.
///
/// Implementations must define a **strict total order** suitable for use in
/// sorted data structures (memtable skip list, SST block index, merge heap).
/// Specifically:
///
/// - **Totality**: for all `a`, `b`, exactly one of `Less`, `Equal`, `Greater` holds
/// - **Transitivity**: `a < b` and `b < c` implies `a < c`
/// - **Antisymmetry**: `compare(a, b) == Less` iff `compare(b, a) == Greater`
/// - **Reflexivity**: `compare(a, a) == Equal`
///
/// - **Bytewise equality**: `compare(a, b) == Equal` **must** imply `a == b`
///   byte-for-byte. Bloom filters and hash indexes operate on raw bytes;
///   if two byte-different keys compare as equal, hash-based lookups will
///   produce false negatives.
///
/// Violating these invariants corrupts the sort order and produces incorrect
/// query results.
///
/// # Important
///
/// Once a tree is created with a comparator, it must always be opened with the
/// same comparator. The comparator's [`name`](UserComparator::name) is
/// persisted and checked on every subsequent open — a mismatch causes the open
/// to fail with [`Error::ComparatorMismatch`](crate::Error::ComparatorMismatch).
///
/// # Examples
///
/// ```
/// use lsm_tree::UserComparator;
/// use core::cmp::Ordering;
///
/// /// Comparator that orders u64 keys stored as big-endian bytes.
/// struct U64Comparator;
///
/// impl UserComparator for U64Comparator {
///     fn name(&self) -> &'static str {
///         "u64-big-endian"
///     }
///
///     fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
///         if a.len() == 8 && b.len() == 8 {
///             // Length checked, conversion cannot fail.
///             let a_u64 = u64::from_be_bytes(a.try_into().unwrap());
///             let b_u64 = u64::from_be_bytes(b.try_into().unwrap());
///             a_u64.cmp(&b_u64)
///         } else {
///             // Non-8-byte keys: fall back to lexicographic ordering
///             // to preserve the bytewise-equality invariant.
///             a.cmp(b)
///         }
///     }
/// }
/// ```
pub trait UserComparator: Send + Sync + core::panic::RefUnwindSafe + 'static {
    /// Returns a stable identifier for this comparator.
    ///
    /// The name is persisted when a tree is first created. On subsequent
    /// opens the stored name is compared against the caller-supplied
    /// comparator's name — a mismatch causes the open to fail, preventing
    /// silent data corruption from using an incompatible ordering.
    ///
    /// Choose a name that uniquely identifies the ordering logic and will
    /// not change across releases (e.g. `"u64-big-endian"`, `"reverse-lexicographic"`).
    //
    // Intentionally required (no default impl): a shared fallback name would
    // let two distinct comparators pass the mismatch check, silently producing
    // corrupt reads on reopen — the exact scenario this method prevents.
    fn name(&self) -> &'static str;

    /// Compares two user keys, returning their ordering.
    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering;

    /// Returns `true` if this comparator is lexicographic byte ordering.
    ///
    /// When `true`, internal optimizations can avoid allocations in
    /// prefix-compressed block comparisons. Override only if your
    /// comparator is truly equivalent to `a.cmp(b)` on raw bytes.
    fn is_lexicographic(&self) -> bool {
        false
    }
}

/// Default comparator using lexicographic byte ordering.
///
/// This is the comparator used when no custom comparator is configured,
/// preserving backward compatibility with existing trees.
#[derive(Clone, Debug)]
pub struct DefaultUserComparator;

impl UserComparator for DefaultUserComparator {
    fn name(&self) -> &'static str {
        "default"
    }

    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        a.cmp(b)
    }

    #[inline]
    fn is_lexicographic(&self) -> bool {
        true
    }
}

/// Shared reference to a [`UserComparator`].
pub type SharedComparator = Arc<dyn UserComparator>;

/// Blanket impl so `Arc<T: UserComparator + ?Sized>` itself
/// satisfies the trait — used by callers that hold the
/// canonical [`SharedComparator`] (= `Arc<dyn UserComparator>`)
/// and want to pass it to generic APIs that take
/// `C: UserComparator + Clone`. The shared / dyn-dispatch cost
/// stays the same as before for those callers; the blanket only
/// removes a `.as_ref()` ceremony at every call site.
impl<T: UserComparator + ?Sized> UserComparator for Arc<T> {
    #[inline]
    fn name(&self) -> &'static str {
        (**self).name()
    }
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        (**self).compare(a, b)
    }
    #[inline]
    fn is_lexicographic(&self) -> bool {
        (**self).is_lexicographic()
    }
}

/// Maximum byte length for a comparator name.
///
/// Enforced on write (`persist_version`) and read (`Manifest::decode_from`).
pub const MAX_COMPARATOR_NAME_BYTES: usize = 256;

/// Returns the default comparator (lexicographic byte ordering).
///
/// Uses a shared static instance to avoid repeated allocations.
#[must_use]
pub fn default_comparator() -> SharedComparator {
    // OnceBox builds the Arc once; subsequent calls just clone it (ref-count
    // bump). `once_cell::race::OnceBox` is lock-free (atomic pointer) and
    // no_std + alloc, so this path is identical under std and no_std.
    static DEFAULT: once_cell::race::OnceBox<SharedComparator> = once_cell::race::OnceBox::new();
    DEFAULT
        .get_or_init(|| Box::new(Arc::new(DefaultUserComparator)))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_comparator_name() {
        assert_eq!(DefaultUserComparator.name(), "default");
        assert_eq!(default_comparator().name(), "default");
    }

    #[test]
    fn default_comparator_is_lexicographic() {
        assert!(DefaultUserComparator.is_lexicographic());
    }
}
