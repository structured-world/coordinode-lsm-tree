// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::Arc;

/// Trait for custom user key comparison.
///
/// Comparators must be safe across unwind boundaries since they are stored
/// in tree structures that may be referenced inside `catch_unwind` blocks.
///
/// Implementations define the sort order for user keys stored in the LSM-tree.
/// The comparator must be consistent: if `compare(a, b)` returns `Ordering::Less`,
/// then `compare(b, a)` must return `Ordering::Greater`, and vice versa.
///
/// # Important
///
/// Once a tree is created with a comparator, it must always be opened with the
/// same comparator. Using a different comparator on an existing tree will produce
/// incorrect results.
///
/// # Examples
///
/// ```
/// use lsm_tree::UserComparator;
/// use std::cmp::Ordering;
///
/// /// Comparator that orders u64 keys stored as big-endian bytes.
/// struct U64Comparator;
///
/// impl UserComparator for U64Comparator {
///     fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
///         let a = u64::from_be_bytes(a.try_into().unwrap_or([0; 8]));
///         let b = u64::from_be_bytes(b.try_into().unwrap_or([0; 8]));
///         a.cmp(&b)
///     }
/// }
/// ```
pub trait UserComparator: Send + Sync + std::panic::RefUnwindSafe + 'static {
    /// Compares two user keys, returning their ordering.
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;
}

/// Default comparator using lexicographic byte ordering.
///
/// This is the comparator used when no custom comparator is configured,
/// preserving backward compatibility with existing trees.
#[derive(Clone, Debug)]
pub struct DefaultUserComparator;

impl UserComparator for DefaultUserComparator {
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

/// Shared reference to a [`UserComparator`].
pub type SharedComparator = Arc<dyn UserComparator>;

/// Returns the default comparator (lexicographic byte ordering).
#[must_use]
pub(crate) fn default_comparator() -> SharedComparator {
    Arc::new(DefaultUserComparator)
}
