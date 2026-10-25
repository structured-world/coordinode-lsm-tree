// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Caller-defined key domains that table outputs never span.

use crate::UserKey;

/// Splits the key space into domains that no new table spans.
///
/// A domain is a contiguous interval in the tree's comparator order. Flush,
/// ingest and compaction end an output before the first key of the next
/// domain, even below its target size.
///
/// A domain is an application's unit of lifetime, such as one retention
/// epoch of time-bucketed keys. With the keys of each domain in tables of
/// their own, expiring a domain removes whole tables instead of rewriting the
/// survivors that would otherwise share them.
///
/// Whether a table holds one domain follows from its key range alone, since
/// a domain is an interval: no marker is written, and a table written before
/// the policy, or under another one, is judged by the same test.
///
/// # Example
///
/// ```
/// use lsm_tree::{KeyDomains, UserKey};
///
/// /// One domain per value of the first byte.
/// struct FirstByte;
///
/// impl KeyDomains for FirstByte {
///     fn domain_end(&self, key: &[u8]) -> Option<UserKey> {
///         match key.first() {
///             Some(&b) if b < u8::MAX => Some(UserKey::from([b + 1].as_slice())),
///             _ => None,
///         }
///     }
/// }
///
/// assert_eq!(FirstByte.domain_end(b"a1").as_deref(), Some(b"b".as_slice()));
/// assert_eq!(FirstByte.domain_end(b"\xff"), None);
/// ```
pub trait KeyDomains: Send + Sync + core::panic::UnwindSafe + core::panic::RefUnwindSafe {
    /// The first key past the domain that holds `key`, or `None` when that
    /// domain runs to the end of the key space.
    ///
    /// Every key from `key` up to the returned one (exclusive), in the tree's
    /// comparator order, lies in `key`'s domain, and no key at or past it
    /// does. The answer for any key of one domain is the same.
    fn domain_end(&self, key: &[u8]) -> Option<UserKey>;
}

/// Where the domain of the output a writer is filling ends.
enum DomainEnd {
    /// The output has no key yet.
    Unknown,
    /// The domain runs to the end of the key space.
    Open,
    /// The first key of the next domain.
    At(UserKey),
}

/// The domain of the output a writer is filling, so it ends that output
/// before a key of the next one.
pub struct DomainCursor {
    domains: alloc::sync::Arc<dyn KeyDomains>,
    end: DomainEnd,
}

impl DomainCursor {
    pub fn new(domains: alloc::sync::Arc<dyn KeyDomains>) -> Self {
        Self {
            domains,
            end: DomainEnd::Unknown,
        }
    }

    /// Whether `key`, the next new key, lies past the current output's
    /// domain, without moving into it.
    pub fn crosses(&self, key: &[u8], comparator: &dyn crate::comparator::UserComparator) -> bool {
        matches!(&self.end, DomainEnd::At(end)
            if comparator.compare(key, end) != core::cmp::Ordering::Less)
    }

    /// Whether `last`, a key past `first` taken in the same output, still lies
    /// in `first`'s domain.
    #[cfg(feature = "columnar")]
    pub fn holds(
        &self,
        first: &[u8],
        last: &[u8],
        comparator: &dyn crate::comparator::UserComparator,
    ) -> bool {
        self.domains
            .domain_end(first)
            .is_none_or(|end| comparator.compare(last, &end) == core::cmp::Ordering::Less)
    }

    /// Takes `key` into the current output: its domain becomes the output's
    /// when the output starts there or `key` crossed into another one.
    pub fn enter(&mut self, key: &[u8], crossed: bool) {
        if crossed || matches!(self.end, DomainEnd::Unknown) {
            self.end = self
                .domains
                .domain_end(key)
                .map_or(DomainEnd::Open, DomainEnd::At);
        }
    }
}

#[cfg(test)]
mod tests;
