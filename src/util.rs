// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::UserKey;
use crate::range::prefix_upper_range;
use core::ops::RangeBounds;

pub use crate::range::prefix_to_range;

/// Helper function to create a prefixed range.
///
/// Made for phil.
///
/// # Panics
///
/// Panics if the prefix is empty.
pub fn prefixed_range<P: AsRef<[u8]>, K: AsRef<[u8]>, R: RangeBounds<K>>(
    prefix: P,
    range: R,
) -> impl RangeBounds<UserKey> {
    use core::ops::Bound::{Excluded, Included, Unbounded};

    let prefix = prefix.as_ref();

    if prefix.is_empty() {
        return (Unbounded, Unbounded);
    }

    match (range.start_bound(), range.end_bound()) {
        (Unbounded, Unbounded) => prefix_to_range(prefix),
        (lower, Unbounded) => {
            let lower = lower.map(|k| UserKey::fused(prefix, k.as_ref()));
            let upper = prefix_upper_range(prefix);
            (lower, upper)
        }
        (Unbounded, upper) => {
            let upper = match upper {
                Included(k) => Included(UserKey::fused(prefix, k.as_ref())),
                Excluded(k) => Excluded(UserKey::fused(prefix, k.as_ref())),
                Unbounded => unreachable!(),
            };

            (Included(prefix.into()), upper)
        }
        (lower, upper) => {
            let lower = match lower {
                Included(k) => Included(UserKey::fused(prefix, k.as_ref())),
                Excluded(k) => Excluded(UserKey::fused(prefix, k.as_ref())),
                Unbounded => unreachable!(),
            };

            let upper = match upper {
                Included(k) => Included(UserKey::fused(prefix, k.as_ref())),
                Excluded(k) => Excluded(UserKey::fused(prefix, k.as_ref())),
                Unbounded => unreachable!(),
            };

            (lower, upper)
        }
    }
}

#[cfg(test)]
mod tests;
