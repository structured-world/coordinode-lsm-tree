// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The hold a write keeps on the policy it resolved its dictionaries from.

use super::RuntimeConfig;
use alloc::{sync::Arc, vec::Vec};

/// Keeps the dictionaries a write's files name out of a dictionary collection
/// until those files are installed.
///
/// A write resolves its dictionary from the policy snapshot it started under,
/// and a collection spares every dictionary a still-held snapshot names, however
/// the policy has changed since. The pin is that hold: drop it only once the
/// files it covers are part of the tree, since from then on the files name the
/// dictionary themselves. Always empty in builds without zstd, which have no
/// dictionaries to collect.
#[derive(Default)]
#[must_use = "dropping the pin before the files are installed lets a collection take their dictionary"]
pub struct WritePin {
    snapshots: Vec<Arc<RuntimeConfig>>,
}

impl WritePin {
    /// A pin on the snapshot a writer resolved its dictionary from. Empty when
    /// the snapshot names no dictionary: its files name none either, and an
    /// update never records such a snapshot for a collection to consult.
    #[cfg(zstd_any)]
    pub(crate) fn new(snapshot: Arc<RuntimeConfig>) -> Self {
        if snapshot.write_dict_ids(true).next().is_none() {
            return Self::default();
        }
        Self {
            snapshots: alloc::vec![snapshot],
        }
    }

    /// Adds what `other` holds to this pin, for an install whose files came
    /// from more than one writer.
    pub(crate) fn join(&mut self, other: Self) {
        self.snapshots.extend(other.snapshots);
    }

    /// Whether this pin holds nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}

impl core::fmt::Debug for WritePin {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WritePin")
            .field("snapshots", &self.snapshots.len())
            .finish()
    }
}
