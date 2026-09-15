// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The hold a write keeps on the policy it resolved its dictionaries from.

use super::RuntimeConfig;
use alloc::sync::Arc;

/// Keeps the dictionaries a write's files name out of a dictionary collection
/// until those files are installed.
///
/// A write resolves its dictionary from the policy snapshot it started under,
/// and a collection spares every dictionary a still-held snapshot names, however
/// the policy has changed since. The pin is that hold, owned by the operation
/// rather than its writers, since finishing a writer comes before the install:
/// drop it only once the files it covers are part of the tree, from which point
/// the files name the dictionary themselves. A flush and an ingestion carry it
/// from where their files are finished to where they are installed; a
/// compaction, which does both in one call, holds its snapshot directly. Always
/// empty in builds without zstd, which have no dictionaries to collect.
#[derive(Default)]
#[must_use = "dropping the pin before the files are installed lets a collection take their dictionary"]
pub struct WritePin {
    snapshot: Option<Arc<RuntimeConfig>>,
}

impl WritePin {
    /// A pin on the snapshot a write resolves its dictionaries from. Empty when
    /// the snapshot names no dictionary: its files name none either, and an
    /// update never records such a snapshot for a collection to consult.
    #[cfg(zstd_any)]
    pub(crate) fn new(snapshot: &Arc<RuntimeConfig>) -> Self {
        Self {
            snapshot: snapshot
                .write_dict_ids(true)
                .next()
                .map(|_| Arc::clone(snapshot)),
        }
    }

    /// A pin on the snapshot a write resolves its dictionaries from: empty, as
    /// a build without zstd resolves none.
    #[cfg(not(zstd_any))]
    pub(crate) fn new(_snapshot: &Arc<RuntimeConfig>) -> Self {
        Self::default()
    }
}

impl core::fmt::Debug for WritePin {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WritePin")
            .field("holds", &self.snapshot.is_some())
            .finish()
    }
}
