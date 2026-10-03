// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Which blob objects stopped being owned since a reference to them was read.
//!
//! A reference handed out by a reference-aware read names an object its key
//! held in the version the read saw. A later version can let the object go:
//! another write replaces the key, a compaction drops the owner and charges
//! the object as garbage. Writing the old reference back after that would
//! make a row hold an object the accounting already counts as garbage. So
//! every install that releases objects records them here with the version it
//! publishes, and a write refuses a reference whose object was released in a
//! version newer than the one it was read from. The check is in memory: no
//! write reads a table to decide it.
//!
//! The records only matter to references still held, so each read that hands
//! references out registers the version it read for as long as it holds
//! them, and records older than every registered read are dropped. A read
//! registers under the history's read lock while it takes its version, and an
//! install records under the write lock while it publishes its own, so no
//! read can miss a release newer than the version it took.

use alloc::collections::BTreeMap;
use alloc::sync::Arc;

use crate::HashMap;
use crate::vlog::{BlobFileId, ValueHandle};
#[cfg(feature = "std")]
use parking_lot::Mutex;
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// Version ids, as [`Version::id`](crate::version::Version::id) numbers them.
type VersionId = u64;

#[derive(Default)]
struct Records {
    /// The versions held reads took, with how many reads hold each.
    readers: BTreeMap<VersionId, usize>,
    /// Objects released, each with the newest version that released it: a
    /// reference is stale when any release is newer than its read, which is
    /// so exactly when the newest one is, so one lookup answers it.
    objects: HashMap<ValueHandle, VersionId>,
    /// Blob files a whole-table drop charged, each with the newest version
    /// that dropped such a table: the drop knows the files whose objects it
    /// charged, not which objects they were, so a reference into such a file
    /// read before the drop is stale.
    files: HashMap<BlobFileId, VersionId>,
}

impl Records {
    /// Drops the records no held read can be older than.
    fn prune(&mut self) {
        let Some(&oldest) = self.readers.keys().next() else {
            self.objects.clear();
            self.files.clear();
            return;
        };
        self.objects.retain(|_, at| *at > oldest);
        self.files.retain(|_, at| *at > oldest);
    }
}

/// Records `version` as `key`'s release, keeping the newest one.
fn note<K: core::hash::Hash + Eq>(map: &mut HashMap<K, VersionId>, key: K, version: VersionId) {
    let at = map.entry(key).or_insert(version);
    *at = (*at).max(version);
}

/// The releases of one tree, shared by its reads and its installs.
#[derive(Default)]
pub struct ReleasedObjects {
    records: Mutex<Records>,
}

impl ReleasedObjects {
    /// Registers a read of `version`; the read holds the returned token for
    /// as long as it holds the references it hands out.
    pub fn register(self: &Arc<Self>, version: VersionId) -> ReaderToken {
        *self.records.lock().readers.entry(version).or_insert(0) += 1;
        ReaderToken {
            registry: Arc::clone(self),
            version,
        }
    }

    /// Records the objects and files an install publishing `version`
    /// released. Kept only while a held read is older than `version`.
    pub fn record(
        &self,
        version: VersionId,
        objects: impl IntoIterator<Item = ValueHandle>,
        files: impl IntoIterator<Item = BlobFileId>,
    ) {
        let mut records = self.records.lock();
        if records
            .readers
            .keys()
            .next()
            .is_none_or(|&oldest| oldest >= version)
        {
            return;
        }
        for handle in objects {
            note(&mut records.objects, handle, version);
        }
        for file in files {
            note(&mut records.files, file, version);
        }
    }

    /// Whether the object `handle` names was released in a version newer
    /// than `read`, the version a reference to it was read from.
    pub fn released_since(&self, read: VersionId, handle: &ValueHandle) -> bool {
        let records = self.records.lock();
        records.objects.get(handle).is_some_and(|&at| at > read)
            || records
                .files
                .get(&handle.blob_file_id)
                .is_some_and(|&at| at > read)
    }
}

/// A held read's registration; dropping it lets the records it kept go.
pub struct ReaderToken {
    registry: Arc<ReleasedObjects>,
    version: VersionId,
}

impl ReaderToken {
    /// The version the read took.
    #[must_use]
    pub fn version(&self) -> VersionId {
        self.version
    }
}

impl Drop for ReaderToken {
    fn drop(&mut self) {
        let mut records = self.registry.records.lock();
        if let Some(count) = records.readers.get_mut(&self.version) {
            *count -= 1;
            if *count == 0 {
                records.readers.remove(&self.version);
            }
        }
        records.prune();
    }
}

#[cfg(test)]
mod tests;
