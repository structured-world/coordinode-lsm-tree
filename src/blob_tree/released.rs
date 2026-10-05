// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Which blob objects stopped being owned since a reference to them was read.
//!
//! A reference handed out by a reference-aware read names an object its key
//! held in the version the read saw. A later version can let the object go:
//! another write replaces the key, a compaction drops the owner and charges
//! the object as garbage, a whole-table drop takes the table that owned it. Writing the old reference back after that would
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

/// The objects one dropped table owned, as `(blob file, offset)` sorted
/// ascending: its `owned_blob_objects` section, kept as read.
pub type OwnedObjects = Arc<[(BlobFileId, u64)]>;

#[derive(Default)]
struct Records {
    /// The versions held reads took, with how many reads hold each.
    readers: BTreeMap<VersionId, usize>,
    /// Objects released, each with the newest version that released it: a
    /// reference is stale when any release is newer than its read, which is
    /// so exactly when the newest one is, so one lookup answers it.
    objects: HashMap<ValueHandle, VersionId>,
    /// The objects of tables a whole-table drop took, each list with the
    /// version that dropped it. Kept as the sorted lists the tables carry, not
    /// spread into `objects`: a large drop then holds no more memory than its
    /// sections, and a lookup is a binary search per list.
    tables: Vec<(VersionId, OwnedObjects)>,
}

impl Records {
    /// Drops the records no held read can be older than.
    fn prune(&mut self) {
        let Some(&oldest) = self.readers.keys().next() else {
            self.objects.clear();
            self.tables.clear();
            return;
        };
        self.objects.retain(|_, at| *at > oldest);
        self.tables.retain(|(at, _)| *at > oldest);
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

    /// Whether any read holds references. An install asks under the history's
    /// write lock, where no read can register, so every read held then is
    /// older than the version it publishes; with none, nothing it releases
    /// needs recording and it reads nothing to learn what that is.
    pub fn holds_reads(&self) -> bool {
        !self.records.lock().readers.is_empty()
    }

    /// Records what an install publishing `version` released: single
    /// `objects`, and the owned-object lists of the `tables` it dropped
    /// whole. Kept only while a held read is older than `version`.
    pub fn record(
        &self,
        version: VersionId,
        objects: impl IntoIterator<Item = ValueHandle>,
        tables: impl IntoIterator<Item = OwnedObjects>,
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
        for owned in tables {
            if !owned.is_empty() {
                records.tables.push((version, owned));
            }
        }
    }

    /// Whether the object `handle` names was released in a version newer
    /// than `read`, the version a reference to it was read from.
    pub fn released_since(&self, read: VersionId, handle: &ValueHandle) -> bool {
        let records = self.records.lock();
        let object = (handle.blob_file_id, handle.offset);
        records.objects.get(handle).is_some_and(|&at| at > read)
            || records
                .tables
                .iter()
                .any(|(at, owned)| *at > read && owned.binary_search(&object).is_ok())
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
