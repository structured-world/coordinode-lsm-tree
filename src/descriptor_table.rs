// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::sharded_cache::{ShardedCache, UnitWeighter};
use alloc::sync::Arc;

use crate::{GlobalTableId, fs::FsFile};

const TAG_BLOCK: u8 = 0;
const TAG_BLOB: u8 = 1;

type Item = Arc<dyn FsFile>;

#[derive(Clone, Copy, Eq, core::hash::Hash, PartialEq)]
struct CacheKey(u8, u64, u64);

/// Number of shards in the FD cache. Smaller than the block cache: the FD cache
/// holds at most a few thousand descriptors, so a large shard array would leave
/// most shards empty.
const FD_CACHE_SHARDS: usize = 16;

/// Caches file descriptors to tables and blob files
pub struct DescriptorTable {
    inner: ShardedCache<CacheKey, Item, UnitWeighter, rustc_hash::FxBuildHasher>,
    /// Advanced after every table descriptor removed: a scan reading through
    /// a descriptor it resolved under an earlier epoch takes it afresh, so a
    /// descriptor removed because its path now names another inode (an
    /// in-place heal's detached copy) is not read through again.
    table_epoch: portable_atomic::AtomicU64,
}

impl DescriptorTable {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        // Unit weight → the byte capacity is a max entry count. `est_items`
        // seeds the ghost-queue sizing; the descriptor count is the natural
        // estimate.
        let inner = ShardedCache::with_weighter(
            capacity as u64,
            FD_CACHE_SHARDS,
            capacity,
            UnitWeighter,
            rustc_hash::FxBuildHasher,
        );

        Self {
            inner,
            table_epoch: portable_atomic::AtomicU64::new(0),
        }
    }

    /// The table descriptor epoch, see [`Self::remove_for_table`].
    pub(crate) fn table_epoch(&self) -> u64 {
        self.table_epoch.load(portable_atomic::Ordering::Acquire)
    }

    /// Advances the table descriptor epoch after a removal. Release, so a
    /// read that sees the new epoch also sees the entry gone and takes the
    /// descriptor the path names now.
    fn advance_table_epoch(&self) {
        self.table_epoch
            .fetch_add(1, portable_atomic::Ordering::Release);
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    /// The most descriptors the cache keeps open.
    pub(crate) fn capacity(&self) -> u64 {
        self.inner.capacity()
    }

    /// Closes the cached descriptors of every tree whose id is `first` or
    /// later, tables and blob files alike.
    pub(crate) fn remove_trees_from(&self, first: crate::tree::inner::TreeId) {
        self.inner.remove_where(|key| key.1 >= first);
        self.advance_table_epoch();
    }

    #[must_use]
    pub fn access_for_table(&self, id: &GlobalTableId) -> Option<Arc<dyn FsFile>> {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.get(&key)
    }

    /// Like [`Self::access_for_table`], but without promoting the entry, for
    /// a read that must leave the table as it found it.
    #[must_use]
    pub(crate) fn peek_for_table(&self, id: &GlobalTableId) -> Option<Arc<dyn FsFile>> {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.peek(&key)
    }

    pub fn insert_for_table(&self, id: GlobalTableId, item: Item) {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.insert(key, item);
    }

    #[must_use]
    pub fn access_for_blob_file(&self, id: &GlobalTableId) -> Option<Arc<dyn FsFile>> {
        let key = CacheKey(TAG_BLOB, id.tree_id(), id.table_id());
        self.inner.get(&key)
    }

    pub fn insert_for_blob_file(&self, id: GlobalTableId, item: Item) {
        let key = CacheKey(TAG_BLOB, id.tree_id(), id.table_id());
        self.inner.insert(key, item);
    }

    /// Closes the table's cached descriptor and advances the table descriptor
    /// epoch, so no scan reads through that descriptor again.
    pub fn remove_for_table(&self, id: &GlobalTableId) {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.remove(&key);
        self.advance_table_epoch();
    }

    pub fn remove_for_blob_file(&self, id: &GlobalTableId) {
        let key = CacheKey(TAG_BLOB, id.tree_id(), id.table_id());
        self.inner.remove(&key);
    }
}
