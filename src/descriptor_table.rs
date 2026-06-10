// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::sharded_cache::{ShardedCache, UnitWeighter};
use crate::{GlobalTableId, fs::FsFile};
use std::sync::Arc;

const TAG_BLOCK: u8 = 0;
const TAG_BLOB: u8 = 1;

type Item = Arc<dyn FsFile>;

#[derive(Clone, Copy, Eq, std::hash::Hash, PartialEq)]
struct CacheKey(u8, u64, u64);

/// Number of shards in the FD cache. Smaller than the block cache: the FD cache
/// holds at most a few thousand descriptors, so a large shard array would leave
/// most shards empty.
const FD_CACHE_SHARDS: usize = 16;

/// Caches file descriptors to tables and blob files
pub struct DescriptorTable {
    inner: ShardedCache<CacheKey, Item, UnitWeighter, rustc_hash::FxBuildHasher>,
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

        Self { inner }
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    #[must_use]
    pub fn access_for_table(&self, id: &GlobalTableId) -> Option<Arc<dyn FsFile>> {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.get(&key)
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

    pub fn remove_for_table(&self, id: &GlobalTableId) {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.remove(&key);
    }

    pub fn remove_for_blob_file(&self, id: &GlobalTableId) {
        let key = CacheKey(TAG_BLOB, id.tree_id(), id.table_id());
        self.inner.remove(&key);
    }
}
