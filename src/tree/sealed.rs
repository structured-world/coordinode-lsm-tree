// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{Memtable, tree::inner::MemtableId};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Stores references to all sealed memtables
///
/// Memtable IDs are monotonically increasing, so we don't really
/// need a search tree; also there are only a handful of them at most.
#[derive(Clone, Default)]
pub struct SealedMemtables(Vec<Arc<Memtable>>);

impl SealedMemtables {
    /// Copy-and-writes a new list with additional Memtable.
    pub fn add(&self, memtable: Arc<Memtable>) -> Self {
        let mut copy = self.clone();
        copy.0.push(memtable);
        copy
    }

    /// Copy-and-writes a new list with the specified Memtable removed.
    pub fn remove(&self, id_to_remove: MemtableId) -> Self {
        let mut copy = self.clone();
        copy.0.retain(|mt| mt.id != id_to_remove);
        copy
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Arc<Memtable>> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}
