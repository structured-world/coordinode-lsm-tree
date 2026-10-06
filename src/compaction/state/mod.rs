// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

pub mod hidden_set;

use hidden_set::HiddenSet;

#[derive(Default)]
pub struct CompactionState {
    /// Set of table IDs that are masked.
    ///
    /// While consuming tables (because of compaction) they will not appear in the list of tables
    /// as to not cause conflicts between multiple compaction threads (compacting the same tables).
    hidden_set: HiddenSet,

    /// The blob files a memtable row references, as of the choice being made:
    /// such a file stays when its last table goes.
    memtable_blob_files: crate::HashSet<crate::vlog::BlobFileId>,
}

impl CompactionState {
    pub fn hidden_set(&self) -> &HiddenSet {
        &self.hidden_set
    }

    pub fn hidden_set_mut(&mut self) -> &mut HiddenSet {
        &mut self.hidden_set
    }

    /// Whether a memtable row references blob file `id`, as of the choice
    /// being made.
    pub(crate) fn memtable_references_blob_file(&self, id: crate::vlog::BlobFileId) -> bool {
        self.memtable_blob_files.contains(&id)
    }

    /// Records the blob files memtable rows reference, for the next choice.
    pub(crate) fn set_memtable_blob_files(
        &mut self,
        files: crate::HashSet<crate::vlog::BlobFileId>,
    ) {
        self.memtable_blob_files = files;
    }
}

#[cfg(test)]
mod tests;
