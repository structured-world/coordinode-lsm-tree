// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod hidden_set;

use hidden_set::HiddenSet;

#[derive(Default)]
pub struct CompactionState {
    /// Set of table IDs that are masked.
    ///
    /// While consuming tables (because of compaction) they will not appear in the list of tables
    /// as to not cause conflicts between multiple compaction threads (compacting the same tables).
    hidden_set: HiddenSet,
}

impl CompactionState {
    pub fn hidden_set(&self) -> &HiddenSet {
        &self.hidden_set
    }

    pub fn hidden_set_mut(&mut self) -> &mut HiddenSet {
        &mut self.hidden_set
    }
}

#[cfg(test)]
mod tests;
