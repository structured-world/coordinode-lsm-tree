// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::tree::inner::TreeId;

pub type TableId = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GlobalTableId(TreeId, TableId);

impl GlobalTableId {
    #[must_use]
    pub fn tree_id(&self) -> TreeId {
        self.0
    }

    #[must_use]
    pub fn table_id(&self) -> TableId {
        self.1
    }
}

impl From<(TreeId, TableId)> for GlobalTableId {
    fn from((tid, sid): (TreeId, TableId)) -> Self {
        Self(tid, sid)
    }
}

#[cfg(test)]
mod tests;
