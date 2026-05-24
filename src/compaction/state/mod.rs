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
#[expect(clippy::expect_used)]
mod tests {
    use crate::{AbstractTree, SequenceNumberCounter};
    use test_log::test;

    /// Verifies that a failed `major_compact` leaves no compaction
    /// state behind: `hidden_set` empties, `table_count` is unchanged,
    /// no level manifest update lands. The test needs a way to make
    /// the level-manifest write fail mid-compaction; the original
    /// approach (mutating a `folder` field on `CompactionState` to
    /// point at an invalid path, see the commented-out block below)
    /// no longer compiles against the current `CompactionState` shape.
    ///
    /// Re-enabling requires fault injection at the `Fs` trait layer
    /// (a `Fs` impl that fails specific writes by predicate). Until
    /// that infrastructure lands the test stays ignored; tracked
    /// alongside the broader fault-injection / integrity work in
    /// issues #300 (online `VerifyChecksum` APIs) and #303 (`repair_db`),
    /// either of which is the natural place to land a per-test
    /// failing-`Fs` helper.
    #[test]
    #[ignore = "needs Fs-layer fault injection helper; blocked on #300 / #303 infrastructure"]
    fn level_manifest_atomicity() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 1);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 2);
        tree.flush_active_memtable(0)?;

        assert_eq!(3, tree.approximate_len());

        tree.major_compact(u64::MAX, 3)?;

        assert_eq!(1, tree.table_count());

        tree.insert("a", "a", 3);
        tree.flush_active_memtable(0)?;

        let table_count_before_major_compact = tree.table_count();

        let crate::AnyTree::Standard(tree) = tree else {
            unreachable!();
        };

        // {
        //     // NOTE: Purposefully change level manifest to have invalid path
        //     // to force an I/O error
        //     tree.compaction_state
        //         .lock()
        //         .expect("lock is poisoned")
        //         .folder = "/invaliiid/asd".into();
        // }

        assert!(tree.major_compact(u64::MAX, 4).is_err());

        assert!(
            tree.compaction_state
                .lock()
                .expect("lock is poisoned")
                .hidden_set()
                .is_empty()
        );

        assert_eq!(table_count_before_major_compact, tree.table_count());

        Ok(())
    }
}
