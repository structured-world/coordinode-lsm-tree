// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The four choices a compaction strategy makes for each level, each set on
//! its own.
//!
//! Every compaction strategy answers four questions per level (Sarkar et al.,
//! "Constructing and Analyzing the LSM Compaction Design Space", PVLDB 14(11)):
//! when the level compacts (its [`Trigger`]), how its data lies (its
//! [`Layout`]: one run, or runs that accumulate), how much one compaction out
//! of it takes (its [`Granularity`]), and which data that is (its
//! [`Movement`]). No combination is best for every workload, so each is a
//! setting of its own. The leveled defaults are one combination of them.

/// When a level compacts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// When its runs reach `runs`, or its tables reach `tables`: a level of
    /// runs that accumulate, read once per run, compacts by its shape.
    ///
    /// Runs are counted as a read meets them: tables that overlap no table of
    /// a run join it, so batches of disjoint keys make one run, read once.
    /// `tables` is what bounds such a level's size.
    Shape {
        /// The run count that triggers it.
        runs: u8,
        /// The table count that triggers it whatever the run count.
        tables: u16,
    },
    /// When its bytes pass the target the level's place in the tree gives
    /// it, by how far past.
    Size,
    /// Never on its own: the level only receives compactions. It takes no
    /// free move either, into the level below or into the last.
    Never,
}

/// How a level holds its data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// One run: a compaction into the level merges with the tables of it
    /// that it overlaps.
    Leveled,
    /// Runs that accumulate: a compaction into the level takes in nothing of
    /// it and writes its output beside the runs there. Output overlapping no
    /// table of a run joins that run, as any level below L0 lays its tables
    /// out: a read visits disjoint tables once, whichever run holds them.
    Tiered,
}

/// How much one compaction out of a level takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Granularity {
    /// Every table of the level. With `through_full_next`, when the level it
    /// compacts into is past its own target, that level goes along into the
    /// one below it, so its bytes are not written twice in a row.
    Level {
        /// Whether a level past its target is compacted through.
        through_full_next: bool,
    },
    /// The tables of the level a window of the next level's tables
    /// contains, the window chosen by the level's [`Movement`].
    Window,
}

/// Which data a compaction out of a level takes, when it takes part of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Movement {
    /// The window that rewrites least per byte it moves down, among those
    /// that move no more than the level's overshoot plus `slack` bytes;
    /// `None` = one table target.
    LeastRewrite {
        /// The bytes past the overshoot a window may move and still be
        /// preferred.
        slack: Option<u64>,
    },
    /// The window whose tables were read the most per byte rewritten: the
    /// data reads touch is compacted first, and a cold range is left as it
    /// is. Ties fall to the least rewrite, as [`Self::LeastRewrite`] ranks.
    Hottest,
}

/// The four choices for one level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelPolicy {
    /// When the level compacts.
    pub trigger: Trigger,
    /// How it holds its data.
    pub layout: Layout,
    /// How much one compaction out of it takes.
    pub granularity: Granularity,
    /// Which data that is.
    pub movement: Movement,
}
