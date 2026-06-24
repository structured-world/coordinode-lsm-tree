// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Contains compaction strategies

pub(crate) mod fifo;
pub(crate) mod leveled;
// pub(crate) mod maintenance;
#[cfg(feature = "std")]
pub(crate) mod delete_materialize;
pub(crate) mod drop_range;
pub mod filter;
mod flavour;
pub(crate) mod heal;
pub(crate) mod major;
pub(crate) mod movedown;
pub(crate) mod pulldown;
pub(crate) mod seqno_zeroer;
pub(crate) mod state;
pub(crate) mod stream;
pub(crate) mod tiered;
pub(crate) mod worker;

pub use fifo::Strategy as Fifo;
pub use filter::{CompactionFilter, Factory, ItemAccessor, Verdict};
pub use heal::Strategy as EccHeal;
pub use leveled::Strategy as Leveled;
pub use tiered::Strategy as SizeTiered;

pub use {
    fifo::NAME as FIFO_COMPACTION_NAME, leveled::NAME as LEVELED_COMPACTION_NAME,
    tiered::NAME as TIERED_COMPACTION_NAME,
};

/// Alias for `Leveled`
pub type Levelled = Leveled;

#[doc(hidden)]
pub use movedown::Strategy as MoveDown;

#[doc(hidden)]
pub use pulldown::Strategy as PullDown;

use crate::{
    HashSet, KvPair, TableId, compaction::state::CompactionState, config::Config, version::Version,
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// The action taken during a compaction run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionAction {
    /// Strategy chose to do nothing.
    Nothing,

    /// Tables were merged (rewritten) into a destination level.
    Merged,

    /// Tables were moved to a deeper level without rewriting.
    Moved,

    /// Tables were dropped without compaction.
    Dropped,
}

/// Result of a compaction operation, describing what happened.
///
/// Returned by [`crate::AbstractTree::compact`] to give callers
/// observability into which compaction path was taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    /// The action that was taken.
    pub action: CompactionAction,

    /// The destination level, if applicable.
    pub dest_level: Option<u8>,

    /// Number of input tables consumed.
    pub tables_in: usize,

    /// Number of output tables produced.
    pub tables_out: usize,
}

impl CompactionResult {
    /// Creates a result for the "do nothing" case.
    #[must_use]
    pub fn nothing() -> Self {
        Self {
            action: CompactionAction::Nothing,
            dest_level: None,
            tables_in: 0,
            tables_out: 0,
        }
    }
}

/// Input for compactor
///
/// The compaction strategy chooses which tables to compact and how.
/// That information is given to the compactor.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Input {
    /// Tables to compact
    pub table_ids: HashSet<TableId>,

    /// Level to put the created tables into
    pub dest_level: u8,

    /// The logical level the tables are part of
    pub canonical_level: u8,

    /// Table target size
    ///
    /// If a table merge reaches the size threshold, a new table is started.
    /// This results in a sorted "run" of tables.
    pub target_size: u64,
}

/// Describes what to do (compact or not)
#[derive(Debug, Eq, PartialEq)]
pub enum Choice {
    /// Just do nothing.
    DoNothing,

    /// Moves tables into another level without rewriting.
    Move(Input),

    /// Compacts some tables into a new level.
    Merge(Input),

    /// Delete tables without doing compaction.
    ///
    /// This may be used by a compaction strategy that wants to delete old data
    /// without having to compact it away, like [`fifo::Strategy`].
    Drop(HashSet<TableId>),
}

/// Trait for a compaction strategy
///
/// The strategy receives the levels of the LSM-tree as argument
/// and emits a choice on what to do.
#[expect(clippy::module_name_repetitions)]
pub trait CompactionStrategy: Send + Sync {
    /// Gets the compaction strategy name.
    fn get_name(&self) -> &'static str;

    #[doc(hidden)]
    fn get_config(&self) -> Vec<KvPair> {
        vec![]
    }

    /// Decides on what to do based on the current state of the LSM-tree's levels
    ///
    /// This is the purely *structural* decision (level shape, run counts, size
    /// targets); it deliberately does not see the live runtime config. The
    /// orchestrator layers the runtime-config-driven housekeeping fallback on
    /// top (the density-based columnar rewrite).
    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice;

    /// Estimated bytes pending compaction: on-disk data currently sitting above
    /// its level's target size that must eventually be rewritten downward (a
    /// `RocksDB` `estimate-pending-compaction-bytes` analog). A scheduler /
    /// tiering consumer reads it as a compaction-debt signal; `0` means the tree
    /// is at or below its target shape.
    ///
    /// The default is `0` for strategies without a size-target notion of debt
    /// (FIFO, drop-range, major one-shot); the leveled strategy overrides it with
    /// the per-level overflow sum.
    fn pending_compaction_bytes(&self, _version: &Version) -> u64 {
        0
    }
}

/// Runs a strategy's structural decision and, only when it would otherwise idle,
/// layers the runtime-config-driven density rewrite on top.
///
/// This keeps [`CompactionStrategy::choose`] purely structural (no runtime-config
/// dependency) while applying the housekeeping fallback uniformly to every
/// strategy at the orchestration layer, where the live `runtime_config` already
/// lives. The fallback never preempts real work: it only fires when `choose`
/// returned [`Choice::DoNothing`], and [`pick_density_rewrite`] self-gates to a
/// no-op unless a level runs an [`Adaptive`](crate::config::DeleteStrategy::Adaptive)
/// strategy and holds a dense-enough segment.
pub(crate) fn choose_with_density_rewrite(
    strategy: &dyn CompactionStrategy,
    version: &Version,
    config: &Config,
    runtime_config: &crate::runtime_config::RuntimeConfig,
    state: &CompactionState,
) -> Choice {
    let structural = strategy.choose(version, config, state);
    if matches!(structural, Choice::DoNothing)
        && let Some(input) = pick_density_rewrite(version, runtime_config, state)
    {
        return Choice::Merge(input);
    }
    structural
}

/// Picks a columnar segment whose positional delete-bitmap density has grown
/// past its level's adaptive purge threshold, for a single-input materializing
/// rewrite: the masked scan drops the dead rows and the fresh segment carries no
/// bitmap, reclaiming the merge-on-read mask cost paid on every scan.
///
/// Returns the densest eligible candidate, or `None` when no level runs an
/// [`Adaptive`](crate::config::DeleteStrategy::Adaptive) strategy (a fixed
/// `CopyOnWrite` never accumulates a bitmap, a fixed `MergeOnRead` never purges)
/// or no live segment is dense enough. Segments already enrolled in another
/// compaction (`hidden_set`) are skipped. This self-gating is why the orchestrator
/// can apply it uniformly after any strategy.
///
/// The live `runtime_config` (not the boot snapshot) supplies the per-level
/// threshold, so an operator's `update_runtime_config` takes effect. The rewrite
/// targets the candidate's own on-disk size, so the single survivor run stays one
/// segment.
pub(crate) fn pick_density_rewrite(
    version: &Version,
    runtime_config: &crate::runtime_config::RuntimeConfig,
    state: &CompactionState,
) -> Option<Input> {
    // (density, id, level, file_size) of the densest eligible candidate.
    let mut best: Option<(u8, TableId, usize, u64)> = None;
    for (level_idx, level) in version.iter_levels().enumerate() {
        let crate::config::DeleteStrategy::Adaptive {
            purge_threshold_percent,
        } = runtime_config.delete_strategy.get(level_idx)
        else {
            continue;
        };
        for table in level.iter().flat_map(|run| run.iter()) {
            if state.hidden_set().is_hidden(table.id()) {
                continue;
            }
            let Some(density) = table.delete_density() else {
                continue;
            };
            if density < purge_threshold_percent {
                continue;
            }
            let take = match best {
                None => true,
                Some((best_density, _, _, _)) => density > best_density,
            };
            if take {
                best = Some((density, table.id(), level_idx, table.file_size()));
            }
        }
    }

    let (_, table_id, level_idx, file_size) = best?;
    let mut table_ids = HashSet::default();
    table_ids.insert(table_id);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "level index is bounded by level count (<= 7, technically 255)"
    )]
    let level = level_idx as u8;
    Some(Input {
        table_ids,
        // In-place single-input rewrite: dest == source level. The masked scan
        // materializes the survivors; plan_merge_on_read declines a
        // bitmap-carrying source, so this runs the copy-on-write merge path.
        dest_level: level,
        canonical_level: level,
        // Target the source's own size so the survivor run stays a single segment.
        target_size: file_size,
    })
}
