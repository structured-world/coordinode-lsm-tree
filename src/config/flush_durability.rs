// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

/// When the tables a flush writes become durable.
///
/// Set via [`Config::flush_durability`](crate::Config::flush_durability).
// no-std: pure data type - `Copy` enum, no allocator needed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushDurability {
    /// A flush syncs its tables, its blob files and their directories before
    /// it returns. The default.
    ///
    /// It writes no manifest edit: each table records which flush wrote it
    /// and its place in that flush's run, and an open after a power loss
    /// installs every flush whose tables are all there, so one that returned
    /// is kept and one cut short is left out. The next install that writes
    /// the manifest (a compaction, [`AbstractTree::persist_flushes`](crate::AbstractTree::persist_flushes),
    /// a tree that is dropped) names the tables.
    #[default]
    Synced,

    /// A flush writes its tables without syncing them and installs them in
    /// memory only; [`AbstractTree::persist_flushes`](crate::AbstractTree::persist_flushes)
    /// makes every flush installed before it durable.
    ///
    /// For a caller that keeps its own write-ahead log: its log makes a write
    /// durable, so the syncs a flush would issue buy nothing until the caller
    /// wants to drop that part of its log. Every other install (a compaction,
    /// an ingestion, a drop) stays durable and persists the deferred flushes
    /// with it, and so does a checkpoint. A tree that is dropped persists them
    /// when it can: a failure there is logged, not returned, and the flushes
    /// it left out reopen as after a power loss.
    ///
    /// After a power loss the tree opens at the last persisted state: the
    /// tables of later flushes are named by no manifest and are removed on
    /// open, as if those flushes never ran. A caller replays its log from
    /// there, and may drop its log only up to what the last
    /// `persist_flushes` covered.
    Deferred,
}
