// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

/// When the tables a flush writes become durable.
///
/// Set via [`Config::flush_durability`](crate::Config::flush_durability).
// no-std: pure data type - `Copy` enum, no allocator needed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushDurability {
    /// A flush syncs its tables, their directory and the manifest edit naming
    /// them before it returns. The default.
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
    /// with it, and so does a checkpoint and a tree that is dropped.
    ///
    /// After a power loss the tree opens at the last persisted state: the
    /// tables of later flushes are named by no manifest and are removed on
    /// open, as if those flushes never ran. A caller replays its log from
    /// there, and may drop its log only up to what the last
    /// `persist_flushes` covered.
    Deferred,
}
