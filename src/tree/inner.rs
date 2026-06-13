// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    SequenceNumberCounter, TableId,
    compaction::state::CompactionState,
    config::Config,
    deletion_pause::DeletionPause,
    runtime_config::handle::RuntimeConfigHandle,
    stop_signal::StopSignal,
    version::{SuperVersions, Version, persist_version},
};
use alloc::sync::Arc;
use portable_atomic::AtomicU64;
// no-std: parking_lot is std-only; spin provides the same Mutex/RwLock API
// without an allocator. parking_lot wins on the std hot path (8-byte,
// userspace fast path), so keep it for std and fall back to spin for no_std.
#[cfg(feature = "std")]
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(not(feature = "std"))]
use spin::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

// RwLock guard aliases: parking_lot's and spin's read/write guards both take
// `<'a, T>` (spin defaults its relax-strategy param to `Spin`), so one alias
// per guard covers both backends.
/// Write guard over the tree's version history. Backend-specific guard type
/// (`parking_lot` under std, `spin` under `no_std`), aliased so
/// [`crate::AbstractTree`] and its impls share one signature.
pub type VersionsWriteGuard<'a> = RwLockWriteGuard<'a, SuperVersions>;
/// Read guard over the tree's version history. See [`VersionsWriteGuard`].
pub type VersionsReadGuard<'a> = RwLockReadGuard<'a, SuperVersions>;

// spin's `MutexGuard` (unlike its RwLock guards) has no default relax-strategy
// param, so the Mutex-based aliases name the backend guard explicitly.
/// Guard over the tree's compaction state. See [`VersionsWriteGuard`].
#[cfg(feature = "std")]
pub type CompactionGuard<'a> = parking_lot::MutexGuard<'a, CompactionState>;
/// Guard over the tree's compaction state. See [`VersionsWriteGuard`].
#[cfg(not(feature = "std"))]
pub type CompactionGuard<'a> = spin::MutexGuard<'a, CompactionState, spin::Spin>;
/// Guard over the tree's flush-serialization mutex. See [`VersionsWriteGuard`].
#[cfg(feature = "std")]
pub type FlushGuard<'a> = parking_lot::MutexGuard<'a, ()>;
/// Guard over the tree's flush-serialization mutex. See [`VersionsWriteGuard`].
#[cfg(not(feature = "std"))]
pub type FlushGuard<'a> = spin::MutexGuard<'a, (), spin::Spin>;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Unique tree ID
///
/// Tree IDs are monotonically increasing integers.
pub type TreeId = u64;

/// Unique memtable ID
///
/// Memtable IDs map one-to-one to some table.
pub type MemtableId = u64;

/// Hands out a unique (monotonically increasing) tree ID.
pub fn get_next_tree_id() -> TreeId {
    static TREE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    TREE_ID_COUNTER.fetch_add(1, core::sync::atomic::Ordering::Relaxed)
}

pub struct TreeInner {
    /// Unique tree ID
    pub id: TreeId,

    /// Hands out a unique (monotonically increasing) memtable ID
    #[doc(hidden)]
    pub memtable_id_counter: SequenceNumberCounter,

    /// Hands out a unique (monotonically increasing) table ID
    #[doc(hidden)]
    pub table_id_counter: SequenceNumberCounter,

    // This is not really used in the normal tree, but we need it in the blob tree
    /// Hands out a unique (monotonically increasing) blob file ID
    pub(crate) blob_file_id_counter: SequenceNumberCounter,

    pub(crate) version_history: Arc<RwLock<SuperVersions>>,

    /// Lock-free mirror of the latest `SuperVersion` (shared with
    /// [`version_history`](Self::version_history) via
    /// [`SuperVersions::latest_handle`]). The point-read hot path loads this
    /// for `MAX_SEQNO` reads instead of taking the history `RwLock`.
    ///
    /// `std`-only: `arc-swap` is not `#![no_std]`, so the lock-free mirror is
    /// absent under no-std and point reads fall back to the history `RwLock`.
    #[cfg(feature = "std")]
    pub(crate) latest_super_version: Arc<arc_swap::ArcSwap<crate::version::SuperVersion>>,

    pub(crate) compaction_state: Arc<Mutex<CompactionState>>,

    /// Tree configuration
    pub config: Arc<Config>,

    /// Compaction may take a while; setting the signal to `true`
    /// will interrupt the compaction and kill the worker.
    pub(crate) stop_signal: StopSignal,

    /// Used by major compaction to be the exclusive compaction going on.
    ///
    /// Minor compactions use `major_compaction_lock.read()` instead, so they
    /// can be concurrent next to each other.
    pub(crate) major_compaction_lock: RwLock<()>,

    /// Serializes flush operations.
    pub(crate) flush_lock: Mutex<()>,

    /// Holds the cross-process exclusive directory lock for this tree's
    /// lifetime: the locked `LOCK` file handle acquired by `Config::open` (via
    /// `acquire_directory_lock`). Dropping it on tree close releases the OS
    /// advisory lock so another process can open the directory. `None` when the
    /// lock is disabled via
    /// [`Config::with_directory_lock`](crate::Config::with_directory_lock). The
    /// field is never read directly (the `_` prefix marks it as a lifetime
    /// guard); its sole job is to keep the lock held.
    // std-only: cross-process file locking needs real OS files; a no_std build
    // is single-context and has nothing to guard against.
    #[cfg(feature = "std")]
    pub(crate) _directory_lock: Option<Box<dyn crate::fs::FsFile>>,

    /// Tree-wide gate that defers SST / blob-file deletions while a
    /// [`Tree::create_checkpoint`](crate::Tree::create_checkpoint) is in
    /// flight. Acquired by checkpoint code via
    /// [`DeletionPause::acquire`]; consulted by [`Drop`] impls on
    /// [`Table`](crate::Table) and [`BlobFile`](crate::BlobFile).
    pub(crate) deletion_pause: Arc<DeletionPause>,

    /// Tree-wide background file deleter. Installed into every table / blob
    /// file so an obsolete file's directory-entry `unlink` runs off the
    /// foreground path (its blocks are freed synchronously at the Drop site).
    /// Held here so the deleter — and its worker thread — outlives every table
    /// (each table holds a clone, so the worker drains and joins only once the
    /// last reference, including this one, is dropped).
    // std-only: the deleter spawns a thread. A no_std build keeps the
    // synchronous Drop path and never constructs one.
    #[cfg(feature = "std")]
    pub(crate) background_deleter: Arc<crate::BackgroundDeleter>,

    /// Tree-wide ECC heal-hint sink. A block read that recovers a payload from
    /// its Page-ECC parity, and confirms the on-disk fault is persistent via a
    /// cache-bypassing re-read, records the SST id here. The compaction picker
    /// drains it to rewrite the faulty file clean. Installed into every table so
    /// the read path can record without a back-reference to the tree.
    pub(crate) heal_hints: Arc<crate::heal_hints::HealHints>,

    /// Runtime-toggleable configuration. Lockless atomic snapshot.
    ///
    /// Reachable through the public Tree API
    /// ([`crate::Tree::runtime_config`] /
    /// [`crate::Tree::update_runtime_config`]). Manifest-touching
    /// write paths — `version::persist_version`,
    /// `compaction::worker::Options`,
    /// `checkpoint::write_current_for_version`, and the flush /
    /// ingestion sites that drive them — load a snapshot of this
    /// handle to pick the manifest writer's `BlockTransform`
    /// variant (footer mirror on/off, `page_ecc`, encryption). SST
    /// data-block write paths still consume the static
    /// `Config::page_ecc` only — wiring through the SST writer
    /// is a follow-up. Read paths stay config-independent: each
    /// Block self-describes via its own header.
    //
    // no-std: Tree itself is std-bound. For no_std consumers needing
    // runtime-toggleable config, use spin::RwLock<RuntimeConfig> as
    // alternative (slower hot path, but compiles under alloc-only).
    pub(crate) runtime_config: Arc<RuntimeConfigHandle>,

    #[doc(hidden)]
    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl TreeInner {
    pub(crate) fn create_new(config: Config) -> crate::Result<Self> {
        // Acquire the cross-process directory lock before the first manifest
        // write below. The caller (`Tree::create_new`) has already created the
        // tree directory, so the `LOCK` file can be opened here.
        #[cfg(feature = "std")]
        let directory_lock = crate::config::acquire_directory_lock(
            &*config.fs,
            &config.path,
            config.directory_lock,
        )?;

        let version = Version::new(
            0,
            if config.kv_separation_opts.is_some() {
                crate::TreeType::Blob
            } else {
                crate::TreeType::Standard
            },
        );
        // Seed the runtime snapshot for the first persist from the
        // Config-supplied initial RuntimeConfig (defaults to
        // `RuntimeConfig::default()` when the caller never touched
        // `Config::with_runtime_config`). Reused below to initialize
        // the Tree's `RuntimeConfigHandle` so the on-disk manifest
        // bytes and the live runtime handle agree on byte zero.
        let initial_runtime = Arc::new(config.initial_runtime_config.clone());
        persist_version(
            &config.path,
            &version,
            config.comparator.name(),
            &*config.fs,
            Arc::clone(&initial_runtime),
            config.encryption.clone(),
            config.sync_mode,
        )?;

        let comparator = config.comparator.clone();
        let sync_mode = config.sync_mode;

        // The first persist above wrote the full snapshot `v{version.id()}` and
        // pointed CURRENT at it, so that id is the initial manifest snapshot.
        let snapshot_id = version.id();
        let super_versions = SuperVersions::new(
            version,
            &comparator,
            sync_mode,
            snapshot_id,
            config.manifest_log_rotate_bytes,
        );
        #[cfg(feature = "std")]
        let latest_super_version = super_versions.latest_handle();

        Ok(Self {
            id: get_next_tree_id(),
            memtable_id_counter: SequenceNumberCounter::new(1),
            table_id_counter: SequenceNumberCounter::default(),
            blob_file_id_counter: SequenceNumberCounter::default(),
            config: Arc::new(config),
            version_history: Arc::new(RwLock::new(super_versions)),
            #[cfg(feature = "std")]
            latest_super_version,
            stop_signal: StopSignal::default(),
            major_compaction_lock: RwLock::default(),
            flush_lock: Mutex::default(),
            #[cfg(feature = "std")]
            _directory_lock: directory_lock,
            compaction_state: Arc::new(Mutex::new(CompactionState::default())),
            deletion_pause: DeletionPause::new_shared(),
            #[cfg(feature = "std")]
            background_deleter: Arc::new(crate::BackgroundDeleter::new(None)),
            heal_hints: crate::heal_hints::HealHints::new_shared(initial_runtime.auto_heal),
            runtime_config: Arc::new(RuntimeConfigHandle::new((*initial_runtime).clone())),

            #[cfg(feature = "metrics")]
            metrics: Metrics::default().into(),
        })
    }

    pub fn get_next_table_id(&self) -> TableId {
        self.table_id_counter.next()
    }
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::trace!("Sending stop signal to compactors");
        self.stop_signal.send();
    }
}
