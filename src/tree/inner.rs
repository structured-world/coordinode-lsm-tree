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
use std::sync::{Arc, Mutex, RwLock, atomic::AtomicU64};

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
    TREE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

    /// Tree-wide gate that defers SST / blob-file deletions while a
    /// [`Tree::create_checkpoint`](crate::Tree::create_checkpoint) is in
    /// flight. Acquired by checkpoint code via
    /// [`DeletionPause::acquire`]; consulted by [`Drop`] impls on
    /// [`Table`](crate::Table) and [`BlobFile`](crate::BlobFile).
    pub(crate) deletion_pause: Arc<DeletionPause>,

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
        let initial_runtime = std::sync::Arc::new(config.initial_runtime_config.clone());
        persist_version(
            &config.path,
            &version,
            config.comparator.name(),
            &*config.fs,
            std::sync::Arc::clone(&initial_runtime),
            config.encryption.clone(),
        )?;

        let comparator = config.comparator.clone();

        Ok(Self {
            id: get_next_tree_id(),
            memtable_id_counter: SequenceNumberCounter::new(1),
            table_id_counter: SequenceNumberCounter::default(),
            blob_file_id_counter: SequenceNumberCounter::default(),
            config: Arc::new(config),
            version_history: Arc::new(RwLock::new(SuperVersions::new(version, &comparator))),
            stop_signal: StopSignal::default(),
            major_compaction_lock: RwLock::default(),
            flush_lock: Mutex::default(),
            compaction_state: Arc::new(Mutex::new(CompactionState::default())),
            deletion_pause: DeletionPause::new_shared(),
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
