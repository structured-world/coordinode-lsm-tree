// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Live progress counters for long-running recovery work.
//!
//! A manifest repair over a large store runs for minutes to hours (it streams
//! every SST and blob file, and salvage walks corrupt tables block by block).
//! [`RecoveryProgress`] is the observation seam: hand a shared handle to
//! [`Config::with_recovery_progress`](crate::Config::with_recovery_progress)
//! (or [`SalvageOptions::progress`](crate::salvage::SalvageOptions)) before
//! starting the operation, then poll [`RecoveryProgress::snapshot`] from any
//! other thread while it runs.

use portable_atomic::AtomicU64;

use core::sync::atomic::Ordering::Relaxed;

/// Shared live-progress counters for a repair / salvage run.
///
/// All counters are cumulative for the lifetime of the handle and updated with
/// relaxed atomics as the work proceeds — per discovered file, per walked
/// block, per recovered row — so a UI thread can poll
/// [`snapshot`](Self::snapshot) at any rate without synchronizing with the
/// recovery. Reuse a fresh handle per operation to start the counts at zero.
///
/// In-place ECC heal events outside salvage (patrol scrub) are exported
/// through the `metrics` feature's counters, not through this handle.
///
/// # Examples
///
/// ```
/// use lsm_tree::RecoveryProgress;
/// use std::sync::Arc;
///
/// let progress = Arc::new(RecoveryProgress::default());
/// // Hand a clone to `Config::with_recovery_progress(..)`, run the repair on
/// // another thread, and poll:
/// let snap = progress.snapshot();
/// assert_eq!(snap.blocks_scanned, 0);
/// ```
#[derive(Debug, Default)]
pub struct RecoveryProgress {
    tables_discovered: AtomicU64,
    tables_recovered: AtomicU64,
    blob_files_discovered: AtomicU64,
    blob_files_recovered: AtomicU64,
    blocks_scanned: AtomicU64,
    blocks_recovered: AtomicU64,
    blocks_dropped: AtomicU64,
    blocks_healed: AtomicU64,
    kvs_recovered: AtomicU64,
    columns_recovered: AtomicU64,
    /// Encoded [`RecoveryPhase`]; see [`RecoveryPhase::from_u8`].
    phase: portable_atomic::AtomicU8,
    bytes_total: AtomicU64,
    bytes_processed: AtomicU64,
    /// Cooperative cancellation flag; see [`Self::request_cancel`].
    cancel: portable_atomic::AtomicBool,
}

/// Which stage of a recovery operation is currently running.
///
/// Published through [`RecoveryProgress`] as the work proceeds, for progress
/// display; stages are coarse on purpose (the counters carry the fine grain).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum RecoveryPhase {
    /// No operation has started on this handle yet.
    #[default]
    Idle = 0,
    /// Finishing a previous run's committed-but-unswapped replacements and
    /// removing files its manifest no longer references.
    PendingSwaps = 1,
    /// Scanning and recovering SSTs (salvage runs inside this phase; its
    /// per-block counters show the fine grain).
    ScanningTables = 2,
    /// Recovering and, where needed, salvaging blob files.
    RecoveringBlobFiles = 3,
    /// Durably committing the rebuilt manifest.
    Committing = 4,
    /// Post-commit swaps and removal of superseded files.
    Cleanup = 5,
    /// A patrol scrub is walking the tree (see
    /// [`PatrolScrubOptions::progress`](crate::scrub::PatrolScrubOptions)).
    Scrubbing = 6,
    /// The operation completed successfully. A failed run leaves the phase
    /// where it stopped (the call's own error carries the verdict), which
    /// tells a display exactly which stage failed.
    Done = 7,
}

impl RecoveryPhase {
    /// Decodes the atomic representation; unknown values (impossible within
    /// one build) read as [`RecoveryPhase::Idle`].
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::PendingSwaps,
            2 => Self::ScanningTables,
            3 => Self::RecoveringBlobFiles,
            4 => Self::Committing,
            5 => Self::Cleanup,
            6 => Self::Scrubbing,
            7 => Self::Done,
            _ => Self::Idle,
        }
    }
}

impl RecoveryProgress {
    /// Returns a point-in-time copy of every counter.
    ///
    /// Each field is read individually with relaxed ordering, so the snapshot
    /// is not a single atomic cut across counters — adjacent fields can be a
    /// few events apart — which is exactly enough for progress display.
    #[must_use]
    pub fn snapshot(&self) -> RecoveryProgressSnapshot {
        RecoveryProgressSnapshot {
            tables_discovered: self.tables_discovered.load(Relaxed),
            tables_recovered: self.tables_recovered.load(Relaxed),
            blob_files_discovered: self.blob_files_discovered.load(Relaxed),
            blob_files_recovered: self.blob_files_recovered.load(Relaxed),
            blocks_scanned: self.blocks_scanned.load(Relaxed),
            blocks_recovered: self.blocks_recovered.load(Relaxed),
            blocks_dropped: self.blocks_dropped.load(Relaxed),
            blocks_healed: self.blocks_healed.load(Relaxed),
            kvs_recovered: self.kvs_recovered.load(Relaxed),
            columns_recovered: self.columns_recovered.load(Relaxed),
            phase: RecoveryPhase::from_u8(self.phase.load(Relaxed)),
            bytes_total: self.bytes_total.load(Relaxed),
            bytes_processed: self.bytes_processed.load(Relaxed),
        }
    }

    /// Requests cooperative cancellation of the operation this handle is
    /// attached to. The running repair observes the flag at file boundaries
    /// (and once more just before its manifest commit) and aborts with
    /// [`Error::Cancelled`](crate::Error::Cancelled). A pre-commit abort
    /// removes any blob replacements the run had already published under
    /// fresh ids, so it leaves nothing behind except unpublished
    /// `{id}.repair-tmp` staging copies — which the retry itself overwrites
    /// (the scan is otherwise read-only, so the directory is exactly what the
    /// retry expects). An abort requested after the commit is ignored: the
    /// manifest already names the rebuilt state, and stopping mid-cleanup
    /// would leave work the next open must redo anyway. Idempotent; there is
    /// no un-cancel (use a fresh handle for a new run).
    pub fn request_cancel(&self) {
        self.cancel.store(true, Relaxed);
    }

    /// Whether [`request_cancel`](Self::request_cancel) was called on this
    /// handle.
    #[must_use]
    pub fn is_cancel_requested(&self) -> bool {
        self.cancel.load(Relaxed)
    }

    /// One table file recognized in the scan (by its numeric id).
    pub(crate) fn table_discovered(&self) {
        self.tables_discovered.fetch_add(1, Relaxed);
    }

    /// Tables recovered into the rebuilt manifest (whole or salvaged).
    /// Published once the survivor set is final — after duplicate displacement
    /// and blob-dependency filtering — so the counter never exceeds what the
    /// manifest actually holds.
    pub(crate) fn tables_recovered_add(&self, n: u64) {
        if n > 0 {
            self.tables_recovered.fetch_add(n, Relaxed);
        }
    }

    /// Blob files that reached the rebuilt manifest. Added once the surviving
    /// reference set is final, since a file no table points at is dropped from
    /// the manifest and removed.
    pub(crate) fn blob_files_recovered_add(&self, n: u64) {
        if n > 0 {
            self.blob_files_recovered.fetch_add(n, Relaxed);
        }
    }

    /// One blob file recognized in the scan (by its numeric id).
    pub(crate) fn blob_file_discovered(&self) {
        self.blob_files_discovered.fetch_add(1, Relaxed);
    }

    /// Publishes a salvage walk's per-block deltas (scanned / re-emitted /
    /// dropped / ECC-healed).
    pub(crate) fn add_blocks(&self, scanned: u64, recovered: u64, dropped: u64, healed: u64) {
        if scanned > 0 {
            self.blocks_scanned.fetch_add(scanned, Relaxed);
        }
        if recovered > 0 {
            self.blocks_recovered.fetch_add(recovered, Relaxed);
        }
        if dropped > 0 {
            self.blocks_dropped.fetch_add(dropped, Relaxed);
        }
        if healed > 0 {
            self.blocks_healed.fetch_add(healed, Relaxed);
        }
    }

    /// Publishes a salvage walk's per-row deltas (KV entries and, for a
    /// columnar source, the value sub-columns those rows carried).
    pub(crate) fn add_rows(&self, kvs: u64, columns: u64) {
        if kvs > 0 {
            self.kvs_recovered.fetch_add(kvs, Relaxed);
        }
        if columns > 0 {
            self.columns_recovered.fetch_add(columns, Relaxed);
        }
    }

    /// Publishes the stage the operation is in.
    pub(crate) fn set_phase(&self, phase: RecoveryPhase) {
        self.phase.store(phase as u8, Relaxed);
    }

    /// Publishes the total on-disk bytes the operation expects to take up,
    /// from an upfront directory listing.
    pub(crate) fn set_bytes_total(&self, total: u64) {
        self.bytes_total.store(total, Relaxed);
    }

    /// Adds bytes the operation has taken up (a file counts when its
    /// processing STARTS, so the percentage lags by at most one file).
    /// Display-only counter: saturating, since clamping at `u64::MAX` merely
    /// freezes the display while a checked overflow would fail the recovery
    /// over a progress number.
    pub(crate) fn add_bytes_processed(&self, n: u64) {
        if n > 0 {
            let _ = self
                .bytes_processed
                .fetch_update(Relaxed, Relaxed, |v| Some(v.saturating_add(n)));
        }
    }
}

/// A point-in-time copy of [`RecoveryProgress`], returned by
/// [`RecoveryProgress::snapshot`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryProgressSnapshot {
    /// Table files recognized by the repair scan so far.
    pub tables_discovered: u64,
    /// Tables recovered into the rebuilt manifest (whole or salvaged).
    pub tables_recovered: u64,
    /// Blob files recognized by the repair scan so far.
    pub blob_files_discovered: u64,
    /// Blob files recovered into the rebuilt manifest.
    pub blob_files_recovered: u64,
    /// Data blocks a salvage walk has inspected.
    pub blocks_scanned: u64,
    /// Data blocks a salvage walk re-emitted into a recovered copy, or a
    /// patrol scrub corrected (its healed payload readable again in place).
    pub blocks_recovered: u64,
    /// Data blocks a salvage walk had to drop (corrupt / undecodable).
    pub blocks_dropped: u64,
    /// Of [`blocks_recovered`](Self::blocks_recovered), how many needed ECC
    /// recovery to read (their healed payload was re-encoded).
    pub blocks_healed: u64,
    /// KV entries re-emitted into recovered copies.
    pub kvs_recovered: u64,
    /// Value sub-columns carried by recovered columnar blocks.
    pub columns_recovered: u64,
    /// The stage the operation is currently in.
    pub phase: RecoveryPhase,
    /// Total on-disk bytes the operation expects to take up (from an upfront
    /// directory listing; `0` until published, or when no handle-aware
    /// operation ran).
    pub bytes_total: u64,
    /// Bytes taken up so far (a file counts when its processing starts).
    /// `bytes_processed as f64 / bytes_total as f64` is the display
    /// percentage once `bytes_total` is non-zero.
    pub bytes_processed: u64,
}
