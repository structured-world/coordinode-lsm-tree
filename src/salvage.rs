// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Block-granular SST salvage: recover the readable blocks of an SST whose
//! whole-file verification fails, quarantining the corrupted ones.
//!
//! Where [`crate::repair`] rebuilds the manifest *around* unreadable SSTs and
//! [`crate::verify`] reports per-block health read-only, salvage walks an SST
//! block-by-block, re-emits every data block that passes its checksum (and ECC
//! recovery where present) into a fresh, fully-valid SST, and reports the key
//! ranges it had to drop. A single corrupted block then costs only its own key
//! range instead of the whole file.
//!
//! The salvaged SST is written through the normal [`crate::table`] writer, so
//! it carries fresh per-block checksums, a fresh index, and a fresh filter:
//! the corruption is not propagated into the recovered copy.

use crate::UserKey;
use crate::encryption::EncryptionProvider;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use std::path::PathBuf;

/// The recovery + write context salvage needs to recover an SST that is
/// encrypted and/or zstd-dictionary compressed.
///
/// Block salvage opens the source and rewrites the recovered copy through the
/// normal table path, so both ends need the same crypto / dictionary context as
/// the live tree: without the [`EncryptionProvider`] an encrypted source cannot
/// be decrypted to read its blocks (and the rewritten copy would be plaintext,
/// inconsistent with an encrypted reopen); without the dictionary a
/// dictionary-compressed source cannot be decompressed (and the copy could not
/// be re-compressed to match). [`crate::repair`] fills this from the tree's
/// `Config`; [`salvage_sst`] defaults it to empty (a plain, unencrypted source).
#[derive(Clone, Default)]
pub struct SalvageOptions {
    /// Encryption provider matching the source's at-rest encryption, or `None`
    /// for an unencrypted source.
    pub encryption: Option<Arc<dyn EncryptionProvider>>,
    /// zstd dictionary matching the source's dictionary compression, or `None`
    /// when the source uses no dictionary.
    #[cfg(zstd_any)]
    pub zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    /// The open / decrypt context id for an ENCRYPTED source: block AAD binds
    /// the table identity, so an encrypted source sealed under a non-zero id
    /// only decrypts when the same id is supplied here.
    /// [`crate::repair`] passes the table's real id; the standalone default of
    /// `0` matches an unencrypted or id-`0` encrypted source. An UNENCRYPTED
    /// source needs no id at all — the salvage-mode open reads the stored one
    /// from the metadata — and the recovered copy is always stamped with the
    /// SOURCE's stored id (its identity), never with this field.
    pub table_id: crate::TableId,
    /// The durable table id the caller knows OUT-OF-BAND (the SST file name /
    /// manifest entry), or `None` (the standalone default) when the source's
    /// stored id is the identity. When `Some`, the salvage open cross-checks
    /// the meta payload against it — with fallback to the mirrored MID meta
    /// copy — so a checksum-clean tail meta whose stored id was forged cannot
    /// poison the recovered copy's identity. [`crate::repair`] passes the id
    /// derived from the file name.
    pub expected_stored_id: Option<crate::TableId>,
    /// Opt-in to salvaging a delete-bearing columnar SST whose positional
    /// delete bitmap cannot be applied (the bitmap section is unreadable, or a
    /// readable bitmap's positioning zone map is unreadable). The degraded
    /// recovery emits EVERY row live — positionally-deleted rows are
    /// resurrected in the salvaged copy. `false` (the default) fails such a
    /// salvage closed instead, preserving delete semantics at the cost of
    /// recovering nothing from that SST.
    pub allow_delete_resurrection: bool,
    /// Durability mode for the recovered copy's final sync (file + parent
    /// directory). [`crate::repair`] passes the tree's `Config::sync_mode`,
    /// so a Full-durability repair persists the salvaged SST as strongly as
    /// the manifest it rebuilds around it; the standalone default is
    /// [`crate::fs::SyncMode::Normal`].
    pub sync_mode: crate::fs::SyncMode,
    /// Prefix extractor matching the tree's
    /// [`Config::prefix_extractor`](crate::config::Config::prefix_extractor),
    /// or `None` when the tree indexes no prefixes. The extractor is not
    /// persisted in the SST (it is configuration), so the rebuilt filter can
    /// only carry the source's prefix hashes when the caller supplies it —
    /// without it, prefix scans see the salvaged copy as a false negative
    /// and its matching rows vanish from every prefix read.
    /// [`crate::repair`] passes the tree's configured extractor.
    pub prefix_extractor: Option<Arc<dyn crate::prefix::PrefixExtractor>>,
    /// Per-blob-file handle rewrite applied to every re-emitted indirection
    /// entry, or `None` (the default) to re-emit handles untouched.
    /// [`crate::repair`] fills it after reshaping blob files (a blob salvage
    /// compacted a file, or a punched file's frontier was recovered), so the
    /// rewritten SST's handles land on live records; an entry whose record no
    /// longer exists is DROPPED and counted in
    /// [`SalvageReport::entries_dropped_by_rewrite`]. A set rewrite disables
    /// verbatim block copy-through (raw block bytes would carry stale handles).
    pub blob_rewrite: Option<Arc<crate::HashMap<crate::vlog::BlobFileId, BlobFileRewrite>>>,
    /// Shared live-progress counters the block walk ticks per inspected /
    /// re-emitted / dropped block and per recovered row, or `None` (the
    /// default) to skip publishing. [`crate::repair`] forwards the handle set
    /// via [`Config::with_recovery_progress`](crate::Config::with_recovery_progress);
    /// a standalone [`salvage_sst_with_options`] caller sets it directly and
    /// polls [`RecoveryProgress::snapshot`](crate::RecoveryProgress::snapshot)
    /// from another thread.
    pub progress: Option<Arc<crate::RecoveryProgress>>,
}

/// How a referencing SST's blob handles into ONE blob file must be rewritten
/// when repair reshaped that file. Keyed per blob-file id in
/// [`SalvageOptions::blob_rewrite`].
#[derive(Debug, Clone)]
pub enum BlobFileRewrite {
    /// The blob file was salvaged into a COMPACTED copy: every surviving
    /// record moved (and, when re-compressed, may have changed on-disk size),
    /// per [`BlobSalvageReport::offset_remap`]. A handle whose source offset
    /// is absent points at a record the salvage dropped — its entry is removed
    /// from the rewritten SST (its key then reads as absent instead of
    /// erroring on damaged bytes).
    Remap(crate::HashMap<u64, BlobRecordRelocation>),
    /// The blob file is intact but its consumed prefix was punched at this
    /// live-data frontier: a handle below it (a pre-relocation SST left behind
    /// by a crash) points into zeroed bytes — its entry is removed; handles at
    /// or above the frontier are kept untouched.
    DropBelow(u64),
}

/// Why a block could not be salvaged and had to be dropped.
#[derive(Debug, Clone)]
pub enum DropReason {
    /// The block header failed to decode: corrupt magic, an invalid length, or
    /// a mismatch on the header's own checksum.
    HeaderCorrupted(String),
    /// The data segment did not match the XXH3 checksum stored in its header and
    /// error-correcting codes (when present) could not recover it.
    ChecksumMismatch,
    /// The block could not be read from disk: an I/O error or a truncated tail.
    ReadError(String),
    /// The block verified intact but its entries could not be decoded (an
    /// unexpected format / version inside an otherwise checksum-clean block).
    DecodeError(String),
}

/// A block the salvage walk could not recover, with the key range it covered
/// (when the index can still resolve it) so an operator knows exactly what data
/// the salvaged copy is missing.
#[derive(Debug, Clone)]
pub struct DroppedBlock {
    /// Byte offset of the block within the source SST.
    pub offset: u64,
    /// The SFA section the block belonged to (e.g. `b"data"`).
    pub section: Vec<u8>,
    /// Why the block was dropped.
    pub reason: DropReason,
    /// The block's `[first, last]` user-key range, if the index could still
    /// resolve it; `None` when the index entry for the block is itself lost.
    pub key_range: Option<(UserKey, UserKey)>,
}

/// The outcome of salvaging a single SST.
///
/// Produced by the salvage walk over one source file. Inspect [`is_complete`]
/// to tell a clean recovery (every block re-emitted) from a lossy one (some key
/// ranges dropped); [`dropped`] lists exactly what was lost.
///
/// [`is_complete`]: SalvageReport::is_complete
/// [`dropped`]: SalvageReport::dropped
#[derive(Debug)]
pub struct SalvageReport {
    /// Path of the freshly written salvaged SST, or `None` when no block was
    /// recoverable and nothing was written.
    pub salvaged_path: Option<PathBuf>,
    /// Total data blocks the walk INSPECTED. This is not partitioned by
    /// `blocks_salvaged + dropped.len()`: a columnar block whose rows were all
    /// positionally deleted is inspected and counted here, yet re-emits nothing
    /// (not in `blocks_salvaged`) and loses nothing (not in `dropped`). Derive
    /// completeness from [`is_complete`](Self::is_complete) (`dropped.is_empty()`),
    /// never from `blocks_salvaged == blocks_total`.
    pub blocks_total: usize,
    /// Data blocks successfully re-emitted into the salvaged SST.
    pub blocks_salvaged: usize,
    /// Of [`blocks_salvaged`](Self::blocks_salvaged), how many read back cleanly
    /// (checksum passed without ECC recovery) and were copied through **verbatim**
    /// — their raw on-disk bytes byte-copied, skipping the decode + re-encode +
    /// recompression the rest pay. The remainder
    /// (`blocks_salvaged - blocks_copied_verbatim`) were re-emitted rather than
    /// byte-copied: ECC-recovered blocks (re-encoded from their healed payload)
    /// and, for a columnar SST that carries deletes, its clean blocks too
    /// (re-emitted with the delete mask applied so deleted rows are not
    /// resurrected). A high ratio means a mostly-healthy, delete-free SST was
    /// recovered cheaply.
    pub blocks_copied_verbatim: usize,
    /// Entries recovered into the salvaged SST.
    pub entries_salvaged: u64,
    /// Entries REMOVED by the [`SalvageOptions::blob_rewrite`] handle rewrite
    /// (their blob record was dropped by a blob salvage, or lies below a
    /// punched frontier). Always zero without a configured rewrite. These are
    /// accounted separately from [`dropped`](Self::dropped): the block itself
    /// was healthy, only individual value records no longer exist.
    pub entries_dropped_by_rewrite: u64,
    /// Blocks the walk had to drop, with their key ranges where known.
    pub dropped: Vec<DroppedBlock>,
    /// `true` when the source carried positional deletes that could NOT be
    /// applied faithfully and [`SalvageOptions::allow_delete_resurrection`] let
    /// the salvage proceed anyway: the recovered copy re-emits those rows LIVE,
    /// so positionally-deleted rows came back. Always `false` for a normal
    /// salvage (and when the opt-in is set but no unappliable delete mask was
    /// encountered), so a caller can warn ONLY when resurrection actually
    /// happened.
    pub delete_rows_resurrected: bool,
}

impl SalvageReport {
    /// Returns `true` when no block had to be dropped: every block the walk
    /// inspected was either recovered or carried no live rows, so no key range
    /// was lost.
    ///
    /// This is orthogonal to whether a file was written: a source whose every
    /// block is wholly deleted drops nothing yet recovers nothing, so
    /// `is_complete()` is `true` while [`salvaged_path`](Self::salvaged_path) is
    /// `None`. Always check `salvaged_path` before using the recovered copy.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::salvage::SalvageReport;
    ///
    /// let clean = SalvageReport {
    ///     salvaged_path: None,
    ///     blocks_total: 4,
    ///     blocks_salvaged: 4,
    ///     blocks_copied_verbatim: 4,
    ///     entries_salvaged: 100,
    ///     entries_dropped_by_rewrite: 0,
    ///     dropped: Vec::new(),
    ///     delete_rows_resurrected: false,
    /// };
    /// assert!(clean.is_complete());
    /// ```
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.dropped.is_empty()
    }
}

/// Salvages the readable blocks of the SST at `source` into a fresh SST at
/// `dest`.
///
/// Opens `source` (its metadata, index, and SFA trailer must be intact), walks
/// every data block in key order, re-emits the entries of each block that loads
/// cleanly into a brand-new SST at `dest`, and records the key range of every
/// block it had to drop. The salvaged SST is written through the normal table
/// writer, so it carries fresh per-block checksums, a fresh index, and a fresh
/// filter: a single corrupt source block costs only its own key range, not the
/// whole file.
///
/// The salvaged copy mirrors the source's persisted layout (data + index
/// compression, ECC, restart interval, columnar layout with a regenerated zone
/// map, per-KV checksum footers). A columnar source is recovered as columnar:
/// the recovered rows are transposed back into PAX blocks, so the copy keeps the
/// columnar layout and its zone map (a readable delete-bitmap is applied on
/// read, so the surviving rows are already post-delete and the copy needs no
/// delete-bitmap). Per-field value sub-columns collapse to a single value
/// column in this row round-trip; preserving them verbatim is a separate step.
/// When the delete bitmap CANNOT be applied (an unreadable bitmap section, or a
/// readable bitmap whose positioning zone map is unreadable), the salvage fails
/// closed by default — recovering "all rows live" would resurrect
/// positionally-deleted rows — unless the caller opts in via
/// [`SalvageOptions::allow_delete_resurrection`].
///
/// The positional walk re-emits only point entries, so an SST that carries
/// range tombstones cannot be salvaged without dropping them (which would let
/// lower-level keys they cover reappear after repair). Such a source fails
/// closed rather than salvaging into a copy with broken merge semantics.
///
/// The walk is positional (block-index order): iteration is not
/// comparator-driven, so the recovered entries keep their on-disk order. This
/// entry point opens and rewrites under the default lexicographic comparator;
/// [`crate::repair`] recovers under the tree's configured comparator so a
/// custom-comparator table is rebuilt and reopened consistently.
///
/// # Errors
///
/// Returns an error when `source` cannot be opened at all (its metadata, index,
/// or SFA trailer is unreadable), when it carries range tombstones (salvage
/// fails closed rather than dropping them), when its positional delete bitmap
/// cannot be applied (fails closed rather than resurrecting deleted rows; see
/// [`SalvageOptions::allow_delete_resurrection`]), or when writing `dest`
/// fails. Per-block corruption is not an error: such blocks are dropped and
/// listed in the returned [`SalvageReport`].
///
/// # Examples
///
/// ```no_run
/// use lsm_tree::fs::{Fs, StdFs};
/// use lsm_tree::salvage::salvage_sst;
/// use std::sync::Arc;
///
/// let fs: Arc<dyn Fs> = Arc::new(StdFs);
/// let report = salvage_sst("tables/5".as_ref(), "tables/5.salvaged".into(), &fs)?;
/// if report.is_complete() {
///     println!("fully recovered {} block(s)", report.blocks_salvaged);
/// } else {
///     println!(
///         "recovered {} block(s), dropped {}",
///         report.blocks_salvaged,
///         report.dropped.len(),
///     );
/// }
/// # Ok::<(), lsm_tree::Error>(())
/// ```
pub fn salvage_sst(
    source: &std::path::Path,
    dest: std::path::PathBuf,
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
) -> crate::Result<SalvageReport> {
    salvage_sst_with_options(source, dest, fs, &SalvageOptions::default())
}

/// Salvages `source` into `dest` with an explicit recovery + write context.
///
/// Use this over [`salvage_sst`] to salvage an SST that is encrypted and/or
/// zstd-dictionary compressed: supply the matching [`EncryptionProvider`] and
/// dictionary in `options` so the source can be decrypted / decompressed to read
/// its blocks and the recovered copy is written under the same context. Opens and
/// rewrites under the default lexicographic comparator; [`crate::repair`] uses the
/// tree's configured comparator instead via the crate-internal path.
///
/// # Errors
///
/// As [`salvage_sst`]; additionally fails to open the source when `options` does
/// not carry the encryption / dictionary context the source was written with.
pub fn salvage_sst_with_options(
    source: &std::path::Path,
    dest: std::path::PathBuf,
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    options: &SalvageOptions,
) -> crate::Result<SalvageReport> {
    salvage_with_context(
        source,
        dest,
        fs,
        &crate::comparator::default_comparator(),
        options,
    )
}

/// Salvages `source` into `dest` under a caller-supplied `comparator` and
/// recovery context.
///
/// [`crate::repair`] calls this with the tree's configured comparator and the
/// `Config`'s encryption provider + zstd dictionary, so the rewritten SST opens,
/// orders, and decrypts / decompresses consistently with the rest of the tree;
/// the public entry points wrap it with the default lexicographic comparator.
pub(crate) fn salvage_with_context(
    source: &std::path::Path,
    dest: std::path::PathBuf,
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    comparator: &crate::comparator::SharedComparator,
    options: &SalvageOptions,
) -> crate::Result<SalvageReport> {
    // Arbitrate DIVERGENT meta mirrors. When both copies decode under the
    // expected id but disagree in ANY field, neither is provably genuine: an
    // internally-consistent forged tail (a changed compression tag, a changed
    // columnar descriptor) would make the tail-first open mis-decode every
    // healthy data block and drop it — repair would then quarantine a table
    // whose intact MID mirror recovers everything. Since no copy can be
    // proven authoritative, run the walk under BOTH mirror orders and keep
    // the attempt that recovers more.
    let diverged = meta_mirrors_diverge(source, fs, options)?;
    // Divergent mirrors disable the verbatim copy-through: neither copy is
    // provably genuine, and a divergence confined to a DECODE-TRANSPARENT
    // layout field (a re-stamped restart interval — full block decoding is
    // trailer-driven) would byte-copy blocks whose encoding disagrees with
    // the chosen meta, silently truncating the partial-decode read path's
    // synthesized blocks. Re-encoding under the chosen meta keeps the copy
    // self-consistent whichever mirror wins.
    if !diverged {
        // No arbitration: write directly to `dest`. There is no second attempt,
        // so `dest` is never a live intermediate another process could replace.
        return salvage_attempt(source, dest, fs, comparator, options, false, true);
    }
    // Divergent mirrors: run BOTH attempts to PRIVATE temp paths and publish
    // only the selected winner, so `dest` is never a live intermediate a foreign
    // process could atomically replace and this path then delete. Verbatim
    // copy-through stays disabled — neither mirror is provably genuine.
    let tail_dest = next_arb_temp(fs, &dest)?;
    let tail = salvage_attempt(
        source,
        tail_dest.clone(),
        fs,
        comparator,
        options,
        false,
        false,
    );
    // A tail attempt that saw blocks and dropped nothing cannot be improved on:
    // the tie-break prefers the tail (authoritative-by-convention) copy.
    if let Ok(r) = &tail
        && r.blocks_total > 0
        && r.dropped.is_empty()
    {
        return publish_from_temp(fs, tail, &tail_dest, &dest, options);
    }
    let mid_dest = next_arb_temp(fs, &dest)?;
    let mid = salvage_attempt(
        source,
        mid_dest.clone(),
        fs,
        comparator,
        options,
        true,
        false,
    );

    // Publish the winner from its temp; discard the loser's temp first so no
    // artifact lingers outside the `.healtmp-` sweep namespace, but ONLY when
    // this invocation actually created that temp. An attempt whose `create_new`
    // lost a race to a concurrent salvage returns `AlreadyExists` WITHOUT
    // creating the file, so discarding its path would delete the file the race
    // winner owns (see [`attempt_owns_temp`]).
    match arbitrate_mirrors(&tail, &mid) {
        MirrorArbitration::Propagate => {
            // A transient failure lost only to an INCOMPLETE success; a retry
            // could recover the dropped blocks. Clean up any temp this
            // invocation created, then propagate the transient error so the
            // caller retries the whole salvage.
            if attempt_owns_temp(&tail) {
                discard_partial(fs, &tail_dest);
            }
            if attempt_owns_temp(&mid) {
                discard_partial(fs, &mid_dest);
            }
            // Return whichever attempt raised the transient error.
            if matches!(&tail, Err(crate::Error::Io(e)) if e.kind().is_transient()) {
                tail
            } else {
                mid
            }
        }
        MirrorArbitration::PublishMid => {
            if attempt_owns_temp(&tail) {
                discard_partial(fs, &tail_dest);
            }
            publish_from_temp(fs, mid, &mid_dest, &dest, options)
        }
        MirrorArbitration::PublishTail => {
            if attempt_owns_temp(&mid) {
                discard_partial(fs, &mid_dest);
            }
            publish_from_temp(fs, tail, &tail_dest, &dest, options)
        }
    }
}

/// Whether a salvage attempt left a destination temp for the caller to clean up.
///
/// Ownership is proven by the actual OUTCOME — a present `salvaged_path` — not
/// inferred from the final error kind. An attempt leaves a temp to discard ONLY
/// when it succeeded AND actually wrote one (`Ok` with `salvaged_path == Some`).
/// Every other outcome leaves nothing for the caller to remove:
/// - an error BEFORE `Writer::new` (a checksum / recover / range-tombstone /
///   deletion-guard failure) never created the temp — inferring ownership from
///   the non-`AlreadyExists` error kind would delete a concurrent creator's file
///   on shared storage;
/// - an error AFTER `Writer::new` (a walk / finish failure) already discarded its
///   partial output internally;
/// - an `AlreadyExists` from `create_new` lost the race and created nothing;
/// - an `Ok` attempt that recovered nothing discarded its empty temp too
///   (`salvaged_path == None`).
fn attempt_owns_temp(result: &crate::Result<SalvageReport>) -> bool {
    matches!(result, Ok(r) if r.salvaged_path.is_some())
}

/// Which divergent-mirror attempt to publish, or whether to propagate a
/// transient failure.
#[derive(Debug, PartialEq, Eq)]
enum MirrorArbitration {
    /// Publish the tail (authoritative-by-convention) attempt.
    PublishTail,
    /// Publish the mid attempt.
    PublishMid,
    /// Propagate the transient failure: a retry could recover more than the
    /// incomplete success it would otherwise lose to.
    Propagate,
}

/// Chooses which divergent-mirror attempt to publish, or whether to propagate a
/// transient failure so the caller retries.
///
/// A TRANSIENT failure on one attempt must NOT lose to an INCOMPLETE success on
/// the other: a retry of the transiently-failing mirror could recover the blocks
/// the incomplete winner dropped, so propagate it. A COMPLETE success still wins
/// (a retry cannot improve on it), and a PERSISTENT failure always loses to any
/// success (a retry cannot help there). Between two successes the strictly more
/// complete recovery wins; an exact tie keeps the tail copy (authoritative by
/// convention — the recovered copy re-derives every authoritative field from the
/// re-emitted entries, so the losing mirror's non-derivable metadata never
/// reaches it and the tie-break only chooses which layout drives the re-encode).
fn arbitrate_mirrors(
    tail: &crate::Result<SalvageReport>,
    mid: &crate::Result<SalvageReport>,
) -> MirrorArbitration {
    let is_transient = |r: &crate::Result<SalvageReport>| matches!(r, Err(crate::Error::Io(e)) if e.kind().is_transient());
    let complete = |r: &crate::Result<SalvageReport>| matches!(r, Ok(rep) if rep.is_complete());
    if (is_transient(tail) && !complete(mid)) || (is_transient(mid) && !complete(tail)) {
        return MirrorArbitration::Propagate;
    }
    // Completeness ranking: more recovered blocks first, then more recovered
    // entries (equal block counts can hold different key counts — block sizes
    // vary), then fewer dropped blocks.
    let completeness = |rep: &SalvageReport| {
        (
            rep.blocks_salvaged,
            rep.entries_salvaged,
            core::cmp::Reverse(rep.dropped.len()),
        )
    };
    match (tail, mid) {
        (Err(_), Ok(_)) => MirrorArbitration::PublishMid,
        (_, Err(_)) => MirrorArbitration::PublishTail,
        (Ok(t), Ok(m)) => {
            if completeness(m) > completeness(t) {
                MirrorArbitration::PublishMid
            } else {
                MirrorArbitration::PublishTail
            }
        }
    }
}

/// Probes forward from a process-local counter to a free `.healtmp-{n}` sibling
/// of `dest` for an arbitration attempt's PRIVATE output. The `.healtmp-`
/// namespace is the recovery-owned temp space a hard crash leaves for the next
/// open to sweep. A foreign artifact is never reclaimed (it may belong to a
/// concurrent salvage); termination holds because every probe advances the
/// counter and the on-disk artifacts are finite.
fn next_arb_temp(
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    dest: &std::path::Path,
) -> crate::Result<std::path::PathBuf> {
    static ARB_TMP_SEQ: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);
    // Fold the process id into the HIGH bits so two concurrent standalone salvage
    // processes never select the SAME temp name: without this both process-local
    // counters start at 0, and a loser reading a shared name could `discard_partial`
    // the winner's in-progress output. The result stays a single u64, so recovery
    // still parses and sweeps `{id}.healtmp-{numeric}` on a crash. Pid reuse after
    // exit is handled by the exists-probe / AlreadyExists retry below.
    let pid_hi = u64::from(std::process::id()) << 32;
    loop {
        let counter = ARB_TMP_SEQ.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        let seq = pid_hi | (counter & 0xFFFF_FFFF);
        let candidate = dest.with_extension(alloc::format!("healtmp-{seq}"));
        match fs.exists(&candidate) {
            Ok(false) => return Ok(candidate),
            Ok(true) => {}
            Err(e) => return Err(e.into()),
        }
    }
}

/// Publishes the winning arbitration attempt (`result`, written to `temp`) to
/// `dest` with NO-REPLACE semantics, so a foreign file already at `dest` is
/// never destroyed — the whole reason both attempts write to private temps.
/// `hard_link` claims `dest` atomically (it fails `AlreadyExists` if anything is
/// there); a backend without `hard_link` falls back to a probe-then-rename. On
/// any failure the temp is discarded and the error propagates, so a retry and
/// the repair caller both see `dest` as it was (free, or still owned by whoever
/// holds it). A successful publish is made durable before the temp name drops.
fn publish_from_temp(
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    result: crate::Result<SalvageReport>,
    temp: &std::path::Path,
    dest: &std::path::Path,
    options: &SalvageOptions,
) -> crate::Result<SalvageReport> {
    let already_exists = || {
        crate::Error::Io(crate::io::Error::new(
            crate::io::ErrorKind::AlreadyExists,
            "salvage destination already exists",
        ))
    };
    let mut rep = match result {
        Ok(rep) => rep,
        Err(e) => {
            // An erroring attempt leaves nothing for the caller to remove, so
            // NEVER discard `temp` here (ownership is proven by the OUTCOME, not
            // inferred from the error kind — see [`attempt_owns_temp`]): a
            // failure BEFORE `Writer::new` never created `temp` (deleting it
            // would remove a concurrent creator's file on shared storage, e.g.
            // across PID namespaces with the same numeric id), a failure AFTER
            // `Writer::new` already discarded its own partial internally, and an
            // `AlreadyExists` race loser created nothing.
            return Err(e);
        }
    };
    if rep.salvaged_path.is_none() {
        // The attempt wrote nothing (an empty source, or a failure that left no
        // file): there is nothing to publish.
        return Ok(rep);
    }
    match fs.hard_link(temp, dest) {
        Ok(()) => {}
        Err(e) if e.kind() == crate::io::ErrorKind::AlreadyExists => {
            discard_partial(fs, temp);
            return Err(already_exists());
        }
        Err(e) if e.kind() == crate::io::ErrorKind::Unsupported => {
            // A backend without `hard_link` can still create + rename ordinary
            // files; fall back to a best-effort no-replace publish. This reopens
            // a narrow check-then-rename window, but ONLY on backends that cannot
            // claim a destination atomically at all — every in-tree `Fs`
            // implements `hard_link`.
            match fs.exists(dest) {
                Ok(false) => {
                    if let Err(e) = fs.rename(temp, dest) {
                        discard_partial(fs, temp);
                        return Err(e.into());
                    }
                }
                Ok(true) => {
                    discard_partial(fs, temp);
                    return Err(already_exists());
                }
                Err(e) => {
                    discard_partial(fs, temp);
                    return Err(e.into());
                }
            }
        }
        Err(e) => {
            discard_partial(fs, temp);
            return Err(e.into());
        }
    }
    // `dest` now links the copy, but the new directory entry is a fresh
    // mutation: without its own sync a power loss can leave the manifest
    // referencing a `dest` that survives only under the temp name. Make the
    // entry durable BEFORE dropping the temp. A sync failure removes the
    // just-published copy (and the temp) and propagates, so a retry and the
    // repair caller both see the destination free.
    if let Err(e) = fs.sync_directory_with(entry_directory(dest), options.sync_mode) {
        discard_partial(fs, dest);
        discard_partial(fs, temp);
        return Err(e.into());
    }
    // Durable; drop the temp name (the inode lives on under `dest`). A crash
    // before this leaves the temp in the recovery-owned `.healtmp-` namespace.
    discard_partial(fs, temp);
    rep.salvaged_path = Some(dest.to_path_buf());
    Ok(rep)
}

/// Decodes BOTH meta mirrors under the caller's id/encryption context and
/// reports whether they decode to DIVERGENT contents. Any unreadable copy is
/// `false` — the ordinary tail-with-MID-fallback machinery already covers
/// broken mirrors; arbitration is only for two VALID copies that disagree.
fn meta_mirrors_diverge(
    source: &std::path::Path,
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    options: &SalvageOptions,
) -> crate::Result<bool> {
    // A TRANSIENT open failure must not return Ok(false) (which would skip the
    // dual-attempt arbitration and salvage from the tail view alone): the source
    // is being salvaged, so it EXISTS — a failure to open it is a retryable I/O
    // fault, not a structurally-absent mirror. `Fs::open` only ever fails with an
    // `io::Error`, so propagate it; the caller (repair) then retries instead of
    // publishing a tail-preferred recovery that omits healthy blocks a divergent
    // MID mirror would have kept.
    let mut file = match fs.open(source, &crate::fs::FsOpenOptions::new().read(true)) {
        Ok(f) => f,
        Err(e) => return Err(crate::Error::Io(e)),
    };
    // A TRANSIENT trailer read must not return Ok(false) (which would skip the
    // dual-attempt arbitration and salvage directly from the tail view): a source
    // with divergent mirrors could then be re-encoded under a forged layout.
    // Propagate sfa::Error::Io; reserve Ok(false) for structural unreadability.
    let trailer = match crate::sfa::Reader::from_reader(&mut file) {
        Ok(t) => t,
        Err(crate::sfa::Error::Io(e)) => return Err(crate::Error::Io(e)),
        Err(_) => return Ok(false),
    };
    let Ok(regions) = crate::table::regions::ParsedRegions::parse_from_toc(trailer.toc()) else {
        return Ok(false);
    };
    let Some(mid_handle) = regions.metadata_mid else {
        return Ok(false);
    };
    // Mirror recover_inner's id policy: an encrypted open always binds the
    // caller's AAD id; an unencrypted one uses the out-of-band durable id
    // when the caller knows it (repair), else no cross-check.
    let expected_id = if options.encryption.is_some() {
        Some(options.table_id)
    } else {
        options.expected_stored_id
    };
    let tail = crate::table::meta::ParsedMeta::load_with_handle(
        &*file,
        &regions.metadata,
        expected_id,
        options.encryption.as_deref(),
    );
    let mid = crate::table::meta::ParsedMeta::load_with_handle(
        &*file,
        &mid_handle,
        expected_id,
        options.encryption.as_deref(),
    );
    match (tail, mid) {
        (Ok(t), Ok(m)) => Ok(t != m),
        // Only a TRANSIENT read on either mirror must not masquerade as "mirrors
        // agree" (which would skip the MID recovery attempt and salvage under a
        // possibly forged tail): propagate it so the caller retries.
        (Err(crate::Error::Io(io)), _) | (_, Err(crate::Error::Io(io)))
            if io.kind().is_transient() =>
        {
            Err(crate::Error::Io(io))
        }
        // A structurally unreadable mirror OR a PERSISTENT I/O failure of one
        // mirror is the ordinary tail-with-MID-fallback case (`recover_inner`
        // recovers from the readable copy): arbitration is only for two valid
        // copies that disagree, and a retry cannot make a persistently unreadable
        // mirror decode.
        _ => Ok(false),
    }
}

/// One salvage walk of `source` into `dest` under a fixed meta-mirror order
/// (`prefer_mid_meta`; see [`salvage_with_context`] for the arbitration).
fn salvage_attempt(
    source: &std::path::Path,
    dest: std::path::PathBuf,
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    comparator: &crate::comparator::SharedComparator,
    options: &SalvageOptions,
    prefer_mid_meta: bool,
    allow_verbatim: bool,
) -> crate::Result<SalvageReport> {
    // Digest the source through the injected `Fs`, not `std::fs`: salvage runs
    // over MemFs / fault-injected / routed backends (repair passes its own `fs`),
    // where a direct `std::fs` read would miss the file or hash the wrong bytes.
    // A persistent bad sector fails the whole-file digest, but salvage does not
    // need it: the block walk classifies unreadable blocks itself, and the
    // recovered copy is written under its own fresh digest. Fall back to a
    // placeholder so a bad-sector source still reaches block-level recovery
    // (mirrors the blob salvage path, which also opens with `from_raw(0)`); a
    // healthy source keeps its real digest.
    let checksum = match crate::repair::compute_table_checksum(&**fs, source) {
        Ok(c) => crate::Checksum::from_raw(c),
        Err(_) => crate::Checksum::from_raw(0),
    };
    let cache = Arc::new(crate::cache::Cache::with_capacity_bytes(8 * 1024 * 1024));
    let descriptor = Arc::new(crate::descriptor_table::DescriptorTable::new(64));
    #[cfg(feature = "metrics")]
    let metrics = Arc::new(crate::Metrics::default());

    let table = crate::table::Table::recover_inner(
        source.to_path_buf(),
        checksum,
        0,
        0,
        // The source's table id: encrypted block AAD binds it, so an encrypted
        // source only decrypts when opened under the same id (`0` for the legacy
        // standalone / unencrypted path).
        options.table_id,
        cache,
        Some(descriptor),
        Arc::clone(fs),
        false,
        false,
        // Decrypt / decompress the source with the caller's context: without it an
        // encrypted or dictionary-compressed source cannot be read at all.
        options.encryption.clone(),
        #[cfg(zstd_any)]
        options.zstd_dictionary.clone(),
        comparator.clone(),
        #[cfg(feature = "metrics")]
        metrics,
        // Salvage mode: a corrupt delete-bitmap / missing zone map degrades to
        // "all rows live" instead of failing, so a damaged sidecar still
        // opens. A caller-known durable id (repair) keeps the meta id
        // cross-check live, so a forged tail id falls back to the MID mirror.
        crate::table::RecoveryMode::Salvage {
            expected_id: options.expected_stored_id,
            prefer_mid_meta,
        },
    )?;

    // Fail closed on range tombstones, present OR hidden: the positional walk
    // re-emits only point entries, so salvaging an SST that carries range
    // tombstones would drop them and let lower-level keys they cover reappear
    // after repair (a merge-semantics violation). A re-stamped TOC can also
    // RENAME the range_tombstones section to a recognized name whose block
    // decodes cleanly (an empty `filter`), hiding it from `range_tombstones()`
    // without tripping the degradation flag — but the persisted
    // `range_tombstone_count` still records it, so cross-check the count too.
    // Reject either way until the writer path can re-emit range tombstones.
    if !table.range_tombstones().is_empty() || table.metadata.range_tombstone_count > 0 {
        return Err(crate::Error::FeatureUnsupported(
            "salvage of an SST with range tombstones",
        ));
    }

    // Fail closed when the salvage open DEGRADED a rebuildable side section
    // (filter / filter_tli, seqno bounds, zone map, locator) AND the table
    // exposes NO deletion metadata. A re-stamped TOC can rename a
    // `range_tombstones` / `delete_bitmap` section to one of those names and
    // re-role its block: it passes the byte-level walk AND the tiling check
    // (the catalogue stays uniquely named), and the parsed table reports no
    // deletion, but its CONTENT is not what the name claims, so the open
    // degrades it. Salvage re-derives every such section from the recovered
    // entries, so it would DISCARD the relabeled deletion and re-emit the
    // suppressed rows as live. A genuinely rotted section is indistinguishable
    // from the relabel, so both fail closed; the operator recovers the
    // quarantined original by hand. The signal is purely STRUCTURAL (each
    // section decodes its own bytes, independent of the data blocks), so a
    // corrupt DATA block still salvages. A table that DOES carry a visible
    // deletion (a delete bitmap; range tombstones were rejected above) is
    // exempt: its deletions are accounted for and applied.
    #[cfg(feature = "columnar")]
    let has_visible_deletion = table.has_delete_bitmap_section();
    #[cfg(not(feature = "columnar"))]
    let has_visible_deletion = false;
    if !has_visible_deletion && table.salvage_degraded_a_rebuildable_section() {
        return Err(crate::Error::FeatureUnsupported(
            "salvage of an SST with a degraded rebuildable section that may hide \
             a relabeled deletion",
        ));
    }

    // Fail closed when the on-disk TOC catalogue could CONCEAL a deletion section
    // without degrading any block: an OMITTED, RENAMED, SHADOWED, or gap-leaving
    // `delete_bitmap` / `range_tombstones` entry (behind a re-stamped TOC
    // checksum) leaves the parsed table reporting no deletion while every
    // remaining block still passes its byte-level checks: the relabel guard
    // above only catches a re-roled block whose catalogue stays perfectly tiled.
    // Repair routes such a table to quarantine via this same check, but the
    // standalone `salvage_sst` / CLI path never runs the repair verifier, so a
    // positional walk here would re-emit the suppressed rows as live. A read
    // structural ambiguity grades closed, a transient read PROPAGATES (see
    // [`crate::repair::toc_may_hide_deletions`]) so a flaky probe aborts salvage
    // for a retry rather than refusing a recoverable table. A table with a VISIBLE
    // deletion is exempt: its deletions are applied.
    if !has_visible_deletion && crate::repair::toc_may_hide_deletions(fs, source)? {
        return Err(crate::Error::FeatureUnsupported(
            "salvage of an SST whose TOC may hide a deletion section \
             (an omitted, renamed, or shadowed entry)",
        ));
    }

    // Fail closed when the delete mask cannot be applied FAITHFULLY: the
    // salvage-mode open degraded it (an unreadable bitmap, or a readable
    // bitmap whose zone map was unreadable), or the zone map decodes but its
    // claimed positions do not match the actual per-block row counts (a
    // checksum-repatched tamper that would silently mask the WRONG rows).
    // Emitting "all rows live" instead resurrects positionally-deleted rows,
    // which the caller must explicitly opt into via
    // `allow_delete_resurrection`; under that opt-in the walk re-emits
    // UNMASKED — it never masks against unverified positions.
    #[cfg(feature = "columnar")]
    let delete_mask_unpositionable = table.delete_bitmap_degraded
        // A PRESENT delete_bitmap section that decodes to an EMPTY bitmap is a
        // forge: the writer only emits the section when it holds positions, so a
        // checksum-consistent corruption to empty would otherwise pass as
        // positionable and let the masked path re-emit every deleted row live.
        || (table.has_delete_bitmap_section() && table.delete_bitmap().is_empty())
        // A transient I/O fault while cross-checking the delete positions aborts
        // salvage (propagated so repair retries) rather than being read as a
        // persistent unpositionable mask — which under the default
        // `allow_delete_resurrection == false` would drop the table from the
        // rebuilt manifest a retry could have recovered.
        || (!table.delete_bitmap().is_empty() && !table.delete_positions_verified()?)
        // Authenticate the bitmap CONTENTS, not just its positions: an
        // equal-cardinality substitution (a different, checksum-valid bitmap) is
        // structurally positionable yet masks the WRONG rows. With no original
        // whole-file digest here, the meta-recorded content hash is the only way to
        // catch it; a present section without a matching hash cannot be
        // authenticated, so mask it as unpositionable (fail closed).
        || !table.delete_bitmap_authenticated();
    #[cfg(not(feature = "columnar"))]
    let delete_mask_unpositionable = table.delete_bitmap_degraded;
    if delete_mask_unpositionable && !options.allow_delete_resurrection {
        return Err(crate::Error::InvalidHeader(
            "salvage: the delete bitmap cannot be applied; recovering would resurrect deleted \
             rows (opt in with allow_delete_resurrection)",
        ));
    }

    // The recovered copy is written under the SAME layout as the source —
    // compression, ECC, restart interval, columnar (+ zone map), per-KV
    // checksums (`mirror_from`) — plus the caller's encryption provider and zstd
    // dictionary, so a columnar / encrypted / dictionary source salvages into a
    // faithful copy that reopens under the live tree's `Config` instead of a
    // degraded row-major / plaintext mismatch.
    // The recovered copy is stamped with the SOURCE's stored table id (its
    // identity), not the caller's open/AAD context id: an unencrypted
    // salvage-mode open accepts any stored id (`options.table_id` stays the
    // default 0), and the copy must keep the source's identity so it reopens
    // consistently when swapped in for the original. For an encrypted source
    // the two are necessarily equal (the open's AAD binds the caller's id).
    // A KV-separated source's entries hold ValueHandles into blob files, and
    // blob GC / relocation consults the table's linked_blob_files section to
    // decide whether a blob is still referenced. The SOURCE's list is IGNORED
    // entirely — it is not authoritative in either direction: a forged count
    // word can under-report (hiding a blob GC would then delete) and a forged
    // record can OVER-report an id that exists nowhere (a corrupt reference
    // downstream consumers must never see). The walk derives the copy's links
    // exactly from the indirections of its recovered rows: a dropped block's
    // indirections do not exist in the copy, so no source-only id can ever be
    // needed by it.
    let writer = crate::table::Writer::new(dest.clone(), table.metadata.id, 0, Arc::clone(fs))?
        .mirror_from(
            &table.metadata,
            table.has_zone_map(),
            table.has_seqno_bounds(),
        )
        .use_sync_mode(options.sync_mode)
        // The extractor is configuration (never persisted in the SST), so
        // the rebuilt filter only carries the source's prefix hashes when
        // the caller supplies it.
        .use_prefix_extractor(options.prefix_extractor.clone())
        .use_encryption(options.encryption.clone());
    // Without an extractor, DISABLE the filter entirely rather than rebuild one
    // from complete-key hashes: the source's prefix-indexing intent is
    // unknowable (the extractor is not persisted and cannot be inferred), and a
    // complete-key-only filter answers `maybe_contains_prefix` DEFINITELY-ABSENT
    // for a source that came from a prefix-indexed tree, silently dropping every
    // recovered row from prefix scans. No filter answers "maybe present" (a full
    // block read), which is always correct; the point-lookup speedup is
    // sacrificed for correctness.
    let writer = if options.prefix_extractor.is_none() {
        writer.use_bloom_policy(crate::config::BloomConstructionPolicy::BitsPerKey(0.0))
    } else {
        writer
    };
    #[cfg(zstd_any)]
    let writer = writer.use_zstd_dictionary(options.zstd_dictionary.clone());

    let walk = match salvage_blocks(
        &table,
        writer,
        comparator,
        !delete_mask_unpositionable,
        allow_verbatim,
        options.blob_rewrite.as_deref(),
        options.progress.as_deref(),
    ) {
        Ok(walk) => walk,
        Err(e) => {
            // A `write` / `finish` failure after `Writer::new` created `dest`
            // leaves a partial SST there. Remove it before propagating: in the
            // repair path `dest` is the original table path, so a leftover
            // fragment would be re-opened and re-quarantined on every later run.
            discard_partial(fs, &dest);
            return Err(e);
        }
    };

    let salvaged_path = if walk.wrote {
        Some(dest)
    } else {
        // Nothing recoverable. `Writer::new` already created `dest` and the walk
        // dropped the writer, so remove the empty file: a repair caller would
        // otherwise see a stray broken table file in its place.
        discard_partial(fs, &dest);
        None
    };

    Ok(SalvageReport {
        salvaged_path,
        blocks_total: walk.blocks_total,
        blocks_salvaged: walk.blocks_salvaged,
        blocks_copied_verbatim: walk.blocks_copied_verbatim,
        entries_salvaged: walk.entries_salvaged,
        entries_dropped_by_rewrite: walk.entries_dropped_by_rewrite,
        dropped: walk.dropped,
        // Resurrection happened exactly when the delete mask was unappliable and
        // the opt-in let the walk re-emit unmasked (the fail-closed branch above
        // already returned otherwise).
        delete_rows_resurrected: delete_mask_unpositionable && options.allow_delete_resurrection,
    })
}

/// The tally a [`salvage_blocks`] walk returns: the report counters plus whether
/// a destination file was actually finished (`wrote`), which the caller uses to
/// decide between keeping `dest` and removing the empty placeholder.
struct SalvageWalk {
    blocks_total: usize,
    blocks_salvaged: usize,
    blocks_copied_verbatim: usize,
    entries_salvaged: u64,
    entries_dropped_by_rewrite: u64,
    dropped: Vec<DroppedBlock>,
    wrote: bool,
}

/// Best-effort removal of a destination salvage could not complete (an empty or
/// partially-written SST). A repair caller writes the salvaged copy straight
/// into the original table path, so a leftover fragment there would be
/// re-quarantined on the next run; failure is logged, not propagated, so the
/// original error stays the one the caller sees.
fn discard_partial(fs: &alloc::sync::Arc<dyn crate::fs::Fs>, dest: &std::path::Path) {
    if let Err(e) = fs.remove_file(dest) {
        log::warn!(
            "salvage: could not remove the incomplete destination {}: {e}",
            dest.display(),
        );
    }
}

/// The directory to fsync so `path`'s new directory entry is durable.
///
/// [`std::path::Path::parent`] yields an EMPTY path for a bare relative
/// destination (`Path::new("blob").parent() == Some("")`), which is not a
/// syncable directory: fsyncing it fails and the caller would discard the
/// recovered file it had just written. Map the empty (and absent) parent to the
/// current directory so a bare relative destination still gets its entry synced.
fn entry_directory(path: &std::path::Path) -> &std::path::Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => std::path::Path::new("."),
    }
}

/// Classifies a block load / read failure into a [`DroppedBlock`], distinguishing
/// a bit-rot checksum mismatch from a structural decode error from a raw
/// read / decompress failure, and attaching the block's `(prev_end, end_key]`
/// range as the lower/upper bound of the lost keys.
fn classify_drop(
    e: &crate::Error,
    offset: u64,
    prev_end: Option<&UserKey>,
    end_key: Option<&UserKey>,
) -> DroppedBlock {
    use alloc::format;
    let reason = match e {
        crate::Error::ChecksumMismatch { .. } => DropReason::ChecksumMismatch,
        crate::Error::InvalidHeader(_) | crate::Error::InvalidTag(_) => {
            DropReason::DecodeError(format!("{e:?}"))
        }
        _ => DropReason::ReadError(format!("{e:?}")),
    };
    DroppedBlock {
        offset,
        section: b"data".to_vec(),
        reason,
        // A gap-probed block (omitted by a forged index) has no separator,
        // so its key range is unknown until decode — `None`, like a block
        // whose index entry is lost.
        key_range: end_key.map(|ek| (prev_end.cloned().unwrap_or_else(UserKey::empty), ek.clone())),
    }
}

/// Applies a [`BlobFileRewrite`] set to a block's decoded entries: handles
/// into a remapped blob are re-encoded at their salvaged offset, and entries
/// whose record no longer exists (absent from the remap, or below a punched
/// frontier) are removed and counted. An indirection that fails to decode is
/// corrupt content — the caller drops the whole block, as everywhere else.
/// Runs BEFORE blob-link derivation so the rewritten SST's
/// `linked_blob_files` reflects the NEW handles.
fn rewrite_block_indirections(
    entries: Vec<crate::InternalValue>,
    rewrite: &crate::HashMap<crate::vlog::BlobFileId, BlobFileRewrite>,
    dropped_entries: &mut u64,
) -> crate::Result<Vec<crate::InternalValue>> {
    use crate::coding::{Decode, Encode};

    let mut out = Vec::with_capacity(entries.len());
    for mut entry in entries {
        if entry.key.value_type != crate::ValueType::Indirection {
            out.push(entry);
            continue;
        }
        let mut cursor = &entry.value[..];
        let mut ind = crate::blob_tree::handle::BlobIndirection::decode_from(&mut cursor)?;
        match rewrite.get(&ind.vhandle.blob_file_id) {
            None => out.push(entry),
            Some(BlobFileRewrite::Remap(map)) => match map.get(&ind.vhandle.offset) {
                Some(&relocation) => {
                    // BOTH fields: a live read cross-checks the handle's size
                    // against the frame header, so a stale size from the
                    // source file would reject the salvaged record.
                    ind.vhandle.offset = relocation.offset;
                    ind.vhandle.on_disk_size = relocation.on_disk_size;
                    let mut buf = Vec::new();
                    ind.encode_into(&mut buf)?;
                    entry.value = buf.into();
                    out.push(entry);
                }
                None => *dropped_entries += 1,
            },
            Some(BlobFileRewrite::DropBelow(frontier)) => {
                if ind.vhandle.offset < *frontier {
                    *dropped_entries += 1;
                } else {
                    out.push(entry);
                }
            }
        }
    }
    Ok(out)
}

/// Decodes the [`crate::blob_tree::handle::BlobIndirection`] of every
/// indirection entry in `entries`. An entry TAGGED as an indirection whose
/// value fails to decode is corrupt content the live read path could not
/// follow either — the caller drops the block rather than laundering it into
/// the recovered copy.
fn collect_indirections(
    entries: &[crate::InternalValue],
) -> crate::Result<Vec<crate::blob_tree::handle::BlobIndirection>> {
    use crate::coding::Decode;

    let mut out = Vec::new();
    for entry in entries {
        if entry.key.value_type == crate::ValueType::Indirection {
            let mut cursor = &entry.value[..];
            out.push(crate::blob_tree::handle::BlobIndirection::decode_from(
                &mut cursor,
            )?);
        }
    }
    Ok(out)
}

/// [`collect_indirections`] for a columnar batch: a cheap value-type-column
/// scan first, so the per-row materialization is only paid when the batch
/// actually holds indirections (KV-separated columnar sources are rare).
#[cfg(feature = "columnar")]
fn collect_columnar_indirections(
    batch: &crate::table::columnar::ColumnBatch,
) -> crate::Result<Vec<crate::blob_tree::handle::BlobIndirection>> {
    let tag = u8::from(crate::ValueType::Indirection);
    // Columns are key / seqno / value-type / values...; the value-type column
    // holds one tag byte per row.
    let has_indirections = batch.columns.get(2).is_some_and(|c| c.data.contains(&tag));
    if !has_indirections {
        return Ok(Vec::new());
    }
    let entries = crate::table::columnar::column_batch_to_entries(batch)?;
    collect_indirections(&entries)
}

/// Folds one block's recovered indirections into the walk's derived blob-link
/// map, mirroring the accumulation the live write path does per entry.
fn fold_blob_links(
    derived: &mut crate::HashMap<crate::vlog::BlobFileId, crate::table::writer::LinkedFile>,
    indirections: &[crate::blob_tree::handle::BlobIndirection],
) {
    for ind in indirections {
        derived
            .entry(ind.vhandle.blob_file_id)
            .and_modify(|link| {
                link.bytes += u64::from(ind.size);
                link.on_disk_bytes += u64::from(ind.vhandle.on_disk_size);
                link.len += 1;
            })
            .or_insert_with(|| crate::table::writer::LinkedFile {
                blob_file_id: ind.vhandle.blob_file_id,
                bytes: u64::from(ind.size),
                on_disk_bytes: u64::from(ind.vhandle.on_disk_size),
                len: 1,
            });
    }
}

/// The walk totals already published to a [`crate::RecoveryProgress`] handle,
/// so each [`publish_progress`] call sends only the delta (the shared counters
/// are cumulative across every table of one repair).
#[derive(Default)]
struct PublishedProgress {
    blocks_scanned: usize,
    blocks_recovered: usize,
    blocks_dropped: usize,
    blocks_healed: u64,
    kvs: u64,
    columns: u64,
}

/// Publishes the walk's running totals as deltas against `published`, then
/// records them as published. A `None` handle is a no-op.
#[expect(
    clippy::too_many_arguments,
    reason = "a plain projection of the walk's running totals; bundling them into a struct would only rename the call sites"
)]
fn publish_progress(
    progress: Option<&crate::RecoveryProgress>,
    published: &mut PublishedProgress,
    blocks_scanned: usize,
    blocks_recovered: usize,
    blocks_dropped: usize,
    blocks_healed: u64,
    kvs: u64,
    columns: u64,
) {
    let Some(p) = progress else { return };
    p.add_blocks(
        (blocks_scanned - published.blocks_scanned) as u64,
        (blocks_recovered - published.blocks_recovered) as u64,
        (blocks_dropped - published.blocks_dropped) as u64,
        blocks_healed - published.blocks_healed,
    );
    p.add_rows(kvs - published.kvs, columns - published.columns);
    *published = PublishedProgress {
        blocks_scanned,
        blocks_recovered,
        blocks_dropped,
        blocks_healed,
        kvs,
        columns,
    };
}

/// Walks `table`'s data blocks in index order, re-emitting every block that
/// loads and decodes cleanly into `writer` and recording the rest.
///
/// `apply_delete_mask` gates the delete-masked re-emit of a delete-bearing
/// columnar source: `false` means the mask is unpositionable (degraded bitmap
/// or unverified zone-map positions) and the caller explicitly opted into
/// resurrection — the walk then re-emits every row LIVE rather than masking
/// against unverified positions. Ignored for sources without a delete-bitmap
/// section.
///
/// Consumes `writer`: on success it is finished (when at least one block was
/// emitted) or dropped (when none were). On a `write` / `finish` error the
/// writer is dropped as the error unwinds, so the caller must remove the partial
/// destination it left behind.
#[cfg_attr(
    not(feature = "columnar"),
    expect(
        unused_variables,
        reason = "the delete mask exists only for columnar sources; without the feature the flag has no consumer"
    )
)]
fn salvage_blocks(
    table: &crate::table::Table,
    mut writer: crate::table::Writer,
    comparator: &crate::comparator::SharedComparator,
    apply_delete_mask: bool,
    allow_verbatim: bool,
    blob_rewrite: Option<&crate::HashMap<crate::vlog::BlobFileId, BlobFileRewrite>>,
    progress: Option<&crate::RecoveryProgress>,
) -> crate::Result<SalvageWalk> {
    use crate::table::block::ParsedItem;
    use alloc::format;

    // A handle rewrite invalidates every raw block byte (it carries the old
    // handles), so the verbatim copy-through is disabled for the whole walk.
    let allow_verbatim = allow_verbatim && blob_rewrite.is_none();
    let mut blocks_total = 0usize;
    let mut blocks_salvaged = 0usize;
    let mut blocks_copied_verbatim = 0usize;
    let mut entries_salvaged = 0u64;
    let mut blocks_healed = 0u64;
    let mut columns_salvaged = 0u64;
    let mut entries_dropped_by_rewrite = 0u64;
    let mut published = PublishedProgress::default();
    let mut dropped: Vec<DroppedBlock> = Vec::new();
    // Blob links DERIVED from the recovered entries' indirections, keyed by
    // blob file id — exact for the recovered copy (only emitted rows count).
    // The source's own linked_blob_files section is deliberately not
    // consulted: it is not authoritative in either direction (see the caller).
    let mut derived_blob_links: crate::HashMap<
        crate::vlog::BlobFileId,
        crate::table::writer::LinkedFile,
    > = crate::HashMap::default();
    // Lower bound for a dropped block's range: the previous block's last key,
    // since the index stores each block's last key (so block N covers
    // `(end_key[N-1], end_key[N]]`).
    let mut prev_end: Option<UserKey> = None;

    // Enumerate the index handles first. A corrupt index entry stops the
    // collection after reporting it: once the index stream desyncs, later
    // entries are unknowable. This is NOT the end of recovery — the physical
    // data section is still writer-ordered and self-framing, so the tiling
    // walk below recovers every block the broken enumeration could not reach
    // (a mid-partition rot must not cost the failed and later partitions).
    // Capture (do NOT yet record) a broken enumeration: whether it is data loss
    // depends on the fallback below. When the physical data section is readable,
    // the tiling walk recovers every block INDEPENDENTLY of the index, so a
    // corrupt index partition costs nothing and must not be reported as a
    // dropped block; only when there is no physical fallback (unreadable TOC) is
    // the un-enumerated remainder truly lost.
    let mut indexed: Vec<crate::table::KeyedBlockHandle> = Vec::new();
    let mut index_enum_error: Option<String> = None;
    for handle in table.data_block_handles() {
        match handle {
            Ok(k) => indexed.push(k),
            Err(e) => {
                index_enum_error = Some(format!("{e:?}"));
                break;
            }
        }
    }

    // Walk the PHYSICAL data section regardless of how the index enumeration
    // went. Two failure modes both need it:
    // - A CLEANLY enumerated index can still OMIT a handle (both TLI mirrors
    //   forged to the same truncated list pass every byte-level check and the
    //   mirror comparison), invisible to the open.
    // - A BROKEN index (a rotted leaf partition) yields only a prefix, leaving
    //   the failed and later partitions' blocks unreferenced.
    // The writer emits blocks back-to-back, so the section tiling is the only
    // ground truth: frame each uncovered byte range from its block headers and
    // salvage those blocks too (their end key is unknown until decode); an
    // unframeable gap is reported dropped, never silently skipped. The index
    // handles that DID enumerate still contribute their end keys.
    let mut items: Vec<(crate::table::BlockHandle, Option<UserKey>)> = Vec::new();
    let data_section = {
        let mut file = table
            .fs
            .open(&table.path, &crate::fs::FsOpenOptions::new().read(true))?;
        match crate::sfa::Reader::from_reader(&mut file) {
            Ok(t) => {
                let toc_pos = t.toc_pos();
                t.toc().section(b"data").and_then(|s| {
                    // checked, not saturating: a re-stamped `data` length that
                    // overflows `pos + len` must NOT saturate to a `u64::MAX`
                    // section end — the byte-at-a-time resync loop below would then
                    // probe every nonexistent offset up to it, hanging salvage. A
                    // section that ends past where the TOC begins is equally corrupt
                    // (it would overlap the index / meta / TOC), so require the end
                    // to land at or before `toc_pos`.
                    let end = s.pos().checked_add(s.len())?;
                    (end <= toc_pos).then_some((s.pos(), end))
                })
            }
            // Only a TRANSIENT I/O error re-reading the trailer would silently
            // disable the physical walk that recovers index-omitted blocks: if the
            // index is checksum-consistent but omits a block, salvage would then
            // publish the indexed subset and report a COMPLETE recovery while
            // losing the omitted keys. Abort so the caller retries. A PERSISTENT
            // I/O failure (a retry cannot fix a bad sector / truncated trailer) or
            // a STRUCTURAL trailer error is the genuine "no physical fallback" case
            // (an unreadable TOC): fall back to the index enumeration alone, so the
            // already-open table's indexed blocks stay recoverable.
            Err(crate::sfa::Error::Io(e)) if e.kind().is_transient() => {
                return Err(crate::Error::Io(e));
            }
            Err(_) => None,
        }
    };
    if let Some((section_pos, section_end)) = data_section {
        // One open handle for the WHOLE physical walk: the resync scan steps one
        // byte at a time (block starts are not aligned), so opening the file per
        // probe would make salvage O(section_len) opens instead of O(blocks).
        let probe_file = table
            .fs
            .open(&table.path, &crate::fs::FsOpenOptions::new().read(true))?;
        // The gap walk must accept a candidate only after its PAYLOAD loads, not
        // just its header frame: a header-checksum-valid but FAKE header inside
        // corrupt bytes can declare a forged size that spans real blocks after
        // it. Advancing by that unvalidated size would skip the intact blocks
        // (the later load pass drops only the fake candidate, losing the rest).
        // So fully load each candidate here; the block type matches the SST.
        let probe_block_type = {
            #[cfg(feature = "columnar")]
            {
                if table.metadata.columnar {
                    crate::table::block::BlockType::Columnar
                } else {
                    crate::table::block::BlockType::Data
                }
            }
            #[cfg(not(feature = "columnar"))]
            {
                crate::table::block::BlockType::Data
            }
        };
        // A candidate is REAL only if its header frames AND its payload loads.
        // `Ok(Some)` = a framed, loaded block; `Ok(None)` = structurally not a
        // block, or a PERSISTENTLY unreadable one (a header that did not frame, a
        // payload that did not decode, or a bad-sector read a retry can't fix) —
        // the gap walk treats it as a chain break and drops it. `Err(Io)` is
        // reserved for a TRANSIENT read failure, propagated so a retryable fault
        // is never mis-recorded as a permanently dropped gap.
        let frames_and_loads = |at: u64,
                                to: u64|
         -> crate::Result<Option<crate::table::BlockHandle>> {
            match table.probe_block_handle_in(&*probe_file, at, to) {
                Ok(h) => match table.salvage_load_block(&h, probe_block_type) {
                    Ok(_) => Ok(Some(h)),
                    Err(crate::Error::Io(io)) if io.kind().is_transient() => {
                        Err(crate::Error::Io(io))
                    }
                    Err(_) => Ok(None),
                },
                Err(crate::Error::Io(io)) if io.kind().is_transient() => Err(crate::Error::Io(io)),
                Err(_) => Ok(None),
            }
        };
        // Returns `true` when the gap `[from, to)` tiled CONTIGUOUSLY to `to`
        // (so `to` is a proven boundary), `false` when the chain broke before
        // reaching it.
        let probe_gap = |from: u64,
                         to: u64,
                         items: &mut Vec<(crate::table::BlockHandle, Option<UserKey>)>,
                         dropped: &mut Vec<DroppedBlock>|
         -> crate::Result<bool> {
            // `from` is a TRUSTED boundary: the data section start, or the end
            // of a block whose frame this walk already validated. Blocks that
            // tile CONTIGUOUSLY from it inherit that provenance — each
            // framed-and-loaded block's validated size anchors the next offset.
            // The moment an offset does NOT frame a loadable block, the chain
            // breaks: every later offset is reachable only by byte scanning,
            // which cannot prove a boundary. An uncompressed block can carry a
            // complete checksum-valid SST block inside a user value, so a scan
            // that resynced past a broken header could frame that NESTED forge
            // and re-emit its interior entries as genuine data. Fail closed
            // exactly as the blob resync path does: drop the whole remaining gap
            // rather than emit unanchored candidates. A contiguous intact
            // section still recovers every block; only bytes after a broken
            // boundary are surrendered.
            let mut at = from;
            while at < to {
                if let Some(h) = frames_and_loads(at, to)? {
                    let next = at + u64::from(h.size());
                    // A zero-size frame would not advance `at` (an infinite loop)
                    // and cannot anchor the next offset. Treat it as a broken
                    // boundary: fall through to the drop-and-stop below rather
                    // than spin or emit an unanchored candidate.
                    if next > at {
                        items.push((h, None));
                        at = next;
                        continue;
                    }
                }
                // The contiguous chain broke here (a header that did not frame,
                // or one that framed with a checksum-valid but FAKE size whose
                // payload does not load). Report the unanchored remainder as one
                // dropped region and stop — no resync-and-emit, because a byte
                // scan past this point cannot distinguish an original block
                // start from a nested frame inside a corrupt block's user bytes.
                dropped.push(DroppedBlock {
                    offset: at,
                    section: b"data".to_vec(),
                    reason: DropReason::HeaderCorrupted(
                        "unanchored bytes after a broken block boundary; the tail \
                         cannot be proven to be original block starts"
                            .to_owned(),
                    ),
                    key_range: None,
                });
                return Ok(false);
            }
            Ok(true)
        };
        // Records an indexed handle the walk surrenders because the physical
        // chain broke below its offset: with no proven boundary, a forged index
        // could point it at a checksum-valid frame nested in a corrupt block's
        // value bytes, so its own claimed key range is untrusted too (`None`).
        let drop_unanchored_handle = |off: u64, dropped: &mut Vec<DroppedBlock>| {
            dropped.push(DroppedBlock {
                offset: off,
                section: b"data".to_vec(),
                reason: DropReason::HeaderCorrupted(
                    "indexed block after a broken physical chain has no \
                         authenticated boundary"
                        .to_owned(),
                ),
                key_range: None,
            });
        };
        let mut cursor = section_pos;
        // The tiling below is offset-driven, but the index yields KEY order,
        // which a forged index can decouple from the physical order: an
        // out-of-place handle would be covered twice (once by the gap probe,
        // once by itself), and the duplicate emit would be rejected as an
        // ordering violation — misreporting an intact block as dropped.
        // Offset order equals the writer's key order for the blocks
        // themselves, so re-sorting also keeps the emit order valid.
        indexed.sort_unstable_by_key(|k| *k.as_ref().offset());
        // Whether the index (TLI + its mirror) is TRUSTWORTHY: the mirrors agree,
        // the binary-index pointers authenticate, and the decoded handles TILE
        // their section. A checksum-restamped index that points a handle at a
        // frame nested inside another block's value bytes fails the tiling check
        // (the nested offset overlaps its host block), so a passing verification
        // proves every indexed offset is an ORIGINAL block boundary. With that
        // proof, each indexed offset is its own provenance: a block whose header
        // is corrupt costs only its own keys and the walk recovers the rest by
        // their trusted offsets (block-granular salvage). WITHOUT it, an indexed
        // offset is no more trustworthy than a byte-scanned one — a forged index
        // could point it at a nested frame — so the physical chain is the only
        // provenance and the walk fails closed past the first break.
        // A transient I/O fault while authenticating the index STRUCTURE aborts
        // the walk (propagated up so repair retries) instead of degrading to
        // physical-chain-only provenance: reading `false` here would surrender
        // every indexed block past the first header break, dropping healthy keys
        // the intact TLI could anchor on retry.
        let tli_trusted = table.tli_structure_authenticated()?;
        // Tracks the contiguous physical chain from `section_pos`, consulted only
        // when the index is NOT trusted. Once a boundary breaks there, every
        // later offset is reachable only past unprovable bytes, so it is
        // surrendered rather than trusted.
        let mut chain_anchored = true;
        for keyed in indexed {
            let off = *keyed.as_ref().offset();
            // A handle whose offset is at or beyond the section end points
            // outside the data region (a checksum-repatched / forged index).
            // Probing the gap up to it would scan past the section, potentially
            // to an attacker-controlled u64 (an unbounded hang, and later SST
            // sections framed as data). Skip it; the final gap probe still
            // covers the rest of the section from the cursor.
            if off >= section_end {
                continue;
            }
            // A handle starting inside already-covered bytes (a duplicate or
            // overlapping forge) is skipped: its span was walked physically as
            // part of the anchored prefix.
            if off < cursor {
                continue;
            }
            // Untrusted index only: the chain already broke below this offset,
            // so it has no proven boundary — surrender it (and every one after).
            if !chain_anchored {
                drop_unanchored_handle(off, &mut dropped);
                continue;
            }
            if off > cursor {
                let reached = probe_gap(cursor, off, &mut items, &mut dropped)?;
                if !reached && !tli_trusted {
                    // Untrusted index: the gap up to this handle broke, so `off`
                    // sits past the last proven boundary and is unanchored too.
                    // Surrender it and arm the flag for the rest of the walk.
                    chain_anchored = false;
                    drop_unanchored_handle(off, &mut dropped);
                    continue;
                }
                // The gap tiled cleanly, OR the trusted index proves `off` is an
                // original boundary regardless of the intervening (now dropped)
                // index-omitted bytes. Either way `off` is a boundary.
                cursor = off;
            }
            // Trust the indexed SPAN only after the block's own header
            // confirms it: an oversized forged handle would otherwise
            // advance the cursor past back-to-back blocks the gap walk
            // should discover (the oversized non-ECC frame still decodes
            // its first payload, so nothing later would flag the loss).
            let (handle, end_key) =
                match table.probe_block_handle_in(&*probe_file, off, section_end) {
                    Ok(probed) if probed.size() == keyed.as_ref().size() => {
                        (*keyed.as_ref(), Some(keyed.end_key().clone()))
                    }
                    // The physical frame disagrees: walk the physically framed
                    // block instead (the lying handle's separator is just as
                    // untrusted as its span).
                    Ok(probed) => (probed, None),
                    // Only a TRANSIENT read failure is retryable: propagate it
                    // rather than surrender the block (and, when untrusted, the
                    // whole tail) to a permanent drop over a flaky read. A
                    // PERSISTENT read failure is not fixed by a retry, so it falls
                    // through to the unframeable-header arm below and drops just
                    // this block instead of aborting the whole salvage.
                    Err(crate::Error::Io(io)) if io.kind().is_transient() => {
                        return Err(crate::Error::Io(io));
                    }
                    // The indexed block's header does not frame, so its size is
                    // UNVERIFIED. With a TRUSTED index the block's offset is still
                    // an original boundary, so leave the cursor here and let the
                    // next handle's gap probe frame from it — it records the
                    // corrupt block as a drop and recovers the intact blocks after
                    // it by their trusted offsets (block-granular). With an
                    // UNTRUSTED index the byte range past this unframeable header
                    // cannot be proven to be original block starts (a nested forge
                    // would frame just as well), so surrender this block and every
                    // offset after it.
                    Err(_) => {
                        if !tli_trusted {
                            chain_anchored = false;
                            drop_unanchored_handle(off, &mut dropped);
                        }
                        continue;
                    }
                };
            // Both surviving arms probed the frame within `section_end`, so the
            // block ends there by construction: `off + size <= section_end`,
            // which cannot overflow a `u64` bounded by the validated section.
            let next = (off + u64::from(handle.size())).min(section_end);
            items.push((handle, end_key));
            cursor = cursor.max(next);
        }
        // The trailing gap is probed only while the chain is still anchored:
        // once it broke, the remaining bytes were already surrendered above.
        if chain_anchored && cursor < section_end {
            probe_gap(cursor, section_end, &mut items, &mut dropped)?;
        }
    } else {
        // Unreadable TOC (no physical data section to tile against): walk
        // exactly what the index enumeration gave. Here a broken enumeration IS
        // data loss: with no physical fallback, the un-enumerated handles are
        // unrecoverable, so record the structural index error as a drop.
        if let Some(reason) = index_enum_error {
            dropped.push(DroppedBlock {
                offset: 0,
                section: b"index".to_vec(),
                reason: DropReason::HeaderCorrupted(reason),
                key_range: None,
            });
        }
        for keyed in indexed {
            let handle = *keyed.as_ref();
            items.push((handle, Some(keyed.end_key().clone())));
        }
    }

    // Drops recorded BEFORE the emit loop (a corrupt index entry and every
    // `probe_gap` region: a header-corrupt block the tiling walk resynced past)
    // never enter `items`, so the per-item increment below would miss them.
    // They were still INSPECTED and lost, so count them in the total: without
    // this a header-corrupt block yields `blocks_total == blocks_salvaged`
    // while a block was dropped, reporting full coverage despite the loss.
    blocks_total += dropped.len();

    for (block_handle, end_key) in items {
        // Publish the previous iteration's outcome (top-of-loop so every
        // `continue` path is covered; the totals-vs-published delta form makes
        // the call idempotent per state).
        publish_progress(
            progress,
            &mut published,
            blocks_total,
            blocks_salvaged,
            dropped.len(),
            blocks_healed,
            entries_salvaged,
            columns_salvaged,
        );
        blocks_total += 1;
        let offset = *block_handle.offset();

        // Columnar source: a clean block is byte-copied verbatim — preserving its
        // PAX value sub-columns, zone map, and per-row seqnos without the transpose
        // + recompression a re-encode pays — and an ECC-recovered block is
        // re-emitted from its healed `ColumnBatch`. When the SST carries
        // materialized positional deletes, a verbatim copy would resurrect deleted
        // rows (the bitmap is not carried into the recovered SST), so every block
        // is instead re-emitted as a delete-masked batch. Per-block corruption is
        // isolated either way.
        #[cfg(feature = "columnar")]
        if table.metadata.columnar {
            // A delete-bearing SST (it carries a delete-bitmap section) always
            // takes the re-emit path: byte-copying its blocks verbatim would
            // resurrect positionally-deleted rows (the recovered copy carries no
            // bitmap), and a salvage-mode open degrades a corrupt bitmap to empty,
            // so `delete_bitmap().is_empty()` cannot tell "no deletes" from "deletes
            // whose bitmap was lost". A degraded bitmap still recovers all rows
            // live (the documented salvage degradation) — but never via a verbatim
            // copy. Only a genuinely delete-free SST is eligible for copy-through.
            // The MASKED re-emit additionally requires verified positions
            // (`apply_delete_mask`); an unpositionable mask under the explicit
            // resurrection opt-in re-emits every row live via the unmasked arm.
            if table.has_delete_bitmap_section() && apply_delete_mask {
                // An INDEX-OMITTED block (recovered by the physical gap walk,
                // `end_key` unknown) has no verified delete-start position:
                // `delete_positions_verified` walked only the indexed blocks,
                // and the masked load would treat an unmapped batch as all
                // rows live — permanently resurrecting the rows the bitmap
                // marked there, without the resurrection opt-in. Fail closed
                // per block: drop it and report the loss.
                if end_key.is_none() {
                    dropped.push(classify_drop(
                        &crate::Error::InvalidHeader(
                            "delete positions unverifiable for an index-omitted block",
                        ),
                        offset,
                        prev_end.as_ref(),
                        None,
                    ));
                    continue;
                }
                // Re-emit each block as a delete-masked batch so the recovered copy
                // keeps any (readable) deletes applied.
                match table.load_columnar_block_masked(&block_handle) {
                    Ok(Some(batch)) => {
                        // Handle rewrite on the delete-masked path: same
                        // entry round-trip as the unmasked columnar arm (the
                        // mask is already applied, so only surviving rows are
                        // transformed), with the same sub-column fail-closed.
                        let batch = match blob_rewrite {
                            Some(rw) => {
                                if batch.columns.len() > 4 {
                                    return Err(crate::Error::FeatureUnsupported(
                                        "blob-handle rewrite of a columnar block \
                                         with value sub-columns",
                                    ));
                                }
                                let step = crate::table::columnar::column_batch_to_entries(&batch)
                                    .and_then(|entries| {
                                        rewrite_block_indirections(
                                            entries,
                                            rw,
                                            &mut entries_dropped_by_rewrite,
                                        )
                                    });
                                match step {
                                    Ok(entries) if entries.is_empty() => {
                                        // Every surviving record removed by the
                                        // rewrite: nothing to emit, nothing lost.
                                        prev_end = end_key.or(prev_end);
                                        continue;
                                    }
                                    Ok(entries) => {
                                        match crate::table::columnar::entries_to_column_batch(
                                            &entries,
                                        ) {
                                            Ok(batch) => batch,
                                            Err(e) => {
                                                dropped.push(classify_drop(
                                                    &e,
                                                    offset,
                                                    prev_end.as_ref(),
                                                    end_key.as_ref(),
                                                ));
                                                prev_end = end_key.or(prev_end);
                                                continue;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        dropped.push(classify_drop(
                                            &e,
                                            offset,
                                            prev_end.as_ref(),
                                            end_key.as_ref(),
                                        ));
                                        prev_end = end_key.or(prev_end);
                                        continue;
                                    }
                                }
                            }
                            None => batch,
                        };
                        let rows = u64::from(batch.row_count);
                        // Indirections of the SURVIVING (unmasked) rows,
                        // BEFORE emit: an undecodable indirection is corrupt
                        // content — drop the block, don't launder it.
                        let block_links = match collect_columnar_indirections(&batch) {
                            Ok(links) => links,
                            Err(e) => {
                                dropped.push(classify_drop(
                                    &e,
                                    offset,
                                    prev_end.as_ref(),
                                    end_key.as_ref(),
                                ));
                                prev_end = end_key.or(prev_end);
                                continue;
                            }
                        };
                        // A writer REJECTION (ordering / framing validation,
                        // `InvalidHeader` / `InvalidTag`) is block-local
                        // malformed content — drop the block and keep walking;
                        // destination I/O errors stay hard.
                        match writer.write_columnar_block_verbatim(&batch, comparator) {
                            Ok(_) => {
                                entries_salvaged += rows;
                                blocks_salvaged += 1;
                                columns_salvaged += batch.columns.len() as u64;
                                fold_blob_links(&mut derived_blob_links, &block_links);
                            }
                            Err(
                                e @ (crate::Error::InvalidHeader(_) | crate::Error::InvalidTag(_)),
                            ) => {
                                dropped.push(classify_drop(
                                    &e,
                                    offset,
                                    prev_end.as_ref(),
                                    end_key.as_ref(),
                                ));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    // Wholly-deleted block: nothing to recover, nothing lost.
                    Ok(None) => {}
                    // Only a TRANSIENT read (a masked block re-read after cache
                    // eviction) propagates so repair retries; a persistent I/O
                    // failure or a structural failure drops just this block,
                    // mirroring the unmasked salvage_load_block arm — a truncated
                    // or unreadable block must not sink every intact sibling.
                    Err(crate::Error::Io(io)) if io.kind().is_transient() => {
                        return Err(crate::Error::Io(io));
                    }
                    Err(e) => dropped.push(classify_drop(
                        &e,
                        offset,
                        prev_end.as_ref(),
                        end_key.as_ref(),
                    )),
                }
            } else {
                match table
                    .salvage_load_block(&block_handle, crate::table::block::BlockType::Columnar)
                {
                    // Row materialization validates the batch content (framing,
                    // value-type tags, key invariants) beyond the outer frame
                    // decode. A checksum-consistent block that fails EITHER step
                    // is malformed content — drop this one block and keep
                    // walking, exactly like a row-major block whose entries fail
                    // to decode. Only writer errors (I/O to the destination)
                    // stay hard errors.
                    Ok(mut sb) => {
                        let ecc_healed = sb.ecc_recovered;
                        if !allow_verbatim {
                            sb.verbatim = None;
                        }
                        match crate::table::columnar::ColumnBatch::decode(&sb.block.data).and_then(
                            |batch| {
                                crate::table::columnar::column_batch_to_entries(&batch)
                                    .map(|entries| (batch, entries))
                            },
                        ) {
                            // A real writer never emits an empty data block, so a
                            // checksum-clean ZERO-ROW batch is malformed input:
                            // the writer primitives below would emit NOTHING for
                            // it, and counting it as salvaged would misreport an
                            // unrecovered block (an SST whose only block is empty
                            // would even report a salvaged path that the
                            // empty-table finish just removed).
                            Ok((batch, _)) if batch.row_count == 0 => {
                                dropped.push(classify_drop(
                                    &crate::Error::InvalidHeader("columnar: zero-row data block"),
                                    offset,
                                    prev_end.as_ref(),
                                    end_key.as_ref(),
                                ));
                            }
                            Ok((batch, entries)) => {
                                // Apply the blob-handle rewrite by round-tripping
                                // through the row entries, then rebuilding the
                                // batch, so the emitted block carries the NEW
                                // handles. A batch with value SUB-COLUMNS cannot
                                // round-trip (they are not reconstructible from
                                // entries) — a KV-separated columnar flush never
                                // writes them (indirections live in the opaque
                                // value column), so fail closed on the exotic
                                // combination instead of silently dropping data.
                                let (batch, entries) = match blob_rewrite {
                                    Some(rw) => {
                                        if batch.columns.len() > 4 {
                                            return Err(crate::Error::FeatureUnsupported(
                                                "blob-handle rewrite of a columnar block \
                                                 with value sub-columns",
                                            ));
                                        }
                                        let entries = match rewrite_block_indirections(
                                            entries,
                                            rw,
                                            &mut entries_dropped_by_rewrite,
                                        ) {
                                            Ok(entries) => entries,
                                            Err(e) => {
                                                dropped.push(classify_drop(
                                                    &e,
                                                    offset,
                                                    prev_end.as_ref(),
                                                    end_key.as_ref(),
                                                ));
                                                prev_end = end_key.or(prev_end);
                                                continue;
                                            }
                                        };
                                        if entries.is_empty() {
                                            // Every record removed by the rewrite:
                                            // nothing to emit, nothing lost.
                                            prev_end = end_key.or(prev_end);
                                            continue;
                                        }
                                        match crate::table::columnar::entries_to_column_batch(
                                            &entries,
                                        ) {
                                            Ok(batch) => (batch, entries),
                                            Err(e) => {
                                                dropped.push(classify_drop(
                                                    &e,
                                                    offset,
                                                    prev_end.as_ref(),
                                                    end_key.as_ref(),
                                                ));
                                                prev_end = end_key.or(prev_end);
                                                continue;
                                            }
                                        }
                                    }
                                    None => (batch, entries),
                                };
                                let rows = u64::from(batch.row_count);
                                // Indirections BEFORE emit: an entry tagged as an
                                // indirection whose value fails to decode is
                                // corrupt content — drop the block rather than
                                // laundering it into the copy.
                                let block_links = match collect_indirections(&entries) {
                                    Ok(links) => links,
                                    Err(e) => {
                                        dropped.push(classify_drop(
                                            &e,
                                            offset,
                                            prev_end.as_ref(),
                                            end_key.as_ref(),
                                        ));
                                        prev_end = end_key.or(prev_end);
                                        continue;
                                    }
                                };
                                // A delete-bearing SST is never byte-copied, even
                                // on this unmasked (resurrection opt-in) arm: the
                                // re-encode keeps the recovered copy's layout
                                // consistent with the degraded-bitmap path.
                                let verbatim_source = if table.has_delete_bitmap_section() {
                                    None
                                } else {
                                    sb.verbatim
                                };
                                // A writer REJECTION (ordering / framing validation)
                                // is block-local malformed content — drop the block
                                // and keep walking; destination I/O errors stay hard.
                                let emitted = match verbatim_source {
                                    // Clean: copy the block's raw bytes as-is,
                                    // carrying the block's per-column zone-map
                                    // stats (this is the columnar path, so the
                                    // synthetic row-block stat would be rejected
                                    // by the copy's own `verify_zone_map`).
                                    Some((raw, header, layout)) => writer
                                        .append_verbatim_data_block(
                                            &raw,
                                            header,
                                            layout,
                                            &entries,
                                            Some(batch.zone_stats()),
                                            comparator,
                                        )
                                        .map(|_| true),
                                    // ECC-recovered (or delete-bearing): re-encode the
                                    // batch so the recovered copy carries clean bytes.
                                    None => writer
                                        .write_columnar_block_verbatim(&batch, comparator)
                                        .map(|_| false),
                                };
                                match emitted {
                                    Ok(verbatim) => {
                                        if verbatim {
                                            blocks_copied_verbatim += 1;
                                        }
                                        if ecc_healed {
                                            blocks_healed += 1;
                                        }
                                        entries_salvaged += rows;
                                        blocks_salvaged += 1;
                                        columns_salvaged += batch.columns.len() as u64;
                                        fold_blob_links(&mut derived_blob_links, &block_links);
                                    }
                                    Err(
                                        e @ (crate::Error::InvalidHeader(_)
                                        | crate::Error::InvalidTag(_)),
                                    ) => {
                                        dropped.push(classify_drop(
                                            &e,
                                            offset,
                                            prev_end.as_ref(),
                                            end_key.as_ref(),
                                        ));
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                            // Only a transient I/O read is retryable and propagates,
                            // so a partial columnar table is not published with
                            // healthy rows permanently lost. A persistent or
                            // structural failure drops just this block (truncation /
                            // an unreadable block must not sink its intact siblings).
                            Err(crate::Error::Io(io)) if io.kind().is_transient() => {
                                return Err(crate::Error::Io(io));
                            }
                            Err(e) => {
                                dropped.push(classify_drop(
                                    &e,
                                    offset,
                                    prev_end.as_ref(),
                                    end_key.as_ref(),
                                ));
                            }
                        }
                    }
                    // Same transient-only I/O propagation for the delete-masked arm.
                    Err(crate::Error::Io(io)) if io.kind().is_transient() => {
                        return Err(crate::Error::Io(io));
                    }
                    Err(e) => dropped.push(classify_drop(
                        &e,
                        offset,
                        prev_end.as_ref(),
                        end_key.as_ref(),
                    )),
                }
            }
            prev_end = end_key.or(prev_end);
            continue;
        }

        // Row source: a clean block is byte-copied verbatim; an ECC-recovered block
        // is re-emitted entry by entry from its healed payload.
        match table.salvage_load_block(&block_handle, crate::table::block::BlockType::Data) {
            Ok(mut sb) => {
                let ecc_healed = sb.ecc_recovered;
                if !allow_verbatim {
                    sb.verbatim = None;
                }
                // Footer presence is a per-SST property (`kv_checksum_algo`), not a
                // per-block header flag, so the descriptor supplies it here.
                let has_kv_footer = table.metadata.kv_checksum_algo.is_some();
                // Verify the per-KV digests BEFORE stripping the footer: a
                // block-checksum-re-stamped entry whose stored digest no
                // longer matches its bytes would otherwise be recovered (even
                // byte-copied verbatim) into a copy the live per-KV scrub
                // rejects — laundering the corruption. A mismatch is
                // block-local malformed content: drop the block, keep walking.
                if has_kv_footer
                    && let Err(e) = crate::table::DataBlock::verify_kv_checked(
                        &sb.block.data,
                        sb.block.header,
                        comparator.clone(),
                        table.metadata.kv_checksum_algo,
                    )
                {
                    dropped.push(classify_drop(
                        &e,
                        offset,
                        prev_end.as_ref(),
                        end_key.as_ref(),
                    ));
                    prev_end = end_key.or(prev_end);
                    continue;
                }
                match crate::table::DataBlock::from_loaded(sb.block, has_kv_footer) {
                    // `try_iter`, not `iter`: a checksum-clean but structurally
                    // malformed block (e.g. an invalid trailer) must be reported as
                    // a dropped `DecodeError`, never panic the salvage walk.
                    Ok(data_block) => match data_block.try_iter(comparator.clone()) {
                        Ok(iter) => {
                            let entries: Vec<crate::InternalValue> =
                                iter.map(|p| p.materialize(data_block.as_slice())).collect();
                            // A real writer never emits an empty data block, so
                            // checksum-clean ZERO entries are malformed input:
                            // the emit below would write nothing, and counting
                            // the block as salvaged would misreport it (see the
                            // columnar zero-row arm above).
                            if entries.is_empty() {
                                dropped.push(classify_drop(
                                    &crate::Error::InvalidHeader(
                                        "row block decodes to zero entries",
                                    ),
                                    offset,
                                    prev_end.as_ref(),
                                    end_key.as_ref(),
                                ));
                                prev_end = end_key.or(prev_end);
                                continue;
                            }
                            // The entry decoder turns a mid-stream parse
                            // failure into an ordinary end of iteration, so a
                            // checksum-clean block with a valid prefix and a
                            // malformed tail yields FEWER entries than its
                            // trailer declares. Accepting the prefix would
                            // silently lose the remaining keys (or byte-copy
                            // the still-malformed block verbatim) — drop the
                            // block instead.
                            if entries.len() != data_block.len() {
                                dropped.push(classify_drop(
                                    &crate::Error::InvalidHeader(
                                        "row block iterates to fewer entries than its \
                                         trailer declares",
                                    ),
                                    offset,
                                    prev_end.as_ref(),
                                    end_key.as_ref(),
                                ));
                                prev_end = end_key.or(prev_end);
                                continue;
                            }
                            // Apply the blob-handle rewrite BEFORE deriving
                            // links or emitting: removed entries must not
                            // count as salvaged, and the derived links must
                            // reflect the NEW handles. An undecodable
                            // indirection is corrupt content — drop the block,
                            // like the link derivation below.
                            let entries = match blob_rewrite {
                                Some(rw) => match rewrite_block_indirections(
                                    entries,
                                    rw,
                                    &mut entries_dropped_by_rewrite,
                                ) {
                                    Ok(entries) => entries,
                                    Err(e) => {
                                        dropped.push(classify_drop(
                                            &e,
                                            offset,
                                            prev_end.as_ref(),
                                            end_key.as_ref(),
                                        ));
                                        prev_end = end_key.or(prev_end);
                                        continue;
                                    }
                                },
                                None => entries,
                            };
                            if entries.is_empty() {
                                // Every entry's record was removed by the
                                // rewrite: the block legitimately contributes
                                // nothing (accounted per entry, not a loss).
                                prev_end = end_key.or(prev_end);
                                continue;
                            }
                            let count = entries.len() as u64;
                            // Indirections BEFORE emit: an entry tagged as an
                            // indirection whose value fails to decode is
                            // corrupt content — drop the block rather than
                            // laundering it into the copy.
                            let block_links = match collect_indirections(&entries) {
                                Ok(links) => links,
                                Err(e) => {
                                    dropped.push(classify_drop(
                                        &e,
                                        offset,
                                        prev_end.as_ref(),
                                        end_key.as_ref(),
                                    ));
                                    prev_end = end_key.or(prev_end);
                                    continue;
                                }
                            };
                            // Ordering guard for BOTH emit paths: the verbatim
                            // append validates internally, but the row-by-row
                            // re-emit (`writer.write`) trusts its input, so a
                            // tampered checksum-repatched block must be caught
                            // here. A validation rejection is block-local
                            // malformed content — drop the block and keep
                            // walking; destination I/O errors stay hard.
                            let emitted = writer
                                .validate_direct_block_order(&entries, comparator)
                                .and_then(|()| {
                                    if let Some((raw, header, layout)) = sb.verbatim {
                                        writer
                                            .append_verbatim_data_block(
                                                &raw, header, layout, &entries, None, comparator,
                                            )
                                            .map(|_| true)
                                    } else {
                                        for e in entries {
                                            writer.write(e)?;
                                        }
                                        Ok(false)
                                    }
                                });
                            match emitted {
                                Ok(verbatim) => {
                                    if verbatim {
                                        blocks_copied_verbatim += 1;
                                    }
                                    if ecc_healed {
                                        blocks_healed += 1;
                                    }
                                    entries_salvaged += count;
                                    blocks_salvaged += 1;
                                    fold_blob_links(&mut derived_blob_links, &block_links);
                                }
                                Err(
                                    e @ (crate::Error::InvalidHeader(_)
                                    | crate::Error::InvalidTag(_)),
                                ) => {
                                    dropped.push(classify_drop(
                                        &e,
                                        offset,
                                        prev_end.as_ref(),
                                        end_key.as_ref(),
                                    ));
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(e) => dropped.push(DroppedBlock {
                            offset,
                            section: b"data".to_vec(),
                            reason: DropReason::DecodeError(format!("{e:?}")),
                            key_range: end_key.as_ref().map(|ek| {
                                (prev_end.clone().unwrap_or_else(UserKey::empty), ek.clone())
                            }),
                        }),
                    },
                    Err(e) => dropped.push(DroppedBlock {
                        offset,
                        section: b"data".to_vec(),
                        reason: DropReason::DecodeError(format!("{e:?}")),
                        key_range: end_key.as_ref().map(|ek| {
                            (prev_end.clone().unwrap_or_else(UserKey::empty), ek.clone())
                        }),
                    }),
                }
            }
            // Only a TRANSIENT I/O error on the block read is retryable: dropping
            // the block and finishing dest would let repair install a partial
            // replacement missing keys a retry would recover, so abort and let the
            // caller discard the partial dest. A PERSISTENT I/O failure (a
            // bad-sector `Other` / EIO, `PermissionDenied`) or a truncated final
            // block (`UnexpectedEof`) is not fixed by a retry, so it drops just
            // this block — a persistently-unreadable tail block must not prevent
            // every intact earlier block from being recovered.
            Err(crate::Error::Io(io)) if io.kind().is_transient() => {
                return Err(crate::Error::Io(io));
            }
            Err(e) => dropped.push(classify_drop(
                &e,
                offset,
                prev_end.as_ref(),
                end_key.as_ref(),
            )),
        }
        prev_end = end_key.or(prev_end);
    }
    // Final publish: the last iteration's outcome (the loop publishes at the
    // TOP, so the tail is otherwise unaccounted).
    publish_progress(
        progress,
        &mut published,
        blocks_total,
        blocks_salvaged,
        dropped.len(),
        blocks_healed,
        entries_salvaged,
        columns_salvaged,
    );

    let wrote = blocks_salvaged > 0;
    if wrote {
        // Blob links: EXACTLY the derived map. A dropped block's indirections
        // do not exist in the copy, so no id beyond the recovered rows can be
        // needed — and copying a source-only id would let a forged record
        // plant a reference to a blob that exists nowhere.
        let mut links: Vec<crate::table::writer::LinkedFile> =
            derived_blob_links.into_values().collect();
        // Deterministic section order regardless of hash-map iteration.
        links.sort_unstable_by_key(|l| l.blob_file_id);
        for link in links {
            writer.link_blob_file(link.blob_file_id, link.len, link.bytes, link.on_disk_bytes);
        }
        writer.finish()?;
    } else {
        drop(writer);
    }

    Ok(SalvageWalk {
        blocks_total,
        blocks_salvaged,
        blocks_copied_verbatim,
        entries_salvaged,
        entries_dropped_by_rewrite,
        dropped,
        wrote,
    })
}

/// Why a blob (vlog) record could not be salvaged.
#[derive(Debug, Clone)]
pub enum BlobDropReason {
    /// The record's stored checksum did not match its key + value bytes
    /// (bit-rot). The walk re-syncs at the next record, so only this record is
    /// lost.
    ChecksumMismatch,
    /// A structural failure (bad frame magic, header CRC, or a frame that runs
    /// past the data section) that desynchronizes the record stream: the walk
    /// cannot locate later records and stops at this point.
    Corrupt(String),
}

/// A blob record the salvage walk could not recover.
#[derive(Debug, Clone)]
pub struct DroppedBlob {
    /// Why the record was dropped.
    pub reason: BlobDropReason,
}

/// The outcome of salvaging a single blob (vlog) file.
///
/// Inspect [`is_complete`](BlobSalvageReport::is_complete) to tell a clean
/// recovery (every record re-emitted) from a lossy one; [`dropped`] lists what
/// was lost. Always check [`salvaged_path`] before using the recovered copy.
///
/// [`dropped`]: BlobSalvageReport::dropped
/// [`salvaged_path`]: BlobSalvageReport::salvaged_path
#[derive(Debug)]
pub struct BlobSalvageReport {
    /// Path of the freshly written salvaged blob file, or `None` when no record
    /// was recoverable and nothing was written.
    pub salvaged_path: Option<PathBuf>,
    /// Total records the walk inspected (recovered plus dropped).
    pub records_total: usize,
    /// Records successfully re-emitted into the salvaged blob file.
    pub records_salvaged: usize,
    /// `(source_offset, relocation)` for every re-emitted record, in walk
    /// order. The salvaged file is written COMPACTED — after the first dropped
    /// record every later record lands at a NEW offset, and a compressed
    /// record is re-compressed, so its on-disk size may change too — so
    /// existing SST entries whose `ValueHandle`
    /// points into the source file must be rewritten through this table
    /// (BOTH fields; a live read cross-checks the handle against the frame
    /// header) before the salvaged file can replace the original under the
    /// same id. A source offset absent from this map (and implied by
    /// [`Self::dropped`]) is lost: its handle has no target.
    pub offset_remap: Vec<(u64, BlobRecordRelocation)>,
    /// Records the walk had to drop.
    pub dropped: Vec<DroppedBlob>,
}

impl BlobSalvageReport {
    /// Returns `true` when no record had to be dropped.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.dropped.is_empty()
    }
}

/// Where one blob record landed in the salvaged replacement file.
///
/// Carried per source offset in [`BlobSalvageReport::offset_remap`]. Both
/// fields must be installed into a referencing SST's rewritten
/// `ValueHandle`: the compacted rewrite moves the
/// record, and the decompress + re-compress re-emit may change its on-disk
/// size (compressor output is not stable across versions or implementations),
/// while a live read cross-checks the handle's size against the frame header
/// and rejects a mismatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobRecordRelocation {
    /// Offset of the re-emitted record in the salvaged file.
    pub offset: u64,
    /// On-disk size of the re-emitted (possibly re-compressed) value.
    pub on_disk_size: u32,
}

/// Decompresses one blob record's on-disk bytes, mirroring the live reader's
/// per-compression dispatch: the frame checksum covered the ON-DISK bytes, so
/// a clean-checksum record must additionally prove its content round-trips —
/// the salvage re-emit and repair's frame validation both gate on this.
/// `None` verifies the length equality the uncompressed format guarantees.
/// Dictionary compression decodes only when the caller supplies the matching
/// dictionary (repair has the tree's config); the standalone salvage entry
/// passes none and rejects dictionary sources up front.
pub(crate) fn decompress_blob_value<'a>(
    compression: crate::CompressionType,
    on_disk: &'a [u8],
    real_len: usize,
    #[cfg(zstd_any)] zstd_dictionary: Option<&crate::compression::ZstdDictionary>,
) -> crate::Result<alloc::borrow::Cow<'a, [u8]>> {
    match compression {
        crate::CompressionType::None => {
            if on_disk.len() != real_len {
                return Err(crate::Error::InvalidHeader("Blob"));
            }
            Ok(alloc::borrow::Cow::Borrowed(on_disk))
        }
        #[cfg(feature = "lz4")]
        crate::CompressionType::Lz4 => {
            let mut buf = alloc::vec![0u8; real_len];
            let written = lz4_flex::block::decompress_into(on_disk, &mut buf)
                .map_err(|_| crate::Error::Decompress(compression))?;
            if written != real_len {
                return Err(crate::Error::Decompress(compression));
            }
            Ok(alloc::borrow::Cow::Owned(buf))
        }
        #[cfg(zstd_any)]
        crate::CompressionType::Zstd(_) => {
            use crate::compression::CompressionProvider as _;
            let decompressed = crate::compression::ZstdBackend::decompress(on_disk, real_len)
                .map_err(|_| crate::Error::Decompress(compression))?;
            if decompressed.len() != real_len {
                return Err(crate::Error::Decompress(compression));
            }
            Ok(alloc::borrow::Cow::Owned(decompressed))
        }
        #[cfg(zstd_any)]
        crate::CompressionType::ZstdDict { dict_id, .. } => {
            use crate::compression::CompressionProvider as _;
            let Some(dict) = zstd_dictionary else {
                return Err(crate::Error::FeatureUnsupported(
                    "salvage of a dictionary-compressed blob file",
                ));
            };
            if dict.id() != dict_id {
                return Err(crate::Error::ZstdDictMismatch {
                    expected: dict_id,
                    got: Some(dict.id()),
                });
            }
            let decompressed =
                crate::compression::ZstdBackend::decompress_with_dict(on_disk, dict, real_len)
                    .map_err(|_| crate::Error::Decompress(compression))?;
            if decompressed.len() != real_len {
                return Err(crate::Error::Decompress(compression));
            }
            Ok(alloc::borrow::Cow::Owned(decompressed))
        }
    }
}

/// Whether `entry`'s internal key sorts BEFORE `prev` under the blob-file order
/// (ascending user key, ties broken by DESCENDING seqno, newest first). A `None`
/// `prev` (the first accepted record) never regresses. The user-key ordering uses
/// the tree's `comparator`, not raw bytes: a KV-separated tree may be configured
/// with a reverse or otherwise non-lexicographic comparator, and a valid blob
/// file from such a tree must not be judged as regressing.
pub(crate) fn blob_key_regresses(
    comparator: &crate::comparator::SharedComparator,
    prev: Option<&(crate::UserKey, crate::SeqNo)>,
    entry: &crate::vlog::blob_file::scanner::ScanEntry,
) -> bool {
    let Some((prev_key, prev_seqno)) = prev else {
        return false;
    };
    match comparator.compare(entry.key.as_ref(), prev_key.as_ref()) {
        core::cmp::Ordering::Less => true,
        // Same user key: newest-first means the higher seqno comes first, so a
        // seqno ABOVE the previous one at an equal key is out of order.
        core::cmp::Ordering::Equal => entry.seqno > *prev_seqno,
        core::cmp::Ordering::Greater => false,
    }
}

/// Salvages the readable records of the blob (vlog) file at `source` into a fresh
/// blob file at `dest`.
///
/// Where [`crate::repair`] rebuilds the blob-file *manifest* around whole files,
/// this walks one blob file record by record and re-emits every record whose
/// checksum verifies, recording the rest. Corruption that leaves the frame
/// header intact (a checksum mismatch, or a header-CRC / structural break) makes
/// the record stream re-synchronize at the next frame magic. That magic (and
/// every frame chained after it) has an UNPROVEN boundary (it may be nested in
/// the damaged frame's user bytes), so the walk drops the ENTIRE tail past the
/// first resync (fail closed): the conservative loss is more than one record
/// (as much as everything after the first damage), because a fabricated chain of
/// checksum-valid frames is byte-for-byte indistinguishable from genuine ones
/// and re-emitting it would forge records. Only the records BEFORE the first
/// resync are re-emitted; a genuine truncation (a frame running past the data
/// section) still terminates the walk cleanly.
///
/// `blob_file_id` is the source's id (its file name), recorded in the recovered
/// file's metadata. The recovered file is written with no value compression, so
/// a **compressed** source is rejected with [`Error::FeatureUnsupported`] rather
/// than re-emitted under a mismatched descriptor (the scanner yields on-disk
/// bytes; faithfully recompressing them is a separate step).
///
/// The salvaged file is written COMPACTED: after the first dropped record,
/// every later record lands at a new offset, so it is **not a drop-in
/// replacement** for the source while SST entries still hold
/// `ValueHandle::offset` values into it. Re-target those handles through
/// [`BlobSalvageReport::offset_remap`] first; a source offset absent from the
/// map is a lost record.
///
/// `live_data_start` is the source's tight-space live-data frontier (`0` for a
/// whole, unreclaimed file): the walk starts there, so a punched prefix — which
/// reads as zeros and would otherwise rot the first header, arm the sticky
/// resync taint, and surrender the entire LIVE suffix — is never inspected.
/// Records below the frontier are dead by definition (their relocated copies
/// live elsewhere), so skipping them loses nothing.
///
/// [`Error::FeatureUnsupported`]: crate::Error::FeatureUnsupported
///
/// # Errors
///
/// Returns an error when `source` cannot be opened at all (its metadata / SFA
/// trailer is unreadable), when it is a compressed blob file, or when writing
/// `dest` fails. Per-record corruption is not an error: such records are dropped
/// and listed in the returned [`BlobSalvageReport`].
pub fn salvage_blob_file(
    source: &std::path::Path,
    dest: std::path::PathBuf,
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    blob_file_id: crate::vlog::BlobFileId,
    comparator: &crate::comparator::SharedComparator,
    live_data_start: u64,
    #[cfg(zstd_any)] zstd_dictionary: Option<&alloc::sync::Arc<crate::compression::ZstdDictionary>>,
) -> crate::Result<BlobSalvageReport> {
    use crate::vlog::blob_file::{scanner::Scanner, writer::Writer as BlobWriter};
    use alloc::format;

    // Read the source's metadata (only the TOC + meta section, NOT the data
    // section, so a data-corrupt file still opens) for its compression type:
    // the scanner yields on-disk bytes, so a compressed record is DECOMPRESSED
    // here (proving it round-trips) and re-emitted through a writer stamped
    // with the same compression — never copied through verbatim under a
    // mismatched descriptor. A DICTIONARY-compressed source decodes only when
    // the caller supplies the matching dictionary (manifest repair passes the
    // tree's configured one); without it this entry fails closed, the same
    // way SST salvage fails closed on range tombstones.
    //
    // A placeholder checksum is passed on purpose: `recover_blob_file` only STORES
    // it (it never verifies the source against it), and only `compression()` is
    // read from the handle. Computing the real whole-file digest here would stream
    // every byte — including a persistently unreadable later frame or truncated
    // tail sector — and abort before the scanner and writer are even created,
    // making the valid-prefix recovery below unreachable. The salvaged dest gets
    // its own digest on finish.
    let source_handle =
        crate::vlog::recover_blob_file(source, blob_file_id, crate::Checksum::from_raw(0), 0, fs)?;
    let compression = source_handle.compression();
    #[cfg(zstd_any)]
    if matches!(compression, crate::CompressionType::ZstdDict { .. }) && zstd_dictionary.is_none() {
        return Err(crate::Error::FeatureUnsupported(
            "salvage of a dictionary-compressed blob file without its dictionary",
        ));
    }

    let scanner = if live_data_start > 0 {
        Scanner::resume(source, &**fs, blob_file_id, live_data_start)?
    } else {
        Scanner::new(source, &**fs, blob_file_id)?
    };
    // Destination ownership is decided by the writer's `create_new` open, and
    // the CONSTRUCTOR owns cleanup of any partial file it created: on a
    // constructor error this call created nothing (or the constructor already
    // removed it), so no caller-side cleanup — an existence pre-check here
    // would race a concurrent creator (TOCTOU) and delete a file this salvage
    // does not own. Later `write` / `finish` failures still clean up below:
    // by then `create_new` has proven `dest` is ours.
    // Blob salvage is a rare recovery operation, so sync at the strongest
    // durability: the writer fsyncs the file's bytes and the parent directory is
    // synced below, so the recovered file survives a power loss the moment the
    // report claims success.
    let sync_mode = crate::fs::SyncMode::Full;
    let mut writer = BlobWriter::new(&dest, blob_file_id, 0, &**fs)?
        .use_sync_mode(sync_mode)
        .use_compression(compression);
    #[cfg(zstd_any)]
    {
        // The re-emit re-compresses under the source's descriptor; a
        // dictionary descriptor needs the dictionary on the write side too.
        writer = writer.use_zstd_dictionary(zstd_dictionary.cloned());
    }

    let mut records_total = 0usize;
    let mut records_salvaged = 0usize;
    let mut offset_remap: Vec<(u64, BlobRecordRelocation)> = Vec::new();
    let mut dropped: Vec<DroppedBlob> = Vec::new();
    // The internal key of the last record accepted for re-emit, to enforce the
    // `BlobWriter` sorted-input contract on the salvaged file (see the accept
    // arm). Blob files order by user key ascending, ties broken by DESCENDING
    // seqno (newest first).
    let mut prev_written: Option<(crate::UserKey, crate::SeqNo)> = None;
    // Emit every recoverable record. A `write` failure here (not a per-record
    // checksum/corruption drop, which the match arms absorb) is a hard error: it
    // leaves a partial `dest`, removed on the error path below the same way the
    // SST salvage path removes its partial output.
    let walk = (|| -> crate::Result<()> {
        for item in scanner {
            records_total += 1;
            match item {
                // A frame the scanner reached at or after a byte-wise RESYNC has
                // an UNPROVEN boundary: the resync magic may be an original frame
                // boundary OR a checksum-valid `BLO4` frame nested inside the
                // damaged frame's user-controlled value bytes, and every frame
                // CHAINED past it inherits that unanchored start; the two are
                // byte-for-byte indistinguishable. The taint is sticky, so this
                // arm drops the ENTIRE tail after the first resync: re-emitting
                // any of it would FABRICATE records (and bogus `offset_remap`
                // entries pointing inside the damaged region), which is worse
                // than losing the tail (fail closed on unprovable provenance).
                Ok(entry) if entry.resynced => {
                    // The taint is STICKY: every frame from here to EOF inherits the
                    // same unprovable boundary, so the whole tail is surrendered.
                    // Record the loss ONCE and STOP the walk. Continuing would
                    // re-drop each tainted frame (wasted work plus an allocation per
                    // record) and, worse, keep reading the already-surrendered tail,
                    // where a TRANSIENT read fault would reach the transient-error
                    // arm and abort the whole salvage, discarding the valid prefix
                    // output that needed none of those bytes.
                    let _ = entry;
                    dropped.push(DroppedBlob {
                        reason: BlobDropReason::Corrupt(
                            "tail surrendered at the first resync: every frame past a \
                             damaged frame has an unprovable boundary, dropped as one"
                                .to_string(),
                        ),
                    });
                    break;
                }
                // A frame whose CRCs are internally consistent but whose
                // key_len is ZERO is malformed input (the writer's ingest
                // never emits an empty key and asserts against one): route it
                // through the corrupt-record path — the scanner is already
                // positioned past the frame, so the walk continues.
                Ok(entry) if entry.key.is_empty() => {
                    dropped.push(DroppedBlob {
                        reason: BlobDropReason::Corrupt("frame carries an empty key".to_string()),
                    });
                }
                // A frame whose declared `real_val_len` disagrees with the
                // bytes actually stored (for an UNCOMPRESSED source the two
                // must be equal; a compressed record proves its length through
                // the decompression below instead) is rejected by the live
                // blob reader — re-emitting it would restamp a consistent
                // length and launder a record live reads treat as corrupt.
                // Drop it; the scanner is already past the frame, so the walk
                // continues.
                Ok(entry)
                    if compression == crate::CompressionType::None
                        && entry.uncompressed_len as usize != entry.value.len() =>
                {
                    dropped.push(DroppedBlob {
                        reason: BlobDropReason::Corrupt(
                            "frame's declared value length disagrees with its stored bytes"
                                .to_string(),
                        ),
                    });
                }
                // A frame whose internal key regresses below the last accepted
                // record: `BlobWriter` requires records in key order (ascending
                // user key, ties by descending seqno), and re-emitting an
                // out-of-order frame would corrupt the salvaged file's key range
                // and break the later merge scanner's per-reader sorted-input
                // assumption (mismatched blob relocation, or a panic in the
                // relocating compaction). The frame's own checksum is intact, so
                // this is not payload rot; drop it and keep the sorted prefix.
                Ok(entry) if blob_key_regresses(comparator, prev_written.as_ref(), &entry) => {
                    dropped.push(DroppedBlob {
                        reason: BlobDropReason::Corrupt(
                            "frame's internal key regresses below the previous salvaged \
                             record; re-emitting it would violate the blob writer's \
                             sorted-input contract"
                                .to_string(),
                        ),
                    });
                }
                Ok(entry) => {
                    // A compressed record's frame checksum covered its ON-DISK
                    // bytes; prove the content itself round-trips by
                    // decompressing here (the re-emit below re-compresses it
                    // under the same descriptor). A record that fails to
                    // decompress despite a clean checksum is structural
                    // corruption — drop it and keep walking.
                    let value = match decompress_blob_value(
                        compression,
                        &entry.value,
                        entry.uncompressed_len as usize,
                        #[cfg(zstd_any)]
                        zstd_dictionary.map(alloc::sync::Arc::as_ref),
                    ) {
                        Ok(value) => value,
                        Err(e) => {
                            dropped.push(DroppedBlob {
                                reason: BlobDropReason::Corrupt(format!(
                                    "frame's value does not decompress: {e:?}"
                                )),
                            });
                            continue;
                        }
                    };
                    // Record the frame relocation BEFORE the write advances the
                    // writer: existing SST ValueHandles point at SOURCE frame
                    // offsets, and the compacted rewrite shifts every record
                    // after the first drop, so the caller needs this map to
                    // re-target handles before the salvaged file can replace
                    // the original.
                    let salvaged_offset = writer.offset();
                    // `write` re-compresses under the salvaged file's own
                    // descriptor and returns the resulting ON-DISK value size —
                    // recorded per record because compressor output is not
                    // stable across versions, so the source handle's size
                    // cannot be assumed to survive the round-trip.
                    let on_disk_size = writer.write(&entry.key, entry.seqno, &value)?;
                    offset_remap.push((
                        entry.offset,
                        BlobRecordRelocation {
                            offset: salvaged_offset,
                            on_disk_size,
                        },
                    ));
                    prev_written = Some((entry.key.clone(), entry.seqno));
                    records_salvaged += 1;
                }
                // Payload rot: the checksum failed, so the scanner RESYNCS at the
                // next magic and arms the sticky taint. This record drops here;
                // every frame after it comes back through the `resynced` arm
                // above and drops too (the tail's provenance is now unprovable).
                Err(crate::Error::ChecksumMismatch { .. }) => dropped.push(DroppedBlob {
                    reason: BlobDropReason::ChecksumMismatch,
                }),
                // Header rot (rotted magic or a length field caught by the
                // header CRC): the scanner has RESYNCHRONIZED at the next
                // frame magic (arming the sticky taint, so the tail after it
                // drops) or TERMINATED, when the CRC-vouched frame end overruns
                // the data section (real truncation). Either way the walk is safe.
                Err(
                    e @ (crate::Error::HeaderCrcMismatch { .. } | crate::Error::InvalidHeader(_)),
                ) => {
                    dropped.push(DroppedBlob {
                        reason: BlobDropReason::Corrupt(format!("{e:?}")),
                    });
                }
                // Only a TRANSIENT I/O error is retryable and distinguishable: it
                // may strike AFTER at least one record was emitted, so recording
                // it as corruption and finishing `dest` would publish a lossy
                // report whose offset map silently omits the healthy UNREAD tail.
                // Propagate it so the caller retries and the partial dest is
                // discarded (below) rather than accepting avoidable data loss.
                Err(crate::Error::Io(io)) if io.kind().is_transient() => {
                    return Err(crate::Error::Io(io));
                }
                // Any other error — a PERSISTENT I/O failure (a bad-sector `Other`,
                // `PermissionDenied`, or a truncated tail `UnexpectedEof`, none
                // fixed by a retry), an allocation failure, or a decode fault the
                // scanner does not re-sync from: an error that leaves the read
                // position before `data_end` without terminating would make the
                // iterator keep yielding it. Record the corrupt tail as a drop and
                // stop the walk, keeping the valid prefix salvageable — this is
                // the last record it can inspect.
                Err(e) => {
                    dropped.push(DroppedBlob {
                        reason: BlobDropReason::Corrupt(format!("{e:?}")),
                    });
                    break;
                }
            }
        }
        Ok(())
    })();

    let salvaged_path = match walk {
        // A write failed mid-walk: drop the writer and remove the partial dest
        // before propagating, so a retry / repair caller never sees a half-written
        // blob file.
        Err(e) => {
            drop(writer);
            discard_partial(fs, &dest);
            return Err(e);
        }
        Ok(()) if records_salvaged > 0 => {
            // A `finish` failure likewise leaves a partial dest — remove it before
            // propagating.
            if let Err(e) = writer.finish() {
                discard_partial(fs, &dest);
                return Err(e);
            }
            // The writer synced the file's bytes; sync the parent directory too
            // so the new directory entry is durable before the report claims
            // success (without it a power loss can discard the entry). A bare
            // relative `dest` has an EMPTY parent, so resolve it to the current
            // directory first — otherwise the sync fails and this discards the
            // recovered file. A sync failure removes the file and propagates, so
            // a caller never sees a salvaged_path whose entry is not durable.
            if let Err(e) = fs.sync_directory_with(entry_directory(&dest), sync_mode) {
                discard_partial(fs, &dest);
                return Err(e.into());
            }
            Some(dest)
        }
        // Nothing recoverable: `BlobWriter::new` created `dest`, so remove the
        // empty placeholder a repair caller would otherwise re-quarantine.
        Ok(()) => {
            drop(writer);
            discard_partial(fs, &dest);
            None
        }
    };

    Ok(BlobSalvageReport {
        salvaged_path,
        records_total,
        records_salvaged,
        offset_remap,
        dropped,
    })
}

#[cfg(test)]
mod tests;
