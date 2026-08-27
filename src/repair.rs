// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Last-resort `MANIFEST` reconstruction from the SST files on disk.
//!
//! Once a tree has a `MANIFEST`, that manifest is a single point of failure for
//! the database as a whole: a corrupt manifest means the tree cannot open at
//! all, even when every SST on disk is intact. Repair scans the table folder(s),
//! reads each SST's own metadata, and writes a fresh manifest referencing what
//! is actually present.
//!
//! ## What is recovered, what is lost
//!
//! Every readable SST is preserved. What the rebuilt manifest cannot know is the
//! LSM level structure (which file lived at which level) and any version edits
//! that had not yet been durably logged (an in-flight compaction's output
//! placement, recent table deletions). Following the RocksDB `RepairDB()`
//! pattern, all recovered SSTs are placed at L0 ordered by sequence number
//! (newest first) and a normal background compaction redistributes them into
//! proper levels on the next open. Reads are correct throughout: L0 permits
//! overlapping runs, and the merge reader resolves the latest value by sequence
//! number regardless of physical placement.
//!
//! ## Correctness of the recomputed table checksum
//!
//! The manifest binds each table by its whole-file XXH3-128 checksum. A normal
//! write computes that digest incrementally as the file is streamed out, and the
//! file is written strictly sequentially (no seek-back rewrites after the digest
//! is taken), so the on-disk bytes equal the hashed byte stream. Repair therefore
//! recomputes the identical digest by streaming the file start to end. The data
//! itself is protected independently by per-block checksums, which
//! [`Table::recover`] validates as it parses, so an SST that survives recovery is
//! structurally sound.
//!
//! ## Scope
//!
//! KV-separated (blob) trees are supported: the `blobs/` folder is scanned to
//! rediscover the blob files and record them in the rebuilt manifest. Blob-file
//! fragmentation statistics cannot be reconstructed from a directory scan
//! (they are derived from compaction history), so they start empty; blob GC is
//! advisory and re-learns reclaimable space over time without dropping data.

use crate::{
    SeqNo, Table, TableId, UserKey,
    config::{Config, TreeType},
    version::{BlobFileList, Level, Run, Version},
};
use std::{path::PathBuf, sync::Arc};

/// Per-file repair failures: `(path, human-readable reason)`. Mirrors
/// [`RepairReport::unreadable_files`].
type UnreadableFiles = Vec<(PathBuf, String)>;

/// Outcome of a [`Config::repair`] run.
///
/// `recovered` plus `unreadable` accounts for every SST-named file the scan
/// considered. `unreadable_files` carries the per-file reason a file was skipped
/// so an operator can decide whether to investigate or discard it.
#[derive(Debug)]
pub struct RepairReport {
    /// Number of SSTs whose metadata parsed and that are now referenced by the
    /// rebuilt manifest (including any recovered by salvage; see [`salvaged`]).
    ///
    /// [`salvaged`]: RepairReport::salvaged
    pub recovered: usize,

    /// Of [`recovered`](RepairReport::recovered), how many were recovered by
    /// block-level salvage (their original failed whole-file recovery, so the
    /// salvaged copy may be missing the key ranges of corrupt blocks). Always
    /// zero unless repair ran with salvage enabled
    /// ([`Config::repair_with_salvage`]).
    pub salvaged: usize,

    /// Number of SST-named files that could not be opened or parsed and were
    /// therefore left out of the manifest.
    pub unreadable: usize,

    /// Path and human-readable error for each unreadable file.
    pub unreadable_files: Vec<(PathBuf, String)>,

    /// Key coverage the rebuilt manifest LOST, for every excluded table whose
    /// metadata still parsed: `(path, first key, last key, highest seqno)`.
    ///
    /// Losing a table's bytes loses what it said about those keys. Older
    /// versions of them survive in other tables and become visible again — a
    /// value the lost table had overwritten, or a key its tombstone had
    /// deleted. No repair can tell those apart without the lost bytes, and
    /// deleting the range instead would destroy intact data (see
    /// [`repair_with_resurrection`](Config::repair_with_resurrection)), so the
    /// range is reported rather than acted on: within these bounds, at or below
    /// the given sequence number, the tree may now serve a superseded value.
    ///
    /// The sequence bound is `None` when the table's own sequence base lived in
    /// the lost manifest — a bulk-ingested SST stores every entry at local
    /// seqno `0` and takes its effective ordering from a manifest-only offset,
    /// which is exactly why such a table is excluded. Reporting the on-disk
    /// local maximum there would scope the affected history far too low, so the
    /// bound is reported as unknown and the whole history of that key range has
    /// to be treated as affected.
    ///
    /// A KEPT salvaged copy contributes an entry too: it dropped corrupt
    /// blocks (or blob records), so within its bounds a superseded value may
    /// likewise now be served, even though the table itself is in the
    /// manifest. The entry carries the SOURCE's coverage (captured while its
    /// metadata was readable), not the replacement's: salvage may have
    /// dropped the block holding the source's outermost keys or highest
    /// seqno, and bounds derived from the survivors would exclude exactly
    /// the lost part.
    ///
    /// Empty when nothing was lost. A table whose metadata was unreadable
    /// contributes no entry here — its coverage is unknowable, and it is
    /// listed in [`unknowable_losses`](Self::unknowable_losses) instead.
    pub lost_coverage: Vec<(PathBuf, UserKey, UserKey, Option<SeqNo>)>,

    /// Table files whose loss cannot be scoped at all: their metadata never
    /// parsed, so neither the affected key range nor a seqno bound is
    /// derivable. Covers both an EXCLUDED unreadable table and a KEPT lossy
    /// salvaged copy whose source's metadata was unreadable (the copy's own
    /// metadata only bounds what survived). Any entry here forces
    /// [`wal_replay_scope`](Self::wal_replay_scope) to
    /// [`WalReplayScope::FullHistory`], since no bound can prove a retained
    /// record is NOT affected. Blob files are not listed: losing blob content
    /// surfaces through the referencing tables, which the other fields cover.
    pub unknowable_losses: Vec<PathBuf>,

    /// Damaged blob files whose salvaged replacement IS installed in the
    /// rebuilt manifest: the canonical (installed) path plus a note on what
    /// was recovered and where the damaged original is preserved. Disjoint
    /// from [`unreadable_files`](RepairReport::unreadable_files), which lists
    /// only files LEFT OUT of the manifest. Empty for standard trees and for
    /// repairs where no blob file needed salvage.
    pub blob_files_salvaged: Vec<(PathBuf, String)>,

    /// Description of the level-assignment strategy used (constant for now;
    /// surfaced so the report is self-explanatory and forward-compatible).
    pub method: &'static str,

    /// Operator-facing caveats about the rebuilt state.
    pub warnings: Vec<&'static str>,
}

/// What an external write-ahead log must replay after this repair, derived
/// from [`RepairReport::lost_coverage`] by
/// [`RepairReport::wal_replay_scope`].
///
/// A repair can REGRESS persisted state below the WAL's trim watermark `W`
/// (a dropped or salvaged table loses versions at or below `W`, while
/// [`get_highest_persisted_seqno`] stays high because neighbouring tables
/// survived), so the standard `seqno > W` tail replay is not always
/// sufficient. See `docs/external-wal.md` § Replay after repair for the full
/// recipe, including why a merge operand needs a presence check
/// ([`scan_since_seqno_in_range`]) while a put / delete may be re-applied
/// blindly.
///
/// [`get_highest_persisted_seqno`]: crate::AbstractTree::get_highest_persisted_seqno
/// [`scan_since_seqno_in_range`]: crate::Tree::scan_since_seqno_in_range
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReplayScope {
    /// Nothing was excluded: the standard tail replay (`seqno > W`) is
    /// sufficient.
    TailOnly,
    /// Coverage was lost at or below this sequence number. In addition to the
    /// tail, replay every RETAINED record whose key falls inside a
    /// [`RepairReport::lost_coverage`] range and whose seqno is at or below
    /// this bound (presence-checking merge operands).
    LostUpTo(SeqNo),
    /// At least one excluded table's sequence base was itself lost with the
    /// manifest, so no seqno bound scopes the damage: the whole retained
    /// history of the affected ranges must be replayed.
    FullHistory,
}

impl RepairReport {
    /// Derives the WAL replay obligation from
    /// [`lost_coverage`](Self::lost_coverage) and
    /// [`unknowable_losses`](Self::unknowable_losses):
    /// [`WalReplayScope::TailOnly`] when nothing was lost,
    /// [`WalReplayScope::FullHistory`] when any loss is unscopable (an
    /// unknown seqno bound, or an excluded table whose coverage never
    /// parsed), else [`WalReplayScope::LostUpTo`] the highest lost bound. The
    /// per-range detail (which KEYS are affected) stays in `lost_coverage`;
    /// this is the aggregate a WAL uses to decide how far back its archive
    /// must reach.
    #[must_use]
    pub fn wal_replay_scope(&self) -> WalReplayScope {
        if !self.unknowable_losses.is_empty() {
            return WalReplayScope::FullHistory;
        }
        let mut ceiling: Option<SeqNo> = None;
        for (_, _, _, bound) in &self.lost_coverage {
            match bound {
                None => return WalReplayScope::FullHistory,
                Some(b) => ceiling = Some(ceiling.map_or(*b, |c| c.max(*b))),
            }
        }
        match ceiling {
            None => WalReplayScope::TailOnly,
            Some(b) => WalReplayScope::LostUpTo(b),
        }
    }
}

/// Streams `path` from byte `start` to end through XXH3-128. `start == 0`
/// reproduces the whole-file digest a normal write accumulates; a non-zero
/// `start` digests only the LIVE suffix of a tight-space RESTRICTED table,
/// whose `[0, start)` prefix was hole-punched (reads back as zeros) once a
/// superseding output table took over those keys. The suffix bytes are
/// untouched by the punch, so this digest is stable across it.
pub(crate) fn compute_table_checksum_from(
    fs: &dyn crate::fs::Fs,
    path: &std::path::Path,
    start: u64,
) -> crate::Result<u128> {
    // The offset-only case is the override-splicing digest with no overrides.
    compute_table_checksum_with_overrides(fs, path, start, &[])
}

/// Streams `path` start to end through XXH3-128, matching the digest a normal
/// table write accumulates via `ChecksummedWriter`.
pub(crate) fn compute_table_checksum(
    fs: &dyn crate::fs::Fs,
    path: &std::path::Path,
) -> crate::Result<u128> {
    // The whole-file case is the override-splicing digest from offset 0 with no
    // overrides: one shared read loop in `compute_table_checksum_with_overrides`.
    compute_table_checksum_with_overrides(fs, path, 0, &[])
}

/// As [`compute_table_checksum`], but streams the file with `overrides` spliced
/// in: each `(offset, bytes)` replaces the on-disk bytes at `[offset,
/// offset + bytes.len())`. Used to predict the digest an in-place heal WILL
/// produce, from the corrected block frames it will write, before any write
/// lands — so the heal attestation can bind that intended post-heal state.
///
/// The overrides are size-preserving block frames at distinct, non-overlapping
/// offsets (the heal rewrites each corrupt block at its existing offset and
/// size), so splicing them is a byte-for-byte substitution that keeps the file
/// length and every other byte unchanged. `start` matches
/// [`compute_table_checksum_from`]: a restricted view predicts only its live
/// suffix (its corrections all lie there), so the digest starts at the punch
/// offset.
pub(crate) fn compute_table_checksum_with_overrides(
    fs: &dyn crate::fs::Fs,
    path: &std::path::Path,
    start: u64,
    overrides: &[(u64, Vec<u8>)],
) -> crate::Result<u128> {
    use std::io::{Seek, SeekFrom};
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    // Seek + sequential read (see `compute_table_checksum_from`): keeps the
    // `start == 0` read pattern identical to the plain digest.
    if start != 0 {
        file.seek(SeekFrom::Start(start))?;
    }
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut buf = vec![0u8; 256 * 1024];
    let mut chunk_start = start;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break; // EOF
        }
        let chunk_end = chunk_start + n as u64;
        let Some(chunk) = buf.get_mut(..n) else { break };
        // Splice every override overlapping this chunk. Overrides are few (one
        // per corrupt block), so scanning them per chunk is negligible.
        for (off, bytes) in overrides {
            let ov_end = *off + bytes.len() as u64;
            let lo = (*off).max(chunk_start);
            let hi = ov_end.min(chunk_end);
            // Skip a non-overlapping override BEFORE computing relative offsets:
            // the bound subtractions below are unsigned, and an override ending
            // before this chunk (or starting after it) would otherwise underflow
            // (a debug panic). Once `lo < hi` holds, `chunk_start <= lo < hi <=
            // chunk_end` and `off <= lo < hi <= ov_end`, so all four differences
            // are non-negative.
            if lo >= hi {
                continue;
            }
            // The overlap lies inside a `<= 256 KiB` chunk, so every difference
            // fits `usize`; `try_from` handles the 32-bit target without a cast.
            let (Ok(dst_lo), Ok(dst_hi), Ok(src_lo), Ok(src_hi)) = (
                usize::try_from(lo - chunk_start),
                usize::try_from(hi - chunk_start),
                usize::try_from(lo - *off),
                usize::try_from(hi - *off),
            ) else {
                continue;
            };
            if let (Some(dst), Some(src)) =
                (chunk.get_mut(dst_lo..dst_hi), bytes.get(src_lo..src_hi))
            {
                dst.copy_from_slice(src);
            }
        }
        hasher.update(&*chunk);
        chunk_start = chunk_end;
    }
    Ok(hasher.digest128())
}

/// Highest existing `v{N}` manifest id in `folder`, if any. The rebuilt manifest
/// uses `max + 1` so it supersedes any stale version file and the `current`
/// pointer never races a half-written predecessor.
///
/// A directory-read failure is propagated (not swallowed as "no versions"): a
/// transient scan error must not silently reset the version chain to `0` and
/// risk reusing a live version id.
fn highest_existing_version_id(
    fs: &dyn crate::fs::Fs,
    folder: &std::path::Path,
) -> crate::Result<Option<u64>> {
    Ok(fs
        .read_dir(folder)?
        .into_iter()
        .filter_map(|e| {
            e.file_name
                .strip_prefix('v')
                .and_then(|rest| rest.parse::<u64>().ok())
        })
        .max())
}

/// Removes a file the rebuilt manifest does not name, together with any
/// companion `.restrict-bound` sidecar, and makes the removal durable.
///
/// A repair has exactly two outcomes: a committed tree that opens, or an error.
/// A file left behind is neither — `Tree::open` rejects a foreign name in
/// `tables/` and sweeps an id the manifest does not reference, so a removal that
/// cannot happen here becomes an open that fails. The removal is therefore not
/// best-effort: a failure fails the repair.
///
/// The bytes are NOT preserved anywhere. Recovering the content of a damaged
/// file is replication, checkpoint plus journal replay, or a backup; a directory
/// hidden beside the tree recovers nothing and only makes a later run derive
/// from a different world than the one it was handed.
///
/// Callers run this strictly AFTER the manifest commit. Until then every source
/// is still the only copy of its rows, and a crash must leave the directory
/// exactly as the retry expects to find it.
///
/// `NotFound` counts as done, so a retry finishes a sweep a crash interrupted.
fn discard_unreferenced(
    fs: &dyn crate::fs::Fs,
    path: &std::path::Path,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    let remove = |target: &std::path::Path| -> crate::Result<()> {
        match fs.remove_file(target) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    };
    // Sidecar first: an SST removed while its `.restrict-bound` survives leaves a
    // bound that a later run would match by id against an unrelated table. Most
    // files have none (blob files never do), so probe before asking.
    let sidecar = crate::restrict_bound::sidecar_path(path);
    if fs.exists(&sidecar)? {
        remove(&sidecar)?;
    }
    remove(path)?;
    if let Some(dir) = path.parent() {
        fs.sync_directory_with(dir, sync_mode)?;
    }
    Ok(())
}

pub(crate) use crate::file::{REPAIR_TMP_SUFFIX, table_id_from_repair_tmp_name};

/// `{id}` -> `{id}.repair-tmp`, the path a repair builds a replacement at.
///
/// The replacement CANNOT be built at `{id}` directly: that would either destroy
/// the source before the manifest commit (a crash then loses the only copy) or,
/// under a fresh id beside the source, leave both readable after a crash — and
/// the retry, unable to tell a half-finished replacement from a real table,
/// would rebuild BOTH into L0, applying every merge operand of that history
/// twice. A name no scan adopts has neither problem: the source stays intact and
/// authoritative until the commit, and a leftover temp is garbage.
fn repair_tmp_path(table_path: &std::path::Path) -> PathBuf {
    let mut name = table_path.file_name().unwrap_or_default().to_os_string();
    name.push(REPAIR_TMP_SUFFIX);
    table_path.with_file_name(name)
}

/// Swaps a finished replacement onto the name the committed manifest gives it:
/// `{id}.repair-tmp` becomes `{id}`, destroying the damaged source it replaces,
/// with any companion `.restrict-bound` sidecar carried along.
///
/// Strictly POST-COMMIT. Before the commit the source is still the only copy of
/// its rows and the manifest still names it; after it, the manifest names this
/// content and the source is what the tree no longer references.
///
/// Sidecar first, and the sidecar's own removal-or-rename settled before the
/// table moves: a table adopted at `{id}` while a STALE `{id}.restrict-bound`
/// survives would be reopened restricted at an unrelated bound, silently hiding
/// its prefix.
///
/// `manifest_restricted` is whether the COMMITTED manifest restricts this id.
/// It disambiguates a retry of an interrupted swap: with the sidecar step
/// preceding the table rename, a crash between them leaves the replacement's
/// sidecar already at the destination and no temp sidecar — a state that is
/// byte-identical to "unrestricted replacement beside the source's stale
/// sidecar". Only the manifest can tell them apart, and it is the authority
/// for the bound anyway.
pub(crate) fn commit_repair_tmp(
    fs: &dyn crate::fs::Fs,
    tmp_path: &std::path::Path,
    table_path: &std::path::Path,
    sync_mode: crate::fs::SyncMode,
    manifest_restricted: bool,
) -> crate::Result<()> {
    let tmp_sidecar = crate::restrict_bound::sidecar_path(tmp_path);
    let dest_sidecar = crate::restrict_bound::sidecar_path(table_path);
    if fs.exists(&tmp_sidecar)? {
        fs.rename(&tmp_sidecar, &dest_sidecar)?;
    } else if manifest_restricted {
        // The manifest restricts this id, so a sidecar already at the
        // destination is NOT the source's stale metadata — it is the
        // replacement's own sidecar, moved by an interrupted earlier attempt.
        // Deleting it would adopt a restricted replacement unrestricted on the
        // next scan (the fresh copy is unpunched, so no geometry re-derives
        // the bound) and resurrect the straddling block's sub-bound rows.
        // Keep it; a missing or disagreeing sidecar is republished from the
        // manifest by the next open.
    } else if fs.exists(&dest_sidecar)? {
        // The replacement is unrestricted, so the source's bound describes bytes
        // that are about to stop existing. Left in place it would reopen the
        // replacement restricted at an unrelated bound and hide its prefix.
        match fs.remove_file(&dest_sidecar) {
            Ok(()) => {}
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    fs.rename(tmp_path, table_path)?;
    if let Some(dir) = table_path.parent() {
        fs.sync_directory_with(dir, sync_mode)?;
    }
    Ok(())
}

/// Decides whether a leftover `{id}.repair-tmp` is the file the committed
/// manifest describes. The manifest names `id` in BOTH crash cases — after the
/// commit (the entry describes the replacement) and before it (the entry still
/// describes the source) — so id membership alone cannot tell a committed swap
/// from an abandoned build, and swapping the latter in would destroy the file
/// the manifest actually names (a mid-build crash even leaves the temp
/// truncated). The entry's checksum can tell them apart: only a committed
/// repair recorded the temp's digest — the live-suffix digest for a restricted
/// replacement, whose punch offset comes from the temp's own index. A temp
/// that does not even open is the mid-build crash; only transient I/O
/// propagates for a retry.
#[cfg(feature = "std")]
pub(crate) fn repair_tmp_is_published(
    config: &Config,
    fs: &Arc<dyn crate::fs::Fs>,
    tmp_path: &std::path::Path,
    table_id: TableId,
    manifest_checksum: crate::Checksum,
    restriction: Option<&crate::UserKey>,
) -> crate::Result<bool> {
    let Some(bound) = restriction else {
        let digest = compute_table_checksum(&**fs, tmp_path)?;
        return Ok(crate::Checksum::from_raw(digest) == manifest_checksum);
    };
    let table = match crate::table::Table::recover(repair_recover_params(
        config,
        tmp_path.to_path_buf(),
        manifest_checksum,
        table_id,
        Arc::clone(fs),
    )) {
        Ok(table) => table,
        Err(e) if is_environmental_io(&e) => return Err(e),
        Err(_) => return Ok(false),
    };
    let punch_offset = match table.punch_offset_for(bound.as_ref()) {
        Ok(offset) => offset,
        Err(e) if is_environmental_io(&e) => return Err(e),
        Err(_) => return Ok(false),
    };
    let digest = compute_table_checksum_from(&**fs, tmp_path, punch_offset)?;
    Ok(crate::Checksum::from_raw(digest) == manifest_checksum)
}

/// Recover params for a repair's TRANSIENT table open: the tree's configured
/// comparator / crypto / dictionary context (so the table decodes consistently
/// with how it was written), and everything else neutral — tree id 0 and no
/// descriptor table keep the open from polluting shared caches keyed by the
/// real tree id, and global seqno 0 because a recovered table's intrinsic
/// sequence numbers are authoritative (there is no ingestion-time offset).
#[cfg(feature = "std")]
fn repair_recover_params(
    config: &Config,
    file_path: PathBuf,
    checksum: crate::Checksum,
    table_id: TableId,
    fs: Arc<dyn crate::fs::Fs>,
) -> crate::table::RecoverParams {
    let mut params = crate::table::RecoverParams::new(
        file_path,
        checksum,
        table_id,
        fs,
        config.comparator.clone(),
        config.cache.clone(),
    );
    params.encryption.clone_from(&config.encryption);
    #[cfg(zstd_any)]
    {
        params.zstd_dictionary.clone_from(&config.zstd_dictionary);
    }
    params
}

/// Whether an I/O failure must PROPAGATE out of the repair instead of grading
/// the file it came from — either UNAMBIGUOUSLY TRANSIENT, or an
/// ENVIRONMENTAL access failure that does not implicate the bytes on disk.
///
/// The allowlist is deliberately narrow: `Interrupted` (`EINTR`) and
/// `WouldBlock` (`EAGAIN`) — the interrupted-syscall errors a retry genuinely
/// clears, and which a corrupt on-disk structure can NEVER produce — plus
/// `PermissionDenied` (`EACCES` / `EPERM`): an ACL / ownership mistake the
/// operator fixes, while grading the healthy file unreadable commits a
/// manifest that excludes it and then REMOVES it, turning a recoverable
/// configuration error into permanent data loss.
///
/// `Other` is NOT on the list, even though an injected fault or a raw `EIO`
/// lands there, because a STRUCTURAL corruption lands there too: a corrupt
/// trailer that decodes to a bad offset makes the reader seek before the start of
/// the file, which Windows reports as `ERROR_NEGATIVE_SEEK` — an unmapped OS
/// error the `From<std::io::Error>` bridge folds into `ErrorKind::Other`. Treating
/// `Other` as propagating would then abort the WHOLE repair (blocking recovery of
/// every healthy sibling table) on a single genuinely-corrupt SST, and the class
/// is platform-dependent (the same corruption reads back `InvalidInput` on Unix).
/// A hardware `EIO` is likewise usually a persistent bad-sector failure, so
/// recording that table unreadable — while the rest recover — is the right
/// outcome. Fault-injection tests therefore inject `Interrupted` to model a
/// retryable fault.
///
/// This inspects the CRATE's [`crate::io::ErrorKind`], which is what a
/// `crate::Error::Io` always carries.
fn is_environmental_io(e: &crate::Error) -> bool {
    matches!(e, crate::Error::Io(io) if io.kind().is_environmental())
}

/// Whether manifest repair must fail closed on a table because its bulk-ingest
/// sequence offset cannot be reconstructed from the SST alone (the rebuilt
/// manifest would install it with offset 0 and silently mis-order / mis-expose
/// its entries).
///
/// - `Some(true)`: authoritatively bulk-ingested — always fail closed.
/// - `Some(false)`: a newer non-ingested table — safe (offset genuinely 0), even
///   when its entries all sit at seqno 0 (a fresh tree's first batch).
/// - `None`: a LEGACY SST written before the provenance flag existed — UNKNOWN.
///   Treat it as bulk-ingested ONLY when its entries carry the ingest signature
///   (present, and every LOCAL seqno 0), which a legacy bulk-ingest produces. A
///   legacy first-batch-at-seqno-0 flush matches too and is conservatively
///   dropped; the ambiguity is unavoidable without the flag.
fn has_unrecoverable_ingest_offset(
    bulk_ingested: Option<bool>,
    item_count: u64,
    max_local_seqno: crate::SeqNo,
) -> bool {
    match bulk_ingested {
        Some(flagged) => flagged,
        None => item_count > 0 && max_local_seqno == 0,
    }
}

/// How faithfully a recovered candidate carries its source's content. Drives
/// two DISTINCT report facts: anything but [`Complete`](Self::Complete)
/// contributes a `lost_coverage` entry (scoped by the source's coverage), while
/// only [`Salvaged`](Self::Salvaged) counts toward `RepairReport::salvaged` —
/// a geometry-restricted original is lossy but was never block-salvaged, and
/// it arises in plain (salvage-off) repairs too.
#[derive(Copy, Clone, PartialEq, Eq)]
enum Fidelity {
    /// Every live row of the source is present.
    Complete,
    /// The ORIGINAL file reopened under a punch-GEOMETRY-derived restriction
    /// (no exact sidecar, resurrection off): the bound is the straddling
    /// block's END key, so that block's still-live suffix rows may be excluded.
    GeometryRestricted,
    /// A block-salvage rewrite: corrupt blocks (or blob records) were dropped.
    Salvaged,
}

impl Fidelity {
    fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// A recovered table plus its [`Fidelity`]. Repair keeps the best copy per
/// table id, so a duplicate id in another table folder can supersede an
/// earlier DAMAGED copy.
///
/// The physical location (`fs` / `path`) travels with the candidate so that when
/// a duplicate SUPERSEDES it, the loser's file can be removed from `tables/`. The
/// rebuilt manifest records only `id + checksum`, so two same-id files left in
/// different folders would let recovery resolve the stale one by folder order and
/// reopen it against the kept copy's mismatched checksum.
struct TableCandidate {
    table: Table,
    fidelity: Fidelity,
    fs: Arc<dyn crate::fs::Fs>,
    path: PathBuf,
}

/// Records `candidate` for `id`, keeping the BETTER of the existing and the new
/// copy: a COMPLETE recovery replaces a lossy salvage, so an intact duplicate in
/// a later-scanned folder supersedes an earlier lossy one. Two completes (or two
/// salvages) are equivalent for the rebuilt manifest — which needs only one
/// readable copy per id — so the first-seen stays.
///
/// Returns the DISPLACED loser (the rejected new candidate, or the superseded old
/// one) so the caller can record its file for removal; `None` when `id` was
/// previously unseen (nothing displaced).
#[must_use = "the displaced duplicate's file must be removed from tables/"]
fn keep_best_candidate(
    map: &mut crate::HashMap<TableId, TableCandidate>,
    id: TableId,
    candidate: TableCandidate,
) -> Option<TableCandidate> {
    match map.get(&id) {
        // Keep the existing copy when it is already complete, or when the new one
        // is not an improvement (a lossy duplicate of a lossy copy): the NEW
        // candidate is displaced.
        Some(existing) if existing.fidelity.is_complete() || !candidate.fidelity.is_complete() => {
            Some(candidate)
        }
        // The new candidate supersedes: `insert` returns the displaced old copy.
        _ => map.insert(id, candidate),
    }
}

/// Removes a blob-salvage temp on a path where the repair goes on to COMMIT a
/// manifest. A missing temp is fine; ANY other failure fails the repair: the
/// rebuilt manifest never references the temp, so left in `blobs/` the next
/// open's orphan sweep would hit the same removal failure — reporting success
/// for a tree that cannot open would be a lie. Quarantine is NOT an out here:
/// it preserves damaged DATA for the operator, and a temp is discardable
/// garbage — while a directory that refuses removal refuses the rename too.
/// Paths where the repair itself returns an error need only best-effort
/// removal: the retry re-attempts it and fails honestly if it still cannot.
#[cfg(feature = "std")]
fn remove_temp(config: &Config, temp: &std::path::Path) -> crate::Result<()> {
    match config.fs.remove_file(temp) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            log::error!(
                "repair: cannot remove salvage temp {} ({e}); failing the repair — \
                 left in place, the next open's orphan sweep would hit the same error",
                temp.display(),
            );
            Err(e.into())
        }
    }
}

/// Whether `a` and `b` name the SAME physical file — a symlink, junction, or
/// case-insensitive alias resolving to one directory entry (two configured table
/// folders pointing at the same location). Used so a repeated sighting of one SST
/// through an alias is never removed as a "duplicate" (which would destroy the
/// kept copy and orphan the manifest entry).
///
/// The two candidates must live in the SAME filesystem namespace for a path
/// comparison to mean anything: a virtual (`MemFs`) table can sit at a path that
/// also exists on the host filesystem, and canonicalizing both spellings through
/// the host would call those distinct files aliases — the loser would then survive
/// and a later reopen could resolve that leftover against the kept copy's
/// manifest checksum. Backends advertise namespace identity through
/// [`Fs::backend_id`](crate::fs::Fs::backend_id), whose `None` means "no shared
/// namespace guarantee" and is therefore treated as DISTINCT.
///
/// Within one namespace, alias resolution is the backend's own
/// ([`Fs::same_file`](crate::fs::Fs::same_file)): kernel-backed backends
/// canonicalize through the host, virtual ones compare paths literally — a
/// host symlink must never alias two distinct virtual files. A probe that
/// cannot decide answers `false` (distinct), so a genuine duplicate is still
/// removed.
#[cfg(feature = "std")]
fn same_physical_file(
    fs_a: &dyn crate::fs::Fs,
    a: &std::path::Path,
    fs_b: &dyn crate::fs::Fs,
    b: &std::path::Path,
) -> crate::Result<bool> {
    match (fs_a.backend_id(), fs_b.backend_id()) {
        (Some(id_a), Some(id_b)) if id_a == id_b => {}
        // Two backends that each CLAIM an identity and disagree are provably
        // distinct namespaces: path spellings are not comparable.
        (Some(_), Some(_)) => return Ok(false),
        // A backend WITHOUT an identity claim (a custom `Fs` keeping the
        // default `None`): the SAME instance is trivially one namespace, so
        // its own probe below decides. Two DIFFERENT instances cannot be
        // proven distinct — and `false` here AUTHORIZES deleting what may be
        // an alias of the kept file — so the verdict is inconclusive and the
        // repair aborts for the operator to sort out.
        _ => {
            if !core::ptr::addr_eq(
                core::ptr::from_ref::<dyn crate::fs::Fs>(fs_a),
                core::ptr::from_ref::<dyn crate::fs::Fs>(fs_b),
            ) {
                return Err(crate::Error::Io(crate::io::Error::new(
                    crate::io::ErrorKind::InvalidInput,
                    "alias identity is inconclusive: the backend reports no namespace id",
                )));
            }
        }
    }
    // Alias resolution belongs to the BACKEND, not the host: canonicalizing a
    // virtual backend's paths through the host filesystem would let a host
    // symlink alias two unrelated virtual files, and the "duplicate" loser
    // would survive. Kernel-backed backends canonicalize; virtual ones compare
    // literally. An INCONCLUSIVE probe propagates: `false` authorizes
    // duplicate deletion, and deleting what may be an alias of the kept copy
    // unlinks the very directory entry the rebuilt manifest retains.
    Ok(fs_a.same_file(a, b)?)
}

/// Records a duplicate table file that lost to a better same-id copy: reported
/// as considered-but-not-referenced, and removed once the manifest is durable so
/// recovery can never resolve it instead of the kept copy.
#[cfg(feature = "std")]
fn discard_duplicate(
    loser: TableCandidate,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    discard_after_commit: &mut Vec<(Arc<dyn crate::fs::Fs>, PathBuf, String)>,
) {
    let TableCandidate {
        table, fs, path, ..
    } = loser;
    drop(table); // release the open file handle
    let reason = "duplicate table id superseded by another copy";
    discard_after_commit.push((fs, path.clone(), reason.to_string()));
    unreadable_files.push((path, reason.to_string()));
}

/// Builds a [`TableCandidate`] from a recovered table plus its physical location,
/// records it as the best copy for `id`, and marks any duplicate displaced by
/// the decision for post-commit removal. The one path every recovered table
/// takes to enter the manifest, so a superseded same-id file is never left
/// discoverable once the rebuild is durable.
#[cfg(feature = "std")]
#[expect(
    clippy::too_many_arguments,
    reason = "location + report threaded through"
)]
fn record_best(
    map: &mut crate::HashMap<TableId, TableCandidate>,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    discard_after_commit: &mut Vec<(Arc<dyn crate::fs::Fs>, PathBuf, String)>,
    id: TableId,
    table: Table,
    fidelity: Fidelity,
    fs: &Arc<dyn crate::fs::Fs>,
    path: &std::path::Path,
) -> crate::Result<()> {
    let candidate = TableCandidate {
        table,
        fidelity,
        fs: Arc::clone(fs),
        path: path.to_path_buf(),
    };
    if let Some(loser) = keep_best_candidate(map, id, candidate) {
        // If the displaced copy physically ALIASES the kept one (same directory
        // entry via a symlink / junction / case-insensitive path), it is the SAME
        // file — removing it would destroy the kept copy and orphan the manifest
        // entry. Drop the loser's handle in place instead. An INCONCLUSIVE
        // probe aborts the repair for a retry: guessing "distinct" would
        // authorize exactly that destruction.
        let is_alias = match map.get(&id) {
            Some(kept) => same_physical_file(&*loser.fs, &loser.path, &*kept.fs, &kept.path)?,
            None => false,
        };
        if is_alias {
            return Ok(());
        }
        discard_duplicate(loser, unreadable_files, discard_after_commit);
    }
    Ok(())
}

/// Block-salvages a corrupt SST during repair: reads the UNTOUCHED original in
/// place, writes a fresh SST holding its recoverable blocks to `table_path` (the
/// unpublished `{id}.repair-tmp`), and reopens it.
///
/// Returns `Ok(None)` when nothing was recoverable (the source is untouched and
/// no replacement exists), or `Err` when even salvage cannot open the source
/// (its metadata / index / SFA trailer is itself unreadable).
/// Whether a freshly-recovered SST passes the salvage-mode block verify.
///
/// One uniform path for encrypted and unencrypted tables: the out-of-band
/// section walk. Block headers and payload checksums are PLAINTEXT, so the
/// walk needs the provider only to decode the meta block (the per-SST ECC
/// descriptor); every section — data, index/TLI, filter, zone map, delete
/// bitmap, locator, meta — is then verified against its raw on-disk checksum,
/// which flags even a persistent ECC-CORRECTABLE fault (a live read would
/// silently heal it in memory while the corrupt bytes stay on disk).
/// Classifies a block-verifier result for the salvage gate. A structural
/// divergence (a checksum / decode / cross-check mismatch) is genuine
/// corruption: `Ok(true)`, route the table through salvage. Only a TRANSIENT
/// [`crate::Error::Io`] (the [`is_environmental_io`] allowlist) aborts the repair
/// (`Err`) for a retry, rather than dropping a healthy block into a partial
/// replacement. A PERSISTENT I/O failure is NOT retryable — a bad sector, or a
/// structural corruption surfacing as `Io(Other)` on some platforms — so it is
/// graded as corruption and salvaged too, rather than aborting the whole repair
/// and stranding every other healthy table on one unrecoverable read.
fn is_corruption(res: crate::Result<()>) -> crate::Result<bool> {
    match res {
        Ok(()) => Ok(false),
        Err(e) if is_environmental_io(&e) => Err(e),
        Err(_) => Ok(true),
    }
}

fn block_verify_verdict(
    config: &Config,
    folder_fs: &Arc<dyn crate::fs::Fs>,
    table_path: &std::path::Path,
    table: &Table,
) -> crate::Result<BlockVerifyVerdict> {
    // Walk only the recovered view's LIVE data: for a tight-space RESTRICTED view
    // (a valid `.restrict-bound` sidecar was accepted) the `[0, punch_offset)`
    // prefix is hole-punched and reads as zeros, so starting at byte 0 would
    // report those dead blocks as corruption and repair would then drop an
    // otherwise-healthy restricted SST. `punch_offset()` is `0` for a normal table.
    let data_start = table.punch_offset()?;
    let report = crate::verify::verify_sst_file_with_context(
        folder_fs,
        table_path,
        config.encryption.as_ref(),
        // Repair KNOWS the durable id (recovery already cross-checked it
        // against the file name), so the verify probe enforces the same meta
        // id check — a checksum-clean forged tail meta falls back to the
        // intact MID mirror instead of dictating a forged ECC descriptor.
        Some(table.metadata.id),
        data_start,
    );
    // A TRANSIENT read failure DURING the walk (a retryable `Interrupted` /
    // `WouldBlock`) is not block corruption — routing it through salvage would
    // re-read the same bytes and drop a healthy block — and neither is an
    // ENVIRONMENTAL `PermissionDenied` (an ACL / ownership mistake the operator
    // fixes; the bytes on disk are not implicated). Propagate both so the repair
    // aborts and the operator retries, mirroring the decode-load gate below. Any
    // OTHER kind falls through to the corruption verdict: a truncation
    // (`UnexpectedEof`) is genuine on-disk damage, and a PERSISTENT failure
    // (`Other` / EIO) is not fixed by a retry, so aborting forever would strand
    // every healthy sibling table on one bad SST.
    // This matches the `is_corruption` allowlist policy exactly.
    //
    // This gate depends on the walk CLASSIFYING transient faults as one of these
    // two I/O-bearing variants: a mid-walk seek failure, a transient block-header
    // read, and a raw-section read all surface as `DataReadError` rather than
    // being folded into `HeaderCorrupted` / `TocCorrupted`, so a flaky read here
    // is not mistaken for corruption and salvaged.
    for e in &report.errors {
        if let crate::verify::BlockVerifyError::SstFileUnreadable { error, .. }
        | crate::verify::BlockVerifyError::DataReadError { error, .. } = e
            && error.kind().is_environmental()
        {
            // Preserve the ErrorKind: re-wrapping as `Other` would make the
            // caller's `is_environmental_io` check see a non-propagating kind
            // and re-grade this retryable / environmental failure as
            // corruption, defeating the propagation intent of this very gate.
            return Err(crate::Error::Io(crate::io::Error::new(
                error.kind(),
                error.to_string(),
            )));
        }
    }

    // A non-parity error is corruption regardless of any warnings.
    let verdict = if !report
        .errors
        .iter()
        .all(|e| matches!(e, crate::verify::BlockVerifyError::EccParityMismatch { .. }))
    {
        BlockVerifyVerdict::Corrupt
    } else if report
        .warnings
        .iter()
        .any(|w| matches!(w, crate::verify::BlockVerifyWarning::UnrecognizedEcc { .. }))
    {
        // Unrecognized ECC descriptor: the walk SKIPPED the SST-block
        // sections entirely (their trailer length is underivable), so
        // NOTHING about the data was verified — a stronger degradation
        // than a checked-but-unverifiable-parity report. Graded BEFORE the
        // parity-only arm below: parity mismatches in the still-walked
        // self-describing meta blocks must not mask the skipped data /
        // index sections.
        BlockVerifyVerdict::DegradedUnscanned
    } else if is_corruption(table.verify_kv_checksums())? {
        // The walk verifies raw block checksums but never DECODES entries,
        // so a stale per-KV footer behind a re-stamped block checksum still
        // reads clean at the block level. Footer-bearing tables must pass
        // the per-KV verification (a no-op without footers) BEFORE the
        // degradation arms below: a forged footer also leaves the parity
        // trailer mismatched, and grading that "parity-only degradation"
        // would let the keep-decision retain a table with a KNOWN-stale
        // entry digest. The salvage row path validates footers and drops
        // the forged block, so route it there. (Runs after the
        // unrecognized-ECC arm: the live table opened under its OWN
        // recovered descriptor, but an out-of-band unrecognized descriptor
        // already means nothing about the data was verified.)
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_blob_links())? {
        // Same reasoning for the blob-link list: the section carries no
        // per-section checksum, so the walk can only validate its SHAPE — a
        // flipped blob id passes it. Cross-check against the table's own
        // indirection entries (a no-op without the section); a mismatch is
        // corruption, and salvage derives the links from the recovered
        // indirections rather than copying the forged list.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_tli_mirrors())? {
        // Each TLI mirror is independently checksum-clean to the walk, but a
        // forged copy that DECODES to a different handle list would steer
        // the next recovery (which prefers the tail) away from real blocks.
        // Diverging decoded mirrors are corruption; salvage walks the HEAD
        // copy, so the recovered SST is rebuilt from a single, fully
        // re-verified handle list. BOTH mirrors forged to the SAME list that
        // OMITS a physical block are covered too: the salvage walk
        // cross-checks the index against the physical data-section tiling
        // and frames the uncovered bytes from their block headers, so the
        // hidden block is recovered (or reported dropped), never silently
        // missing from an apparently complete copy.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_seqno_bounds())? {
        // The seqno_bounds block is checksum-clean to the walk even when
        // its payload was re-stamped to another structurally valid map, and
        // scan_since_seqno trusts it to SKIP blocks — keeping the table
        // would silently omit live entries from every seqno-scoped scan.
        // Salvage re-derives the bounds from the re-emitted entries.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_block_entry_counts())? {
        // The out-of-band walk verifies only the outer frame and the per-KV
        // gate is a no-op without footers, so a checksum-clean block whose
        // trailer declares more entries than it decodes (a valid prefix, a
        // malformed tail) grades clean while a later scan silently omits the
        // tail. Full-decode every block; a count mismatch routes the table
        // through salvage (whose row path drops the under-decoding block).
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_zone_map())? {
        // A checksum-clean zone_map re-stamped to another structurally valid
        // map would let a predicate scan skip blocks its forged min/max
        // excludes, silently omitting matching rows. Diverging stats are
        // corruption; salvage re-derives the zone map from the re-emitted
        // blocks.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_locator())? {
        // A checksum-clean locator re-stamped to resolve a key to a block
        // other than its newest-version block would make point_read return a
        // stale value without falling back to the sorted index. A mapping
        // that disagrees with the decoded blocks is corruption; salvage
        // rebuilds the locator from the re-emitted entries.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_filter(config.prefix_extractor.as_ref()))? {
        // A checksum-clean filter re-stamped to another parseable filter
        // makes check_bloom silently skip point reads for any key turned
        // into a false negative. An existing key the filter reports as
        // definitely absent is corruption; salvage rebuilds the filter from
        // the re-emitted keys.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_block_layout())? {
        // A checksum-clean block_layout re-stamped to another structurally
        // valid boundary set mis-maps the partial range-read path's
        // decompression bounds, silently omitting keys. Boundaries that
        // disagree with the frames' actual inner blocks are corruption;
        // salvage re-derives the layout when re-encoding.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_point_read_reachability())? {
        // A checksum-clean embedded hash / binary index re-stamped to hide a
        // key (a MARKER_FREE bucket, a misdirected offset) makes point_read
        // miss existing data. Keys the block decodes but point_read cannot
        // retrieve are corruption; salvage re-emits the block with fresh
        // indexes.
        BlockVerifyVerdict::Corrupt
    } else if is_corruption(table.verify_metadata_bounds(false))? {
        // Both meta mirrors re-stamped CONSISTENTLY pass the mirror
        // comparison, yet run selection trusts the recorded key range — a
        // narrowed range hides real keys (and the range tombstones masking
        // older tables). Bounds that disagree with the decoded contents are
        // corruption; salvage re-derives the metadata from the re-emitted
        // entries.
        BlockVerifyVerdict::Corrupt
    } else if !report.is_ok() {
        // Parity-ONLY rot: every payload checksum verified clean, only the
        // recovery margin is dead. The data is fully readable, so it grades
        // like a warning-bearing report — salvage preferred (the rewrite
        // regenerates fresh parity), but never at the cost of dropping data
        // salvage cannot re-emit.
        BlockVerifyVerdict::DegradedButReadable
    } else if report.has_warnings() {
        // Everything scanned verified clean, but the parity trailers could
        // not be recomputed (a parity-less build). The caller decides
        // between salvage (a rewrite under fully-verifiable framing) and
        // keeping the table when salvage cannot re-emit it.
        BlockVerifyVerdict::DegradedButReadable
    } else {
        BlockVerifyVerdict::Clean
    };
    Ok(verdict)
}

/// Outcome of the salvage-mode block verify, from the repair gate's point of
/// view (see [`block_verify_verdict`]).
enum BlockVerifyVerdict {
    /// Every section verified against its raw on-disk checksum.
    Clean,
    /// Every payload the walk checked verified clean, but the table is
    /// DEGRADED: its parity trailers rotted or could not be recomputed while
    /// the payloads stayed intact. Prefer a salvage rewrite, but never at
    /// the cost of dropping data salvage cannot re-emit.
    DegradedButReadable,
    /// The walk could not scan the SST-block sections at all (an
    /// unrecognized ECC descriptor): the data is UNVERIFIED, not merely
    /// degraded — any keep decision must first verify it another way.
    DegradedUnscanned,
    /// At least one payload / section failed verification.
    Corrupt,
}

/// What the repair should do with a freshly-recovered table, based on the
/// salvage-mode block verify.
#[derive(Debug)]
enum RepairKeepDecision {
    /// The table joins the rebuilt manifest as-is.
    Keep,
    /// The table is routed through block salvage: its readable blocks are
    /// rewritten into a fresh copy beside it.
    Salvage,
    /// The table can be neither trusted nor faithfully salvaged under the
    /// active resurrection policy: it is EXCLUDED from the rebuilt manifest,
    /// with this reason, and its file removed once that manifest is durable.
    /// The tree still opens. Its rows come back through recompaction, a replica,
    /// a checkpoint plus journal replay, or a backup — and, where the reason
    /// says so, from a repair run WITH resurrection enabled instead.
    Drop(&'static str),
}

/// Whether the on-disk TOC catalogue could HIDE a deletion section — see
/// [`crate::verify::toc_may_hide_deletion_section`]. A STRUCTURAL catalogue
/// ambiguity grades `Ok(true)` (fail closed): if the catalogue cannot be parsed
/// to prove no section is hidden, salvage must not trust the parsed absence of
/// deletion metadata.
///
/// # Errors
///
/// Propagates a TRANSIENT [`crate::Error::Io`] from opening or reading the
/// trailer. Grading a retryable read as `true` would send a table
/// [`repair_with_salvage`](Self) already found corrupt to `Quarantine` — dropping
/// its healthy ranges from the rebuilt manifest — when a retry of the probe could
/// have let block salvage recover them.
pub(crate) fn toc_may_hide_deletions(
    folder_fs: &Arc<dyn crate::fs::Fs>,
    table_path: &std::path::Path,
) -> crate::Result<bool> {
    let mut file = match folder_fs.open(table_path, &crate::fs::FsOpenOptions::new().read(true)) {
        Ok(file) => file,
        // A TRANSIENT open failure propagates (a retry could open the file and
        // prove no hidden section); a PERSISTENT one fails closed as catalogue
        // ambiguity — we cannot read the TOC to prove it hides no deletion
        // section, so `true` drops the table rather than resurrecting masked rows.
        Err(e) => {
            let err = crate::Error::Io(e);
            return if is_environmental_io(&err) {
                Err(err)
            } else {
                Ok(true)
            };
        }
    };
    match crate::sfa::Reader::from_reader(&mut file) {
        Ok(reader) => Ok(crate::verify::toc_may_hide_deletion_section(
            reader.toc(),
            reader.toc_pos(),
        )),
        // A transient trailer read propagates (retry could prove no hidden
        // section); a persistent I/O failure or a structural trailer failure is
        // genuine catalogue ambiguity that fails closed.
        Err(crate::sfa::Error::Io(e)) => {
            let err = crate::Error::Io(e);
            if is_environmental_io(&err) {
                Err(err)
            } else {
                Ok(true)
            }
        }
        Err(_) => Ok(true),
    }
}

/// Grades a freshly-recovered table into a [`RepairKeepDecision`].
///
/// `Corrupt` always salvages. `DegradedButReadable` (payloads verified clean,
/// only the parity trailers rotted or could not be recomputed) salvages ONLY
/// when salvage can faithfully re-emit the table: a range-tombstone SST is
/// rejected by the block walk, so routing it through salvage would drop
/// healthy, verified data over dead parity — it is kept as-is (with an
/// operator-facing warning) instead. `DegradedUnscanned` (unrecognized ECC
/// descriptor: the walk verified NOTHING about the data) never keeps: a
/// rewritable table salvages, and a range-tombstone table — which cannot be
/// verified in full (every lazy side structure would need its own
/// handle-based check) and cannot be re-emitted — is excluded (recompact under
/// a supported scheme to re-admit it) instead of riding unverified into the
/// rebuilt manifest.
///
/// `allow_resurrection` governs the one ambiguous case: a corrupt catalogue
/// that could conceal a deletion section. Off (default) excludes the table (its
/// visibility is unrecoverable, so admitting it would resurrect masked rows);
/// on, it salvages, accepting that suppressed rows reappear.
fn verify_keep_decision(
    config: &Config,
    folder_fs: &Arc<dyn crate::fs::Fs>,
    table_path: &std::path::Path,
    table: &Table,
    allow_resurrection: bool,
    // Whether this repair may REWRITE a damaged table (block salvage). Off,
    // the degraded verdicts resolve without a rewrite: corrupt / unverifiable
    // content is set aside (with the reason pointing at the salvage-enabled
    // repair), while rotted-parity-but-readable content is KEPT — its payloads
    // verified clean, and blessing its digest is the entry into the normal
    // attributable heal (a patrol re-stamps the parity and reconciles).
    salvage: bool,
) -> crate::Result<RepairKeepDecision> {
    Ok(
        match block_verify_verdict(config, folder_fs, table_path, table)? {
            BlockVerifyVerdict::Clean => RepairKeepDecision::Keep,
            BlockVerifyVerdict::Corrupt => {
                // A `Corrupt` verdict from a catalogue that could HIDE a deletion
                // section (an omitted / renamed / shadowed `range_tombstones` or
                // `delete_bitmap`) makes the table's visibility unrecoverable: the
                // positional salvage walk reopens the same forged TOC, sees no
                // deletion section in the parsed state, and re-emits the suppressed
                // rows as LIVE. The salvage-side resurrection guard only inspects
                // the PARSED deletion state, which the concealment defeats, so the
                // decision has to happen here, governed by the resurrection flag:
                // off, exclude the table (admitting it would resurrect masked
                // rows); on, salvage and accept the resurrection. A relabel that
                // keeps the tiling intact but re-roles the block is caught inside
                // salvage itself (`salvage_with_context` fails closed on a corrupt
                // rebuildable section when no deletion is visible), which both this
                // path and the recovery-failure salvage path funnel through.
                //
                // Probed BEFORE the salvage-off branch below on purpose: with
                // salvage off the table is dropped either way, but this reason
                // is the accurate one — pointing that operator at a
                // salvage-enabled repair would mislead (it drops the
                // concealment case too, unless resurrection is enabled).
                if toc_may_hide_deletions(folder_fs, table_path)? && !allow_resurrection {
                    RepairKeepDecision::Drop(
                        "TOC corruption may hide deletion metadata (range tombstones \
                     / delete bitmap); its visibility is unrecoverable, so the table \
                     is excluded to avoid resurrecting masked rows. Enable \
                     resurrection to salvage it, accepting that suppressed rows \
                     reappear",
                    )
                } else if salvage {
                    RepairKeepDecision::Salvage
                } else {
                    RepairKeepDecision::Drop(
                        "verification found corrupt data blocks; run a salvage-enabled \
                         repair to rewrite the readable blocks",
                    )
                }
            }
            BlockVerifyVerdict::DegradedButReadable => {
                if salvage && table.range_tombstones().is_empty() {
                    RepairKeepDecision::Salvage
                } else {
                    log::warn!(
                        "table {} at {}: every payload verified clean but its ECC is \
                     partially uncheckable or rotted, and this repair cannot rewrite it \
                     (salvage off, or range tombstones it cannot re-emit) — keeping the \
                     table as-is; a patrol heal or recompaction re-stamps it under \
                     fresh, verifiable parity",
                        table.metadata.id,
                        table_path.display(),
                    );
                    RepairKeepDecision::Keep
                }
            }
            BlockVerifyVerdict::DegradedUnscanned => {
                if salvage && table.range_tombstones().is_empty() {
                    RepairKeepDecision::Salvage
                } else if salvage {
                    RepairKeepDecision::Drop(
                        "ECC descriptor unrecognized (the block walk cannot verify the \
                     table) and salvage cannot re-emit its range tombstones; the table \
                     is excluded (recompact it under a supported scheme to re-admit it)",
                    )
                } else {
                    RepairKeepDecision::Drop(
                        "ECC descriptor unrecognized (the block walk cannot verify the \
                     table); run a salvage-enabled repair to rewrite it under fresh, \
                     verifiable parity",
                    )
                }
            }
        },
    )
}

/// Outcome of [`try_salvage_table`].
enum SalvageOutcome {
    /// A clean replacement was written and reopened, ready to install.
    Salvaged(Table),
    /// Nothing was recoverable, or the replacement was rejected (an
    /// unreconstructible bulk-ingest offset); the caller records the table
    /// unreadable and removes its file once the manifest is durable.
    Unusable,
    /// The `reject_punched_without_bound` guard fired: a salvage-dropped data
    /// extent of the source reads as zeros (the hole-punch signature), so the
    /// source lost data to a punch whose bound is unrecoverable. The
    /// unrestricted replacement was rejected and removed — installing it would
    /// resurrect the reclaimed region's superseded rows.
    PunchedBoundLost,
}

/// What one [`try_salvage_table`] call operates on: the source paths and the
/// per-call policy. Bundled so the salvage entry keeps a small signature as
/// its policy surface grows.
#[cfg(feature = "std")]
struct TableSalvage<'a> {
    /// The damaged original to read, UNTOUCHED and still at its own path. It is
    /// never displaced first: the copy goes to a fresh name beside it, and the
    /// source is removed only once the manifest names the copy. A salvage that
    /// fails therefore leaves the directory exactly as it was found, and the
    /// retry re-derives the same result from the same bytes.
    source: &'a std::path::Path,
    /// Where the recovered copy is written (a fresh, unused path).
    table_path: &'a std::path::Path,
    /// The durable table id (its file name).
    table_id: TableId,
    /// Fail closed when the salvage walk reveals the source was PUNCHED (a
    /// dropped data extent reads as zeros) — set by the recovery-failure arm
    /// when it has no recoverable restriction bound and resurrection is off.
    /// The pre-salvage first-bytes probe catches a punched FIRST block cheaply,
    /// but a partial punch (the punch-on-drop reclaim continues past an
    /// individual `punch_hole` failure) can leave the first block intact while
    /// later prefix blocks are zeroed; only the walk sees those. The
    /// verification arm passes `false`: it derives the bound from its restricted
    /// view, so its salvage output is re-restricted, never ambiguous.
    reject_punched_without_bound: bool,
    /// Per-blob handle rewrite for a table referencing a blob file this repair
    /// reshaped (salvaged into a compacted copy, or recovered with a punched
    /// frontier); `None` on the plain corrupt-table salvage paths.
    blob_rewrite:
        Option<Arc<crate::HashMap<crate::vlog::BlobFileId, crate::salvage::BlobFileRewrite>>>,
}

fn try_salvage_table(
    config: &Config,
    fs: &Arc<dyn crate::fs::Fs>,
    allow_resurrection: bool,
    salvage: TableSalvage<'_>,
) -> crate::Result<SalvageOutcome> {
    let TableSalvage {
        source,
        table_path,
        table_id,
        reject_punched_without_bound,
        blob_rewrite,
    } = salvage;
    // Salvage under the tree's configured comparator + crypto/dictionary context
    // so the rewritten SST opens, orders, and decrypts / decompresses consistently
    // with the rest of the tree on reopen (the reopen below uses the same
    // `config.encryption` / `config.zstd_dictionary`).
    let report = crate::salvage::salvage_with_context(
        source,
        table_path.to_path_buf(),
        fs,
        &config.comparator,
        &crate::salvage::SalvageOptions {
            encryption: config.encryption.clone(),
            #[cfg(zstd_any)]
            zstd_dictionary: config.zstd_dictionary.clone(),
            // The real table id, so encrypted block AAD (which binds it) decrypts
            // and the recovered copy reopens under the same id below.
            table_id,
            // Repair KNOWS the durable id (the file name), so the salvage
            // open cross-checks the meta payload against it: a forged tail
            // id falls back to the intact MID mirror instead of stamping the
            // recovered copy with an identity the reopen below would reject.
            expected_stored_id: Some(table_id),
            // Governed by the recovery-wide resurrection flag: off (default), a
            // delete-bearing SST whose bitmap cannot be authenticated is excluded
            // rather than masked against an unverified bitmap; on, its rows are
            // re-emitted live, accepting that deleted rows reappear.
            allow_delete_resurrection: allow_resurrection,
            // The recovered SST is persisted at the tree's configured
            // durability, matching the manifest rebuilt around it.
            sync_mode: config.sync_mode,
            // The extractor is configuration, not persisted state: without
            // it the rebuilt filter loses the source's prefix hashes and
            // prefix scans see the salvaged copy as definitely absent.
            prefix_extractor: config.prefix_extractor.clone(),
            blob_rewrite,
            // A repair replacement always keeps the SOURCE's identity: it is
            // built at `{id}.repair-tmp` and swapped onto `{id}`, so the name it
            // ends up under is the identity it was stamped with.
            output_id: None,
            // Forward the caller's live-progress handle so the block walk
            // ticks per inspected / recovered block while it runs.
            progress: config.recovery_progress.clone(),
        },
    )?;
    if report.salvaged_path.is_none() {
        return Ok(SalvageOutcome::Unusable);
    }
    if !report.dropped.is_empty() {
        log::warn!(
            "salvaged table {table_id}: recovered {} block(s), dropped {} corrupt block(s)",
            report.blocks_salvaged,
            report.dropped.len(),
        );
    }
    if reject_punched_without_bound
        && dropped_data_extent_is_zeroed(&**fs, source, &report.dropped)?
    {
        // The source was punched but its bound is unrecoverable: the salvaged
        // replacement re-emits every intact block — including consumed,
        // superseded blocks a partial punch left inside the reclaimed prefix —
        // with nothing to restrict them. Reject it: this copy is our own
        // byproduct, holds nothing the untouched source does not, and no
        // manifest will ever name it, so it is removed outright. A removal
        // failure PROPAGATES — left behind, the numeric SST is an orphan the
        // next open must delete, and that open would fail on the same error.
        discard_unreferenced(&**fs, table_path, config.sync_mode)?;
        return Ok(SalvageOutcome::PunchedBoundLost);
    }

    // Reopen the freshly-written (clean) salvaged SST so it joins the rebuilt
    // manifest like any cleanly-recovered table.
    let checksum = crate::Checksum::from_raw(compute_table_checksum(&**fs, table_path)?);
    let table = Table::recover(repair_recover_params(
        config,
        table_path.to_path_buf(),
        checksum,
        table_id,
        Arc::clone(fs),
    ))?;
    // A salvaged copy of a bulk-ingested source still relies on the manifest-only
    // global_seqno offset the rebuilt manifest cannot recover: its entries stay at
    // local seqno 0, so installing it with offset 0 would silently mis-order and
    // over-expose them. Treat it as unsalvageable — remove the freshly-written
    // copy and let the caller record the table unreadable.
    if has_unrecoverable_ingest_offset(
        table.metadata.bulk_ingested,
        table.metadata.item_count,
        table.max_local_seqno(),
    ) {
        drop(table);
        // Remove the rejected replacement, and do NOT swallow the error. A
        // discarded `remove_file` failure would leave the freshly-written
        // numeric SST in `tables/`; repair would still install a manifest that
        // omits it and report success, but the next open classifies it as an
        // orphan and fails on the SAME persistent deletion, so the "repaired"
        // tree cannot reopen. Propagating instead keeps the two outcomes intact.
        discard_unreferenced(&**fs, table_path, config.sync_mode)?;
        return Ok(SalvageOutcome::Unusable);
    }
    Ok(SalvageOutcome::Salvaged(table))
}

/// Whether any salvage-dropped DATA extent of `source` contains a
/// structure-anchored all-zero run — the hole-punch signature. A real data
/// block's bytes are never all zero across a whole block, so a header-length
/// run that ends where intact structure begins (a decodable block header, the
/// next dropped extent, or the data-section end) was physically reclaimed, not
/// merely corrupted. Restricting the scan to the DROPPED extents keeps
/// legitimate zero runs inside intact (checksum-clean) blocks from
/// false-positiving, and the structural anchor keeps header-sized zero runs
/// inside a damaged extent's VALUE payloads from doing the same.
///
/// The scan covers each dropped extent IN FULL, up to the next dropped extent
/// or the end of the `data` section, not just its opening window: when the
/// physical chain breaks, the salvage walk surrenders the whole remaining tail
/// as ONE extent whose offset is the first DAMAGED (nonzero) frame, so punched
/// blocks deeper inside it would otherwise stay invisible and the salvaged
/// output would publish consumed records unrestricted.
///
/// # Errors
///
/// Propagates the open / read failure (a transient one aborts the repair for a
/// retry, exactly like the other salvage-path reads).
#[cfg(feature = "std")]
fn dropped_data_extent_is_zeroed(
    fs: &dyn crate::fs::Fs,
    source: &std::path::Path,
    dropped: &[crate::salvage::DroppedBlock],
) -> crate::Result<bool> {
    Ok(!excised_extents(fs, source, dropped)?.is_empty())
}

/// The physically excised byte ranges of `source`'s data section: the
/// structure-anchored all-zero runs inside its dropped extents, which is what
/// a hole punch leaves behind. Derived purely by scanning, so the result
/// survives any crash without being recorded anywhere — the property the
/// recovery model depends on for in-place repair (see
/// `docs/manifest-recovery.md`).
///
/// Ranges come back ascending and disjoint. A PREFIX punch yields one range
/// starting at the data section; a mid-file punch yields an interior range,
/// which the prefix-only restriction model cannot express — see the weak-spot
/// list in the same document.
///
/// # Errors
///
/// Propagates the open / read failure (a transient one aborts the repair for a
/// retry, exactly like the other salvage-path reads).
#[cfg(feature = "std")]
fn excised_extents(
    fs: &dyn crate::fs::Fs,
    source: &std::path::Path,
    dropped: &[crate::salvage::DroppedBlock],
) -> crate::Result<Vec<(u64, u64)>> {
    // Shortest run accepted as a punch. A hole is punched per DATA BLOCK, so a
    // punched block contributes a zero run at least a block long — while inside
    // an intact block a zero run is bounded by its framing (header, key/value
    // lengths and checksums are never all zero across a whole block). The
    // block-header length is the smallest possible block, so a run of that many
    // zeros cannot come from one intact framed block.
    const MIN_RUN: u64 = crate::table::block::Header::MIN_LEN as u64;
    let mut excised: Vec<(u64, u64)> = Vec::new();
    if dropped.is_empty() {
        return Ok(excised);
    }
    let mut file = fs.open(source, &crate::fs::FsOpenOptions::new().read(true))?;
    let file_len = crate::fs::FsFile::metadata(&*file)?.len;
    // The `data` section's physical end bounds every extent: a dropped extent
    // runs to the next dropped one or to that end, whichever comes first.
    let data_end = match crate::sfa::Reader::from_reader(&mut file) {
        Ok(reader) => reader
            .toc()
            .iter()
            .find(|e| e.name() == b"data")
            .map_or(file_len, |e| e.pos().saturating_add(e.len()).min(file_len)),
        // No readable TOC (the very corruption salvage is recovering from):
        // fall back to the file end — a superset of the data section, and the
        // per-extent scan below is bounded by the next extent anyway.
        Err(_) => file_len,
    };
    let mut starts: Vec<u64> = dropped
        .iter()
        .filter(|d| d.section == b"data" && d.offset < data_end)
        .map(|d| d.offset)
        .collect();
    starts.sort_unstable();
    starts.dedup();

    // A qualifying run must additionally be STRUCTURE-ANCHORED: it counts as
    // punch evidence only when it ends where intact structure begins — a
    // decodable block header (magic + type + the header's own checksum), the
    // next dropped extent, or the data-section end. SST values are arbitrary
    // bytes, so a header-sized zero run INSIDE a damaged extent's payload is
    // otherwise indistinguishable from a punch by length alone, and a bare
    // length test would reject an otherwise usable salvage as bound-lost under
    // the default no-resurrection policy.
    let header_decodes_at = |pos: u64| -> crate::Result<bool> {
        use crate::coding::Decode;
        let max = crate::table::block::Header::MAX_LEN as u64;
        let want = usize::try_from(file_len.saturating_sub(pos).min(max)).unwrap_or(0);
        if want < crate::table::block::Header::MIN_LEN {
            return Ok(false);
        }
        let bytes = crate::file::read_exact(&*file, pos, want)?;
        Ok(crate::table::block::Header::decode_from(&mut &bytes[..]).is_ok())
    };

    const CHUNK: usize = 64 * 1024;
    for (i, &start) in starts.iter().enumerate() {
        let end = starts.get(i + 1).copied().unwrap_or(data_end).min(data_end);
        let mut offset = start;
        let mut run: u64 = 0;
        while offset < end {
            let want = usize::try_from(end - offset).unwrap_or(CHUNK).min(CHUNK);
            let bytes = crate::file::read_exact(&*file, offset, want)?;
            for (j, &b) in bytes.iter().enumerate() {
                if b == 0 {
                    run += 1;
                } else {
                    let run_end = offset + j as u64;
                    if run >= MIN_RUN && header_decodes_at(run_end)? {
                        excised.push((run_end - run, run_end));
                    }
                    run = 0;
                }
            }
            offset += want as u64;
        }
        // A run reaching the extent end needs no header anchor: it terminates
        // at the next dropped extent or the data-section end, both of which
        // are structural boundaries themselves.
        if run >= MIN_RUN {
            excised.push((end - run, end));
        }
    }
    // The per-extent walks are already ascending and cannot overlap (each is
    // bounded by the next extent), but a run that ends exactly where the next
    // begins is one hole physically — merge so callers see maximal ranges.
    excised.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(excised.len());
    for (start, end) in excised {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    // Zeros are the SHAPE of a reclaim, not the proof: corruption that destroys
    // a data block leaves the same read-as-zeros run, and calling that a punch
    // condemns an otherwise salvageable table as bound-lost. A reclaim
    // deallocates, so each run must cover an actual HOLE; a backend that cannot
    // answer leaves it unproven, which keeps the zeros as damage.
    //
    // Probed as CONTAINS-a-hole over the run rather than whole-span-is-a-hole
    // or a single midpoint byte: the reclaim punches per block with unaligned
    // extents, so the filesystem deallocates only the wholly-contained pages
    // and leaves zero-filled boundary pages allocated — the whole span is
    // never a hole, and a midpoint byte can land on one of those allocated
    // boundary pages. Any hole inside the zeroed run is attributable to it.
    // Only the presence of the hole matters here — the single caller asks
    // whether the extent was reclaimed at all, not exactly where.
    let mut proven = Vec::with_capacity(merged.len());
    for (start, end) in merged {
        if fs.extent_contains_hole(source, start, end - start)? == Some(true) {
            proven.push((start, end));
        }
    }
    Ok(proven)
}

/// Whether the source SST's first data block reads as all zeros, the signature of
/// a hole-punched prefix. Probed at offset 0 (the `data` section is written first)
/// over a small window that stays WITHIN the first block, so even a punch of only
/// the first block is detected; a real data block's opening bytes (its first
/// entry's key-length varint and key) are never all zero, so an unpunched SST can
/// never false-positive.
///
/// Used by the recovery-failure salvage arm to fail closed on a PUNCHED source
/// whose bound is unrecoverable (missing / corrupt sidecar and no `Table` to
/// derive from): salvaging it into an unrestricted output would resurrect the
/// straddling block's sub-bound rows.
///
/// This is the CHEAP pre-salvage fast path only: a PARTIAL punch (the
/// punch-on-drop reclaim continues past an individual `punch_hole` failure) can
/// leave the first block intact while later prefix blocks are zeroed, which this
/// probe cannot see. [`dropped_data_extent_is_zeroed`] closes that gap after the
/// salvage walk, whose dropped extents expose every zeroed block.
///
/// # Errors
///
/// Propagates the open / read failure.
#[cfg(feature = "std")]
fn source_prefix_is_punched(
    fs: &dyn crate::fs::Fs,
    table_path: &std::path::Path,
) -> crate::Result<bool> {
    const PROBE: usize = 64;
    let file = fs.open(table_path, &crate::fs::FsOpenOptions::new().read(true))?;
    let len = crate::fs::FsFile::metadata(&*file)?.len;
    if len == 0 {
        return Ok(false);
    }
    let n = usize::try_from(len).unwrap_or(PROBE).min(PROBE);
    let bytes = crate::file::read_exact(&*file, 0, n)?;
    if !bytes.iter().all(|&b| b == 0) {
        return Ok(false);
    }
    // Zeros are not the evidence — corruption produces them too, and reading
    // ordinary damage as a lost punch bound sets the whole table aside instead
    // of salvaging the blocks that are still readable. A reclaim deallocates,
    // so the probed window must carry a HOLE — contains-a-hole, the same rule
    // the block-level punch classifier applies (an unaligned punch keeps its
    // zero-filled edges allocated); a backend that cannot answer leaves it
    // unproven.
    Ok(fs.extent_contains_hole(table_path, 0, n as u64)? == Some(true))
}

/// Re-imposes a tight-space restriction on a SALVAGED replacement SST, the single
/// point every salvage output funnels through so the restriction can never be
/// dropped on one path and kept on another.
///
/// Salvage rewrites its source as a fresh, UNPUNCHED table that re-emits the
/// straddling block's sub-bound rows, so a punched source's restriction must be
/// re-applied to the output or those superseded / deleted rows resurrect. With a
/// known `bound` and resurrection off, the sidecar is re-written (so a later
/// manifest-loss repair honors it against the now-unpunched file) and the table
/// reopened restricted. Otherwise (resurrection on, or no recoverable bound) the
/// salvaged table is kept whole and any stale sidecar cleared, since a lingering
/// sidecar would wrongly restrict the unpunched replacement on a later repair.
///
/// ANY failure re-imposing the restriction (sidecar write or restricted reopen)
/// removes the half-finished replacement before propagating. It carries the
/// straddling block's sub-bound rows with no valid sidecar, so a run that found
/// it would adopt it UNRESTRICTED and resurrect exactly the rows the restriction
/// hid. The source is untouched either way, so the retry re-salvages and
/// re-restricts from it. This holds for a PERSISTENT failure (an ENOSPC on the
/// sidecar write) as much as a transient one.
#[cfg(feature = "std")]
fn restrict_salvaged_output(
    folder_fs: &dyn crate::fs::Fs,
    config: &Config,
    table_path: &std::path::Path,
    salvaged: Table,
    restrict_bound: Option<crate::UserKey>,
    allow_resurrection: bool,
) -> crate::Result<Table> {
    match restrict_bound {
        Some(bound) if !allow_resurrection => {
            let table_id = salvaged.metadata.id;
            let restricted = crate::restrict_bound::write(
                folder_fs,
                table_path,
                config.encryption.as_deref(),
                table_id,
                &bound,
                config.sync_mode,
            )
            .and_then(|()| salvaged.reopen_restricted(bound));
            match restricted {
                Ok(table) => Ok(table),
                Err(e) => {
                    // Drop the salvaged handle's open file BEFORE removing it: a
                    // backend that refuses to unlink an OPEN file (Windows; the
                    // deletion path closes handles for this same reason) would
                    // fail while `salvaged` still holds it.
                    drop(salvaged);
                    // Remove on EVERY failure, transient or persistent: the
                    // replacement is unpunched with no valid sidecar, so a run
                    // that adopted it would resurrect the sub-bound rows. The
                    // source is untouched, so the retry re-salvages from it. A
                    // removal that itself fails propagates THAT error — an
                    // undiscardable half-finished replacement is the one thing
                    // that could corrupt the retry.
                    discard_unreferenced(folder_fs, table_path, config.sync_mode)?;
                    Err(e)
                }
            }
        }
        _ => {
            crate::restrict_bound::remove(folder_fs, table_path, config.sync_mode);
            Ok(salvaged)
        }
    }
}

/// The scanned totals of a validated blob file's LIVE frames. For a punched
/// file these cover only the suffix, and the difference to the whole-file
/// metadata totals is exactly the punched prefix's garbage — what seeds the
/// rebuilt manifest's fragmentation accounting.
#[cfg(feature = "std")]
struct BlobLiveTotals {
    items: u64,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
}

/// What a blob file's punch-geometry walk proved about its live data.
#[cfg(feature = "std")]
#[derive(Debug)]
enum BlobFrontier {
    /// No punched prefix: the whole file is live.
    Whole,
    /// Live data begins at this byte offset; the `[data_start, offset)` prefix
    /// is punched.
    Punched(u64),
    /// The punch consumed EVERY frame: the relocation completed and only the
    /// file's removal lagged the crash. No live data remains.
    FullyConsumed,
}

/// Derives a blob file's tight-space live-data frontier from its on-disk punch
/// geometry, for a manifest-loss repair.
///
/// The frontier — where a tight-space relocation's punched `[data_start,
/// frontier)` prefix ends and the live suffix begins — is recorded only in the
/// manifest's `blob_restrictions` section. Unlike an SST's restriction bound (a
/// KEY, which the block-aligned punch cannot reproduce and which therefore
/// needs its `.restrict-bound` sidecar), the blob frontier is a byte offset at
/// a frame boundary, so the geometry recovers it EXACTLY: the punch zeroes
/// precisely `[data_start, frontier)` and the first live frame's magic sits at
/// `frontier`.
///
/// Anchoring is structural, never length-based: a zeroed run counts only when a
/// frame decodes cleanly at its end, so a zero-filled value payload inside the
/// live suffix (stepped over by frame framing) can never move the frontier. A
/// partially completed punch (a crash mid-reclaim can leave intact-but-consumed
/// frames between holes) is walked hole-by-hole, and the frontier is the end of
/// the LAST zeroed run the anchored walk reaches. Non-zero bytes that fail to
/// decode end the walk at the last anchored frontier: content corruption is not
/// punch geometry, and it surfaces exactly as it would on an unpunched file.
///
/// Returns [`BlobFrontier::Whole`] when the first data byte is non-zero: the
/// punch always starts at the data start, so an unpunched file — including one
/// whose committed punch never ran before a crash — short-circuits without a
/// walk, keeping the common repair path at zero extra read cost. The redundant
/// unpunched prefix is superseded by relocated copies and reclaimed later, the
/// same safe fallback the SST path takes for a committed-but-unpunched slice.
/// Zeros through the whole data section are [`BlobFrontier::FullyConsumed`]:
/// a completed relocation whose file removal lagged the crash.
///
/// # Errors
///
/// Propagates I/O and TOC errors (the caller classifies transient ones for
/// retry, like every other per-file repair probe).
fn derive_blob_frontier(
    fs: &Arc<dyn crate::fs::Fs>,
    path: &std::path::Path,
    blob_id: crate::vlog::BlobFileId,
) -> crate::Result<BlobFrontier> {
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    let (data_start, data_end) = {
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        let data = reader
            .toc()
            .section(b"data")
            .ok_or(crate::Error::InvalidHeader("BlobFile"))?;
        let end = data
            .pos()
            .checked_add(data.len())
            .ok_or(crate::Error::InvalidHeader("BlobFile"))?;
        (data.pos(), end)
    };
    if data_start >= data_end {
        return Ok(BlobFrontier::Whole);
    }

    // Ends of the contiguous all-zero run starting at `from` (chunked reads,
    // capped by the data-section end).
    let skip_zeros = |from: u64| -> crate::Result<u64> {
        const CHUNK: u64 = 64 * 1024;
        let mut pos = from;
        while pos < data_end {
            // `#[allow]`, not `#[expect]`: target-width-dependent lint (`u64 as
            // usize`) — on 64-bit targets Clippy proves the `min()` bound fits
            // usize and an `#[expect]` would be unfulfilled under `-D warnings`.
            #[allow(
                clippy::cast_possible_truncation,
                reason = "min() bounds the window by CHUNK, which fits usize"
            )]
            let want = (data_end - pos).min(CHUNK) as usize;
            let chunk = crate::file::read_exact(&*file, pos, want)?;
            match chunk.iter().position(|b| *b != 0) {
                Some(hit) => return Ok(pos + hit as u64),
                None => pos += want as u64,
            }
        }
        Ok(data_end)
    };

    // Whether a zero run was physically RECLAIMED rather than merely zeroed.
    // Zeros are not the evidence: ordinary corruption produces exactly the same
    // bytes, and reading damage as geometry drops every handle below a
    // fabricated frontier — or, for a wholly zeroed section, removes a file
    // whose records were only damaged. A punch deallocates, so the run must
    // read back as a hole. The same rule the SST classifiers apply.
    //
    // Probed as CONTAINS-a-hole over the run: the run is bounded by the
    // neighbouring non-zero bytes (so demanding the whole span be unallocated
    // would reject genuine punches), and the punch's zero-filled edge pages
    // stay allocated on a real filesystem (so a single midpoint byte can land
    // on an allocated page even inside a genuine reclaim). A backend that
    // cannot answer leaves the run unproven, which keeps the zeros classified
    // as damage.
    let is_reclaimed = |from: u64, to: u64| -> crate::Result<bool> {
        if to <= from {
            return Ok(false);
        }
        Ok(fs.extent_contains_hole(path, from, to - from)? == Some(true))
    };

    // Fast path: an unpunched file's first frame magic (non-zero) sits at the
    // data start.
    if skip_zeros(data_start)? == data_start {
        return Ok(BlobFrontier::Whole);
    }

    // The last structure-anchored frontier: committed only once a frame has
    // decoded cleanly at a zeroed run's end. Zero = no run was ever anchored,
    // which reports the file whole (an unproven punch is content, not
    // geometry).
    let committed = |c: u64| {
        if c == 0 {
            BlobFrontier::Whole
        } else {
            BlobFrontier::Punched(c)
        }
    };
    let mut pos = data_start;
    let mut anchored: u64 = 0;
    loop {
        let run_end = skip_zeros(pos)?;
        // An UNRECLAIMED run is destroyed content, not geometry: stop at the
        // last proven frontier and let validation surface the damage exactly as
        // it would on an unpunched file.
        if !is_reclaimed(pos, run_end)? {
            return Ok(committed(anchored));
        }
        if run_end >= data_end {
            // Zeros to the section end. Only a completed relocation when
            // NOTHING was anchored below them: reclaim punches the consumed
            // prefix top-down from the data start, so a zeroed tail that
            // FOLLOWS intact anchored frames cannot be punch geometry — it is
            // destroyed data. Reporting it as consumed would drop a file whose
            // live frames are still referenced (and every table pointing at
            // it); keeping the anchored frontier instead lets the damaged
            // suffix surface through validation, exactly as the same damage
            // would on an unpunched file.
            return Ok(if anchored == 0 {
                BlobFrontier::FullyConsumed
            } else {
                committed(anchored)
            });
        }
        let mut scanner = crate::vlog::BlobFileScanner::resume(path, &**fs, blob_id, run_end)?;
        match scanner.next() {
            Some(Ok(entry)) if !entry.resynced => {
                anchored = run_end;
                pos = entry.frame_end;
            }
            Some(Err(e)) if is_environmental_io(&e) => return Err(e),
            // The zeroed run is not punch geometry (no frame decodes at its
            // end): keep the last anchored frontier.
            _ => return Ok(committed(anchored)),
        }
        // Chain frames from the anchor until the section ends cleanly or the
        // chain breaks (another hole, or content corruption).
        loop {
            match scanner.next() {
                None => return Ok(committed(anchored)),
                Some(Ok(entry)) if !entry.resynced => pos = entry.frame_end,
                Some(Err(e)) if is_environmental_io(&e) => return Err(e),
                Some(Ok(_) | Err(_)) => {
                    // The frame starting at `pos` failed (or the scanner
                    // resynced past unproven bytes). Another zeroed hole
                    // continues the walk; anything else is content corruption
                    // and ends it at the last anchored frontier.
                    if skip_zeros(pos)? == pos {
                        return Ok(committed(anchored));
                    }
                    break;
                }
            }
        }
    }
}

/// Records a recovered table the blob-dependency stage cannot publish: reported
/// unreadable, and removed once the manifest is durable. Consumes the handle.
///
/// The file is NOT touched here. This stage runs before the commit, where the
/// source is still the only copy of its rows and a crash must leave the
/// directory exactly as a retry expects to find it.
#[cfg(feature = "std")]
fn set_aside_table(
    table: Table,
    reason: &str,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    discard_after_commit: &mut Vec<(Arc<dyn crate::fs::Fs>, PathBuf, String)>,
) {
    let path = (*table.path).clone();
    let fs = table.fs.clone();
    drop(table); // release the handle
    discard_after_commit.push((fs, path.clone(), reason.to_string()));
    unreadable_files.push((path, reason.to_string()));
}

/// Same as [`set_aside_table`] for a source that is no longer open.
#[cfg(feature = "std")]
fn set_aside_path(
    fs: &Arc<dyn crate::fs::Fs>,
    path: &std::path::Path,
    reason: &str,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    discard_after_commit: &mut Vec<(Arc<dyn crate::fs::Fs>, PathBuf, String)>,
) {
    discard_after_commit.push((Arc::clone(fs), path.to_path_buf(), reason.to_string()));
    unreadable_files.push((path.to_path_buf(), reason.to_string()));
}

/// Whether every frame in `path`'s live data range (`[live_data_start, end)`)
/// verifies. Repair must not record a digest over damaged content: the
/// restamped digest would launder the corruption past every later integrity
/// check while reads of the affected values still fail — such a file is
/// salvaged instead. Framing checks alone are not enough, because the frame
/// checksum is unkeyed and covers only the ON-DISK bytes; each acceptance
/// criterion below closes a distinct restamp/reorder shape:
///
/// - every frame decodes and checksums cleanly, with no resynchronization;
/// - a compressed frame's payload DECOMPRESSES (a re-stamped checksum over an
///   undecodable compressed payload frames cleanly, yet every live read of
///   the value fails);
/// - frame keys never regress under the tree comparator (individually-valid
///   frames reordered on disk break the sorted-input contract every blob
///   reader and the relocation merge scanner rely on);
/// - for an unpunched file, the metadata counters match the scanned frames
///   (the meta block's item count, uncompressed byte total, and key range are
///   what blob GC's dead-file arithmetic trusts — an understated total lets
///   `is_dead` reclaim a file whose uncounted frames are still referenced).
///   A punched file's metadata describes the whole original file while the
///   scan covers only the live suffix, so it is checked against the LOWER
///   bounds the subset relation implies instead: totals at least the suffix
///   totals, key range containing the scanned suffix.
///
/// Returns the scanned live totals on success — the caller seeds the punched
/// prefix's garbage accounting from their difference to the whole-file
/// metadata.
///
/// # Errors
///
/// Propagates transient I/O for retry; any structural or persistent frame
/// failure is a conclusive `Ok(None)`.
#[cfg(feature = "std")]
fn validate_blob_frames(
    config: &Config,
    path: &std::path::Path,
    blob_id: crate::vlog::BlobFileId,
    live_data_start: u64,
) -> crate::Result<Option<BlobLiveTotals>> {
    let fs = &config.fs;
    // Metadata + compression via a placeholder-checksum open (the handle is
    // never read through; `recover_blob_file` only stores the checksum).
    let handle =
        crate::vlog::recover_blob_file(path, blob_id, crate::Checksum::from_raw(0), 0, fs)?;
    let compression = handle.compression();
    let comparator = &config.comparator;

    let scanner = if live_data_start > 0 {
        crate::vlog::BlobFileScanner::resume(path, &**fs, blob_id, live_data_start)?
    } else {
        crate::vlog::BlobFileScanner::new(path, &**fs, blob_id)?
    };
    let mut count: u64 = 0;
    let mut uncompressed_total: u64 = 0;
    let mut compressed_total: u64 = 0;
    let mut first_key: Option<crate::UserKey> = None;
    let mut prev: Option<(crate::UserKey, crate::SeqNo)> = None;
    for item in scanner {
        match item {
            Ok(entry) if !entry.resynced => {
                if crate::salvage::blob_key_regresses(comparator, prev.as_ref(), &entry) {
                    log::warn!(
                        "blob file {blob_id} at {}: frame at {} regresses below the \
                         previous frame's key — the frames were reordered",
                        path.display(),
                        entry.offset,
                    );
                    return Ok(None);
                }
                if crate::salvage::decompress_blob_value(
                    compression,
                    &entry.value,
                    entry.uncompressed_len as usize,
                    #[cfg(zstd_any)]
                    config.zstd_dictionary.as_deref(),
                )
                .is_err()
                {
                    log::warn!(
                        "blob file {blob_id} at {}: frame at {} does not decompress \
                         despite a clean checksum",
                        path.display(),
                        entry.offset,
                    );
                    return Ok(None);
                }
                count += 1;
                uncompressed_total += u64::from(entry.uncompressed_len);
                // Sum of u32-bounded on-disk lengths within one file: cannot
                // overflow u64 (the file itself cannot reach 2^64 bytes).
                compressed_total += entry.value.len() as u64;
                if first_key.is_none() {
                    first_key = Some(entry.key.clone());
                }
                prev = Some((entry.key.clone(), entry.seqno));
            }
            Err(e) if is_environmental_io(&e) => return Err(e),
            // A resynced frame has an unprovable boundary (damage upstream);
            // any other error is a structural or persistent frame failure.
            // Both are conclusive: this file's frames do not all verify.
            Ok(_) | Err(_) => return Ok(None),
        }
    }

    let meta = handle.meta();
    if live_data_start == 0 {
        let range_matches = match (&first_key, &prev) {
            (Some(first), Some((last, _))) => {
                meta.key_range.min().as_ref() == first.as_ref()
                    && meta.key_range.max().as_ref() == last.as_ref()
            }
            _ => count == 0,
        };
        if meta.item_count != count
            || meta.total_uncompressed_bytes != uncompressed_total
            || meta.total_compressed_bytes != compressed_total
            || !range_matches
        {
            log::warn!(
                "blob file {blob_id} at {}: metadata disagrees with the scanned frames \
                 (meta: {} items / {} uncompressed bytes; scanned: {count} / \
                 {uncompressed_total})",
                path.display(),
                meta.item_count,
                meta.total_uncompressed_bytes,
            );
            return Ok(None);
        }
    } else {
        // A punched file's metadata describes the WHOLE original file while
        // the scan covers only the live suffix — a subset — so exact equality
        // is impossible. The subset relation still bounds the metadata from
        // BELOW: totals must be at least the suffix totals and the key range
        // must contain the scanned suffix. Blessing understated totals would
        // let blob GC's dead-file arithmetic reclaim a file whose uncounted
        // frames are still referenced.
        let range_contains = match (&first_key, &prev) {
            (Some(first), Some((last, _))) => {
                comparator.compare(meta.key_range.min().as_ref(), first.as_ref())
                    != core::cmp::Ordering::Greater
                    && comparator.compare(last.as_ref(), meta.key_range.max().as_ref())
                        != core::cmp::Ordering::Greater
            }
            // An empty suffix constrains nothing.
            _ => true,
        };
        if meta.item_count < count
            || meta.total_uncompressed_bytes < uncompressed_total
            || meta.total_compressed_bytes < compressed_total
            || !range_contains
        {
            log::warn!(
                "blob file {blob_id} at {}: metadata understates the scanned live \
                 suffix (meta: {} items / {} uncompressed bytes; suffix: {count} / \
                 {uncompressed_total})",
                path.display(),
                meta.item_count,
                meta.total_uncompressed_bytes,
            );
            return Ok(None);
        }
    }
    Ok(Some(BlobLiveTotals {
        items: count,
        uncompressed_bytes: uncompressed_total,
        compressed_bytes: compressed_total,
    }))
}

/// Whether any of `table`'s blob indirections points BELOW a recovered punched
/// blob file's live-data frontier — i.e. into its zeroed prefix. The
/// id-presence dependency check cannot see this case: the blob EXISTS, but a
/// pre-relocation SST file left behind by a crash still holds handles into the
/// prefix the relocation punched, and publishing it would resolve those reads
/// into zeroed bytes. Returns the first offending handle's description, or
/// `None` when every handle lands in live blob data. Called only when at least
/// one recovered blob file carries a frontier, so the sequential entry scan
/// costs nothing on the common path.
#[cfg(feature = "std")]
fn handle_below_blob_frontier(
    table: &Table,
    frontiers: &crate::HashMap<crate::vlog::BlobFileId, u64>,
) -> crate::Result<Option<String>> {
    use crate::coding::Decode;

    for entry in table.scan()? {
        let entry = entry?;
        if entry.key.value_type != crate::ValueType::Indirection {
            continue;
        }
        let mut cursor = &entry.value[..];
        let ind = crate::blob_tree::handle::BlobIndirection::decode_from(&mut cursor)?;
        if let Some(&frontier) = frontiers.get(&ind.vhandle.blob_file_id)
            && ind.vhandle.offset < frontier
        {
            return Ok(Some(format!(
                "blob handle into file {} at offset {} lies below its recovered \
                 live-data frontier {frontier}",
                ind.vhandle.blob_file_id, ind.vhandle.offset,
            )));
        }
    }
    Ok(None)
}

/// What the blob directory scan recovered, and how, for the rebuilt manifest.
#[cfg(feature = "std")]
struct BlobRecovery {
    /// Blob files to record in the manifest (whole, punched, or salvaged).
    files: Vec<crate::vlog::BlobFile>,
    /// Files LEFT OUT of the manifest, with the reason each was set aside.
    unreadable: UnreadableFiles,
    /// Handle rewrites for reshaped files (salvaged / punched frontiers).
    rewrites: crate::HashMap<crate::vlog::BlobFileId, crate::salvage::BlobFileRewrite>,
    /// Garbage accounting seeded for punched files: the consumed prefix is
    /// stale by construction but can never be observed by a future
    /// compaction, so without this seed `is_dead` could never retire the
    /// file.
    frag: crate::blob_tree::FragmentationMap,
    /// Damaged originals whose salvaged replacement (a FRESH id) is in the
    /// rebuilt manifest, with a note describing what was recovered. They are
    /// still what the not-yet-rewritten SSTs reference, so the caller sets
    /// them aside only AFTER `persist_version`: doing it earlier would let a
    /// failed repair leave tables pointing at a blob id that no longer
    /// exists, and the retry would set those tables aside as unrecoverable.
    stale: Vec<(PathBuf, String)>,
    /// Files the rebuilt manifest will not name: a foreign name, a duplicate id,
    /// a file whose metadata cannot be read. Removed AFTER the commit, for the
    /// same reason `stale` is — the scan must leave the directory exactly as a
    /// retry expects to find it.
    discard: Vec<(PathBuf, String)>,
}

/// Discovers the blob files of a KV-separated tree for `repair` by scanning the
/// single `blobs/` folder, with no manifest id list to filter against.
///
/// Mirrors the table scan in [`repair_tree`]: a non-numeric name is recorded for
/// removal after the commit (the reopened tree's blob recovery parses every name
/// and would abort on a bad one), as is a blob file that cannot be checksummed
/// or whose metadata is unreadable. The recovered checksum is the whole-file
/// XXH3-128 digest, identical to the one the blob writer accumulated via
/// `ChecksummedWriter`, since blob files are written strictly sequentially.
fn recover_blob_files(
    config: &Config,
    published: &mut PublishedBlobReplacements<'_>,
    // Blob ids the recovered tables reference (a conservative SUPERSET: taken
    // before the blob-dependency filtering, so a table dropped later only
    // leaves an extra id here). An INVALID blob outside this set is left for
    // the post-commit sweep instead of being salvaged: the replacement would
    // be filtered out and deleted anyway, and under tight disk space the
    // pointless salvage can abort the whole repair with `StorageFull`.
    referenced: &crate::HashSet<crate::vlog::BlobFileId>,
) -> crate::Result<BlobRecovery> {
    let blobs_folder = config.path.join(crate::file::BLOBS_FOLDER);
    let mut blob_files: Vec<crate::vlog::BlobFile> = Vec::new();
    let mut unreadable: UnreadableFiles = Vec::new();
    let mut discard: Vec<(PathBuf, String)> = Vec::new();
    // How referencing SSTs' handles must be rewritten for the blob files this
    // scan RESHAPED: `Remap` for a file salvaged into a compacted copy,
    // `DropBelow` for an intact file recovered with a punched frontier. Empty
    // on the common path.
    let mut rewrites: crate::HashMap<crate::vlog::BlobFileId, crate::salvage::BlobFileRewrite> =
        crate::HashMap::default();
    // Damaged originals whose salvaged replacement is in the rebuilt manifest.
    // They are still referenced by the not-yet-rewritten SSTs, so they are set
    // aside only AFTER the manifest commit — see `BlobRecovery::stale`.
    let mut stale_originals: Vec<(PathBuf, String)> = Vec::new();
    let mut frag = crate::blob_tree::FragmentationMap::default();

    // No `blobs/` folder = no blob files (a blob tree that never spilled a value
    // to the value log). Nothing to recover; the manifest records an empty list.
    if !config.fs.exists(&blobs_folder)? {
        return Ok(BlobRecovery {
            files: blob_files,
            unreadable,
            rewrites,
            frag,
            stale: stale_originals,
            discard,
        });
    }

    // Collect and ORDER the candidates before recovering: `read_dir` order is
    // FS-dependent, and duplicate-id resolution below must be deterministic.
    // Per id, the writer's own `id.to_string()` spelling is the canonical file
    // and sorts first, so a foreign alternate spelling (`01` for id 1) can
    // never displace it regardless of directory iteration order.
    let mut candidates: Vec<(crate::vlog::BlobFileId, PathBuf, String)> = Vec::new();
    for dirent in config.fs.read_dir(&blobs_folder)? {
        let crate::fs::FsDirEntry {
            path: blob_path,
            file_name,
            is_dir,
        } = dirent;

        if is_dir || file_name == ".DS_Store" || file_name.starts_with("._") {
            continue;
        }

        // The file counts toward the progress percentage as soon as its
        // processing starts — including the entries the early arms below
        // reject (a crashed salvage temp, a non-numeric name), or a finished
        // repair would sit below 100% (`bytes_processed < bytes_total`).
        // Mirrors the table scan; best-effort stat, like the total.
        if let Some(p) = &config.recovery_progress
            && let Ok(meta) = config.fs.metadata(&blob_path)
        {
            p.add_bytes_processed(meta.len);
        }

        // A crashed earlier repair's in-progress blob salvage copy: it is
        // published by an atomic rename, so a surviving one is never
        // referenced and never authoritative. Remove it here rather than
        // classifying it as a foreign name (and re-salvage from the original
        // below if that file still fails validation). Both halves must parse
        // so a foreign name merely ending in the suffix is not treated as ours.
        if file_name
            .strip_suffix(".salvage-tmp")
            .is_some_and(|id| id.parse::<crate::vlog::BlobFileId>().is_ok())
        {
            // A removal failure fails the repair: the temp is outside the
            // rebuilt manifest, so the next open classifies it as an orphan
            // and its sweep hits the same removal failure — reporting success
            // for a tree that cannot open would be a lie. Quarantine is not
            // an out (it preserves damaged DATA; a temp is discardable
            // garbage, and a directory refusing removal refuses the rename
            // too). Retry after the filesystem is fixed.
            remove_temp(config, &blob_path)?;
            continue;
        }

        let Ok(blob_id) = file_name.parse::<crate::vlog::BlobFileId>() else {
            // A non-numeric name aborts the reopen's blob recovery (it parses
            // every name in blobs/), so it MUST go. It is removed after the
            // commit, and a removal that fails then fails the repair — a bad
            // name left in place is a tree that does not reopen.
            discard.push((blob_path.clone(), "file name is not a blob id".to_string()));
            unreadable.push((blob_path, "file name is not a blob id".to_string()));
            continue;
        };
        candidates.push((blob_id, blob_path, file_name));
    }
    // Fresh-id allocator for salvaged replacements. Taken over ALL candidates
    // before any is processed, so an id handed out here can never collide with
    // a file this scan has not reached yet. Repair runs single-threaded on a
    // tree nobody else has open, so no other allocator competes.
    //
    // `None` once the space is exhausted, and the failure is raised only where
    // an id is actually needed — a healthy tree holding a `u64::MAX` blob is
    // still perfectly repairable. Wrapping instead would hand out an id an
    // existing file already holds, and publishing the replacement onto that
    // name would destroy an unrelated original BEFORE the manifest commit, so
    // the loss would not even surface as a failed repair.
    let mut next_blob_id: Option<crate::vlog::BlobFileId> = candidates
        .iter()
        .map(|(id, _, _)| *id)
        .max()
        .map_or(Some(0), |max| max.checked_add(1));
    candidates.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| {
                (a.1 != blobs_folder.join(a.0.to_string()))
                    .cmp(&(b.1 != blobs_folder.join(b.0.to_string())))
            })
            .then_with(|| a.2.cmp(&b.2))
    });

    // The path recovered for each id, for the duplicate-vs-alias decision.
    let mut kept_paths: crate::HashMap<crate::vlog::BlobFileId, PathBuf> =
        crate::HashMap::default();

    for (blob_id, blob_path, _file_name) in candidates {
        if let Some(kept) = kept_paths.get(&blob_id) {
            // A second directory entry for an already-recovered id. An ALIAS
            // (symlink / case-folded spelling of the SAME physical file) is
            // skipped silently. A DISTINCT physical file must go: the manifest
            // records one checksum per id, and a stale duplicate left in
            // `blobs/` would race the kept file for reads on the next open
            // (directory iteration order picks the physical file).
            if same_physical_file(&*config.fs, kept, &*config.fs, &blob_path)? {
                continue;
            }
            let reason = format!("duplicate of blob file id {blob_id}");
            discard.push((blob_path.clone(), reason.clone()));
            unreadable.push((blob_path, reason));
            continue;
        }

        // Pre-commit file boundary: safe to abort. The caller's
        // `PublishedBlobReplacements` guard unwinds the replacements this run
        // already published under fresh ids — unlike the read-only table
        // scan, a successful blob salvage renames its copy to a normal
        // numeric name before the commit, and leaving those behind would make
        // the retry re-create each one beside its orphan (under tight disk
        // space exactly the sequence that fails).
        check_cancel(config)?;
        if let Some(p) = &config.recovery_progress {
            p.blob_file_discovered();
        }

        // A tight-space-punched blob records its live-data frontier only in
        // the manifest; with the manifest lost, re-derive it from the punch
        // geometry so the rebuilt manifest restores the restriction (the
        // snapshot encoder persists it from the recovered `live_data_start`).
        // Rebuilding with frontier 0 would instead leave a later relocation
        // scan starting inside the punched (zeroed) prefix. An unpunched file
        // short-circuits to 0 on its first (non-zero) data byte.
        // Persistent per-file failure: the rebuilt manifest omits this file, so
        // it is recorded for removal after the commit and reported unreadable.
        // The file must not be both omitted and left in place — that is a tree
        // whose next open has an orphan to sweep and may fail doing it.
        let discard_unreadable =
            |blob_path: PathBuf,
             e: &crate::Error,
             unreadable: &mut UnreadableFiles,
             discard: &mut Vec<(PathBuf, String)>| {
                discard.push((blob_path.clone(), e.to_string()));
                unreadable.push((blob_path, e.to_string()));
            };

        let frontier = match derive_blob_frontier(&config.fs, &blob_path, blob_id) {
            Ok(BlobFrontier::Whole) => 0,
            Ok(BlobFrontier::Punched(f)) => f,
            Ok(BlobFrontier::FullyConsumed) => {
                // Every frame is punched away: the relocation that consumed
                // this file completed, only its removal lagged the crash.
                // QUEUE that lagged drop for after the commit instead of
                // finishing it here: the pre-commit scan is read-only on
                // purpose — an abort before the commit (a cancellation, a
                // transient failure on a later file) must leave the directory
                // exactly as found, and the OLD manifest of an explicitly
                // invoked repair over an openable tree may still name this
                // file. Publishing an empty-suffix handle instead is not an
                // option either: whole-file metadata over zero live frames is
                // a file blob GC's stale-byte arithmetic can never retire
                // (its frames are already gone, so the stale count never
                // reaches the recorded totals). No live data is discarded:
                // the walk proved the whole data section reads as zeros. The
                // post-commit removal failing fails the repair, exactly as
                // the immediate removal used to.
                log::info!(
                    "blob file {blob_id} at {}: its punch consumed every frame — \
                     queueing the relocation's lagged file drop for after the commit",
                    blob_path.display(),
                );
                discard.push((
                    blob_path,
                    "fully punched blob file: a completed relocation's lagged drop".to_string(),
                ));
                continue;
            }
            // A TRANSIENT read (flaky I/O) is retryable: recording the blob
            // unreadable commits a manifest without the still-in-place file,
            // which the next open's orphan sweep then DELETES — permanent value
            // loss from a one-shot failure. Propagate so a retry re-reads it,
            // mirroring the table-recovery path.
            Err(e) if is_environmental_io(&e) => return Err(e),
            Err(e) => {
                discard_unreadable(blob_path, &e, &mut unreadable, &mut discard);
                continue;
            }
        };

        // IDENTITY before content: the id under which this file will be
        // published comes from its FILENAME, but the metadata records the id
        // it was written as. A mismatch means a renamed or swapped file — not
        // damaged content — and publishing it under the filename's id would
        // resolve existing SST handles into foreign frames (a read fails on
        // the key cross-check, or, when the foreign frame happens to hold the
        // same key, silently serves another generation's value). Salvage is
        // the WRONG remedy here (it would re-emit the foreign records under
        // the filename's id, laundering the swap); set the file aside.
        {
            // Placeholder-checksum open (stored, never verified). Same
            // transient/persistent split as every other per-file step: an
            // unreadable meta section sets THIS file aside, never the repair.
            let handle = match crate::vlog::recover_blob_file(
                &blob_path,
                blob_id,
                crate::Checksum::from_raw(0),
                0,
                &config.fs,
            ) {
                Ok(handle) => handle,
                Err(e) if is_environmental_io(&e) => return Err(e),
                Err(e) => {
                    discard_unreadable(blob_path, &e, &mut unreadable, &mut discard);
                    continue;
                }
            };
            let stored = handle.meta().id;
            if stored != blob_id {
                let e = crate::Error::InvalidHeader(
                    "blob file's stored metadata id disagrees with its file name",
                );
                log::warn!(
                    "blob file at {}: metadata records id {stored}, file name says \
                     {blob_id} — a renamed or swapped file; setting it aside",
                    blob_path.display(),
                );
                discard_unreadable(blob_path, &e, &mut unreadable, &mut discard);
                continue;
            }
        }

        // Validate the live frame range BEFORE recording a digest: hashing
        // damaged frames would restamp (launder) the corruption past every
        // later integrity check while reads of the affected values still
        // fail. An invalid file is SALVAGED instead of blessed or thrown away
        // whole: its surviving records are re-emitted into a compacted
        // replacement and the offset relocation is recorded so the referencing
        // SSTs are rewritten onto the new offsets.
        //
        // The replacement is written under a FRESH blob file id, so it never
        // displaces the damaged original: the original keeps its id and its
        // bytes, and only the manifest commit makes the replacement live. A
        // crash at any point before that commit therefore leaves the tree
        // exactly as it was found, and the retry re-derives everything from
        // the untouched original — no relocation record has to survive the
        // crash, because nothing was relocated in place. The original is
        // removed only AFTER the commit: removing it earlier would make a
        // crashed attempt leave SSTs referencing a blob id that no longer
        // exists, and the retry would record those tables unrecoverable.
        let Some(live) = validate_blob_frames(config, &blob_path, blob_id, frontier)? else {
            // An INVALID blob no recovered table references never reaches the
            // manifest: salvaging it would allocate a fresh id and burn disk
            // space on a replacement the reference filter deletes anyway —
            // and under tight space that pointless salvage can abort the
            // whole repair with `StorageFull`. Leave it for the post-commit
            // sweep instead; nothing reachable is lost.
            if !referenced.contains(&blob_id) {
                log::debug!(
                    "blob file {blob_id} is invalid and referenced by no recovered \
                     table; skipping its salvage and leaving it for the post-commit \
                     sweep"
                );
                discard.push((
                    blob_path,
                    "invalid and referenced by no recovered table; salvage skipped".to_string(),
                ));
                continue;
            }
            let Some(new_id) = next_blob_id else {
                log::error!(
                    "blob file {blob_id} needs salvaging but the blob id space is \
                     exhausted: there is no fresh name to publish a replacement \
                     under, and reusing one would destroy an existing file"
                );
                return Err(crate::Error::Unrecoverable);
            };
            next_blob_id = new_id.checked_add(1);
            let temp = blobs_folder.join(format!("{new_id}.salvage-tmp"));
            remove_temp(config, &temp)?;
            let salvage = (|| -> crate::Result<Option<(crate::vlog::BlobFile, crate::salvage::BlobSalvageReport)>> {
                let report = crate::salvage::salvage_blob_file(
                    &blob_path,
                    temp.clone(),
                    &config.fs,
                    // The OUTPUT's id: the replacement is stamped as the fresh
                    // file it is, so its metadata id matches the name it will
                    // be published under.
                    new_id,
                    &config.comparator,
                    frontier,
                    // Repair has the tree's dictionary context, so a
                    // dictionary-compressed blob salvages its intact frames
                    // instead of being set aside whole.
                    #[cfg(zstd_any)]
                    config.zstd_dictionary.as_ref(),
                )?;
                let Some(salvaged_path) = report.salvaged_path.clone() else {
                    return Ok(None);
                };
                let checksum = crate::Checksum::from_raw(compute_table_checksum(
                    &*config.fs,
                    &salvaged_path,
                )?);
                let bf = crate::vlog::recover_blob_file_from(
                    &salvaged_path,
                    new_id,
                    checksum,
                    0,
                    &config.fs,
                    0,
                )?;
                Ok(Some((bf, report)))
            })();
            let (bf, report) = match salvage {
                Ok(Some(pair)) => pair,
                // Nothing recoverable, or a persistent failure: report it and
                // queue its removal — exactly like any other unreadable blob.
                Ok(None) => {
                    remove_temp(config, &temp)?;
                    let e = crate::Error::InvalidHeader(
                        "blob value frames failed validation and no record was recoverable",
                    );
                    discard_unreadable(blob_path, &e, &mut unreadable, &mut discard);
                    continue;
                }
                Err(e) if is_environmental_io(&e) => {
                    // Nothing was published; dropping the temp restores the
                    // pre-repair state exactly, so the retry re-salvages and
                    // re-derives the remap. Best-effort is enough on this
                    // ERROR path: no manifest is committed, and the retry's
                    // candidate sweep sets a stuck temp aside.
                    let _ = config.fs.remove_file(&temp);
                    return Err(e);
                }
                Err(e) => {
                    remove_temp(config, &temp)?;
                    discard_unreadable(blob_path, &e, &mut unreadable, &mut discard);
                    continue;
                }
            };

            // Take the fresh id's own (free) name: nothing is displaced, so
            // there is nothing to unwind and no window in which a retry could
            // mistake an unverified file for the live blob. Until the
            // manifest names it, the replacement is an unreferenced file that
            // the next open's orphan sweep would remove.
            let new_path = blobs_folder.join(new_id.to_string());
            if let Err(e) = config.fs.rename(&temp, &new_path) {
                // Best-effort on this ERROR path: no manifest is committed,
                // so a stuck temp is swept by the retry's candidate scan.
                let _ = config.fs.remove_file(&temp);
                return Err(e.into());
            }
            // Under the caller's guard from this moment: any pre-commit exit
            // removes the published file.
            published.publish(new_id);
            config
                .fs
                .sync_directory_with(&blobs_folder, config.sync_mode)?;

            blob_files.push(bf);
            rewrites.insert(
                blob_id,
                crate::salvage::BlobFileRewrite::Remap {
                    new_id,
                    offsets: report.offset_remap.iter().copied().collect(),
                },
            );
            // The damaged original is left in place for now and set aside once
            // the manifest is committed: it is still what the not-yet-rewritten
            // SSTs reference, so removing it earlier would strand them if this
            // repair failed before committing.
            // Report only what the walk can actually account for. A
            // structural failure DESYNCHRONIZES the record stream: the walk
            // stops there and surrenders everything after it, so
            // `records_total` counts the prefix it managed to frame, not the
            // source's true population. Presenting that as "X of Y" would
            // claim a near-complete recovery while an unknown — possibly
            // enormous — tail went with it. Only when every loss was an
            // individually re-synced record is the total trustworthy.
            let surrendered_tail = report
                .dropped
                .iter()
                .any(|d| matches!(d.reason, crate::salvage::BlobDropReason::Corrupt(_)));
            let note = if surrendered_tail {
                format!(
                    "{} records salvaged into blob file {new_id}; the record \
                     stream then desynchronized and the remainder of the file \
                     was surrendered, so the number of records lost with it is \
                     not knowable",
                    report.records_salvaged,
                )
            } else {
                format!(
                    "{} of {} records salvaged into blob file {new_id} \
                     (the rest failed their checksums)",
                    report.records_salvaged, report.records_total,
                )
            };
            stale_originals.push((blob_path.clone(), note));
            kept_paths.insert(blob_id, blob_path);
            continue;
        };

        // The digest covers the live region only — `[frontier, end)` for a
        // punched file, the whole file for `frontier == 0` — matching what
        // `reopen_restricted` records and what integrity checks recompute.
        let checksum = match compute_table_checksum_from(&*config.fs, &blob_path, frontier) {
            Ok(c) => crate::Checksum::from_raw(c),
            // Same transient/persistent split as the frontier probe above.
            Err(e) if is_environmental_io(&e) => return Err(e),
            Err(e) => {
                discard_unreadable(blob_path, &e, &mut unreadable, &mut discard);
                continue;
            }
        };

        match crate::vlog::recover_blob_file_from(
            &blob_path, blob_id, checksum, 0, &config.fs, frontier,
        ) {
            Ok(bf) => {
                if frontier > 0 {
                    // A punched-but-intact file: a stale handle below its
                    // frontier (a pre-relocation SST left behind by a crash)
                    // must be dropped by the table-rewrite stage.
                    rewrites.insert(
                        blob_id,
                        crate::salvage::BlobFileRewrite::DropBelow(frontier),
                    );
                    // Seed the punched prefix's garbage: the recovered handle
                    // keeps its WHOLE-FILE metadata totals, while the prefix's
                    // frames can never be observed by a future compaction. An
                    // empty fragmentation map would pin the recorded stale
                    // bytes below the totals forever, so `is_dead` could
                    // never retire the file even after every suffix handle is
                    // gone. The differences cannot underflow: validation just
                    // proved the metadata totals are at least the suffix's.
                    let meta = bf.meta();
                    let prefix_items = meta.item_count - live.items;
                    let prefix_len =
                        usize::try_from(prefix_items).map_err(|_| crate::Error::Unrecoverable)?;
                    frag.insert(
                        blob_id,
                        crate::blob_tree::FragmentationEntry::new(
                            prefix_len,
                            meta.total_uncompressed_bytes - live.uncompressed_bytes,
                            meta.total_compressed_bytes - live.compressed_bytes,
                        ),
                    );
                }
                kept_paths.insert(blob_id, blob_path);
                blob_files.push(bf);
            }
            // Same transient/persistent split as the checksum read above.
            Err(e) if is_environmental_io(&e) => return Err(e),
            Err(e) => {
                discard_unreadable(blob_path, &e, &mut unreadable, &mut discard);
            }
        }
    }

    Ok(BlobRecovery {
        files: blob_files,
        unreadable,
        rewrites,
        frag,
        stale: stale_originals,
        discard,
    })
}

impl Config {
    /// Rebuilds the `MANIFEST` for the tree at this config's path from the SST
    /// files present on disk, then returns a [`RepairReport`].
    ///
    /// Use this only when a tree fails to open because its manifest is missing
    /// or corrupt but the SST files are intact. After a successful repair the
    /// tree opens normally; all recovered data is at L0 and a background
    /// compaction restructures it into proper levels (expect elevated I/O for a
    /// period proportional to the data size).
    ///
    /// # Exclusive access
    ///
    /// Repair rewrites `CURRENT`, writes a fresh snapshot, and removes the stale
    /// `edits-*` logs in place, so it requires exclusive access to the tree
    /// directory. It acquires the same cross-process directory lock as
    /// [`Config::open`] for the duration of the call: if another live instance
    /// holds the directory (open or repairing), this fails fast with
    /// [`crate::Error::Locked`] instead of corrupting that instance's manifest
    /// state. The lock can be disabled via
    /// [`Config::with_directory_lock`](crate::Config::with_directory_lock) for
    /// embedders enforcing exclusivity at a higher layer.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::FeatureUnsupported`] for KV-separated (blob)
    /// trees, and propagates any I/O error from scanning the directory or
    /// writing the new manifest. Individual unreadable SSTs do not fail the
    /// repair; they are reported in [`RepairReport::unreadable_files`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lsm_tree::{Config, SequenceNumberCounter};
    ///
    /// let config = Config::new(
    ///     "/var/lib/mydb",
    ///     SequenceNumberCounter::default(),
    ///     SequenceNumberCounter::default(),
    /// );
    /// let report = config.repair()?;
    /// println!("recovered {} tables, {} unreadable", report.recovered, report.unreadable);
    ///
    /// // `repair` borrows, so the same config opens the rebuilt tree.
    /// let _tree = config.open()?;
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn repair(&self) -> crate::Result<RepairReport> {
        repair_tree(self, false, false)
    }

    /// Like [`repair`](Self::repair), but when an SST fails whole-file recovery
    /// (`salvage = true`) it is block-salvaged instead of being left out: a fresh
    /// SST holding its recoverable blocks is built beside it, referenced by the
    /// rebuilt manifest, and swapped onto its name once that manifest is durable.
    ///
    /// A salvaged table may be missing the key ranges of its corrupt blocks
    /// (reported per file via [`RepairReport::salvaged`]); use this only as a
    /// last resort when losing the whole SST is worse than losing some keys.
    /// SSTs whose metadata, index, or SFA trailer is itself unreadable still
    /// cannot be salvaged and are reported unreadable.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory lock cannot be taken or the rebuilt
    /// manifest cannot be persisted; per-file recovery / salvage failures are
    /// reported in the [`RepairReport`], not returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lsm_tree::{Config, SequenceNumberCounter};
    ///
    /// let config = Config::new(
    ///     "/var/lib/mydb",
    ///     SequenceNumberCounter::default(),
    ///     SequenceNumberCounter::default(),
    /// );
    /// let report = config.repair_with_salvage(true)?;
    /// println!(
    ///     "recovered {} table(s), {} of them by salvage",
    ///     report.recovered, report.salvaged,
    /// );
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn repair_with_salvage(&self, salvage: bool) -> crate::Result<RepairReport> {
        repair_tree(self, salvage, false)
    }

    /// Like [`repair_with_salvage`](Self::repair_with_salvage), but with an
    /// explicit `allow_resurrection` policy. When `false` (the default the other
    /// entry points use), recovery drops data whose visibility became ambiguous
    /// after a lost restriction bound or a lost / forged delete mask; when
    /// `true`, it keeps that data, accepting that superseded or deleted rows may
    /// reappear. Either setting yields a valid, openable tree; the flag is the
    /// ONLY recovery decision an operator makes.
    ///
    /// The flag governs AMBIGUOUS VISIBILITY, not LOST BYTES. When a table (or
    /// one of its blocks) cannot be read at all, what it said about its keys is
    /// gone with it, and older versions of those keys survive in other tables:
    /// a value it had overwritten, or a key its tombstone had deleted, becomes
    /// visible again. Neither setting changes that, because nothing left on
    /// disk distinguishes "this key was deleted" from "this key was simply
    /// never rewritten". Deleting the lost range instead would destroy intact
    /// data — a flushed table's key range routinely spans most of the keyspace
    /// while its sequence numbers sit above every older level, so one corrupt
    /// block would erase most of the tree, irreversibly, where the stale read
    /// leaves every byte in place. The lost ranges are therefore REPORTED
    /// rather than acted on; see
    /// [`RepairReport::lost_coverage`](crate::RepairReport::lost_coverage).
    ///
    /// # Errors
    ///
    /// Returns an error if the tables directory cannot be scanned or a transient
    /// I/O fault interrupts recovery (retryable), or if the rebuilt manifest
    /// cannot be durably installed. See
    /// [`repair_with_salvage`](Self::repair_with_salvage).
    pub fn repair_with_resurrection(
        &self,
        salvage: bool,
        allow_resurrection: bool,
    ) -> crate::Result<RepairReport> {
        repair_tree(self, salvage, allow_resurrection)
    }

    /// Opens the tree, and when the open fails STRUCTURALLY, repairs per
    /// `policy` and opens again — the one-call recovery entry point for a
    /// caller that pairs the engine with an external write-ahead log.
    ///
    /// `Ok((tree, None))` is a healthy open. `Ok((tree, Some(report)))` is an
    /// open that succeeded only after a repair; the caller MUST then consult
    /// [`RepairReport::wal_replay_scope`] before its WAL replay, since the
    /// repair may have regressed persisted state below the log's trim
    /// watermark (see `docs/external-wal.md` § Replay after repair).
    ///
    /// Only failures that positively identify repairable on-disk structural
    /// damage engage the repair (see `is_repairable_structural`). Everything
    /// else is returned as-is: a TRANSIENT I/O failure (a repair would
    /// rebuild the manifest around files a healthy retry could still read), a
    /// held directory lock ([`Error::Locked`](crate::Error::Locked) — the
    /// repair would contend on the same lock), an UNSUPPORTED format version
    /// ([`Error::InvalidVersion`](crate::Error::InvalidVersion) — the store
    /// needs offline conversion or a matching binary, not a V5-only rebuild
    /// that would reject every table), a ROUTED tree's
    /// [`Error::Unrecoverable`](crate::Error::Unrecoverable) (route
    /// provenance is not persisted, so a missing routed table is
    /// indistinguishable from a route path change or an unmounted tier — a
    /// rebuild would omit every SST still on the old route), and every
    /// CONFIGURATION mismatch (wrong comparator, level route, dictionary, or
    /// encryption key) — repairing under a wrong configuration would rebuild,
    /// and could drop, perfectly healthy data.
    ///
    /// # Errors
    ///
    /// Propagates a transient-I/O or lock failure from the open and any
    /// repair failure. A failure of the POST-repair open surfaces as
    /// [`Error::RepairedButUnopened`](crate::Error::RepairedButUnopened),
    /// carrying the completed [`RepairReport`]: the repair is committed, so a
    /// retry opens without repairing and answers `None` — consume the report
    /// from the error, or the replay obligation it names is lost.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lsm_tree::{Config, RepairPolicy, SequenceNumberCounter};
    ///
    /// let config = Config::new(
    ///     "/var/lib/mydb",
    ///     SequenceNumberCounter::default(),
    ///     SequenceNumberCounter::default(),
    /// );
    /// let (tree, repaired) = config.open_or_repair(RepairPolicy::default().salvage(true))?;
    /// if let Some(report) = repaired {
    ///     println!("repaired; WAL must replay {:?}", report.wal_replay_scope());
    /// }
    /// # Ok::<_, lsm_tree::Error>(())
    /// ```
    pub fn open_or_repair(
        self,
        policy: RepairPolicy,
    ) -> crate::Result<(crate::AnyTree, Option<RepairReport>)> {
        let retry = self.clone();
        match self.open() {
            Ok(tree) => Ok((tree, None)),
            // A ROUTED tree's `Unrecoverable` is ambiguous, not positively
            // structural: route provenance is not persisted, so a table
            // missing from a routed folder is indistinguishable from a
            // same-level route path change or a temporarily unmounted tier —
            // and a rebuild would commit a manifest omitting every SST still
            // sitting on the old route, which the next open then sweeps as
            // orphans. Propagate; the operator verifies the route
            // configuration and invokes `repair` explicitly.
            Err(e @ crate::Error::Unrecoverable) if retry.level_routes.is_some() => Err(e),
            Err(e) if is_repairable_structural(&e) => {
                let report =
                    retry.repair_with_resurrection(policy.salvage, policy.allow_resurrection)?;
                // The follow-up open's failure must not DROP the report: the
                // repair is committed, so the caller's retry opens a healthy
                // tree without a repair and answers `None` — and an
                // external-WAL consumer would never learn the replay
                // obligation the report carries. Ship it WITH the error.
                // The follow-up open's failure must not DROP the report: the
                // repair is committed, so the caller's retry opens a healthy
                // tree without a repair and answers `None` — and an
                // external-WAL consumer would never learn the replay
                // obligation the report carries. Ship it WITH the error.
                let tree = match retry.open() {
                    Ok(tree) => tree,
                    Err(cause) => {
                        return Err(crate::Error::RepairedButUnopened {
                            report: alloc::boxed::Box::new(report),
                            cause: alloc::boxed::Box::new(cause),
                        });
                    }
                };
                Ok((tree, Some(report)))
            }
            Err(e) => Err(e),
        }
    }
}

/// Whether an `open` failure positively identifies repairable ON-DISK
/// structural damage — the only class [`Config::open_or_repair`] may answer
/// with a repair.
///
/// Everything else propagates: transient I/O (a retry could still read the
/// files a repair would rebuild around), a held directory lock (the repair
/// would contend on the same lock), an UNSUPPORTED format version (a pre-V5
/// or future database has no live decoder here — it needs offline conversion
/// or a matching binary, while the V5-only repair would reject every table
/// and commit a fresh manifest around nothing), and every CONFIGURATION
/// mismatch — a wrong comparator, level route, zstd dictionary, a
/// standard-vs-blob tree-type mismatch ([`crate::Error::TreeTypeMismatch`]),
/// or an encryption key surfacing as a decrypt failure. Those are reversible by
/// fixing the call, while a repair under the wrong configuration rebuilds —
/// and can drop — perfectly healthy data (e.g. re-ordering SSTs under a
/// mistyped comparator, or salvage discarding every block it cannot decrypt
/// with the wrong key). Fail closed: an unlisted (including future) error is
/// NOT repaired.
fn is_repairable_structural(e: &crate::Error) -> bool {
    use crate::Error;
    match e {
        // Data-shape I/O kinds are structural: a missing file (the manifest
        // itself, or a file it names), malformed bytes (the manifest-loss
        // open surfaces "current missing but artifacts present" as
        // InvalidData), or a truncated read. Any other I/O kind is
        // environment, and transient kinds are retried by the caller.
        Error::Io(io) => matches!(
            io.kind(),
            crate::io::ErrorKind::NotFound
                | crate::io::ErrorKind::InvalidData
                | crate::io::ErrorKind::UnexpectedEof
        ),
        Error::Unrecoverable
        | Error::Decompress(_)
        | Error::Excised { .. }
        | Error::ChecksumMismatch { .. }
        | Error::HeaderCrcMismatch { .. }
        | Error::InvalidTag(_)
        | Error::InvalidTrailer
        | Error::InvalidHeader(_)
        | Error::DecompressedSizeTooLarge { .. }
        | Error::ManifestFrameChecksumMismatch { .. }
        | Error::ManifestFooterInvalid(_)
        | Error::ManifestSectionInvalid(_)
        | Error::TornManifestEditLog { .. }
        | Error::RangeTombstoneDecode { .. }
        | Error::PageEccUnrecoverable { .. } => true,
        _ => false,
    }
}

/// Which repair capabilities [`Config::open_or_repair`] may engage.
///
/// Consulted when the open fails structurally. The default engages none of
/// them — the plain [`Config::repair`], which drops what it cannot recover
/// whole.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepairPolicy {
    /// Block-salvage SSTs / blob files that fail whole-file recovery instead
    /// of leaving them out (see [`Config::repair_with_salvage`]).
    pub salvage: bool,
    /// Keep ambiguous data (a lost restriction bound, an unauthenticated
    /// delete mask) instead of dropping it (see
    /// [`Config::repair_with_resurrection`]).
    pub allow_resurrection: bool,
}

impl RepairPolicy {
    /// Enables block-level salvage (see [`Self::salvage`]).
    #[must_use]
    pub const fn salvage(mut self, enable: bool) -> Self {
        self.salvage = enable;
        self
    }

    /// Enables resurrection of ambiguous data (see
    /// [`Self::allow_resurrection`]).
    #[must_use]
    pub const fn allow_resurrection(mut self, enable: bool) -> Self {
        self.allow_resurrection = enable;
        self
    }
}

/// Completes the publication a previous repair committed but could not carry out.
///
/// A repair publishes by committing the manifest and only THEN swapping its
/// replacements onto their names and removing what the manifest no longer
/// references; a failure in that last step leaves the directory half-published
/// under a durable, correct manifest. Rebuilding from it instead would salvage
/// the damaged original again while the previous replacement is still there, so
/// one history would enter L0 twice — and duplicated merge operands are applied
/// twice on read.
///
/// The committed manifest is what resolves it. It is the tree's own authority
/// on which files count: an `open()` sweeps everything it does not name as an
/// orphan, so this applies the same rule before the scan runs — every table and
/// blob file it does not reference is removed. No state is carried between
/// runs; this is derived from the durable manifest alone.
///
/// A manifest that does not load cleanly is exactly the case repair exists for,
/// so it is not consulted at all then and the scan rebuilds from everything.
///
/// # Errors
///
/// Propagates a removal failure: leaving a superseded file in place is what
/// corrupts the rebuild, so the repair must fail rather than proceed past it.
#[cfg(feature = "std")]
fn sweep_superseded_by_committed_manifest(config: &Config) -> crate::Result<()> {
    let recovery = match crate::version::recovery::recover(
        &config.path,
        &*config.fs,
        crate::config::ManifestRecoveryMode::AbsoluteConsistency,
        config.encryption.clone(),
    ) {
        Ok(recovery) => recovery,
        // A one-shot read fault on an otherwise healthy manifest must NOT be
        // read as "no committed manifest exists": the repair would then scan
        // and republish every file, and with an unfinished compaction's inputs
        // and outputs both on disk the rebuilt L0 applies duplicate merge
        // operands — where the authoritative manifest would have named which
        // files are live. Propagate for a retry.
        Err(e) if is_environmental_io(&e) => return Err(e),
        // A manifest that does not load cleanly is exactly the case repair
        // exists for: nothing committed to consult, the scan rebuilds from
        // everything.
        Err(_) => return Ok(()),
    };

    let referenced_tables: crate::HashSet<TableId> = recovery
        .table_ids
        .iter()
        .flatten()
        .flatten()
        .map(|t| t.id)
        .collect();
    // For temp-swap resolution: the manifest's per-table checksum is what
    // distinguishes a committed replacement from an abandoned build.
    let manifest_checksums: crate::HashMap<TableId, crate::Checksum> = recovery
        .table_ids
        .iter()
        .flatten()
        .flatten()
        .map(|t| (t.id, t.checksum))
        .collect();
    for (table_base_folder, folder_fs) in config.all_tables_folders() {
        if !folder_fs.exists(&table_base_folder)? {
            continue;
        }
        for dirent in folder_fs.read_dir(&table_base_folder)? {
            if dirent.is_dir {
                continue;
            }
            // A replacement a previous run built. The manifest is the authority
            // on what it is — but the manifest names the id in BOTH crash cases
            // (before the commit its entry still describes the SOURCE), so the
            // entry's checksum is what decides: only a committed run recorded
            // the temp's digest, and swapping an uncommitted build in would
            // destroy the source the manifest names. Both answers still come
            // from the durable manifest alone.
            if let Some(id) = table_id_from_repair_tmp_name(&dirent.file_name) {
                let table_path = table_base_folder.join(id.to_string());
                let published = match manifest_checksums.get(&id) {
                    Some(&manifest_checksum) => repair_tmp_is_published(
                        config,
                        &folder_fs,
                        &dirent.path,
                        id,
                        manifest_checksum,
                        recovery.restrictions.get(&id),
                    )?,
                    None => false,
                };
                if published {
                    commit_repair_tmp(
                        &*folder_fs,
                        &dirent.path,
                        &table_path,
                        config.sync_mode,
                        recovery.restrictions.contains_key(&id),
                    )?;
                    log::info!(
                        "repair: finished the pending swap of table {id} from a previous run",
                    );
                } else {
                    discard_unreferenced(&*folder_fs, &dirent.path, config.sync_mode)?;
                    log::info!("repair: dropped an abandoned replacement for table {id}");
                }
                continue;
            }
            // Only files the scan would ADOPT as tables are swept here; a
            // foreign name is left to the scan's own classification.
            let Ok(id) = dirent.file_name.parse::<TableId>() else {
                continue;
            };
            if referenced_tables.contains(&id) {
                continue;
            }
            // Unreferenced bare-id files are removed even though one COULD be a
            // compaction input whose referenced output turns out corrupt below.
            // Deliberate, not an oversight: an output carries the SAME seqnos as
            // its inputs, so letting the scan adopt both would put one history
            // into L0 twice, and version collapse would then gather a key's
            // merge operands from both copies — applying each operand twice and
            // silently serving a wrong value. An honest loss (reported via
            // `lost_coverage`) beats that corruption; the sweep also only
            // applies the tree's own rule, since any successful open removes
            // the same orphans. Content a corrupt referenced file lost comes
            // back from a replica, a checkpoint plus journal replay, or a
            // backup — never from resurrected orphans.
            discard_unreferenced(&*folder_fs, &dirent.path, config.sync_mode)?;
            log::info!("repair: table {id} is superseded by the committed manifest; removed");
        }
    }

    if config.kv_separation_opts.is_some() {
        let referenced_blobs: crate::HashSet<crate::vlog::BlobFileId> =
            recovery.blob_file_ids.iter().map(|(id, _)| *id).collect();
        let blobs_folder = config.path.join(crate::file::BLOBS_FOLDER);
        if config.fs.exists(&blobs_folder)? {
            for dirent in config.fs.read_dir(&blobs_folder)? {
                let Ok(id) = dirent.file_name.parse::<crate::vlog::BlobFileId>() else {
                    continue;
                };
                if dirent.is_dir || referenced_blobs.contains(&id) {
                    continue;
                }
                match config.fs.remove_file(&dirent.path) {
                    Ok(()) => log::info!(
                        "repair: blob file {id} is superseded by the committed manifest; removed",
                    ),
                    Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
    Ok(())
}

/// Removes the fresh-id blob replacements an aborting run already published,
/// so a pre-commit exit leaves nothing behind that a retry would have to
/// re-create beside an orphan — under tight disk space exactly the sequence
/// that fails. `NotFound` is success (a concurrent sweep, or an in-place
/// rewrite that published no file); any other removal failure propagates to
/// the caller (the guard's `Drop` downgrades it to a log line, since the run
/// is already aborting with its own error).
fn remove_published_blob_replacements(
    config: &Config,
    blobs_folder: &std::path::Path,
    replacement_ids: impl IntoIterator<Item = crate::vlog::BlobFileId>,
) -> crate::Result<()> {
    let mut removed = false;
    for id in replacement_ids {
        match config.fs.remove_file(&blobs_folder.join(id.to_string())) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    if removed {
        config
            .fs
            .sync_directory_with(blobs_folder, config.sync_mode)?;
    }
    Ok(())
}

/// Every fresh-id blob replacement the running repair has PUBLISHED (renamed
/// onto its numeric name) before the manifest commit. Dropping the guard
/// while it is armed removes them: the manifest never adopted the files, and
/// each one left behind makes the retry salvage its original AGAIN beside
/// the orphan — under the tight disk space this recovery targets, exactly
/// the extra full copy that fails with ENOSPC. `Drop`-based so EVERY
/// pre-commit exit — an error `?`, a cancellation, an early return — unwinds
/// them uniformly; disarmed immediately before the manifest commit, from
/// which point the replacements are what the manifest names (a commit
/// FAILURE deliberately leaves them: the retry's unreferenced-file filter
/// removes what it does not adopt, and a post-rename commit error must not
/// delete files a switched manifest may already reference).
struct PublishedBlobReplacements<'a> {
    config: &'a Config,
    blobs_folder: PathBuf,
    ids: Vec<crate::vlog::BlobFileId>,
    armed: bool,
}

impl<'a> PublishedBlobReplacements<'a> {
    fn new(config: &'a Config) -> Self {
        Self {
            config,
            blobs_folder: config.path.join(crate::file::BLOBS_FOLDER),
            ids: Vec::new(),
            armed: true,
        }
    }

    /// Records one replacement as published (its rename succeeded).
    fn publish(&mut self, id: crate::vlog::BlobFileId) {
        self.ids.push(id);
    }

    /// The manifest commit is next: the replacements are about to become the
    /// files it names, so an exit from here on must not remove them.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

/// What the on-disk `CURRENT` pointer says about a failed `persist_version`:
/// did its atomic pointer switch happen before the error?
enum CurrentProbe {
    /// The pointer names the rebuilt version: the switch happened.
    Switched,
    /// The pointer is ABSENT (`NotFound`) or names another version: the
    /// switch did not happen. Absence is conclusive — a successful rename
    /// leaves the pointer in place, so `NotFound` means the rename never
    /// ran; and an OLDER tree's surviving pointer names a lower version id
    /// (the rebuild allocates `max(v*) + 1`), so it never false-positives.
    NotSwitched,
    /// The pointer cannot be read (a transient or permission failure on its
    /// open or read, or a short pointer): NO conclusion. The caller must
    /// fail SAFE — treating this as "not switched" would delete files a
    /// possibly-published manifest references.
    Inconclusive,
}

/// The post-failure probe deciding if a failed `persist_version` died before
/// or after its atomic pointer switch.
fn probe_current(
    fs: &dyn crate::fs::Fs,
    folder: &std::path::Path,
    version_id: u64,
) -> CurrentProbe {
    let file = match fs.open(
        &folder.join(crate::file::CURRENT_VERSION_FILE),
        &crate::fs::FsOpenOptions::new().read(true),
    ) {
        Ok(file) => file,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return CurrentProbe::NotSwitched,
        Err(_) => return CurrentProbe::Inconclusive,
    };
    let Ok(bytes) = crate::file::read_exact(&*file, 0, 8) else {
        return CurrentProbe::Inconclusive;
    };
    let Ok(raw) = <[u8; 8]>::try_from(bytes.as_ref()) else {
        return CurrentProbe::Inconclusive;
    };
    if u64::from_le_bytes(raw) == version_id {
        CurrentProbe::Switched
    } else {
        CurrentProbe::NotSwitched
    }
}

impl Drop for PublishedBlobReplacements<'_> {
    fn drop(&mut self) {
        if !self.armed || self.ids.is_empty() {
            return;
        }
        // Best-effort: the run is already aborting with its own error, and a
        // survivor is an unreferenced file the next open's orphan sweep (or
        // the retry's unreferenced-file filter) removes.
        if let Err(e) =
            remove_published_blob_replacements(self.config, &self.blobs_folder, self.ids.drain(..))
        {
            log::warn!(
                "repair: could not remove a published blob replacement while \
                 aborting ({e}); the next open's orphan sweep removes it",
            );
        }
    }
}

/// Surfaces a caller's cooperative cancellation
/// ([`RecoveryProgress::request_cancel`](crate::RecoveryProgress::request_cancel))
/// as [`Error::Cancelled`](crate::Error::Cancelled). Consulted at file
/// boundaries, and ONLY before the manifest commit: the pre-commit scan is
/// read-only, so an abort there leaves the directory exactly as a retry
/// expects, while aborting the post-commit swaps or removals would leave work
/// the next open must redo anyway.
fn check_cancel(config: &Config) -> crate::Result<()> {
    if let Some(p) = &config.recovery_progress
        && p.is_cancel_requested()
    {
        return Err(crate::Error::Cancelled);
    }
    Ok(())
}

/// Publishes the repair's byte total (every file in the table folders, plus
/// the blob folder for a KV-separated tree) to the progress handle, if any.
/// Best-effort on purpose: a file that cannot be stat'ed is left out of the
/// total rather than failing the repair over a display number — the scan
/// itself will classify it.
fn publish_recovery_bytes_total(config: &Config) {
    let Some(progress) = &config.recovery_progress else {
        return;
    };
    let mut total: u64 = 0;
    let mut add_folder = |folder: &std::path::Path, fs: &dyn crate::fs::Fs| {
        let Ok(true) = fs.exists(folder) else {
            return;
        };
        let Ok(dirents) = fs.read_dir(folder) else {
            return;
        };
        for dirent in dirents {
            // Same skips as the scan loops, so `bytes_processed` can reach
            // `bytes_total` exactly.
            if dirent.is_dir
                || dirent.file_name == ".DS_Store"
                || dirent.file_name.starts_with("._")
            {
                continue;
            }
            if let Ok(meta) = fs.metadata(&dirent.path) {
                // Display-only sum: clamping at u64::MAX merely freezes the
                // percentage, while a checked overflow would fail the repair
                // over a progress number.
                total = total.saturating_add(meta.len);
            }
        }
    };
    for (folder, fs) in config.all_tables_folders() {
        add_folder(&folder, &*fs);
    }
    if config.kv_separation_opts.is_some() {
        add_folder(&config.path.join(crate::file::BLOBS_FOLDER), &*config.fs);
    }
    progress.set_bytes_total(total);
}

fn repair_tree(
    config: &Config,
    salvage: bool,
    allow_resurrection: bool,
) -> crate::Result<RepairReport> {
    // Hold the cross-process directory lock for the whole repair: it rewrites
    // CURRENT, writes a fresh snapshot, and sweeps `edits-*` in place, so a
    // concurrent open / repair of the same directory would corrupt the manifest.
    // A second acquirer fails fast with `Error::Locked`. Dropped at function
    // return, releasing the lock. The directory is expected to exist (repair
    // operates on an existing tree).
    #[cfg(feature = "std")]
    let _directory_lock =
        crate::config::acquire_directory_lock(&*config.fs, &config.path, config.directory_lock)?;

    if let Some(p) = &config.recovery_progress {
        p.set_phase(crate::RecoveryPhase::PendingSwaps);
    }
    check_cancel(config)?;

    // Finish the previous run's outstanding cleanup first. See
    // `sweep_superseded_by_committed_manifest`.
    #[cfg(feature = "std")]
    sweep_superseded_by_committed_manifest(config)?;

    // Byte totals for the progress percentage, from an upfront listing —
    // before the scan phase starts, so `bytes_processed / bytes_total` is
    // meaningful from the first processed file.
    publish_recovery_bytes_total(config);
    if let Some(p) = &config.recovery_progress {
        p.set_phase(crate::RecoveryPhase::ScanningTables);
    }

    // Best recovered copy per table id: a complete recovery beats a lossy salvage,
    // so a duplicate id across aliased / routed table folders keeps only the best
    // (and is never added to two L0 runs). See `keep_best_candidate`.
    let mut recovered_by_id: crate::HashMap<TableId, TableCandidate> = crate::HashMap::default();
    let mut unreadable_files: Vec<(PathBuf, String)> = Vec::new();
    // What each scanned table covers, captured while its metadata was readable.
    // Joined against the exclusions at the end to report the coverage the
    // rebuilt manifest lost.
    let mut coverage_by_path: crate::HashMap<PathBuf, (UserKey, UserKey, Option<SeqNo>)> =
        crate::HashMap::default();
    // Files the rebuilt manifest will not name: a foreign name, a duplicate id,
    // a table no bound can make safe, a source a salvaged copy supersedes. They
    // are removed AFTER the commit and not one moment earlier — the scan is
    // read-only precisely so a crash leaves the directory as the retry expects
    // to find it, and the retry then derives everything again from the same
    // bytes. Each entry carries the backend that owns the file: a per-level
    // route stores its tables through an `Fs` the primary one cannot see.
    let mut discard_after_commit: Vec<(Arc<dyn crate::fs::Fs>, PathBuf, String)> = Vec::new();
    // Replacements built at `{id}.repair-tmp`, swapped onto `{id}` once the
    // manifest that adopts them is durable. Same ordering rule as the removals,
    // for the same reason: before the commit the source is still the only copy
    // of its rows and is what the manifest names.
    // (fs, temp path, final path, replacement is restricted): the last flag
    // rides along so a retried swap can tell an already-moved replacement
    // sidecar from a stale source one (see `commit_repair_tmp`).
    let mut swap_after_commit: Vec<(Arc<dyn crate::fs::Fs>, PathBuf, PathBuf, bool)> = Vec::new();

    // Live sidecars ({id}.heal-attest, {id}.restrict-bound) found by the scan.
    // Judged against the FINAL rebuilt set after the dedup: one whose table
    // survived is preserved, an orphan is removed post-commit.
    let mut live_sidecars: Vec<(TableId, PathBuf, Arc<dyn crate::fs::Fs>)> = Vec::new();

    for (table_base_folder, folder_fs) in config.all_tables_folders() {
        if !folder_fs.exists(&table_base_folder)? {
            continue;
        }

        // ORDER the entries before recovering: `read_dir` order is
        // FS-dependent, and duplicate-id resolution keeps the FIRST complete
        // copy — so an unordered scan would rebuild different contents from the
        // same directory across runs. Per id, the writer's own `id.to_string()`
        // spelling is the canonical file and sorts first, so a foreign alternate
        // spelling (`01` for id 1) can never displace it. Mirrors the blob-file
        // scan.
        let mut dirents = folder_fs.read_dir(&table_base_folder)?;
        dirents.sort_by(|a, b| {
            let key = |e: &crate::fs::FsDirEntry| {
                let id = e.file_name.parse::<TableId>().ok();
                (
                    id.is_none(),
                    id.unwrap_or(0),
                    id.is_none_or(|id| e.file_name != id.to_string()),
                )
            };
            key(a)
                .cmp(&key(b))
                .then_with(|| a.file_name.cmp(&b.file_name))
        });

        'dirent: for dirent in dirents {
            let crate::fs::FsDirEntry {
                path: table_path,
                file_name,
                is_dir,
            } = dirent;

            // https://en.wikipedia.org/wiki/.DS_Store
            if is_dir || file_name == ".DS_Store" || file_name.starts_with("._") {
                continue;
            }

            // Pre-commit file boundary: safe to abort (the scan is read-only),
            // and the file counts toward the progress percentage as soon as
            // its processing starts. Best-effort stat, like the total.
            check_cancel(config)?;
            if let Some(p) = &config.recovery_progress
                && let Ok(meta) = folder_fs.metadata(&table_path)
            {
                p.add_bytes_processed(meta.len);
            }

            // One grammar decides what each name IS (`TableDirEntry`, shared
            // with `Tree::open`'s sweep so the two can never disagree on
            // ownership); this match is the repair scan's POLICY for each kind.
            use crate::file::TableDirEntry;
            let table_id = match TableDirEntry::classify(&file_name) {
                // LIVE sidecar artifacts are not table files, and repair must
                // not remove one whose table survives: `Tree::open` PRESERVES
                // a live `{id}.heal-attest` (the next scrub reconciles a
                // crashed digest refresh through it), and the
                // `.restrict-bound` bound is read FOR its SST via
                // `restrict_bound::read`. Whether a sidecar is live is
                // decided AGAINST THE REBUILT SET after the dedup below —
                // one whose table did not survive is an orphan the repair
                // itself removes, because leaving it hands the next open an
                // unconditional sweep whose refused unlink would turn this
                // run's success into an unopenable tree.
                TableDirEntry::HealAttest(id) | TableDirEntry::RestrictBound(id) => {
                    live_sidecars.push((id, table_path, Arc::clone(&folder_fs)));
                    continue;
                }
                // The disposable `.tmp` shapes are crash leftovers no state
                // references; the open sweeps them unconditionally, so repair
                // removes them itself (post-commit) for the same reason.
                TableDirEntry::HealAttestTmp(_)
                | TableDirEntry::HealTmp(_)
                | TableDirEntry::RestrictBoundTmp(_) => {
                    discard_after_commit.push((
                        Arc::clone(&folder_fs),
                        table_path,
                        "disposable crashed-heal artifact; the next open would \
                         sweep it and fail on a refused removal"
                            .to_string(),
                    ));
                    continue;
                }
                // `{id}.repair-tmp` is a replacement a previous repair was
                // building and never published: no manifest names it (one that
                // did was swapped in before this scan started), and its source
                // is whatever this scan finds under `{id}`. It is therefore
                // garbage, and it is removed NOW rather than after the commit —
                // this run needs the name free to build its own replacement.
                // Removing it cannot lose rows: every row it holds came from a
                // file this scan is about to read.
                TableDirEntry::RepairTmp(_) => {
                    discard_unreferenced(&*folder_fs, &table_path, config.sync_mode)?;
                    log::warn!(
                        "repair: dropped an abandoned replacement {}",
                        table_path.display(),
                    );
                    continue;
                }
                // A foreign name cannot be a table id, and `Tree::open` rejects
                // such a file outright (recovery parses every name in
                // `tables/`). Leaving it there would let repair report success
                // over a tree that still cannot reopen, so it is removed after
                // the commit; a removal that fails then fails the repair.
                TableDirEntry::Foreign => {
                    set_aside_path(
                        &folder_fs,
                        &table_path,
                        "file name is not a table id",
                        &mut unreadable_files,
                        &mut discard_after_commit,
                    );
                    continue;
                }
                TableDirEntry::Table(id) => id,
            };

            if let Some(p) = &config.recovery_progress {
                p.table_discovered();
            }

            // Skip a duplicate id ONLY when we already hold a COMPLETE copy — a
            // duplicate cannot improve on it. A previously-seen LOSSY salvage does
            // NOT skip: this copy is still evaluated and may supersede it.
            if let Some(existing) = recovered_by_id
                .get(&table_id)
                .filter(|c| c.fidelity.is_complete())
            {
                // If this path physically ALIASES the retained copy (a symlink /
                // junction / case-insensitive alias resolving to the same directory
                // entry, e.g. two configured folders pointing at one location), it
                // is the SAME file, not a genuine duplicate. Removing it would
                // destroy the kept copy and leave the manifest referencing a
                // missing SST, so skip it IN PLACE.
                if same_physical_file(&*folder_fs, &table_path, &*existing.fs, &existing.path)? {
                    continue;
                }
                // A genuine duplicate: removed after the commit so recovery
                // cannot later resolve it instead of the kept copy (the manifest
                // records only id + checksum, not a path).
                set_aside_path(
                    &folder_fs,
                    &table_path,
                    "duplicate table id; a complete copy is already held",
                    &mut unreadable_files,
                    &mut discard_after_commit,
                );
                continue;
            }

            // Hash the file and open it. A non-transient hashing failure (a bad
            // data sector) is FOLDED into the recover Result so the salvage arm
            // below can recover the table's intact blocks, instead of recording
            // the whole table unreadable — which the next open's orphan cleanup
            // would then delete. Table::recover would fail on the same bytes, so
            // skip it; block salvage opens with a placeholder digest and drops
            // only the unreadable blocks.
            let recovered = match compute_table_checksum(&*folder_fs, &table_path) {
                Ok(c) => Table::recover(repair_recover_params(
                    config,
                    table_path.clone(),
                    crate::Checksum::from_raw(c),
                    table_id,
                    folder_fs.clone(),
                )),
                // A TRANSIENT read (flaky I/O) while hashing is retryable:
                // recording it unreadable commits a manifest without the
                // still-in-place file, which the next open's orphan cleanup then
                // deletes — permanent loss from a one-shot failure. Propagate it
                // so a retry re-reads the table, mirroring the recover arms below.
                Err(e) if is_environmental_io(&e) => return Err(e),
                // A PERSISTENT read failure (a bad data sector, a corrupt trailer)
                // is genuine damage but does not doom the whole table: fold it into
                // the recover Result so the structural-failure salvage arm below
                // recovers the intact blocks (or records it unreadable with salvage
                // off).
                Err(e) => Err(e),
            };

            // Remember what this table COVERS while its metadata is in hand. If
            // it ends up excluded, that coverage is what the rebuilt manifest
            // lost, and the report names it: older versions of those keys
            // survive elsewhere and become visible again, which no repair can
            // distinguish from "the key was simply never rewritten".
            if let Ok(t) = &recovered {
                let range = t.metadata.key_range.clone();
                // The bound is UNKNOWN when the table's sequence base lived in
                // the lost manifest: this open deliberately passes offset 0, so
                // `get_highest_seqno` would report the on-disk LOCAL maximum
                // (normally 0 for a bulk-ingested SST) and an operator scoping
                // the affected history by it would stop far below the truth.
                let seqno = (!has_unrecoverable_ingest_offset(
                    t.metadata.bulk_ingested,
                    t.metadata.item_count,
                    t.max_local_seqno(),
                ))
                .then(|| t.get_highest_seqno());
                coverage_by_path.insert(
                    table_path.clone(),
                    (range.min().clone(), range.max().clone(), seqno),
                );
            }

            // Fail closed on a table whose bulk-ingest sequence offset cannot be
            // reconstructed. A bulk-ingested SST stores every entry at LOCAL seqno
            // 0 and relies on a manifest-only `global_seqno` for its effective MVCC
            // ordering; the on-disk seqnos carry no trace of it. The rebuilt
            // manifest hard-codes offset 0, so keeping such a table would make its
            // entries appear OLDER than they are — visible to snapshots that never
            // saw them and sorted into the wrong L0 order. Drop it instead of
            // silently corrupting MVCC (see `has_unrecoverable_ingest_offset`).
            if matches!(&recovered, Ok(t) if has_unrecoverable_ingest_offset(
                t.metadata.bulk_ingested,
                t.metadata.item_count,
                t.max_local_seqno(),
            )) {
                drop(recovered); // release the file handle
                set_aside_path(
                    &folder_fs,
                    &table_path,
                    "bulk-ingest sequence offset cannot be reconstructed from the SST",
                    &mut unreadable_files,
                    &mut discard_after_commit,
                );
                continue;
            }

            // Rebuild the restricted view of a tight-space-PUNCHED SST. Tight-space
            // compaction reclaims a table's consumed prefix data blocks in place
            // (hole-punched, reading back as zeros) and records the exact bound in a
            // `.restrict-bound` sidecar beside the SST. A rebuilt manifest must
            // re-apply that restriction, or later reads and compactions traverse the
            // punched blocks and fail. The bound comes from the sidecar (the SST
            // itself is never mutated, so its whole-file checksum stays valid);
            // written strictly post-commit, a valid sidecar is itself proof of a
            // committed restriction, so its bound is honored directly (see below).
            // Whether the recovered view's restriction came from PUNCH GEOMETRY
            // with resurrection off: that bound is the straddling block's END
            // key, which can discard the block's still-live suffix rows — a
            // deliberate, non-salvage loss the report must carry (scoped by the
            // source's coverage), or `wal_replay_scope` stays `TailOnly` while
            // live rows were removed. An exact sidecar bound and the
            // resurrection-mode bound (which keeps the whole readable region)
            // lose nothing.
            let mut geometry_lossy = false;
            let recovered = 'restrict: {
                let Ok(table) = recovered else {
                    break 'restrict recovered;
                };
                // Read the input's `.restrict-bound` sidecar. A TRANSIENT read
                // (Interrupted / WouldBlock) is retryable, so it propagates. A
                // MISSING / id-MISMATCHED / CORRUPT sidecar or a PERSISTENT read
                // leaves no trustworthy exact bound and falls to the punch-geometry
                // path below. See `docs/manifest-recovery.md`.
                let sidecar_bound: Option<crate::UserKey> = match crate::restrict_bound::read(
                    &*folder_fs,
                    &table_path,
                    config.encryption.as_deref(),
                ) {
                    Ok(crate::restrict_bound::SidecarRead::Present(id, b)) if id == table_id => {
                        Some(b.into())
                    }
                    Err(e) if is_environmental_io(&e) => break 'restrict Err(e),
                    Ok(_) | Err(_) => None,
                };

                // A valid sidecar always denotes a COMMITTED restriction: tight-space
                // writes it STRICTLY AFTER the slice's version install commits (see
                // `Table::write_restrict_sidecar` and `docs/manifest-recovery.md`), so
                // an aborted slice never leaves one. Honor its exact bound DIRECTLY,
                // without probing the below-bound prefix: whether or not the punch has
                // run, reopening at the bound is correct. If the prefix is not yet
                // punched (the crash window between the durable commit and the punch,
                // or a punch deferred by a live reader), the committed output already
                // covers the dropped prefix, so honoring resurrects nothing; if it is
                // punched, the reopened view digests only the live suffix. Reading the
                // dead prefix to decide is not just unnecessary — a persistently
                // unreadable sector there would otherwise discard the exact bound and,
                // with salvage off, drop the whole table despite its intact live
                // suffix. `reopen_restricted` reads only from the punch offset up, so a
                // genuinely unreadable SUFFIX still surfaces its error there.
                if let Some(bound) = &sidecar_bound {
                    break 'restrict table.reopen_restricted(bound.clone());
                }

                // No trustworthy exact bound. An unpunched table never carried a
                // restriction, so it opens unrestricted. A punched table lost its
                // exact bound and falls to the punch geometry: with resurrection
                // on, restrict to the FIRST key of the first readable block past
                // the punched region, keeping the whole ambiguous readable region
                // (its consumed rows resurrected, as the flag contracts). With
                // resurrection OFF the geometry is trusted only when the zeroed
                // blocks form a CLEAN prefix — the pattern of a fully successful
                // punch — and the bound is that prefix's straddling block's END
                // key, never resurrecting a superseded key. An IRREGULAR pattern
                // (a readable block below a zeroed one) is positive evidence of
                // failed punches, after which no geometry bound can separate
                // intact-but-consumed blocks from live ones: the table is set
                // aside (see `DerivedRestriction::IrregularPunch`), matching the
                // recovery-failure arm's fail-closed guard for the same state. A
                // fully-punched SST with no live data is set aside too, losing
                // nothing the flag could have kept.
                match table.has_punched_data_block() {
                    Ok(false) => break 'restrict Ok(table),
                    Err(e) => break 'restrict Err(e),
                    Ok(true) => {
                        use crate::table::DerivedRestriction;
                        let derived = if allow_resurrection {
                            match table.derive_resurrection_bound() {
                                Ok(Some(bound)) => Ok(DerivedRestriction::Bound(bound)),
                                Ok(None) => Ok(DerivedRestriction::NoLiveData),
                                Err(e) => Err(e),
                            }
                        } else {
                            table.derive_restriction_bound()
                        };
                        match derived {
                            Ok(DerivedRestriction::Bound(bound)) => {
                                geometry_lossy = !allow_resurrection;
                                break 'restrict table.reopen_restricted(bound);
                            }
                            Err(e) => break 'restrict Err(e),
                            Ok(
                                reason @ (DerivedRestriction::NoLiveData
                                | DerivedRestriction::IrregularPunch),
                            ) => {
                                // The flag decides this table's fate WITHIN this
                                // run and nowhere else. Resurrection would have
                                // kept the readable region; without it there is
                                // no bound that separates consumed rows from
                                // live ones, so the table is dropped. Nothing is
                                // stashed for a later run to reconsider: a run
                                // that stashed would hand the next one a
                                // different directory than the one it derived
                                // from, and the flag would stop being an input
                                // and start being a state machine.
                                let reason = match reason {
                                    DerivedRestriction::NoLiveData => {
                                        "fully hole-punched SST with no live data"
                                    }
                                    _ => {
                                        "partially punched SST with punch failures and no \
                                         trustworthy bound; the consumed/live boundary is \
                                         unknowable (a resurrection repair keeps the readable \
                                         region instead)"
                                    }
                                };
                                drop(table);
                                set_aside_path(
                                    &folder_fs,
                                    &table_path,
                                    reason,
                                    &mut unreadable_files,
                                    &mut discard_after_commit,
                                );
                                continue 'dirent;
                            }
                        }
                    }
                }
            };

            match recovered {
                // In salvage mode a table whose whole-file recovery succeeded can
                // still hold corrupt data blocks (recovery is lazy on the data
                // section). Block-verify it; if any block is corrupt, salvage it
                // rather than keep a table that errors on read. Encrypted and
                // unencrypted tables take the SAME encryption-aware out-of-band
                // walk (block headers and payload checksums are plaintext; the
                // provider only decodes the meta block) — the recovered `table`
                // merely supplies the id the encrypted meta's AAD binds. Without
                // the provider the walk could not decode an encrypted meta block
                // and would misreport every healthy encrypted table as corrupt,
                // rewriting it on every repair.
                Ok(table) if salvage => {
                    match verify_keep_decision(
                        config,
                        &folder_fs,
                        &table_path,
                        &table,
                        allow_resurrection,
                        true,
                    )? {
                        RepairKeepDecision::Keep => {
                            record_best(
                                &mut recovered_by_id,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                                table_id,
                                table,
                                if geometry_lossy {
                                    Fidelity::GeometryRestricted
                                } else {
                                    Fidelity::Complete
                                },
                                &folder_fs,
                                &table_path,
                            )?;
                        }
                        RepairKeepDecision::Drop(reason) => {
                            drop(table);
                            set_aside_path(
                                &folder_fs,
                                &table_path,
                                reason,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                            );
                        }
                        RepairKeepDecision::Salvage => {
                            // A tight-space RESTRICTED punched SST whose live suffix
                            // is ALSO corrupt (a rare double failure) is block-
                            // salvaged like any other, then RE-RESTRICTED to its
                            // original bound: salvage recovers the readable blocks
                            // into a fresh, unpunched SST (dropping the zeroed prefix
                            // and the corrupt blocks), and reopening that restricted
                            // to the recorded bound masks the straddling block's
                            // sub-bound rows again, so nothing superseded is
                            // resurrected. A sidecar re-records the bound so a later
                            // manifest-loss repair honors it (the fresh file is
                            // unpunched). With resurrection on, the whole readable
                            // region is kept instead. The live suffix is never
                            // thrown away.
                            let restrict_bound = table.restrict_lower_bound().cloned();
                            drop(table);
                            // The replacement is built at `{id}.repair-tmp` and
                            // swapped onto `{id}` only after the manifest that
                            // adopts it is durable. The source is never displaced
                            // first: a crash would then leave the only copy
                            // somewhere the retry does not scan, and the table's
                            // keys would silently vanish from the rebuilt
                            // manifest. Reading an untouched source and writing a
                            // name no scan adopts also makes the retry repeat
                            // exactly the same deterministic salvage.
                            let output_path = repair_tmp_path(&table_path);
                            match try_salvage_table(
                                config,
                                &folder_fs,
                                allow_resurrection,
                                TableSalvage {
                                    source: &table_path,
                                    table_path: &output_path,
                                    table_id,
                                    // The bound (when any) comes from the
                                    // restricted view and is re-imposed below,
                                    // so a punched source is never ambiguous
                                    // on this arm.
                                    reject_punched_without_bound: false,
                                    blob_rewrite: None,
                                },
                            ) {
                                Ok(SalvageOutcome::Salvaged(salvaged)) => {
                                    // Re-impose the tight-space restriction on the
                                    // salvaged output (fail-closed unless resurrection
                                    // is on), the shared path both salvage arms use.
                                    let table = restrict_salvaged_output(
                                        &*folder_fs,
                                        config,
                                        &output_path,
                                        salvaged,
                                        restrict_bound.clone(),
                                        allow_resurrection,
                                    )?;
                                    let restricted = table.restrict_lower_bound().is_some();
                                    record_best(
                                        &mut recovered_by_id,
                                        &mut unreadable_files,
                                        &mut discard_after_commit,
                                        table_id,
                                        table,
                                        Fidelity::Salvaged,
                                        &folder_fs,
                                        &table_path,
                                    )?;
                                    swap_after_commit.push((
                                        Arc::clone(&folder_fs),
                                        output_path,
                                        table_path,
                                        restricted,
                                    ));
                                }
                                Ok(SalvageOutcome::Unusable | SalvageOutcome::PunchedBoundLost) => {
                                    let reason = "verify found corrupt blocks; nothing salvageable";
                                    set_aside_path(
                                        &folder_fs,
                                        &table_path,
                                        reason,
                                        &mut unreadable_files,
                                        &mut discard_after_commit,
                                    );
                                }
                                // A TRANSIENT I/O salvage failure is retryable, and
                                // nothing has moved: the source is untouched, so the
                                // retry re-derives the same salvage from it. A
                                // STRUCTURAL failure is genuine unsalvageability.
                                Err(salvage_err) if is_environmental_io(&salvage_err) => {
                                    return Err(salvage_err);
                                }
                                Err(salvage_err) => {
                                    let reason = format!(
                                        "verify found corrupt blocks; salvage failed \
                                         ({salvage_err})"
                                    );
                                    set_aside_path(
                                        &folder_fs,
                                        &table_path,
                                        &reason,
                                        &mut unreadable_files,
                                        &mut discard_after_commit,
                                    );
                                }
                            }
                        }
                    }
                }
                // Salvage OFF: block-verify all the same. Whole-file recovery is
                // lazy on the data section, and the manifest digest is freshly
                // computed over whatever bytes are there — blessing a table with
                // a corrupt data block would LAUNDER the damage (the report
                // counts it recovered and `verify_integrity` passes while reads
                // of the affected block fail). The salvage flag only decides what
                // happens to a damaged table: rewritten (on) or set aside (off,
                // here), with the report pointing at the salvage-enabled repair.
                Ok(table) => {
                    match verify_keep_decision(
                        config,
                        &folder_fs,
                        &table_path,
                        &table,
                        allow_resurrection,
                        false,
                    )? {
                        RepairKeepDecision::Keep => {
                            record_best(
                                &mut recovered_by_id,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                                table_id,
                                table,
                                if geometry_lossy {
                                    Fidelity::GeometryRestricted
                                } else {
                                    Fidelity::Complete
                                },
                                &folder_fs,
                                &table_path,
                            )?;
                        }
                        // `Salvage` is unreachable with the flag off, but a
                        // defensive fallthrough beats a panic in a repair path.
                        decision @ (RepairKeepDecision::Drop(_) | RepairKeepDecision::Salvage) => {
                            let reason = match decision {
                                RepairKeepDecision::Drop(reason) => reason,
                                _ => {
                                    "verification found corrupt data blocks; run a \
                                     salvage-enabled repair to rewrite the readable blocks"
                                }
                            };
                            drop(table);
                            set_aside_path(
                                &folder_fs,
                                &table_path,
                                reason,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                            );
                        }
                    }
                }
                Err(e) if salvage => {
                    // A TRANSIENT recovery failure (Io) is retryable and must NOT
                    // be routed through salvage: salvaging a healthy SST would,
                    // for a range-tombstone table, fail deterministically with
                    // FeatureUnsupported (a non-Io error recorded as
                    // unsalvageable), committing a manifest without the table —
                    // turning a one-shot read failure into permanent loss.
                    // Propagate the I/O error so a retry re-recovers it,
                    // mirroring the verification / salvage-error paths.
                    if is_environmental_io(&e) {
                        return Err(e);
                    }
                    // A tight-space-punched SST that fails whole-file recovery still
                    // carries its restriction in the `.restrict-bound` sidecar, but
                    // recovery produced no `Table` to read the bound from. Read it
                    // directly from the sibling sidecar and re-impose it on the
                    // salvaged output below, exactly as the
                    // verification-failure arm does. A TRANSIENT sidecar read
                    // propagates; anything else leaves no trustworthy bound, so the
                    // salvaged output stays unrestricted (a genuinely unpunched SST).
                    let restrict_bound: Option<crate::UserKey> = match crate::restrict_bound::read(
                        &*folder_fs,
                        &table_path,
                        config.encryption.as_deref(),
                    ) {
                        Ok(crate::restrict_bound::SidecarRead::Present(id, b))
                            if id == table_id =>
                        {
                            Some(b.into())
                        }
                        Err(read_err) if is_environmental_io(&read_err) => return Err(read_err),
                        Ok(_) | Err(_) => None,
                    };
                    // A PUNCHED source with no trustworthy bound (missing / corrupt
                    // sidecar) cannot be salvaged into an UNRESTRICTED output:
                    // recovery produced no `Table` to derive a geometry bound from,
                    // and salvage drops the zeroed prefix but re-emits the straddling
                    // block's sub-bound rows, which would resurrect with nothing to
                    // restrict them. Fail closed: set it aside. An UNPUNCHED source
                    // (the common corrupt-table case) has no zeroed prefix and
                    // salvages normally. Resurrection-on skips the guard, accepting
                    // the re-exposure.
                    if restrict_bound.is_none()
                        && !allow_resurrection
                        && source_prefix_is_punched(&*folder_fs, &table_path)?
                    {
                        // The flag decides this within THIS run: with
                        // resurrection on, the same source salvages and keeps
                        // its readable region. Nothing is stashed for a later
                        // run to reconsider.
                        let reason = "punched SST with no recoverable restriction bound \
                                      (missing / corrupt sidecar and failed recovery); a \
                                      resurrection repair keeps its readable region instead";
                        set_aside_path(
                            &folder_fs,
                            &table_path,
                            reason,
                            &mut unreadable_files,
                            &mut discard_after_commit,
                        );
                        continue;
                    }
                    // Whole-file recovery failed structurally; try block-level
                    // salvage. The recoverable blocks go into `{id}.repair-tmp`,
                    // which is swapped onto `{id}` only once the manifest that
                    // adopts it is durable — so a crash mid-repair leaves the
                    // directory exactly as the retry expects to find it.
                    let output_path = repair_tmp_path(&table_path);
                    // Fail closed when the salvage walk reveals a punched source
                    // with no recoverable bound: the pre-salvage first-bytes
                    // probe above catches a punched FIRST block, but a partial
                    // punch can leave that block intact while later prefix
                    // blocks are zeroed — only the walk's dropped extents expose
                    // those.
                    let reject_punched = restrict_bound.is_none() && !allow_resurrection;
                    match try_salvage_table(
                        config,
                        &folder_fs,
                        allow_resurrection,
                        TableSalvage {
                            source: &table_path,
                            table_path: &output_path,
                            table_id,
                            reject_punched_without_bound: reject_punched,
                            blob_rewrite: None,
                        },
                    ) {
                        Ok(SalvageOutcome::Salvaged(salvaged)) => {
                            // Re-impose the tight-space restriction on the salvaged
                            // output (fail-closed unless resurrection is on), the
                            // shared path both salvage arms use.
                            let table = restrict_salvaged_output(
                                &*folder_fs,
                                config,
                                &output_path,
                                salvaged,
                                restrict_bound,
                                allow_resurrection,
                            )?;
                            let restricted = table.restrict_lower_bound().is_some();
                            record_best(
                                &mut recovered_by_id,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                                table_id,
                                table,
                                Fidelity::Salvaged,
                                &folder_fs,
                                &table_path,
                            )?;
                            swap_after_commit.push((
                                Arc::clone(&folder_fs),
                                output_path,
                                table_path,
                                restricted,
                            ));
                        }
                        Ok(SalvageOutcome::Unusable) => {
                            let reason = format!("unrecoverable ({e}); nothing salvageable");
                            set_aside_path(
                                &folder_fs,
                                &table_path,
                                &reason,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                            );
                        }
                        Ok(SalvageOutcome::PunchedBoundLost) => {
                            // The flag decides this within THIS run: with
                            // resurrection on the same source salvages and keeps
                            // its readable region.
                            let reason = format!(
                                "punched SST with no recoverable restriction bound \
                                 (missing / corrupt sidecar and failed recovery, punched \
                                 extents found during salvage): {e}; a resurrection repair \
                                 keeps its readable region instead"
                            );
                            set_aside_path(
                                &folder_fs,
                                &table_path,
                                &reason,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                            );
                        }
                        // Transient I/O salvage failure: nothing moved, so the
                        // retry re-derives the same salvage from the untouched
                        // source. A structural failure is recorded.
                        Err(salvage_err) if is_environmental_io(&salvage_err) => {
                            return Err(salvage_err);
                        }
                        Err(salvage_err) => {
                            let reason =
                                format!("recovery failed ({e}); salvage failed ({salvage_err})");
                            set_aside_path(
                                &folder_fs,
                                &table_path,
                                &reason,
                                &mut unreadable_files,
                                &mut discard_after_commit,
                            );
                        }
                    }
                }
                Err(e) => {
                    // A TRANSIENT recovery failure (Io) is retryable: recording it
                    // unreadable commits a manifest without the still-in-place
                    // file, which the next open's orphan cleanup then DELETES —
                    // permanent loss from a one-shot read failure. Propagate it so
                    // a retry re-recovers the table; only a structural failure is a
                    // genuine unreadable report.
                    if is_environmental_io(&e) {
                        return Err(e);
                    }
                    // The rebuilt manifest omits this file, so it is removed once
                    // that manifest is durable: a file both omitted and left in
                    // place is an orphan the next open must sweep, and an open
                    // that cannot sweep it fails.
                    let reason = e.to_string();
                    set_aside_path(
                        &folder_fs,
                        &table_path,
                        &reason,
                        &mut unreadable_files,
                        &mut discard_after_commit,
                    );
                }
            }
        }
    }

    // Collect the best copy per id, carrying each candidate's completeness so
    // `salvaged` can be derived from the tables that actually make the
    // manifest — after the blob-dependency filtering below, not before (a
    // salvaged table dropped for an unrecoverable blob dependency must not
    // count, or `salvaged` could exceed `recovered`). A lossy copy superseded
    // by a complete duplicate is likewise already gone from the candidates.
    // Each entry keeps its SOURCE path (the scanned `{id}` file, not a
    // salvage replacement's `{id}.repair-tmp`): it is the key into
    // `coverage_by_path` when a kept lossy copy's loss is scoped below.
    let mut recovered_tables: Vec<(Table, Fidelity, PathBuf, Arc<dyn crate::fs::Fs>)> =
        recovered_by_id
            .into_values()
            .map(|c| (c.table, c.fidelity, c.path, c.fs))
            .collect();

    // Newest first, by DESCENDING recency key. For a flush / ingest table that
    // key is its own id (ids are allocated in increasing order and flushes are
    // serialized, so a higher id is later content). A COMPACTION output's own
    // id is NOT that signal: the id is allocated when the compaction starts
    // writing, while newer flushes with lower ids can install first, and an
    // intra-L0 output is appended at the BACK of L0 regardless of its id — so
    // outputs persist their newest INPUT's recency in meta (`recency`) and
    // sort by it, with id as the tie-break (equal recency means one descends
    // from the other, and the higher id is the superseding copy). A table's
    // highest seqno is NOT the signal either: callers may assign seqnos
    // explicitly, so an older table can top out above a newer one on an
    // unrelated key. The key is total, so repeating a repair over the same
    // files reproduces the same tree instead of inheriting the directory
    // scan's order.
    recovered_tables
        .sort_by_key(|(t, ..)| (std::cmp::Reverse(t.l0_recency()), std::cmp::Reverse(t.id())));

    // KV-separated (blob) trees additionally carry a blob-file list. Discover the
    // blob files from the `blobs/` folder (no manifest to filter against) and
    // record them in the rebuilt manifest with the matching `TreeType::Blob` so
    // the tree reopens (the reopened tree's type must match its config's
    // `kv_separation_opts`). Fragmentation stats are NOT reconstructable from a
    // directory scan (they are derived from compaction history), so they start
    // empty: blob GC is advisory and re-learns them over time. The empty start
    // never drops live data; it only resets GC's view of reclaimable space.
    //
    // Runs BEFORE the L0 runs are built: a table whose indirections point into a
    // blob file this scan could NOT recover must not be published (see the
    // dependency check below), so the surviving blob ids have to be known first.
    let mut blob_rewrites: crate::HashMap<
        crate::vlog::BlobFileId,
        crate::salvage::BlobFileRewrite,
    > = crate::HashMap::default();
    // Fresh-id blob replacements this run publishes; removed on ANY exit —
    // an error, a cancellation — before the manifest commit disarms the
    // guard (see `PublishedBlobReplacements`).
    let mut published_blob_replacements = PublishedBlobReplacements::new(config);
    let mut blob_files_salvaged: Vec<(PathBuf, String)> = Vec::new();
    let mut blob_frag = crate::blob_tree::FragmentationMap::default();
    // Damaged blob originals whose replacement is in the rebuilt manifest.
    // Set aside only after `persist_version` — see `BlobRecovery::stale`.
    let mut stale_blob_originals: Vec<(PathBuf, String)> = Vec::new();
    // Blob files the rebuilt manifest omits because nothing references them.
    // Removed only after `persist_version`, for the same reason.
    let mut unreferenced_blob_files: Vec<PathBuf> = Vec::new();
    let (tree_type, mut blob_file_list) = if config.kv_separation_opts.is_some() {
        if let Some(p) = &config.recovery_progress {
            p.set_phase(crate::RecoveryPhase::RecoveringBlobFiles);
        }
        // The candidate reference set, taken from the tables recovered so far
        // (a conservative superset of the post-filter truth): lets the blob
        // scan skip salvaging invalid files nothing can reach.
        let mut referenced_blob_ids: crate::HashSet<crate::vlog::BlobFileId> =
            crate::HashSet::default();
        for (table, ..) in &recovered_tables {
            for link in table.list_blob_file_references()?.into_iter().flatten() {
                referenced_blob_ids.insert(link.blob_file_id);
            }
        }
        let recovery = recover_blob_files(
            config,
            &mut published_blob_replacements,
            &referenced_blob_ids,
        )?;
        unreadable_files.extend(recovery.unreadable);
        blob_rewrites = recovery.rewrites;
        blob_frag = recovery.frag;
        stale_blob_originals = recovery.stale;
        // Foreign names, duplicates and unreadable blob files: same post-commit
        // removal as the superseded originals (see `BlobRecovery::discard`).
        discard_after_commit.extend(
            recovery
                .discard
                .into_iter()
                .map(|(path, note)| (Arc::clone(&config.fs), path, note)),
        );
        let map: crate::HashMap<crate::vlog::BlobFileId, crate::vlog::BlobFile> =
            recovery.files.into_iter().map(|bf| (bf.id(), bf)).collect();
        (TreeType::Blob, BlobFileList::new(map))
    } else {
        (
            TreeType::Standard,
            BlobFileList::new(crate::HashMap::default()),
        )
    };

    // Drop any recovered table that still references a blob file the scan could
    // not recover: publishing the pair yields a manifest that opens fine while
    // a read of an affected key resolves a handle into a blob file that is not
    // there. A table whose `linked_blob_files` section cannot be read is treated
    // the same way (its dependencies are unknown, so it cannot be proven safe).
    // A table referencing a RESHAPED blob file — one the blob scan salvaged
    // into a compacted copy, or recovered with a punched frontier — is instead
    // REWRITTEN through the salvage pipeline: its handles are re-targeted at
    // the relocated records and only entries whose record no longer exists are
    // dropped, so intact live data is never discarded over a reshaped
    // dependency.
    if config.kv_separation_opts.is_some() {
        // Frontiers of the punched-but-intact blob files (the `DropBelow`
        // rewrite entries): a handle below one dereferences zeroed bytes.
        // Empty on the common path, so no table's handles are scanned.
        let punched_frontiers: crate::HashMap<crate::vlog::BlobFileId, u64> = blob_rewrites
            .iter()
            .filter_map(|(id, rw)| match rw {
                crate::salvage::BlobFileRewrite::DropBelow(f) => Some((*id, *f)),
                crate::salvage::BlobFileRewrite::Remap { .. } => None,
            })
            .collect();
        let blob_rewrites = Arc::new(blob_rewrites);
        let mut kept: Vec<(Table, Fidelity, PathBuf, Arc<dyn crate::fs::Fs>)> =
            Vec::with_capacity(recovered_tables.len());
        for (table, fidelity, source_path, source_fs) in recovered_tables {
            // One reference read drives everything below: the missing-id check
            // and the rewrite decision.
            let links = match table.list_blob_file_references() {
                Ok(links) => links,
                Err(e) if is_environmental_io(&e) => return Err(e),
                Err(e) => {
                    set_aside_table(
                        table,
                        &format!("blob-file reference list unreadable ({e})"),
                        &mut unreadable_files,
                        &mut discard_after_commit,
                    );
                    continue;
                }
            };
            // A referenced id is unrecoverable only when the scan neither kept
            // it NOR salvaged it into a replacement: a `Remap` entry means the
            // records live on under a fresh id, and the rewrite below
            // retargets this table's handles at it.
            if let Some(l) = links.as_ref().and_then(|links| {
                links.iter().find(|l| {
                    !blob_file_list.contains_key(l.blob_file_id)
                        && !blob_rewrites.contains_key(&l.blob_file_id)
                })
            }) {
                set_aside_table(
                    table,
                    &format!("blob file {} is not recoverable", l.blob_file_id),
                    &mut unreadable_files,
                    &mut discard_after_commit,
                );
                continue;
            }
            // Whether this table's handles must be rewritten: any reference to
            // a SALVAGED blob file (its records moved to a fresh id), or a
            // handle that actually lies below a punched blob's frontier (a
            // pre-relocation SST file left behind by a crash — the id-presence
            // check cannot see it).
            let mut needs_rewrite = false;
            if let Some(links) = &links {
                if links.iter().any(|l| {
                    matches!(
                        blob_rewrites.get(&l.blob_file_id),
                        Some(crate::salvage::BlobFileRewrite::Remap { .. })
                    )
                }) {
                    needs_rewrite = true;
                } else if !punched_frontiers.is_empty()
                    && links
                        .iter()
                        .any(|l| punched_frontiers.contains_key(&l.blob_file_id))
                {
                    match handle_below_blob_frontier(&table, &punched_frontiers) {
                        Ok(hit) => needs_rewrite = hit.is_some(),
                        Err(e) if is_environmental_io(&e) => return Err(e),
                        Err(e) => {
                            set_aside_table(
                                table,
                                &format!("blob handles unreadable ({e})"),
                                &mut unreadable_files,
                                &mut discard_after_commit,
                            );
                            continue;
                        }
                    }
                }
            }
            if !needs_rewrite {
                kept.push((table, fidelity, source_path, source_fs));
                continue;
            }
            // Rewrite through the salvage pipeline: re-emit every entry with
            // re-targeted handles, dropping only entries whose blob record no
            // longer exists. The rewritten table counts as salvaged (its
            // content may be lossy relative to the original).
            //
            // The copy is built at `{id}.repair-tmp` — a name no scan adopts —
            // and swapped onto `{id}` only AFTER the manifest commit. Displacing
            // the source first would open a window in which a crash leaves no
            // readable copy where the retry looks, and publishing the copy under
            // a name the scan DOES adopt would leave a crash with both readable,
            // so the retry would rebuild one history into L0 twice. Nothing is
            // carried across runs: the rewrite reads an untouched source, and a
            // leftover temp is garbage the retry replaces.
            let source_id = table.id();
            let path = (*table.path).clone();
            let output_path = repair_tmp_path(&path);
            // A RESTRICTED source's bound must be re-imposed on the copy: the
            // rewrite re-emits the straddling block's sub-bound rows, which the
            // restriction hides, so an unrestricted copy would resurrect them.
            let restrict_bound = table.restrict_lower_bound().cloned();
            let fs = table.fs.clone();
            drop(table); // release the handle before reading the source again
            match try_salvage_table(
                config,
                &fs,
                allow_resurrection,
                TableSalvage {
                    source: &path,
                    table_path: &output_path,
                    table_id: source_id,
                    reject_punched_without_bound: false,
                    blob_rewrite: Some(Arc::clone(&blob_rewrites)),
                },
            ) {
                Ok(SalvageOutcome::Salvaged(rewritten)) => {
                    let rewritten = restrict_salvaged_output(
                        &*fs,
                        config,
                        &output_path,
                        rewritten,
                        restrict_bound,
                        allow_resurrection,
                    )?;
                    let restricted = rewritten.restrict_lower_bound().is_some();
                    kept.push((rewritten, Fidelity::Salvaged, source_path, source_fs));
                    swap_after_commit.push((Arc::clone(&fs), output_path, path, restricted));
                }
                Ok(SalvageOutcome::Unusable | SalvageOutcome::PunchedBoundLost) => {
                    set_aside_path(
                        &fs,
                        &path,
                        "blob-handle rewrite produced nothing",
                        &mut unreadable_files,
                        &mut discard_after_commit,
                    );
                }
                // A retryable failure leaves the source where it was found, so
                // the retry re-derives the same rewrite from it; nothing to
                // restore.
                Err(e) if is_environmental_io(&e) => return Err(e),
                Err(e) => {
                    set_aside_path(
                        &fs,
                        &path,
                        &format!("blob-handle rewrite failed ({e})"),
                        &mut unreadable_files,
                        &mut discard_after_commit,
                    );
                }
            }
        }
        recovered_tables = kept;
    }

    // A crashed compaction that FINALIZED its outputs before its version edit
    // committed leaves BOTH histories on disk — and so does a committed
    // compaction whose input deletion never finished. The recorded lineage
    // settles it: an output whose inputs ALL survived into this rebuild is
    // DERIVED — the inputs are the complete history, a future compaction
    // re-folds them — and publishing both would apply the same merge
    // operands twice on every read. Exclusion cascades correctly in a single
    // pass against the PRE-exclusion id set: an intermediate output excluded
    // for its surviving inputs may itself be named in a later output's
    // lineage, but every exclusion keeps the excluded table's own inputs, so
    // the remaining set always bottoms out at lineage-less leaves carrying
    // the full history. An output with PARTIALLY surviving inputs is KEPT —
    // it is the only complete copy of its span — alongside the surviving
    // inputs, and each overlap is reported below as ambiguity coverage so a
    // reconciling deployment knows reads in that span may double-apply
    // operands. Presence is judged AFTER the blob-dependency filter above: an
    // input dropped for an unrecoverable blob does not count as surviving —
    // and only a COMPLETE recovery counts at all: a salvaged or
    // geometry-restricted input already lost records, so its id proves
    // nothing about the output's contents surviving elsewhere, and trading a
    // healthy output for damaged inputs would convert recoverable data into
    // permanent loss.
    let present_ids: crate::HashSet<TableId> = recovered_tables
        .iter()
        .filter(|(_, fidelity, ..)| fidelity.is_complete())
        .map(|(t, ..)| t.id())
        .collect();
    let mut lineage_partial: Vec<(TableId, PathBuf, UserKey, UserKey, Option<SeqNo>)> = Vec::new();
    let mut inputs_superseded: crate::HashSet<TableId> = crate::HashSet::default();
    // Files excluded as REDUNDANT (a derived output whose inputs all
    // survived, an input a complete output fully covers): their content
    // lives on in the kept tables, so they are reported in
    // `unreadable_files` but must NOT contribute `lost_coverage` — nothing
    // was lost, and a coverage entry would widen the WAL replay for no
    // reason.
    let mut redundant_excluded: crate::HashSet<PathBuf> = crate::HashSet::default();
    {
        let candidates = core::mem::take(&mut recovered_tables);
        let kept_ranges: crate::HashMap<TableId, (UserKey, UserKey, SeqNo)> = candidates
            .iter()
            .map(|(t, ..)| {
                (
                    t.id(),
                    (
                        t.metadata.key_range.min().clone(),
                        t.metadata.key_range.max().clone(),
                        t.get_highest_seqno(),
                    ),
                )
            })
            .collect();
        for candidate in candidates {
            let lineage = candidate.0.metadata.lineage.clone();
            match lineage.as_deref() {
                // A TRANSFORMED output never trades back for its inputs: a
                // compaction filter removed records from its window, so the
                // resurrected inputs would revive them. It takes the partial
                // arm below instead and SUPERSEDES the inputs it covers.
                Some(inputs)
                    if !candidate.0.metadata.lineage_transformed
                        && !inputs.is_empty()
                        && inputs
                            .iter()
                            .all(|id| *id != candidate.0.id() && present_ids.contains(id)) =>
                {
                    let (table, _, path, fs) = candidate;
                    log::info!(
                        "repair: table {} is a compaction output whose inputs {:?} all \
                         survived; excluding the derived copy so its merge operands are \
                         not applied twice",
                        table.id(),
                        inputs,
                    );
                    drop(table);
                    let reason = "derived output of an uncommitted compaction whose inputs \
                                  all survived; excluded so its merge operands are not \
                                  applied twice";
                    redundant_excluded.insert(path.clone());
                    unreadable_files.push((path.clone(), reason.to_string()));
                    discard_after_commit.push((fs, path, reason.to_string()));
                }
                Some(inputs) => {
                    // Partial survival: the output is the only complete copy
                    // of the LOST inputs' span, so it must be kept — and each
                    // surviving input the output's key range fully COVERS is
                    // superseded by it (the compaction read the whole input,
                    // so every one of its records is in a range-covering
                    // output) and is excluded, or reads in the overlap apply
                    // the same merge operands twice; `lost_coverage` cannot
                    // fix that, since the documented reconciliation would see
                    // the duplicated operand as surviving and subtract its
                    // WAL record instead of removing the folded copy. Only a
                    // COMPLETE output supersedes. An input the output covers
                    // partially (its remainder belonged to a LOST sibling
                    // output of the same run) is kept — dropping it would
                    // lose live records — and the residual overlap is
                    // reported.
                    let cmp = config.comparator.as_ref();
                    for input in inputs {
                        if *input == candidate.0.id() {
                            continue;
                        }
                        let Some((in_min, in_max, in_hi)) = kept_ranges.get(input) else {
                            continue;
                        };
                        let out_range = &candidate.0.metadata.key_range;
                        if cmp.compare(out_range.min(), in_max) == core::cmp::Ordering::Greater
                            || cmp.compare(in_min, out_range.max()) == core::cmp::Ordering::Greater
                        {
                            continue;
                        }
                        let covers = candidate.1.is_complete()
                            && cmp.compare(out_range.min(), in_min) != core::cmp::Ordering::Greater
                            && cmp.compare(in_max, out_range.max()) != core::cmp::Ordering::Greater;
                        if covers {
                            inputs_superseded.insert(*input);
                            continue;
                        }
                        let lo =
                            if cmp.compare(out_range.min(), in_min) == core::cmp::Ordering::Less {
                                in_min
                            } else {
                                out_range.min()
                            };
                        let hi =
                            if cmp.compare(out_range.max(), in_max) == core::cmp::Ordering::Less {
                                out_range.max()
                            } else {
                                in_max
                            };
                        lineage_partial.push((
                            *input,
                            candidate.2.clone(),
                            lo.clone(),
                            hi.clone(),
                            Some(candidate.0.get_highest_seqno().min(*in_hi)),
                        ));
                    }
                    recovered_tables.push(candidate);
                }
                _ => recovered_tables.push(candidate),
            }
        }
    }
    // Sibling-UNION supersession: the outputs of one rotated run record the
    // same lineage plus an adjacency link (`lineage_prev`), so an UNBROKEN
    // surviving chain of COMPLETE outputs proves its combined range has no
    // gap a lost sibling could hide — an input inside that union is fully
    // redundant even when no single output contains it. A broken chain
    // (a predecessor that did not survive) proves nothing and unions
    // nothing. A chain that spans the run END TO END — from the FIRST
    // output (no `lineage_prev`) to the one carrying the `lineage_last`
    // marker — is the run's COMPLETE output set and supersedes every input
    // outright: keys outside the written ranges were consumed by the merge
    // (obsolete versions, annihilated weak deletes, filter removals), not
    // lost. That is what preserves a committed TRANSFORMING compaction —
    // its outputs no longer contain the filtered records, so only full-run
    // supersession retires the inputs that still do.
    {
        let mut by_id: crate::HashMap<TableId, usize> = crate::HashMap::default();
        let mut surviving_ranges: crate::HashMap<TableId, (UserKey, UserKey)> =
            crate::HashMap::default();
        for (idx, (t, fidelity, ..)) in recovered_tables.iter().enumerate() {
            surviving_ranges.insert(
                t.id(),
                (
                    t.metadata.key_range.min().clone(),
                    t.metadata.key_range.max().clone(),
                ),
            );
            if fidelity.is_complete() && t.metadata.lineage.is_some() {
                by_id.insert(t.id(), idx);
            }
        }
        let same_lineage = |a: usize, b: usize| {
            recovered_tables.get(a).map(|(t, ..)| &t.metadata.lineage)
                == recovered_tables.get(b).map(|(t, ..)| &t.metadata.lineage)
        };
        let mut next_of: crate::HashMap<TableId, TableId> = crate::HashMap::default();
        for &idx in by_id.values() {
            let Some((t, ..)) = recovered_tables.get(idx) else {
                continue;
            };
            if let Some(prev) = t.metadata.lineage_prev
                && by_id.get(&prev).is_some_and(|&p| same_lineage(p, idx))
            {
                next_of.insert(prev, t.id());
            }
        }
        let cmp = config.comparator.as_ref();
        for (&head_id, &head_idx) in &by_id {
            let Some((head, ..)) = recovered_tables.get(head_idx) else {
                continue;
            };
            // A chain HEAD: its predecessor is absent from the surviving set
            // (or it is the run's first output).
            if head
                .metadata
                .lineage_prev
                .is_some_and(|prev| by_id.contains_key(&prev))
            {
                continue;
            }
            // Walk the unbroken chain; ranges follow the run's key order, so
            // the union is [head.min, tail.max]. A chain that runs from the
            // run's FIRST output (no `lineage_prev`) to its LAST (the
            // `lineage_last` marker, written only by a writer that owned the
            // WHOLE run — never by a parallel or tight-space slice, whose
            // first output also has no persisted predecessor) is the
            // COMPLETE output set: it supersedes every input outright, no
            // range check needed. Requiring BOTH ends keeps a surviving
            // slice from claiming the whole run.
            let open_lo = head.metadata.lineage_prev.is_none();
            let union_min = head.metadata.key_range.min().clone();
            let mut union_max = head.metadata.key_range.max().clone();
            let mut open_hi = head.metadata.lineage_last;
            let mut cursor = head_id;
            while let Some(&next) = next_of.get(&cursor) {
                if let Some(&next_idx) = by_id.get(&next)
                    && let Some((t, ..)) = recovered_tables.get(next_idx)
                {
                    union_max = t.metadata.key_range.max().clone();
                    open_hi = t.metadata.lineage_last;
                }
                cursor = next;
            }
            let complete_run = open_lo && open_hi;
            let Some(inputs) = &head.metadata.lineage else {
                continue;
            };
            for input in inputs {
                if by_id.contains_key(input) {
                    continue;
                }
                let Some((in_min, in_max)) = surviving_ranges.get(input) else {
                    continue;
                };
                if complete_run
                    || (cmp.compare(&union_min, in_min) != core::cmp::Ordering::Greater
                        && cmp.compare(in_max, &union_max) != core::cmp::Ordering::Greater)
                {
                    inputs_superseded.insert(*input);
                }
            }
        }
    }

    // Second pass: drop the inputs a surviving COMPLETE output fully covers.
    // Their residual-coverage entries (recorded against a DIFFERENT,
    // partially-covering output before the covering one was seen) go with
    // them — the ambiguity those entries reported no longer exists.
    if !inputs_superseded.is_empty() {
        let candidates = core::mem::take(&mut recovered_tables);
        for candidate in candidates {
            if inputs_superseded.contains(&candidate.0.id()) {
                let (table, _, path, fs) = candidate;
                log::info!(
                    "repair: table {} is fully covered by a surviving compaction \
                     output; excluding it so its merge operands are not applied twice",
                    table.id(),
                );
                drop(table);
                let reason = "input fully covered by a surviving compaction output; \
                              excluded so its merge operands are not applied twice";
                redundant_excluded.insert(path.clone());
                unreadable_files.push((path.clone(), reason.to_string()));
                discard_after_commit.push((fs, path, reason.to_string()));
            } else {
                recovered_tables.push(candidate);
            }
        }
        lineage_partial.retain(|(input, ..)| !inputs_superseded.contains(input));
    }

    // Only blob files a surviving table REFERENCES go into the manifest.
    // An unreferenced one holds no reachable value by definition, and
    // admitting it would strand it there forever: repair cannot rebuild
    // fragmentation stats from a directory scan, and blob GC retires a
    // file only once its recorded stale bytes reach the totals it never
    // gets. That also settles what a crashed earlier attempt leaves
    // behind — a fully written salvage replacement that the crash kept
    // out of any manifest — which would otherwise be admitted as an
    // ordinary blob and pin a whole copy per failed attempt. Judged against
    // the FINAL table set — after the lineage dedup above — so a blob
    // referenced only by an excluded table (a derived output, a superseded
    // input) is not pinned by a reference the rebuilt manifest no longer
    // holds.
    if config.kv_separation_opts.is_some() {
        let mut referenced: crate::HashSet<crate::vlog::BlobFileId> = crate::HashSet::default();
        for (table, ..) in &recovered_tables {
            for link in table.list_blob_file_references()?.into_iter().flatten() {
                referenced.insert(link.blob_file_id);
            }
        }
        let dropped: Vec<crate::vlog::BlobFileId> = blob_file_list
            .iter()
            .map(crate::vlog::BlobFile::id)
            .filter(|id| !referenced.contains(id))
            .collect();
        for id in dropped {
            log::debug!(
                "blob file {id} is referenced by no recovered table; leaving it out \
                 of the rebuilt manifest and removing it"
            );
            blob_file_list.remove(id);
            blob_frag.remove(&id);
            // Removed POST-COMMIT (see the sweep below): until the manifest is
            // durable the file is still what a failed attempt's inputs point
            // at, and a retry re-derives the same decision from a fresh scan.
            unreferenced_blob_files.push(
                config
                    .path
                    .join(crate::file::BLOBS_FOLDER)
                    .join(id.to_string()),
            );
        }
    }

    // Live sidecars are judged here, against the FINAL rebuilt set: one whose
    // table survived is preserved (the open and the next scrub read it), an
    // ORPHAN — its table excluded, superseded, or never recovered — is
    // removed post-commit. The open sweeps such orphans unconditionally and
    // fails on a refused removal, so leaving one would let a successful
    // repair be followed by an open failure on the very same file.
    {
        let final_ids: crate::HashSet<TableId> =
            recovered_tables.iter().map(|(t, ..)| t.id()).collect();
        for (id, path, fs) in live_sidecars {
            if final_ids.contains(&id) {
                continue;
            }
            discard_after_commit.push((
                fs,
                path,
                "orphaned sidecar; its table did not survive the rebuild".to_string(),
            ));
        }
    }

    // `salvaged` is a subset of `recovered`, so derive it from the tables that
    // survived every filter above. The live progress counter follows the same
    // rule: a candidate displaced by deduplication or dropped by dependency
    // filtering never counts, so the snapshot cannot claim more tables than
    // the rebuilt manifest holds.
    // Only block-salvage REWRITES: a geometry-restricted original is lossy
    // (it contributes coverage below) but was never salvaged, and it arises
    // in plain repairs where `salvaged` is documented to stay zero.
    let salvaged = recovered_tables
        .iter()
        .filter(|(_, fidelity, ..)| *fidelity == Fidelity::Salvaged)
        .count();
    // A KEPT lossy copy — a salvaged rewrite that dropped corrupt blocks (or
    // blob records), or a geometry-restricted original whose derived bound
    // may exclude the straddling block's live suffix — names nothing in
    // `unreadable_files`, so without an entry here `wal_replay_scope()` would
    // answer `TailOnly` while older persisted changes were in fact lost. The
    // loss is scoped by the
    // SOURCE's coverage captured during the scan — NOT by the replacement's
    // own metadata, which only bounds what SURVIVED: salvage may have
    // dropped the block holding the source's outermost keys or its highest
    // seqno, and a range/ceiling derived from the survivors would exclude
    // exactly the lost part. A source whose metadata never parsed
    // (whole-file recovery failed before the coverage was captured) is
    // unscopable and joins `unknowable_losses` instead.
    let mut salvaged_unknowable: Vec<PathBuf> = Vec::new();
    let salvaged_coverage: Vec<(PathBuf, UserKey, UserKey, Option<SeqNo>)> = recovered_tables
        .iter()
        .filter(|(_, fidelity, ..)| !fidelity.is_complete())
        .filter_map(|(_, _, source_path, _)| {
            if let Some((lo, hi, bound)) = coverage_by_path.get(source_path) {
                Some((source_path.clone(), lo.clone(), hi.clone(), *bound))
            } else {
                salvaged_unknowable.push(source_path.clone());
                None
            }
        })
        .collect();
    // A pair of L0 tables whose KEY ranges overlap and whose SEQNO ranges
    // intersect can hold tied entries, and a tied read is settled by run
    // order. The persisted recency key makes that order trustworthy; when
    // EITHER table lacks it, the id fallback restores ALLOCATION order,
    // which a legacy compaction output does not follow (its high id predates
    // a concurrent newer flush's install) — and a missing key cannot tell
    // such an output from a flush. The tree still commits the deterministic
    // id order (an openable tree, always), and the overlap is REPORTED: the
    // key-range intersection under a seqno ceiling of the intersection's top
    // (ties need the seqno present in BOTH tables). A reconciling deployment
    // replays the range and its WAL's authoritative order settles the ties —
    // the replayed memtable copy is the newest source and wins them.
    // Serialized flushes have DISJOINT seqno ranges, so an ordinary tree
    // reports nothing; only caller-assigned-seqno deployments (which have a
    // WAL to heal from) can intersect.
    //
    // An INTERVAL SWEEP, not an all-pairs scan: probes sort by key-range MIN
    // and walk in that order against an active list pruned of every probe
    // whose MAX fell below the incoming MIN — each remaining active overlaps
    // the incoming probe by construction, so the pair work is bounded by the
    // overlapping pairs actually inspected instead of `n²` (50k recovered
    // tables would otherwise cost ~1.25 billion pair visits on top of the
    // full-file verification). A store where every table carries the recency
    // key skips the sweep entirely.
    struct RecencyProbe {
        min: UserKey,
        max: UserKey,
        lo_seqno: SeqNo,
        hi_seqno: SeqNo,
        modern: bool,
        path: PathBuf,
    }
    let mut ambiguous_order_coverage: Vec<(PathBuf, UserKey, UserKey, Option<SeqNo>)> = Vec::new();
    if recovered_tables
        .iter()
        .any(|(t, ..)| t.metadata.recency.is_none())
    {
        let cmp = config.comparator.as_ref();
        let mut probes: Vec<RecencyProbe> = recovered_tables
            .iter()
            .map(|(t, _, path, _)| RecencyProbe {
                min: t.metadata.key_range.min().clone(),
                max: t.metadata.key_range.max().clone(),
                lo_seqno: t.get_lowest_seqno(),
                hi_seqno: t.get_highest_seqno(),
                modern: t.metadata.recency.is_some(),
                path: path.clone(),
            })
            .collect();
        probes.sort_by(|a, b| cmp.compare(&a.min, &b.min));
        let mut active: Vec<&RecencyProbe> = Vec::new();
        for probe in &probes {
            active.retain(|other| cmp.compare(&other.max, &probe.min) != core::cmp::Ordering::Less);
            for other in &active {
                if probe.modern && other.modern {
                    continue;
                }
                if probe.lo_seqno > other.hi_seqno || other.lo_seqno > probe.hi_seqno {
                    continue;
                }
                // Under a configured MERGE OPERATOR the pair is not
                // publishable at all: it may be a pre-lineage compaction
                // output beside its surviving input, and publishing both
                // applies the input's merge operands twice on every read —
                // a multiplicity the reported replay cannot remove (the
                // reconciliation sees both physical copies as survivors, or
                // the operand as folded into the output's value). Without
                // lineage neither side can be proven redundant, so the
                // repair fails closed. Value-only deployments proceed to
                // the report below: their duplicate records are
                // byte-identical and reads dedupe them, so ties are the
                // only hazard and the WAL replay settles those.
                if config.merge_operator.is_some() {
                    log::error!(
                        "repair: tables {} and {} overlap with intersecting seqno \
                         ranges and no trustworthy order or lineage; under a merge \
                         operator publishing both would double-apply operands, and \
                         no replay can undo that",
                        probe.path.display(),
                        other.path.display(),
                    );
                    return Err(crate::Error::Unrecoverable);
                }
                // `other.min <= probe.min` (sort order) and
                // `other.max >= probe.min` (retained above), so the key
                // intersection is `[probe.min, min(maxes)]`.
                let hi = if cmp.compare(&probe.max, &other.max) == core::cmp::Ordering::Less {
                    &probe.max
                } else {
                    &other.max
                };
                ambiguous_order_coverage.push((
                    probe.path.clone(),
                    probe.min.clone(),
                    hi.clone(),
                    Some(probe.hi_seqno.min(other.hi_seqno)),
                ));
            }
            active.push(probe);
        }
    }
    let recovered_tables: Vec<Table> = recovered_tables.into_iter().map(|(t, ..)| t).collect();
    // A rebuilt manifest whose highest table id is the LAST one cannot be
    // committed: the next open seeds its id allocator with `highest + 1`,
    // which overflows — a panic in checked builds, a wrap to an existing low
    // id in release builds (a later flush then collides with that file).
    // Mirrors the version-id and blob-id exhaustion guards: fail the repair
    // instead of publishing a tree that cannot allocate.
    if recovered_tables.iter().map(Table::id).max() == Some(crate::TableId::MAX) {
        log::error!(
            "repair: the table id space is exhausted (id {} is in use); a rebuilt \
             manifest would make the next open's id allocator overflow",
            crate::TableId::MAX,
        );
        return Err(crate::Error::Unrecoverable);
    }
    // The blob-file id space counts on the same rule. A HEALTHY,
    // still-referenced blob file at the last id needs no salvage, so the
    // fresh-id allocator's exhaustion (`next_blob_id == None`) is never
    // consulted for it — yet the next open seeds its blob allocator with
    // `max + 1` all the same, with the same overflow.
    if blob_file_list.iter().map(crate::vlog::BlobFile::id).max()
        == Some(crate::vlog::BlobFileId::MAX)
    {
        log::error!(
            "repair: the blob file id space is exhausted (id {} is referenced); a \
             rebuilt manifest would make the next open's blob id allocator overflow",
            crate::vlog::BlobFileId::MAX,
        );
        return Err(crate::Error::Unrecoverable);
    }
    if let Some(p) = &config.recovery_progress {
        p.tables_recovered_add(recovered_tables.len() as u64);
        // Blob files count on the same rule and for the same reason: the
        // reference filter above removes (and deletes) any the surviving tables
        // do not point at, so counting one at recovery time would claim a
        // recovery the rebuilt manifest does not hold.
        p.blob_files_recovered_add(blob_file_list.len() as u64);
    }

    // Each recovered table becomes its own single-table L0 run. L0 permits
    // overlapping runs, so this is always legal regardless of key overlap;
    // background compaction collapses them into sorted lower levels later.
    // `Run::new` only returns `None` for an empty run, which `vec![t]` never is,
    // so no table is dropped here — but build the runs explicitly and derive the
    // recovered count from what actually lands in the manifest, so the report
    // can never overcount relative to the persisted version.
    let l0_runs = recovered_tables
        .iter()
        .cloned()
        .filter_map(|t| Run::new(vec![t]).map(Arc::new))
        .collect::<Vec<_>>();
    let recovered = l0_runs.len();

    let mut levels = Vec::with_capacity(config.level_count.into());
    levels.push(Level::from_runs(l0_runs));
    for _ in 1..config.level_count {
        levels.push(Level::empty());
    }

    // Next version id after the highest existing one. The max is parsed from
    // on-disk `v{N}` directory names, so a malformed `v{u64::MAX}` entry would
    // overflow; reject it explicitly rather than wrapping the version counter.
    // The rebuilt id also needs HEADROOM: publishing at `u64::MAX` itself
    // hands the first subsequent version edit an `id + 1` overflow (a panic
    // in checked builds, a wrap to version 0 colliding with old generation
    // state otherwise). Mirrors the table-id and blob-id exhaustion guards.
    let version_id = match highest_existing_version_id(&*config.fs, &config.path)? {
        Some(max) => {
            let next = max.checked_add(1).ok_or(crate::Error::Unrecoverable)?;
            if next == u64::MAX {
                log::error!(
                    "repair: the version id space is exhausted (v{max} exists); a \
                     rebuilt manifest at v{next} would make the next version edit \
                     overflow",
                );
                return Err(crate::Error::Unrecoverable);
            }
            next
        }
        None => 0,
    };

    // Seeded with the punched prefixes' garbage: those frames can never be
    // observed by a future compaction, so an empty map would pin every punched
    // file's stale count below its whole-file metadata totals forever.
    let version = Version::from_levels(version_id, tree_type, levels, blob_file_list, blob_frag);

    // The LAST cancellation boundary: per-file checks only run before a file
    // starts, so a cancel requested during the final file's verification or
    // salvage would otherwise be silently outrun by the commit. From here on
    // the run is COMMITTING and then cleaning up, and cancellation is no
    // longer consulted (see `check_cancel`). An abort here — like every
    // earlier pre-commit exit — unwinds the fresh-id blob replacements
    // through the `published_blob_replacements` guard.
    check_cancel(config)?;
    if let Some(p) = &config.recovery_progress {
        p.set_phase(crate::RecoveryPhase::Committing);
    }

    // Persist with the tree's own runtime config, not defaults: it drives the
    // manifest framing (checksum algorithm, page ECC, footer mirror, manifest
    // KV checksums), so defaulting it would rewrite a recovered tree's manifest
    // metadata to settings it never used. The last live runtime config died with
    // the lost manifest; the config supplied to `repair` is the authoritative
    // replacement.
    let persisted = crate::version::persist_version(
        &config.path,
        &version,
        config.comparator.name(),
        &*config.fs,
        Arc::new(config.initial_runtime_config.clone()),
        config.encryption.clone(),
        config.sync_mode,
    );
    // The guard stays ARMED until the commit is decided: every fallible
    // `persist_version` step before its atomic `CURRENT` switch (manifest
    // create / encode / finish / directory sync, and the switch's own
    // rename) leaves the replacements unreferenced garbage a retry would
    // re-derive — keeping them would stack another fresh-id copy per failed
    // attempt, walking recovery toward ENOSPC under exactly the tight-space
    // conditions it targets. The ONE fallible step after the switch is the
    // pointer's directory sync, so on failure the pointer on disk decides
    // which world this is: a `CURRENT` naming the rebuilt version means the
    // manifest is published and the replacements are the files it
    // references. A probe that cannot READ the pointer proves nothing, and
    // fail-safe is to PRESERVE: the worst a preserved orphan costs is disk
    // space a later successful repair's reference filtering reclaims, while
    // a wrong deletion breaks a published manifest permanently.
    if persisted.is_ok()
        || !matches!(
            probe_current(&*config.fs, &config.path, version_id),
            CurrentProbe::NotSwitched
        )
    {
        published_blob_replacements.disarm();
    }
    persisted?;

    // The manifest is DURABLE from this point on: the repair happened, and
    // its report must survive every later failure. A bare error from the
    // cleanup below would discard the only report — once the filesystem
    // fault clears, the next open sweeps the leftover itself and
    // `open_or_repair` answers with no report at all, hiding the committed
    // repair's lost coverage from an external-WAL consumer. The first
    // cleanup failure is therefore RECORDED, the remaining cleanup is
    // skipped, and the completed report rides out inside
    // [`Error::RepairedButUnopened`].
    let mut post_commit_error: Option<crate::Error> = None;

    // A rebuilt snapshot is a complete generation on its own. Sweep every stale
    // edit log so nothing is replayed on top of it: the lost manifest's
    // generation left its log under an OLDER snapshot id (the rebuilt snapshot
    // uses `max(v*) + 1`), so removing only `edits-{version_id}` would normally
    // miss it. Drop all `edits-*` — none belong to the fresh snapshot.
    match config.fs.read_dir(&config.path) {
        Ok(dirents) => {
            for dirent in dirents {
                if dirent.is_dir || !dirent.file_name.starts_with("edits-") {
                    continue;
                }
                match config.fs.remove_file(&dirent.path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        post_commit_error = Some(e.into());
                        break;
                    }
                }
            }
        }
        Err(e) => post_commit_error = Some(e.into()),
    }

    if let Some(p) = &config.recovery_progress {
        p.set_phase(crate::RecoveryPhase::Cleanup);
    }

    // POST-COMMIT, step one: swap every finished replacement onto the name the
    // committed manifest gives it, destroying the damaged source it replaces.
    // Before this the manifest was not durable and the sources were the only
    // copies; after it the manifest describes exactly these bytes. A crash
    // between the commit and a swap leaves `{id}` damaged with `{id}.repair-tmp`
    // beside it — which the next run resolves from the committed manifest alone
    // (see `sweep_superseded_by_committed_manifest`), not from anything this run
    // remembered.
    //
    // NOT best-effort: the manifest already names this content, so a swap that
    // does not happen is a tree whose next open finds the damaged file under the
    // manifest's checksum and fails.
    if post_commit_error.is_none() {
        for (fs, tmp_path, table_path, restricted) in swap_after_commit {
            if let Err(e) =
                commit_repair_tmp(&*fs, &tmp_path, &table_path, config.sync_mode, restricted)
            {
                log::error!(
                    "repair: cannot swap the replacement {} onto {} ({e}); failing the \
                     repair — the committed manifest names the replacement's content",
                    tmp_path.display(),
                    table_path.display(),
                );
                post_commit_error = Some(e);
                break;
            }
        }
    }

    // POST-COMMIT, step two. The manifest is durable and names the salvaged
    // replacements, so the damaged originals they superseded are unreferenced
    // and are removed. This runs AFTER the commit on purpose: until then those
    // originals are what the tables the rewrite had not reached still point at,
    // so removing one earlier would let a failed repair leave a table
    // referencing a blob id that no longer exists — and the retry would then
    // record that table unrecoverable.
    //
    // NOT best-effort. A superseded original left in `blobs/` is outside the
    // committed manifest, so the next open classifies it as an orphan and
    // removes it — and if the directory refuses the removal now, that open
    // FAILS. Reporting a successful repair for a tree that will not open is the
    // one outcome recovery must never produce, so the failure propagates: the
    // manifest is already durable, and a retry once the filesystem is fixed
    // finishes the sweep on the same inputs.
    if post_commit_error.is_none() {
        for (path, note) in stale_blob_originals {
            match discard_unreferenced(&*config.fs, &path, config.sync_mode) {
                Ok(()) => blob_files_salvaged.push((path, format!("{note}; original removed"))),
                Err(e) => {
                    log::error!(
                        "repair: cannot remove the superseded blob original {} ({e}); \
                         failing the repair — left in blobs/ it is an orphan the next \
                         open must remove, and that removal would hit the same error",
                        path.display(),
                    );
                    post_commit_error = Some(e);
                    break;
                }
            }
        }
    }

    // Everything the scan classified out of the rebuilt tree: a foreign name, a
    // duplicate id, a table or blob file no bound could make safe. The scan
    // itself touched nothing, so until this point a crash left the directory
    // exactly as the retry expects to find it; from here the manifest is
    // durable and these files are what the tree no longer references.
    if post_commit_error.is_none() {
        for (fs, path, note) in discard_after_commit {
            match discard_unreferenced(&*fs, &path, config.sync_mode) {
                Ok(()) => log::info!("repair: {} removed ({note})", path.display()),
                Err(e) => {
                    log::error!(
                        "repair: cannot remove {} ({e}); failing the repair — left in \
                         place it is a file the next open must reject or sweep, and it \
                         would hit the same error",
                        path.display(),
                    );
                    post_commit_error = Some(e);
                    break;
                }
            }
        }
    }

    // Same rule for the blob files the manifest omits because nothing
    // references them: their data is unreachable, so there is nothing to
    // preserve — but leaving one behind hands the next open an orphan to
    // sweep, and a sweep that fails there fails the open. Remove them here and
    // propagate, so a repair that reports success leaves an openable tree.
    if post_commit_error.is_none() {
        for path in unreferenced_blob_files {
            match config.fs.remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
                Err(e) => {
                    log::error!(
                        "repair: cannot remove the unreferenced blob file {} ({e}); \
                         failing the repair — left in blobs/ it is an orphan the next \
                         open must remove, and that removal would hit the same error",
                        path.display(),
                    );
                    post_commit_error = Some(e.into());
                    break;
                }
            }
        }
    }

    let mut warnings = vec![
        "All recovered tables placed at L0; background compaction will redistribute them",
        "Recent unlogged version edits (in-flight compactions, recent deletions) are lost",
    ];
    if config.kv_separation_opts.is_some() {
        warnings.push(
            "Blob fragmentation stats reset (punched prefixes reseeded); blob GC re-learns the rest over time",
        );
    }

    // Join the exclusions against the coverage captured during the scan, plus
    // an entry per KEPT lossy salvage (see `salvaged_coverage`). An excluded
    // TABLE whose metadata never parsed has unknowable coverage — recorded
    // separately so `wal_replay_scope()` can force the full-history
    // obligation instead of silently answering as if nothing was lost. Only a
    // NUMERIC table candidate qualifies: a FOREIGN name (`notes.txt`) held no
    // table data by definition, so its exclusion lost nothing — reporting it
    // unknowable would demand an unbounded WAL archive over scribbles. Blob
    // files stay out for the same reason in the other direction: losing blob
    // content surfaces through the referencing tables (a lossy handle
    // rewrite, or their exclusion), which ARE covered above. Provenance, not
    // a path prefix, decides which is which: a level route may nest its
    // tables anywhere (even under the primary tree's blobs directory), so a
    // candidate is a table iff its parent is one of the scanned table
    // folders.
    let table_folders: Vec<PathBuf> = config
        .all_tables_folders()
        .into_iter()
        .map(|(folder, _)| folder)
        .collect();
    let mut lost_coverage: Vec<(PathBuf, UserKey, UserKey, Option<SeqNo>)> = Vec::new();
    let mut unknowable_losses: Vec<PathBuf> = Vec::new();
    let is_table_candidate = |path: &std::path::Path| {
        path.parent()
            .is_some_and(|parent| table_folders.iter().any(|f| f.as_path() == parent))
            && path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                matches!(
                    crate::file::TableDirEntry::classify(n),
                    crate::file::TableDirEntry::Table(_)
                )
            })
    };
    for (path, _) in &unreadable_files {
        // A REDUNDANT exclusion lost nothing — its content lives on in the
        // kept tables — so it contributes no coverage.
        if redundant_excluded.contains(path) {
            continue;
        }
        match coverage_by_path.get(path) {
            Some((lo, hi, seqno)) => {
                lost_coverage.push((path.clone(), lo.clone(), hi.clone(), *seqno));
            }
            None if is_table_candidate(path) => unknowable_losses.push(path.clone()),
            None => {}
        }
    }
    lost_coverage.extend(salvaged_coverage);
    lost_coverage.extend(ambiguous_order_coverage);
    lost_coverage.extend(
        lineage_partial
            .into_iter()
            .map(|(_, path, lo, hi, bound)| (path, lo, hi, bound)),
    );
    unknowable_losses.extend(salvaged_unknowable);

    let report = RepairReport {
        recovered,
        salvaged,
        unreadable: unreadable_files.len(),
        unreadable_files,
        lost_coverage,
        unknowable_losses,
        blob_files_salvaged,
        method: "all-to-L0 with sequence-number ordering",
        warnings,
    };

    // A recorded post-commit failure carries the completed report out: the
    // manifest is durable, so the repair happened, and a retry once the
    // filesystem is fixed finishes the cleanup — but its own open may find
    // nothing left to repair and answer with no report at all.
    if let Some(cause) = post_commit_error {
        return Err(crate::Error::RepairedButUnopened {
            report: Box::new(report),
            cause: Box::new(cause),
        });
    }

    // Success only: a failed run leaves the phase where it stopped, which
    // tells a progress display exactly which stage failed.
    if let Some(p) = &config.recovery_progress {
        p.set_phase(crate::RecoveryPhase::Done);
    }

    Ok(report)
}

#[cfg(test)]
mod tests;
