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
    Table, TableId,
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

/// Moves a file that does not belong in `tables/` (a non-table-id name) into a
/// sibling `repair-quarantine/` directory, so a subsequent `Tree::open` — which
/// rejects non-numeric names in `tables/` — succeeds. Returns the new path.
///
/// Quarantine (move) rather than delete: the file was not created by repair, so
/// it is preserved for the operator to inspect. The quarantine dir is a sibling
/// of the table folder (same filesystem) so the move is a plain rename.
fn quarantine_file(
    fs: &dyn crate::fs::Fs,
    table_base_folder: &std::path::Path,
    src: &std::path::Path,
    file_name: &str,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<PathBuf> {
    let quarantine_dir = table_base_folder
        .parent()
        .unwrap_or(table_base_folder)
        .join("repair-quarantine");
    // Creating the quarantine directory adds its entry to the PARENT directory.
    // That entry is durable only once the parent is synced: the later syncs
    // cover the source directory and the quarantine directory itself, but not
    // the parent that now names it, so without this a power loss after repair
    // returns can drop the whole quarantine directory (and the only preserved
    // copy of the original). Sync UNCONDITIONALLY, before the rename: a prior
    // repair that created the directory but crashed before its own parent sync
    // leaves the entry non-durable, so a retry that skipped the sync (seeing the
    // directory already present) would move the source in without ever making
    // the parent durable. A redundant fsync on an already-durable parent is
    // cheap; skipping it risks losing the whole directory across a
    // crashed-then-retried repair.
    fs.create_dir_all(&quarantine_dir)?;
    if let Some(quarantine_parent) = quarantine_dir.parent() {
        fs.sync_directory_with(quarantine_parent, sync_mode)?;
    }
    // Preserve any EARLIER quarantine copy of the same table: `rename` replaces
    // the destination on Unix, so a fixed `{file_name}` would move the new
    // corrupt source over the only copy of a previously quarantined original
    // set aside for inspection. Probe for a free `{file_name}` / `{file_name}.N`
    // name instead. (A tiny check-then-rename window is acceptable: repair runs
    // single-process against a downed tree.)
    let dest = {
        let mut candidate = quarantine_dir.join(file_name);
        let mut n: u64 = 1;
        while fs.exists(&candidate)? {
            candidate = quarantine_dir.join(format!("{file_name}.{n}"));
            n = n.checked_add(1).ok_or(crate::Error::Unrecoverable)?;
        }
        candidate
    };
    fs.rename(src, &dest)?;
    // Everything after the first rename runs inside one fallible span so ANY
    // failure — the sidecar's existence probe, the sidecar's rename, or the
    // durability syncs — rolls the SST (and the sidecar, when it moved) back
    // under `tables/`. Propagating with the table stranded in quarantine would
    // mean the retried repair no longer discovers it and installs a manifest
    // that omits it; a sync failure additionally means the move is not durably
    // committed, so a later power loss could resurrect the source as an orphan
    // the next open deletes. The rollback leaves both files exactly where a
    // retry can find and re-quarantine them durably.
    //
    // Sidecar: a restricted SST carries its exact recovery bound in a sibling
    // `.restrict-bound` file. Move it WITH the table: left behind in `tables/`,
    // the next open's orphan sweep (the rebuilt manifest no longer names this
    // id) deletes it, permanently stranding the quarantined punched file from
    // its exact bound. Absent for unrestricted tables and for non-table
    // quarantines (a rejected salvage replacement, a foreign file), a no-op
    // there. Both sidecar and SST live in the same two directories, so the two
    // syncs cover both.
    //
    // Sync ordering: a rename is durable only once BOTH affected directory
    // entries are on disk. Sync the DESTINATION (quarantine) directory FIRST,
    // then the source: if the source's deletion were made durable first, a
    // power loss before the quarantine entry is durable would leave NEITHER
    // name after reboot, destroying the only preserved original. This is the
    // same ordering `restore_quarantined` uses for the inverse move.
    let src_sidecar = crate::restrict_bound::sidecar_path(src);
    let dest_sidecar = crate::restrict_bound::sidecar_path(&dest);
    let commit_result = (|| -> crate::Result<()> {
        if fs.exists(&src_sidecar)? {
            fs.rename(&src_sidecar, &dest_sidecar)?;
        }
        fs.sync_directory_with(&quarantine_dir, sync_mode)?;
        if let Some(src_dir) = src.parent() {
            fs.sync_directory_with(src_dir, sync_mode)?;
        }
        Ok(())
    })();
    if let Err(e) = commit_result {
        // Best-effort: undo BOTH moves and re-sync. The sidecar rollback is
        // unconditional — a no-op rename failure (the sidecar never moved, or
        // never existed) is harmless and ignored like the rest. A rollback that
        // itself fails leaves nothing more we can safely do here; surface the
        // original error.
        let _ = fs.rename(&dest, src);
        let _ = fs.rename(&dest_sidecar, &src_sidecar);
        if let Some(src_dir) = src.parent() {
            let _ = fs.sync_directory_with(src_dir, sync_mode);
        }
        let _ = fs.sync_directory_with(&quarantine_dir, sync_mode);
        return Err(e);
    }
    Ok(dest)
}

/// Marker path naming WHY a quarantined file was set aside: its restriction
/// bound was unrecoverable at `resurrection = off`. A resurrection repair
/// reclaims marked files back into `tables/` (see [`reclaim_resurrectable`]),
/// keeping the resurrection knob two-way — no manual file move between a
/// default repair and a resurrection re-run. The marker is NEVER written for
/// flag-independent quarantines (duplicates, corrupt files, bulk-ingest
/// rejects, salvage byproducts), so a reclaim can never resurrect those.
fn resurrectable_marker_path(quarantined: &std::path::Path) -> PathBuf {
    let mut name = quarantined.file_name().unwrap_or_default().to_os_string();
    name.push(".resurrectable");
    quarantined.with_file_name(name)
}

/// Durably writes the resurrectable marker beside a freshly quarantined file.
/// The CALLER must treat a failure as "the set-aside did not commit": roll the
/// quarantine move back (restore the file to `tables/`) and propagate, so a
/// retry re-runs the whole classification instead of leaving an UNMARKED
/// set-aside that a resurrection repair could never reclaim.
fn mark_resurrectable(
    fs: &dyn crate::fs::Fs,
    quarantined: &std::path::Path,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    use std::io::Write;
    let marker = resurrectable_marker_path(quarantined);
    let mut file = fs.open(
        &marker,
        &crate::fs::FsOpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true),
    )?;
    file.write_all(b"resurrectable")?;
    file.flush()?;
    crate::fs::FsFile::sync_all_with(&*file, sync_mode)?;
    drop(file);
    if let Some(dir) = marker.parent() {
        fs.sync_directory_with(dir, sync_mode)?;
    }
    Ok(())
}

/// Returns marked resurrectable set-asides from `repair-quarantine/` to
/// `tables/`, so a resurrection repair recovers them like any other punched
/// file — the flag's two-way half (the marker is written by the flag-dependent
/// set-aside sites). Only files bearing a `.resurrectable` marker move; the
/// marker is consumed by the move. A collision-suffixed quarantine name
/// (`{id}.N`) reclaims to its plain `{id}`; an occupied target skips the file
/// (kept marked for a later run) rather than clobbering. Crash-safe by
/// idempotence: a file either still sits marked in quarantine (retried next
/// run) or already sits in `tables/` (recovered normally; its stale marker is
/// swept here).
#[cfg(feature = "std")]
fn reclaim_resurrectable(
    fs: &dyn crate::fs::Fs,
    table_base_folder: &std::path::Path,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    let quarantine_dir = table_base_folder
        .parent()
        .unwrap_or(table_base_folder)
        .join("repair-quarantine");
    if !fs.exists(&quarantine_dir)? {
        return Ok(());
    }
    for entry in fs.read_dir(&quarantine_dir)? {
        let Some(data_name) = entry.file_name.strip_suffix(".resurrectable") else {
            continue;
        };
        let marker = quarantine_dir.join(&entry.file_name);
        let data = quarantine_dir.join(data_name);
        if !fs.exists(&data)? {
            // A crash between a previous reclaim's move and its marker removal:
            // the file already went back and was recovered; sweep the leftover.
            let _ = fs.remove_file(&marker);
            continue;
        }
        // A collision-suffixed quarantine name ({id}.N) reclaims to plain {id}.
        let id_part = data_name.split('.').next().unwrap_or(data_name);
        if id_part.parse::<TableId>().is_err() {
            continue;
        }
        let target = table_base_folder.join(id_part);
        if fs.exists(&target)? {
            log::warn!(
                "not reclaiming {}: {} is already occupied; kept marked for a later run",
                data.display(),
                target.display(),
            );
            continue;
        }
        fs.rename(&data, &target)?;
        // Everything after the rename runs inside one fallible span: a failure
        // must NOT return with the SST sitting in `tables/` unreferenced by the
        // previously installed manifest — a caller that reacts to the failed
        // repair by simply REOPENING the tree would then have orphan cleanup
        // delete the only recovered copy. On failure the file (and any moved
        // sidecar) rolls BACK into quarantine, marker intact, where the next
        // resurrection repair rediscovers it; quarantine — not `tables/` — is
        // the retry-safe location for THIS flow precisely because the marker
        // machinery re-scans it. The marker is consumed only AFTER both syncs,
        // so a crash inside the span also leaves a marked, reclaimable file
        // (the un-synced rename either rolls back with its directory, or the
        // file lands in `tables/`, is recovered, and the stale marker is swept
        // next run).
        let data_sidecar = crate::restrict_bound::sidecar_path(&data);
        let target_sidecar = crate::restrict_bound::sidecar_path(&target);
        let commit = (|| -> crate::Result<()> {
            // Bring a companion sidecar back too (present only when a
            // corrupt-but-existing sidecar traveled with the quarantine move; a
            // corrupt sidecar is ignored by recovery, so this is harmless
            // bookkeeping).
            if fs.exists(&data_sidecar)? {
                let _ = fs.rename(&data_sidecar, &target_sidecar);
            }
            // Destination first, then source: the same crash ordering the
            // quarantine / restore moves use — the reclaimed name must be
            // durable before its quarantine entry's removal is.
            fs.sync_directory_with(table_base_folder, sync_mode)?;
            fs.sync_directory_with(&quarantine_dir, sync_mode)?;
            Ok(())
        })();
        if let Err(e) = commit {
            // Best-effort: undo both moves and re-sync; surface the original
            // error. A rollback that itself fails leaves nothing more we can
            // safely do here — the marker still names the file for the next run.
            let _ = fs.rename(&target, &data);
            let _ = fs.rename(&target_sidecar, &data_sidecar);
            let _ = fs.sync_directory_with(&quarantine_dir, sync_mode);
            let _ = fs.sync_directory_with(table_base_folder, sync_mode);
            return Err(e);
        }
        let _ = fs.remove_file(&marker);
        log::info!(
            "reclaimed resurrectable set-aside {} -> {}",
            data.display(),
            target.display(),
        );
    }
    Ok(())
}

/// Moves a quarantined original back to its `table_path`, durably. The inverse of
/// [`quarantine_file`], used when a TRANSIENT salvage failure must not strand the
/// only copy in quarantine (which a retry, no longer finding it under `tables/`,
/// would never rediscover). `rename` replaces any partial / salvaged output the
/// aborted salvage left at `table_path`, and both affected directory entries are
/// synced so the restore survives a power loss the same way the quarantine move
/// does.
fn restore_quarantined(
    fs: &dyn crate::fs::Fs,
    quarantined: &std::path::Path,
    table_path: &std::path::Path,
    encryption: Option<&dyn crate::encryption::EncryptionProvider>,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    // Capture the companion sidecar's RAW bytes BEFORE anything moves: if the
    // direct sidecar rename below fails after the SST is already restored, the
    // bytes are re-published at the destination instead, so the exact bound is
    // never stranded in quarantine (where the retried repair — which scans only
    // `tables/` — would silently degrade the restored SST to the lossy
    // geometry fallback). A TRANSIENT probe / read failure propagates while
    // nothing has moved yet; a PERSISTENT one — or a file larger than any
    // VALID sidecar encoding (an attacker-padded / corrupt file must not be
    // trusted into a full-size allocation) — leaves no rescue copy, but the
    // direct rename below (which never reads the content) may still succeed.
    let quarantined_sidecar = crate::restrict_bound::sidecar_path(quarantined);
    let sidecar_state: Option<(bool, Option<Vec<u8>>)> = if fs.exists(&quarantined_sidecar)? {
        match read_raw_file(
            fs,
            &quarantined_sidecar,
            crate::restrict_bound::max_encoded_len(encryption),
        ) {
            Ok(bytes) => Some((true, bytes)),
            Err(e) if is_transient_io(&e) => return Err(e),
            Err(_) => Some((true, None)),
        }
    } else {
        None
    };
    fs.rename(quarantined, table_path)?;
    // Restore the companion `.restrict-bound` sidecar too, mirroring the move
    // `quarantine_file` made: a restored SST without its exact bound would fall
    // back to a conservative punch-geometry bound on the next open. Both files
    // share the same two directories, so the syncs below cover both.
    //
    // The SST is NEVER rolled back into quarantine on a sidecar failure: a
    // pair stranded there is invisible to the retried repair entirely (a
    // silent whole-table loss), while a restored SST with a degraded bound
    // stays recoverable through the deterministic geometry path.
    if let Some((_, bytes)) = sidecar_state {
        let dest_sidecar = crate::restrict_bound::sidecar_path(table_path);
        if let Err(rename_err) = fs.rename(&quarantined_sidecar, &dest_sidecar) {
            match bytes {
                Some(bytes) => {
                    // Fallback: re-publish the captured bytes atomically at the
                    // destination. A failure here propagates (the SST stays
                    // restored; the retry handles the boundless punched table
                    // deterministically).
                    log::warn!(
                        "restoring {}: sidecar rename failed ({rename_err}); re-publishing \
                         the captured bytes instead",
                        table_path.display(),
                    );
                    crate::restrict_bound::publish_raw(fs, table_path, &bytes, sync_mode)?;
                    // Best-effort: drop the now-duplicated quarantine copy so a
                    // stale bound cannot linger beside future quarantines.
                    let _ = fs.remove_file(&quarantined_sidecar);
                }
                None => {
                    // The sidecar could be neither read nor moved: its bound is
                    // unrecoverable through any mechanism. Continue — the
                    // restore itself succeeded, and the retry resolves the
                    // boundless punched table deterministically (geometry
                    // bound, or set-aside when the punch pattern is
                    // ambiguous). Aborting here would change nothing the
                    // retry could use.
                    log::error!(
                        "restoring {}: sidecar is unreadable and its rename failed \
                         ({rename_err}); the exact restriction bound is lost",
                        table_path.display(),
                    );
                }
            }
        }
    }
    if let Some(dst_dir) = table_path.parent() {
        fs.sync_directory_with(dst_dir, sync_mode)?;
    }
    if let Some(src_dir) = quarantined.parent() {
        fs.sync_directory_with(src_dir, sync_mode)?;
    }
    Ok(())
}

/// Reads a small file's complete contents, or `None` when the file exceeds
/// `max_len` — the reported length is untrusted input, so it is validated
/// BEFORE sizing the allocation. Used to capture sidecar bytes before a restore
/// move so they can be re-published if the direct rename fails; an oversized
/// file cannot be a valid sidecar and is not worth rescuing.
#[cfg(feature = "std")]
fn read_raw_file(
    fs: &dyn crate::fs::Fs,
    path: &std::path::Path,
    max_len: u64,
) -> crate::Result<Option<Vec<u8>>> {
    let file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    let len = crate::fs::FsFile::metadata(&*file)?.len;
    if len > max_len {
        return Ok(None);
    }
    let n = usize::try_from(len).map_err(|_| crate::Error::Unrecoverable)?;
    Ok(Some(crate::file::read_exact(&*file, 0, n)?.to_vec()))
}

/// Whether an error is an UNAMBIGUOUSLY TRANSIENT (retryable) I/O failure, as
/// opposed to one a re-read cannot fix.
///
/// The allowlist is deliberately narrow: only `Interrupted` (`EINTR`) and
/// `WouldBlock` (`EAGAIN`) — the interrupted-syscall errors a retry genuinely
/// clears, and which a corrupt on-disk structure can NEVER produce.
///
/// `Other` is NOT treated as transient, even though an injected fault or a raw
/// `EIO` lands there, because a STRUCTURAL corruption lands there too: a corrupt
/// trailer that decodes to a bad offset makes the reader seek before the start of
/// the file, which Windows reports as `ERROR_NEGATIVE_SEEK` — an unmapped OS
/// error the `From<std::io::Error>` bridge folds into `ErrorKind::Other`. Treating
/// `Other` as transient would then abort the WHOLE repair (blocking recovery of
/// every healthy sibling table) on a single genuinely-corrupt SST, and the class
/// is platform-dependent (the same corruption reads back `InvalidInput` on Unix).
/// A hardware `EIO` is likewise usually a persistent bad-sector failure, so
/// recording that table unreadable — while the rest recover — is the right
/// outcome. Fault-injection tests therefore inject `Interrupted` to model a
/// retryable fault.
///
/// This inspects the CRATE's [`crate::io::ErrorKind`], which is what a
/// `crate::Error::Io` always carries.
fn is_transient_io(e: &crate::Error) -> bool {
    matches!(e, crate::Error::Io(io) if io.kind().is_transient())
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
///   quarantined; the ambiguity is unavoidable without the flag.
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

/// A recovered table plus whether its recovery was COMPLETE (a clean whole-file
/// recovery) or a LOSSY block-salvage that may have dropped corrupt blocks'
/// keys. Repair keeps the best copy per table id, so a duplicate id in another
/// table folder can supersede an earlier DAMAGED copy.
///
/// The physical location (`fs` / `base_folder` / `path` / `file_name`) travels
/// with the candidate so that when a duplicate SUPERSEDES it, the loser's file
/// can be quarantined out of `tables/`. The rebuilt manifest records only
/// `id + checksum`, so two same-id files left in different folders would let
/// recovery resolve the stale one by folder order and reopen it against the kept
/// copy's mismatched checksum.
struct TableCandidate {
    table: Table,
    complete: bool,
    fs: Arc<dyn crate::fs::Fs>,
    base_folder: PathBuf,
    path: PathBuf,
    file_name: String,
}

/// Records `candidate` for `id`, keeping the BETTER of the existing and the new
/// copy: a COMPLETE recovery replaces a lossy salvage, so an intact duplicate in
/// a later-scanned folder supersedes an earlier lossy one. Two completes (or two
/// salvages) are equivalent for the rebuilt manifest — which needs only one
/// readable copy per id — so the first-seen stays.
///
/// Returns the DISPLACED loser (the rejected new candidate, or the superseded old
/// one) so the caller can quarantine its on-disk file; `None` when `id` was
/// previously unseen (nothing displaced).
#[must_use = "the displaced duplicate's file must be quarantined out of tables/"]
fn keep_best_candidate(
    map: &mut crate::HashMap<TableId, TableCandidate>,
    id: TableId,
    candidate: TableCandidate,
) -> Option<TableCandidate> {
    match map.get(&id) {
        // Keep the existing copy when it is already complete, or when the new one
        // is not an improvement (a lossy duplicate of a lossy copy): the NEW
        // candidate is displaced.
        Some(existing) if existing.complete || !candidate.complete => Some(candidate),
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
/// through an alias is never quarantined as a "duplicate" (which would move the
/// kept copy and orphan the manifest entry).
///
/// The two candidates must live in the SAME filesystem namespace for a path
/// comparison to mean anything: a virtual (`MemFs`) table can sit at a path that
/// also exists on the host filesystem, and canonicalizing both spellings through
/// the host would call those distinct files aliases — the loser would then escape
/// quarantine and a later reopen could resolve that leftover against the kept
/// copy's manifest checksum. Backends advertise namespace identity through
/// [`Fs::backend_id`](crate::fs::Fs::backend_id), whose `None` means "no shared
/// namespace guarantee" and is therefore treated as DISTINCT.
///
/// Within one namespace, alias resolution is the backend's own
/// ([`Fs::same_file`](crate::fs::Fs::same_file)): kernel-backed backends
/// canonicalize through the host, virtual ones compare paths literally — a
/// host symlink must never alias two distinct virtual files. A probe that
/// cannot decide answers `false` (distinct), so a genuine duplicate is still
/// quarantined.
#[cfg(feature = "std")]
fn same_physical_file(
    fs_a: &dyn crate::fs::Fs,
    a: &std::path::Path,
    fs_b: &dyn crate::fs::Fs,
    b: &std::path::Path,
) -> bool {
    match (fs_a.backend_id(), fs_b.backend_id()) {
        (Some(id_a), Some(id_b)) if id_a == id_b => {}
        // Different backends, or a backend that gives no namespace guarantee:
        // path spellings are not comparable, so never alias them.
        _ => return false,
    }
    // Alias resolution belongs to the BACKEND, not the host: canonicalizing a
    // virtual backend's paths through the host filesystem would let a host
    // symlink alias two unrelated virtual files, and the "duplicate" loser
    // would escape quarantine. Kernel-backed backends canonicalize; virtual
    // ones compare literally.
    fs_a.same_file(a, b)
}

/// Quarantines a duplicate table file that lost to a better same-id copy, moving
/// it out of `tables/` (so recovery cannot resolve it) and recording it as
/// considered-but-not-referenced. A failed quarantine aborts the whole repair:
/// leaving both same-id files in place would let recovery reopen the wrong one.
#[cfg(feature = "std")]
fn quarantine_duplicate(
    loser: TableCandidate,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    let TableCandidate {
        table,
        fs,
        base_folder,
        path,
        file_name,
        ..
    } = loser;
    drop(table); // release the open file handle before the move
    let dest = quarantine_file(&*fs, &base_folder, &path, &file_name, sync_mode)?;
    unreadable_files.push((
        path,
        format!(
            "duplicate table id superseded by another copy; quarantined to {}",
            dest.display()
        ),
    ));
    Ok(())
}

/// Builds a [`TableCandidate`] from a recovered table plus its physical location,
/// records it as the best copy for `id`, and quarantines any duplicate displaced
/// by the decision. The one path every recovered table takes to enter the
/// manifest, so a superseded same-id file is never left discoverable.
#[cfg(feature = "std")]
#[expect(
    clippy::too_many_arguments,
    reason = "location + report threaded through"
)]
fn record_best(
    map: &mut crate::HashMap<TableId, TableCandidate>,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    id: TableId,
    table: Table,
    complete: bool,
    fs: &Arc<dyn crate::fs::Fs>,
    base_folder: &std::path::Path,
    path: &std::path::Path,
    file_name: &str,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    let candidate = TableCandidate {
        table,
        complete,
        fs: Arc::clone(fs),
        base_folder: base_folder.to_path_buf(),
        path: path.to_path_buf(),
        file_name: file_name.to_string(),
    };
    if let Some(loser) = keep_best_candidate(map, id, candidate) {
        // If the displaced copy physically ALIASES the kept one (same directory
        // entry via a symlink / junction / case-insensitive path), it is the SAME
        // file — quarantining it would move the kept copy and orphan the manifest
        // entry. Drop the loser's handle in place instead of quarantining.
        let is_alias = map
            .get(&id)
            .is_some_and(|kept| same_physical_file(&*loser.fs, &loser.path, &*kept.fs, &kept.path));
        if !is_alias {
            quarantine_duplicate(loser, unreadable_files, sync_mode)?;
        }
    }
    Ok(())
}

/// Block-salvages a corrupt SST during repair: reads the ALREADY-QUARANTINED
/// original (the caller performs the move, which frees `table_path`), writes a
/// fresh SST holding its recoverable blocks into that path, and reopens it.
///
/// Returns `Ok(None)` when nothing was recoverable (the original stays in
/// quarantine and the path is left empty), or `Err` when even salvage cannot
/// open the source (its metadata / index / SFA trailer is itself unreadable).
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
/// [`crate::Error::Io`] (the [`is_transient_io`] allowlist) aborts the repair
/// (`Err`) for a retry, rather than dropping a healthy block into a partial
/// replacement. A PERSISTENT I/O failure is NOT retryable — a bad sector, or a
/// structural corruption surfacing as `Io(Other)` on some platforms — so it is
/// graded as corruption and salvaged too, rather than aborting the whole repair
/// and stranding every other healthy table on one unrecoverable read.
fn is_corruption(res: crate::Result<()>) -> crate::Result<bool> {
    match res {
        Ok(()) => Ok(false),
        Err(e) if is_transient_io(&e) => Err(e),
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
    // report those dead blocks as corruption and salvage would then quarantine an
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
    // `WouldBlock`) is not block corruption: routing it through salvage would
    // re-read the same bytes and drop a healthy block. Propagate it so the repair
    // aborts and the operator retries, mirroring the decode-load gate below. Any
    // OTHER kind falls through to the corruption verdict: a truncation
    // (`UnexpectedEof`) is genuine on-disk damage, and a PERSISTENT failure
    // (`Other` / EIO, `PermissionDenied`) is not fixed by a retry either, so
    // aborting forever would strand every healthy sibling table on one bad SST.
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
            && error.kind().is_transient()
        {
            // Preserve the transient ErrorKind: re-wrapping as `Other` would make
            // the caller's `is_transient_io` check see a non-transient kind and
            // re-grade this retryable failure as corruption, defeating the
            // transient-propagation intent of this very gate.
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
    /// The table is routed through block salvage (quarantine + rewrite).
    Salvage,
    /// The table can be neither trusted nor faithfully salvaged under the
    /// active resurrection policy: it is EXCLUDED from the rebuilt manifest and
    /// its file set aside (protecting it from the orphan cleanup a later open
    /// runs) with this reason. The tree still opens; the excluded table is
    /// re-admitted by recompaction or, where the reason allows, by enabling
    /// resurrection.
    Quarantine(&'static str),
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
        // section, so `true` quarantines rather than resurrecting masked rows.
        Err(e) => {
            let err = crate::Error::Io(e);
            return if is_transient_io(&err) {
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
            if is_transient_io(&err) {
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
                // salvage off the table quarantines either way, but this reason
                // is the accurate one — pointing that operator at a
                // salvage-enabled repair would mislead (it quarantines the
                // concealment case too, unless resurrection is enabled).
                if toc_may_hide_deletions(folder_fs, table_path)? && !allow_resurrection {
                    RepairKeepDecision::Quarantine(
                        "TOC corruption may hide deletion metadata (range tombstones \
                     / delete bitmap); its visibility is unrecoverable, so the table \
                     is excluded to avoid resurrecting masked rows. Enable \
                     resurrection to salvage it, accepting that suppressed rows \
                     reappear",
                    )
                } else if salvage {
                    RepairKeepDecision::Salvage
                } else {
                    RepairKeepDecision::Quarantine(
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
                    RepairKeepDecision::Quarantine(
                        "ECC descriptor unrecognized (the block walk cannot verify the \
                     table) and salvage cannot re-emit its range tombstones; the table \
                     is excluded (recompact it under a supported scheme to re-admit it)",
                    )
                } else {
                    RepairKeepDecision::Quarantine(
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
    /// unreadable. The original stays quarantined for inspection.
    Unusable,
    /// The `reject_punched_without_bound` guard fired: a salvage-dropped data
    /// extent of the source reads as zeros (the hole-punch signature), so the
    /// source lost data to a punch whose bound is unrecoverable. The
    /// unrestricted replacement was rejected and quarantined — installing it
    /// would resurrect the reclaimed region's superseded rows.
    PunchedBoundLost,
}

/// What one [`try_salvage_table`] call operates on: the source paths and the
/// per-call policy. Bundled so the salvage entry keeps a small signature as
/// its policy surface grows.
#[cfg(feature = "std")]
struct TableSalvage<'a> {
    /// The already-quarantined corrupt original (the salvage source). The
    /// CALLER performs the quarantine move and aborts the whole repair when it
    /// fails — a manifest omitting a still-in-place file would let the next
    /// open's orphan cleanup delete the only copy. An error returned from the
    /// salvage is therefore always post-quarantine: the original is safely
    /// preserved, and the caller records the failure instead of aborting.
    quarantined: &'a std::path::Path,
    /// Where the recovered copy is written (the original table path).
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
    /// Stamp the recovered copy with a FRESH identity (matching `table_path`'s
    /// file name) instead of the source's, for the caller that publishes it
    /// beside an untouched source. `None` keeps the source's identity, which is
    /// what the in-place replacement paths need.
    output_id: Option<TableId>,
}

fn try_salvage_table(
    config: &Config,
    fs: &Arc<dyn crate::fs::Fs>,
    allow_resurrection: bool,
    salvage: TableSalvage<'_>,
) -> crate::Result<SalvageOutcome> {
    let TableSalvage {
        quarantined,
        table_path,
        table_id,
        reject_punched_without_bound,
        blob_rewrite,
        output_id,
    } = salvage;
    // Salvage under the tree's configured comparator + crypto/dictionary context
    // so the rewritten SST opens, orders, and decrypts / decompresses consistently
    // with the rest of the tree on reopen (the reopen below uses the same
    // `config.encryption` / `config.zstd_dictionary`).
    let report = crate::salvage::salvage_with_context(
        quarantined,
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
            output_id,
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
        && dropped_data_extent_is_zeroed(&**fs, quarantined, &report.dropped)?
    {
        // The source was punched but its bound is unrecoverable: the salvaged
        // replacement re-emits every intact block — including consumed,
        // superseded blocks a partial punch left inside the reclaimed prefix —
        // with nothing to restrict them. Reject it: quarantine the fresh copy
        // (see the bulk-ingest rejection below for why a plain remove is not
        // enough) and let the caller set the table aside.
        let base = table_path.parent().ok_or(crate::Error::Unrecoverable)?;
        let name = table_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(crate::Error::Unrecoverable)?;
        quarantine_file(&**fs, base, table_path, name, config.sync_mode)?;
        return Ok(SalvageOutcome::PunchedBoundLost);
    }

    // Reopen the freshly-written (clean) salvaged SST so it joins the rebuilt
    // manifest like any cleanly-recovered table.
    let checksum = crate::Checksum::from_raw(compute_table_checksum(&**fs, table_path)?);
    let table = Table::recover(
        table_path.to_path_buf(),
        checksum,
        0,
        0,
        output_id.unwrap_or(table_id),
        config.cache.clone(),
        None,
        Arc::clone(fs),
        false,
        false,
        config.encryption.clone(),
        #[cfg(zstd_any)]
        config.zstd_dictionary.clone(),
        config.comparator.clone(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::metrics::Metrics::default()),
    )?;
    // A salvaged copy of a bulk-ingested source still relies on the manifest-only
    // global_seqno offset the rebuilt manifest cannot recover: its entries stay at
    // local seqno 0, so installing it with offset 0 would silently mis-order and
    // over-expose them. Treat it as unsalvageable — drop the freshly-written copy
    // (a leftover reads back as a harmless orphan) and let the caller record it
    // unreadable; the original stays set aside for inspection.
    if has_unrecoverable_ingest_offset(
        table.metadata.bulk_ingested,
        table.metadata.item_count,
        table.max_local_seqno(),
    ) {
        drop(table);
        // QUARANTINE the rejected replacement, don't try-and-forget its removal. A
        // discarded `remove_file` error would leave the freshly-written numeric SST
        // in `tables/`; repair would still install a manifest that omits it and
        // report success, but the next open classifies it as an orphan and fails on
        // the SAME persistent deletion, so the "repaired" tree cannot reopen. Moving
        // it out of `tables/` makes it a non-orphan. A quarantine failure propagates:
        // it is post-quarantine of the ORIGINAL (safely preserved), so the caller
        // records it as unreadable rather than aborting.
        let base = table_path.parent().ok_or(crate::Error::Unrecoverable)?;
        let name = table_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(crate::Error::Unrecoverable)?;
        quarantine_file(&**fs, base, table_path, name, config.sync_mode)?;
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
    // length test would quarantine an otherwise usable salvage as bound-lost
    // under the default no-resurrection policy.
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
    // Probed at the run's MIDPOINT rather than over the whole span: a run is
    // bounded by the neighbouring bytes that happen to be nonzero, so it can
    // reach a byte or two into the allocated blocks around the hole, and
    // demanding the entire span be unallocated would reject genuine punches.
    // Only the presence of the hole matters here — the single caller asks
    // whether the extent was reclaimed at all, not exactly where.
    let mut proven = Vec::with_capacity(merged.len());
    for (start, end) in merged {
        let midpoint = start + (end - start) / 2;
        if fs.extent_is_hole(source, midpoint, 1)? == Some(true) {
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
    // of salvaging the blocks that are still readable. A reclaim deallocates, so
    // the probed extent must be a HOLE. The same rule the block-level punch
    // classifier applies; a backend that cannot answer leaves it unproven.
    Ok(fs.extent_is_hole(table_path, 0, n as u64)? == Some(true))
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
/// restores the quarantined original to `table_path` before propagating, mirroring
/// the salvage-error path. Otherwise the unpunched, sidecar-less salvaged
/// replacement would be left in place, and a retry would recover it UNRESTRICTED
/// and resurrect the sub-bound rows. This holds for a PERSISTENT failure
/// (an ENOSPC on the sidecar write) as much as a transient one: the retry cannot
/// re-derive the bound from a fresh unpunched output, so the punched original must
/// be back in place for it to re-salvage and re-restrict from.
#[cfg(feature = "std")]
enum SalvageRollback<'a> {
    /// The source was moved out of the way before the salvage ran: restore it
    /// over `table_path` so a retry starts from the state it was found in.
    Restore(&'a std::path::Path),
    /// The source is untouched at its own path and the output was published
    /// beside it under a fresh id: discard that output instead. The retry
    /// re-derives the same rewrite from the same source.
    DiscardOutput,
}

#[cfg(feature = "std")]
fn restrict_salvaged_output(
    folder_fs: &dyn crate::fs::Fs,
    config: &Config,
    table_path: &std::path::Path,
    rollback: &SalvageRollback<'_>,
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
                    // Drop the salvaged handle's open file BEFORE restoring. The
                    // restore renames the quarantined original back over
                    // `table_path`, and a backend that rejects replacing an OPEN
                    // destination (Windows; the deletion path closes handles for
                    // this same reason) would fail the rename while `salvaged` still
                    // holds `table_path` open. A failed restore leaves the unpunched
                    // salvaged SST in place with no bound, so the next repair would
                    // recover it UNRESTRICTED and resurrect the sub-bound rows.
                    drop(salvaged);
                    // Restore on EVERY failure, transient or persistent: the
                    // salvaged replacement sits at `table_path` unpunched with no
                    // valid sidecar, so a retry that finds it there recovers it
                    // UNRESTRICTED and resurrects the sub-bound rows. Putting the
                    // punched original back lets the retry re-salvage and
                    // re-restrict from a known state (a `rename` needs no free
                    // space, so it survives the ENOSPC that may have caused `e`).
                    match *rollback {
                        SalvageRollback::Restore(quarantined) => restore_quarantined(
                            folder_fs,
                            quarantined,
                            table_path,
                            config.encryption.as_deref(),
                            config.sync_mode,
                        )?,
                        // Nothing to restore: the source never moved. The output
                        // MUST leave `tables/` though — it carries the
                        // straddling block's sub-bound rows and has no valid
                        // sidecar, so a scan that finds it adopts it
                        // UNRESTRICTED and resurrects exactly the rows the
                        // restriction hid. Removal first; a directory that
                        // refuses it may still accept the rename that moves the
                        // file out of the scan's way, and if neither works the
                        // repair fails on THAT error — an undiscardable output
                        // left in place is the one outcome that corrupts the
                        // retry.
                        SalvageRollback::DiscardOutput => {
                            crate::restrict_bound::remove(folder_fs, table_path, config.sync_mode);
                            if let Err(remove_err) = folder_fs.remove_file(table_path) {
                                log::warn!(
                                    "Failed to discard the unrestrictable salvage output {} \
                                     ({remove_err:?}); moving it out of the scan instead",
                                    table_path.display(),
                                );
                                let (Some(base), Some(name)) = (
                                    table_path.parent(),
                                    table_path.file_name().and_then(|n| n.to_str()),
                                ) else {
                                    return Err(crate::Error::Unrecoverable);
                                };
                                quarantine_file(
                                    folder_fs,
                                    base,
                                    table_path,
                                    name,
                                    config.sync_mode,
                                )?;
                            }
                        }
                    }
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
        if run_end >= data_end {
            // Zeros to the section end. Only a completed relocation when
            // NOTHING was anchored below them: reclaim punches the consumed
            // prefix top-down from the data start, so a zeroed tail that
            // FOLLOWS intact anchored frames cannot be punch geometry — it is
            // destroyed data. Reporting it as consumed would drop a file whose
            // live frames are still referenced (and set aside every table
            // pointing at it); keeping the anchored frontier instead lets the
            // damaged suffix surface through validation, exactly as the same
            // damage would on an unpunched file.
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
            Some(Err(e)) if is_transient_io(&e) => return Err(e),
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
                Some(Err(e)) if is_transient_io(&e) => return Err(e),
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

/// Quarantines a recovered table the blob-dependency stage cannot publish and
/// records the reason. Consumes the handle (released before the move); a
/// failed quarantine aborts the repair — the file must not be both omitted
/// from the manifest and left in place for the next open's orphan sweep.
#[cfg(feature = "std")]
fn set_aside_table(
    table: Table,
    reason: &str,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    let path = (*table.path).clone();
    let (Some(base), Some(name)) = (
        path.parent().map(std::path::Path::to_path_buf),
        path.file_name()
            .and_then(|n| n.to_str())
            .map(str::to_string),
    ) else {
        return Err(crate::Error::Unrecoverable);
    };
    let fs = table.fs.clone();
    drop(table); // release the handle before the quarantine move
    let dest = quarantine_file(&*fs, &base, &path, &name, sync_mode)?;
    unreadable_files.push((path, format!("{reason}; set aside at {}", dest.display())));
    Ok(())
}

/// Same as [`set_aside_table`] for a source that is no longer open: a table
/// left in `tables/` but outside the rebuilt manifest is an orphan the next
/// open DELETES, so every unpublished file must be moved out of the way.
#[cfg(feature = "std")]
fn set_aside_path(
    fs: &dyn crate::fs::Fs,
    base: &std::path::Path,
    path: &std::path::Path,
    reason: &str,
    unreadable_files: &mut Vec<(PathBuf, String)>,
    sync_mode: crate::fs::SyncMode,
) -> crate::Result<()> {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return Err(crate::Error::Unrecoverable);
    };
    let dest = quarantine_file(fs, base, path, name, sync_mode)?;
    unreadable_files.push((
        path.to_path_buf(),
        format!("{reason}; set aside at {}", dest.display()),
    ));
    Ok(())
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
            Err(e) if is_transient_io(&e) => return Err(e),
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
}

/// Discovers the blob files of a KV-separated tree for `repair` by scanning the
/// single `blobs/` folder, with no manifest id list to filter against.
///
/// Mirrors the table scan in [`repair_tree`]: a non-numeric name is quarantined
/// out of `blobs/` (the reopened tree's blob recovery parses every name and
/// would abort on a bad one); a blob file that cannot be checksummed or whose
/// metadata is unreadable is reported and left in place (it reads back as a
/// harmless orphan on the next open). The recovered checksum is the whole-file
/// XXH3-128 digest, identical to the one the blob writer accumulated via
/// `ChecksummedWriter`, since blob files are written strictly sequentially.
fn recover_blob_files(config: &Config) -> crate::Result<BlobRecovery> {
    let blobs_folder = config.path.join(crate::file::BLOBS_FOLDER);
    let mut blob_files: Vec<crate::vlog::BlobFile> = Vec::new();
    let mut unreadable: UnreadableFiles = Vec::new();
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

        // A crashed earlier repair's in-progress blob salvage copy: it is
        // published by an atomic rename, so a surviving one is never
        // referenced and never authoritative. Remove it rather than
        // quarantining it as a foreign name (and re-salvage from the original
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
            // every name in blobs/), so it MUST be moved out of the way. If the
            // quarantine itself fails the bad name stays in place and the tree
            // would not reopen, so fail the repair rather than report a false
            // success.
            let dest = quarantine_file(
                &*config.fs,
                &blobs_folder,
                &blob_path,
                &file_name,
                config.sync_mode,
            )?;
            unreadable.push((
                blob_path,
                format!(
                    "file name is not a blob id; quarantined to {}",
                    dest.display()
                ),
            ));
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

    for (blob_id, blob_path, file_name) in candidates {
        if let Some(kept) = kept_paths.get(&blob_id) {
            // A second directory entry for an already-recovered id. An ALIAS
            // (symlink / case-folded spelling of the SAME physical file) is
            // skipped silently. A DISTINCT physical file must be quarantined:
            // the manifest records one checksum per id, and a stale duplicate
            // left in `blobs/` would race the kept file for reads on the next
            // open (directory iteration order picks the physical file). Same
            // fail-on-quarantine-failure policy as every other set-aside.
            if same_physical_file(&*config.fs, kept, &*config.fs, &blob_path) {
                continue;
            }
            let dest = quarantine_file(
                &*config.fs,
                &blobs_folder,
                &blob_path,
                &file_name,
                config.sync_mode,
            )?;
            unreadable.push((
                blob_path,
                format!(
                    "duplicate of blob file id {blob_id}; quarantined to {}",
                    dest.display()
                ),
            ));
            continue;
        }

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
        // Persistent per-file failure: QUARANTINE before recording unreadable,
        // mirroring the unreadable-SST path. The rebuilt manifest omits this
        // file, so a later `Tree::open` would orphan-clean (DELETE) it from
        // `blobs/`, contradicting the report's promise that an operator can
        // still investigate / salvage it. A FAILED quarantine aborts the whole
        // repair (the file must not be both omitted and left in place).
        let quarantine_unreadable = |blob_path: PathBuf,
                                     file_name: &str,
                                     e: &crate::Error,
                                     unreadable: &mut UnreadableFiles|
         -> crate::Result<()> {
            let dest = quarantine_file(
                &*config.fs,
                &blobs_folder,
                &blob_path,
                file_name,
                config.sync_mode,
            )?;
            unreadable.push((blob_path, format!("{e}; quarantined to {}", dest.display())));
            Ok(())
        };

        let frontier = match derive_blob_frontier(&config.fs, &blob_path, blob_id) {
            Ok(BlobFrontier::Whole) => 0,
            Ok(BlobFrontier::Punched(f)) => f,
            Ok(BlobFrontier::FullyConsumed) => {
                // Every frame is punched away: the relocation that consumed
                // this file completed, only its removal lagged the crash.
                // Finish that drop instead of publishing an empty-suffix
                // handle — whole-file metadata over zero live frames is a
                // file blob GC's stale-byte arithmetic can never retire (its
                // frames are already gone, so the stale count never reaches
                // the recorded totals). No live data is discarded: the walk
                // proved the whole data section reads as zeros.
                log::info!(
                    "blob file {blob_id} at {}: its punch consumed every frame — \
                     completing the relocation's lagged file drop",
                    blob_path.display(),
                );
                match config.fs.remove_file(&blob_path) {
                    Ok(()) => {
                        // Entry durability is best-effort: if the removal's
                        // directory entry resurfaces after a power loss, the
                        // file is outside the rebuilt manifest and the next
                        // open's orphan sweep removes it again.
                        let _ = config
                            .fs
                            .sync_directory_with(&blobs_folder, config.sync_mode);
                    }
                    Err(e) => {
                        // Any removal failure fails the repair: the file is
                        // outside the rebuilt manifest, so the next open
                        // rediscovers it as an orphan and its sweep hits the
                        // same removal failure — reporting success for a tree
                        // that cannot open would be a lie. Quarantine is not
                        // an out (it preserves damaged DATA for the operator;
                        // this file holds none, and a directory refusing the
                        // removal refuses the rename too). Retry once the
                        // filesystem is fixed.
                        let e = crate::Error::from(e);
                        log::error!(
                            "blob file {blob_id} at {}: cannot complete the lagged \
                             drop ({e}); failing the repair",
                            blob_path.display(),
                        );
                        return Err(e);
                    }
                }
                continue;
            }
            // A TRANSIENT read (flaky I/O) is retryable: recording the blob
            // unreadable commits a manifest without the still-in-place file,
            // which the next open's orphan sweep then DELETES — permanent value
            // loss from a one-shot failure. Propagate so a retry re-reads it,
            // mirroring the table-recovery path.
            Err(e) if is_transient_io(&e) => return Err(e),
            Err(e) => {
                quarantine_unreadable(blob_path, &file_name, &e, &mut unreadable)?;
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
                Err(e) if is_transient_io(&e) => return Err(e),
                Err(e) => {
                    quarantine_unreadable(blob_path, &file_name, &e, &mut unreadable)?;
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
                quarantine_unreadable(blob_path, &file_name, &e, &mut unreadable)?;
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
        // crash, because nothing was relocated in place. The original is set
        // aside only AFTER the commit: quarantining it earlier would make a
        // crashed attempt leave SSTs referencing a blob id that no longer
        // exists, and the retry would set those tables aside as
        // unrecoverable.
        let Some(live) = validate_blob_frames(config, &blob_path, blob_id, frontier)? else {
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
                // Nothing recoverable, or a persistent failure: the original is
                // untouched at its canonical path, so preserve it in quarantine
                // and report — exactly like any other unreadable blob.
                Ok(None) => {
                    remove_temp(config, &temp)?;
                    let e = crate::Error::InvalidHeader(
                        "blob value frames failed validation and no record was recoverable",
                    );
                    quarantine_unreadable(blob_path, &file_name, &e, &mut unreadable)?;
                    continue;
                }
                Err(e) if is_transient_io(&e) => {
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
                    quarantine_unreadable(blob_path, &file_name, &e, &mut unreadable)?;
                    continue;
                }
            };

            // Take the fresh id's own (free) name: nothing is displaced, so
            // there is no quarantine to unwind and no window in which a retry
            // could mistake an unverified file for the live blob. Until the
            // manifest names it, the replacement is an unreferenced file that
            // the next open's orphan sweep would remove.
            let new_path = blobs_folder.join(new_id.to_string());
            if let Err(e) = config.fs.rename(&temp, &new_path) {
                // Best-effort on this ERROR path: no manifest is committed,
                // so a stuck temp is swept by the retry's candidate scan.
                let _ = config.fs.remove_file(&temp);
                return Err(e.into());
            }
            config
                .fs
                .sync_directory_with(&blobs_folder, config.sync_mode)?;

            if let Some(p) = &config.recovery_progress {
                p.blob_file_recovered();
            }
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
            Err(e) if is_transient_io(&e) => return Err(e),
            Err(e) => {
                quarantine_unreadable(blob_path, &file_name, &e, &mut unreadable)?;
                continue;
            }
        };

        match crate::vlog::recover_blob_file_from(
            &blob_path, blob_id, checksum, 0, &config.fs, frontier,
        ) {
            Ok(bf) => {
                if let Some(p) = &config.recovery_progress {
                    p.blob_file_recovered();
                }
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
            Err(e) if is_transient_io(&e) => return Err(e),
            Err(e) => {
                quarantine_unreadable(blob_path, &file_name, &e, &mut unreadable)?;
            }
        }
    }

    Ok(BlobRecovery {
        files: blob_files,
        unreadable,
        rewrites,
        frag,
        stale: stale_originals,
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
    /// (`salvage = true`) it is block-salvaged instead of being left out: the
    /// corrupt original is quarantined and a fresh SST holding its recoverable
    /// blocks is written in its place and referenced by the rebuilt manifest.
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
}

/// Completes the set-aside a previous repair committed but could not carry out.
///
/// A repair publishes its replacements by committing the manifest and only THEN
/// moves the superseded originals out of the way; a failure in that last step
/// leaves both on disk with a durable, correct manifest naming the
/// replacements. Rebuilding from that directory would salvage the damaged
/// original again and rewrite its table again while keeping the previous
/// rewrite too, so one history would enter L0 twice — and duplicated merge
/// operands are applied twice on read.
///
/// The committed manifest is what resolves it. It is the tree's own authority
/// on which files count: an `open()` sweeps everything it does not name as an
/// orphan, so this applies the same rule before the scan runs — tables it does
/// not reference are set aside (they hold rows an operator may want), blob
/// files are removed (an unreferenced one holds nothing reachable). No state is
/// carried between runs; this is derived from the durable manifest alone.
///
/// A manifest that does not load cleanly is exactly the case repair exists for,
/// so it is not consulted at all then and the scan rebuilds from everything.
///
/// # Errors
///
/// Propagates a set-aside or removal failure: leaving a superseded file in
/// place is what corrupts the rebuild, so the repair must fail rather than
/// proceed past it.
#[cfg(feature = "std")]
fn sweep_superseded_by_committed_manifest(config: &Config) -> crate::Result<()> {
    let Ok(recovery) = crate::version::recovery::recover(
        &config.path,
        &*config.fs,
        crate::config::ManifestRecoveryMode::AbsoluteConsistency,
        config.encryption.clone(),
    ) else {
        return Ok(());
    };

    let referenced_tables: crate::HashSet<TableId> = recovery
        .table_ids
        .iter()
        .flatten()
        .flatten()
        .map(|t| t.id)
        .collect();
    for (table_base_folder, folder_fs) in config.all_tables_folders() {
        if !folder_fs.exists(&table_base_folder)? {
            continue;
        }
        for dirent in folder_fs.read_dir(&table_base_folder)? {
            // Only files the scan would ADOPT as tables are swept here; a
            // foreign name is left to the scan's own classification.
            let Ok(id) = dirent.file_name.parse::<TableId>() else {
                continue;
            };
            if dirent.is_dir || referenced_tables.contains(&id) {
                continue;
            }
            let dest = quarantine_file(
                &*folder_fs,
                &table_base_folder,
                &dirent.path,
                &dirent.file_name,
                config.sync_mode,
            )?;
            log::info!(
                "repair: table {id} is superseded by the committed manifest; \
                 set aside at {}",
                dest.display(),
            );
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

/// Core repair routine. Separated from the [`Config::repair`] entry point so the
/// logic is testable against a borrowed config.
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

    // Finish the previous run's outstanding cleanup first. See
    // `sweep_superseded_by_committed_manifest`.
    #[cfg(feature = "std")]
    sweep_superseded_by_committed_manifest(config)?;

    // Best recovered copy per table id: a complete recovery beats a lossy salvage,
    // so a duplicate id across aliased / routed table folders keeps only the best
    // (and is never added to two L0 runs). See `keep_best_candidate`.
    let mut recovered_by_id: crate::HashMap<TableId, TableCandidate> = crate::HashMap::default();
    let mut unreadable_files: Vec<(PathBuf, String)> = Vec::new();

    for (table_base_folder, folder_fs) in config.all_tables_folders() {
        if !folder_fs.exists(&table_base_folder)? {
            continue;
        }

        // The two-way half of the resurrection knob: a prior default repair may
        // have set punched-boundless files aside with a resurrectable marker;
        // a resurrection repair returns them to `tables/` FIRST so the scan
        // below recovers them greedily like any other punched file.
        if allow_resurrection {
            reclaim_resurrectable(&*folder_fs, &table_base_folder, config.sync_mode)?;
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

            // Heal artifacts are not table files: `Tree::open` recognizes them
            // and PRESERVES the live `{id}.heal-attest` sidecar (the next scrub
            // reconciles a crashed digest refresh through it). Repair must not
            // quarantine it, or a rebuild that fails before committing the
            // manifest would strand the healed table under its stale pre-heal
            // digest. Match the exact shapes recovery owns (numeric id + heal
            // suffix); a foreign name merely containing the suffix falls through
            // to the id parse below and is quarantined as before.
            let is_sidecar_artifact = file_name
                .strip_suffix(".heal-attest")
                .or_else(|| file_name.strip_suffix(".heal-attest.tmp"))
                // The `.restrict-bound` sidecar (and its crashed `.tmp`) carries a
                // punched SST's restriction bound; repair reads it FOR its SST (via
                // `restrict_bound::read`), so the sidecar file itself is never a
                // table and must not be parsed / quarantined as one.
                .or_else(|| file_name.strip_suffix(".restrict-bound"))
                .or_else(|| file_name.strip_suffix(".restrict-bound.tmp"))
                .is_some_and(|id| id.parse::<TableId>().is_ok())
                // {id}.healtmp-{n}: BOTH the id and the numeric sequence must
                // parse, matching recovery's ownership check. A foreign name like
                // `5.healtmp-backup` is NOT owned (recovery would fail its
                // non-numeric name), so it must fall through to quarantine here.
                || file_name.split_once(".healtmp-").is_some_and(|(id, seq)| {
                    id.parse::<TableId>().is_ok() && seq.parse::<u64>().is_ok()
                });
            if is_sidecar_artifact {
                continue;
            }

            let Ok(table_id) = file_name.parse::<TableId>() else {
                // A non-numeric name cannot be a table id, and `Tree::open`
                // rejects such a file outright (recovery parses every name in
                // `tables/`). Leaving it in place would let repair report
                // success while the tree still cannot reopen, so move it out of
                // `tables/` into a sibling quarantine dir; report where it went.
                // If the quarantine itself fails the bad name stays in place, so
                // fail the repair rather than report a false success.
                let dest = quarantine_file(
                    &*folder_fs,
                    &table_base_folder,
                    &table_path,
                    &file_name,
                    config.sync_mode,
                )?;
                unreadable_files.push((
                    table_path,
                    format!(
                        "file name is not a table id; quarantined to {}",
                        dest.display()
                    ),
                ));
                continue;
            };

            if let Some(p) = &config.recovery_progress {
                p.table_discovered();
            }

            // Skip a duplicate id ONLY when we already hold a COMPLETE copy — a
            // duplicate cannot improve on it. A previously-seen LOSSY salvage does
            // NOT skip: this copy is still evaluated and may supersede it.
            if let Some(existing) = recovered_by_id.get(&table_id).filter(|c| c.complete) {
                // If this path physically ALIASES the retained copy (a symlink /
                // junction / case-insensitive alias resolving to the same directory
                // entry, e.g. two configured folders pointing at one location), it
                // is the SAME file, not a genuine duplicate. Quarantining it would
                // MOVE the kept copy and leave the manifest referencing a missing
                // SST, so skip it IN PLACE.
                if same_physical_file(&*folder_fs, &table_path, &*existing.fs, &existing.path) {
                    continue;
                }
                // A genuine duplicate: quarantine it out of `tables/` so recovery
                // cannot later resolve it instead of the kept copy (the manifest
                // records only id + checksum, not a path).
                let dest = quarantine_file(
                    &*folder_fs,
                    &table_base_folder,
                    &table_path,
                    &file_name,
                    config.sync_mode,
                )?;
                unreadable_files.push((
                    table_path,
                    format!(
                        "duplicate table id; a complete copy is already held; \
                         quarantined to {}",
                        dest.display()
                    ),
                ));
                continue;
            }

            // Hash the file and open it. A non-transient hashing failure (a bad
            // data sector) is FOLDED into the recover Result so the salvage arm
            // below can recover the table's intact blocks, instead of recording
            // the whole table unreadable — which the next open's orphan cleanup
            // would then delete. Table::recover would fail on the same bytes, so
            // skip it; block salvage opens with a placeholder digest and drops
            // only the unreadable blocks. global_seqno = 0: a recovered table's
            // intrinsic sequence numbers are authoritative; there is no
            // ingestion-time translation offset. tree_id = 0 and
            // descriptor_table = None keep the transient open from polluting any
            // shared cache keyed by the real tree id (the tree reopens fresh
            // after repair).
            let recovered = match compute_table_checksum(&*folder_fs, &table_path) {
                Ok(c) => Table::recover(
                    table_path.clone(),
                    crate::Checksum::from_raw(c),
                    0,
                    0,
                    table_id,
                    config.cache.clone(),
                    None,
                    folder_fs.clone(),
                    false,
                    false,
                    config.encryption.clone(),
                    #[cfg(zstd_any)]
                    config.zstd_dictionary.clone(),
                    config.comparator.clone(),
                    #[cfg(feature = "metrics")]
                    Arc::new(crate::metrics::Metrics::default()),
                ),
                // A TRANSIENT read (flaky I/O) while hashing is retryable:
                // recording it unreadable commits a manifest without the
                // still-in-place file, which the next open's orphan cleanup then
                // deletes — permanent loss from a one-shot failure. Propagate it
                // so a retry re-reads the table, mirroring the recover arms below.
                Err(e) if is_transient_io(&e) => return Err(e),
                // A PERSISTENT read failure (a bad data sector, a corrupt trailer)
                // is genuine damage but does not doom the whole table: fold it into
                // the recover Result so the structural-failure salvage arm below
                // recovers the intact blocks (or records it unreadable with salvage
                // off).
                Err(e) => Err(e),
            };

            // Fail closed on a table whose bulk-ingest sequence offset cannot be
            // reconstructed. A bulk-ingested SST stores every entry at LOCAL seqno
            // 0 and relies on a manifest-only `global_seqno` for its effective MVCC
            // ordering; the on-disk seqnos carry no trace of it. The rebuilt
            // manifest hard-codes offset 0, so keeping such a table would make its
            // entries appear OLDER than they are — visible to snapshots that never
            // saw them and sorted into the wrong L0 order. Quarantine it instead of
            // silently corrupting MVCC (see `has_unrecoverable_ingest_offset`).
            if matches!(&recovered, Ok(t) if has_unrecoverable_ingest_offset(
                t.metadata.bulk_ingested,
                t.metadata.item_count,
                t.max_local_seqno(),
            )) {
                drop(recovered); // release the file handle before the move
                match quarantine_file(
                    &*folder_fs,
                    &table_base_folder,
                    &table_path,
                    &file_name,
                    config.sync_mode,
                ) {
                    Ok(dest) => unreadable_files.push((
                        table_path,
                        format!(
                            "bulk-ingest sequence offset cannot be reconstructed from the SST; \
                             quarantined to {}",
                            dest.display()
                        ),
                    )),
                    // A FAILED quarantine aborts the whole repair: a manifest
                    // omitting a still-in-place file would let the next open's
                    // orphan cleanup DELETE the only copy meant to be preserved.
                    Err(e) => return Err(e),
                }
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
                    Err(e) if is_transient_io(&e) => break 'restrict Err(e),
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
                // with salvage off, quarantine the whole table despite its intact live
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
                                break 'restrict table.reopen_restricted(bound);
                            }
                            Err(e) => break 'restrict Err(e),
                            Ok(
                                reason @ (DerivedRestriction::NoLiveData
                                | DerivedRestriction::IrregularPunch),
                            ) => {
                                // FLAG-DEPENDENT set-aside: resurrection would
                                // have kept the readable region, so mark it
                                // reclaimable — a NoLiveData table has nothing
                                // either flag could keep, so it stays unmarked.
                                let resurrectable =
                                    matches!(reason, DerivedRestriction::IrregularPunch);
                                let reason = match reason {
                                    DerivedRestriction::NoLiveData => {
                                        "fully hole-punched SST with no live data"
                                    }
                                    _ => {
                                        "partially punched SST with punch failures and no \
                                         trustworthy bound; the consumed/live boundary is \
                                         unknowable (a resurrection repair reclaims it and \
                                         keeps the readable region)"
                                    }
                                };
                                drop(table);
                                crate::restrict_bound::remove(
                                    &*folder_fs,
                                    &table_path,
                                    config.sync_mode,
                                );
                                match quarantine_file(
                                    &*folder_fs,
                                    &table_base_folder,
                                    &table_path,
                                    &file_name,
                                    config.sync_mode,
                                ) {
                                    Ok(dest) => {
                                        // An unmarked flag-dependent set-aside
                                        // could never be reclaimed: on marker
                                        // failure, undo the set-aside so a
                                        // retry re-runs the classification.
                                        if resurrectable
                                            && let Err(e) = mark_resurrectable(
                                                &*folder_fs,
                                                &dest,
                                                config.sync_mode,
                                            )
                                        {
                                            restore_quarantined(
                                                &*folder_fs,
                                                &dest,
                                                &table_path,
                                                config.encryption.as_deref(),
                                                config.sync_mode,
                                            )?;
                                            return Err(e);
                                        }
                                        unreadable_files.push((
                                            table_path,
                                            format!("{reason}; set aside at {}", dest.display()),
                                        ));
                                    }
                                    Err(e) => return Err(e),
                                }
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
                                table_id,
                                table,
                                true,
                                &folder_fs,
                                &table_base_folder,
                                &table_path,
                                &file_name,
                                config.sync_mode,
                            )?;
                        }
                        RepairKeepDecision::Quarantine(reason) => {
                            drop(table);
                            // Quarantine (not leave-in-place): a later
                            // `Tree::open` orphan-cleans table files the
                            // rebuilt manifest does not reference, so an
                            // unquarantined original would be DELETED.
                            match quarantine_file(
                                &*folder_fs,
                                &table_base_folder,
                                &table_path,
                                &file_name,
                                config.sync_mode,
                            ) {
                                Ok(dest) => unreadable_files.push((
                                    table_path,
                                    format!("{reason}; quarantined to {}", dest.display()),
                                )),
                                // A FAILED quarantine aborts the whole repair:
                                // installing a manifest that omits the
                                // still-in-place file would let the next
                                // open's orphan cleanup DELETE the only copy
                                // set aside for later inspection.
                                Err(e) => return Err(e),
                            }
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
                            // discarded to a dead-end quarantine.
                            let restrict_bound = table.restrict_lower_bound().cloned();
                            drop(table);
                            // Quarantine BEFORE salvage, aborting the repair
                            // on failure: a manifest omitting a still-in-place
                            // file would let the next open's orphan cleanup
                            // delete the only copy. A salvage error AFTER a
                            // successful move is recorded instead: the
                            // original is safely preserved in quarantine.
                            let quarantined = quarantine_file(
                                &*folder_fs,
                                &table_base_folder,
                                &table_path,
                                &file_name,
                                config.sync_mode,
                            )?;
                            match try_salvage_table(
                                config,
                                &folder_fs,
                                allow_resurrection,
                                TableSalvage {
                                    quarantined: &quarantined,
                                    table_path: &table_path,
                                    table_id,
                                    // The bound (when any) comes from the
                                    // restricted view and is re-imposed below,
                                    // so a punched source is never ambiguous
                                    // on this arm.
                                    reject_punched_without_bound: false,
                                    blob_rewrite: None,
                                    output_id: None,
                                },
                            ) {
                                Ok(SalvageOutcome::Salvaged(salvaged)) => {
                                    // Re-impose the tight-space restriction on the
                                    // salvaged output (fail-closed unless resurrection
                                    // is on), the shared path both salvage arms use.
                                    let table = restrict_salvaged_output(
                                        &*folder_fs,
                                        config,
                                        &table_path,
                                        &SalvageRollback::Restore(&quarantined),
                                        salvaged,
                                        restrict_bound.clone(),
                                        allow_resurrection,
                                    )?;
                                    record_best(
                                        &mut recovered_by_id,
                                        &mut unreadable_files,
                                        table_id,
                                        table,
                                        false,
                                        &folder_fs,
                                        &table_base_folder,
                                        &table_path,
                                        &file_name,
                                        config.sync_mode,
                                    )?;
                                }
                                Ok(SalvageOutcome::Unusable | SalvageOutcome::PunchedBoundLost) => {
                                    unreadable_files.push((
                                        table_path,
                                        "verify found corrupt blocks; nothing salvageable"
                                            .to_string(),
                                    ));
                                }
                                Err(salvage_err) => {
                                    // A TRANSIENT I/O salvage failure is retryable:
                                    // committing a manifest that omits the table
                                    // would lose it permanently (the original is in
                                    // quarantine, which the next repair won't
                                    // rediscover). Restore the original to its path
                                    // and abort the whole repair so a retry can
                                    // salvage it. A STRUCTURAL failure is genuine
                                    // unsalvageability: record it (the original
                                    // stays set aside for inspection).
                                    if is_transient_io(&salvage_err) {
                                        restore_quarantined(
                                            &*folder_fs,
                                            &quarantined,
                                            &table_path,
                                            config.encryption.as_deref(),
                                            config.sync_mode,
                                        )?;
                                        return Err(salvage_err);
                                    }
                                    unreadable_files.push((
                                        table_path,
                                        format!(
                                            "verify found corrupt blocks; salvage failed \
                                             ({salvage_err})"
                                        ),
                                    ));
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
                                table_id,
                                table,
                                true,
                                &folder_fs,
                                &table_base_folder,
                                &table_path,
                                &file_name,
                                config.sync_mode,
                            )?;
                        }
                        // `Salvage` is unreachable with the flag off, but a
                        // defensive fallthrough beats a panic in a repair path.
                        decision @ (RepairKeepDecision::Quarantine(_)
                        | RepairKeepDecision::Salvage) => {
                            let reason = match decision {
                                RepairKeepDecision::Quarantine(reason) => reason,
                                _ => {
                                    "verification found corrupt data blocks; run a \
                                     salvage-enabled repair to rewrite the readable blocks"
                                }
                            };
                            drop(table);
                            // Quarantine (not leave-in-place): a later
                            // `Tree::open` orphan-cleans table files the rebuilt
                            // manifest does not reference, so an unquarantined
                            // original would be DELETED. A failed quarantine
                            // aborts the whole repair for the same reason as the
                            // salvage arm.
                            match quarantine_file(
                                &*folder_fs,
                                &table_base_folder,
                                &table_path,
                                &file_name,
                                config.sync_mode,
                            ) {
                                Ok(dest) => unreadable_files.push((
                                    table_path,
                                    format!("{reason}; quarantined to {}", dest.display()),
                                )),
                                Err(e) => return Err(e),
                            }
                        }
                    }
                }
                Err(e) if salvage => {
                    // A TRANSIENT recovery failure (Io) is retryable and must NOT
                    // be routed through salvage: quarantining the healthy SST and
                    // salvaging it would, for a range-tombstone table, fail
                    // deterministically with FeatureUnsupported (a non-Io error
                    // recorded as unsalvageable), committing a manifest without the
                    // table — turning a one-shot read failure into permanent loss.
                    // Propagate the I/O error BEFORE moving the file so a retry
                    // re-recovers it, mirroring the verification / salvage-error
                    // paths.
                    if is_transient_io(&e) {
                        return Err(e);
                    }
                    // A tight-space-punched SST that fails whole-file recovery still
                    // carries its restriction in the `.restrict-bound` sidecar, but
                    // recovery produced no `Table` to read the bound from. Read it
                    // directly BEFORE quarantining moves the SST (the sidecar is a
                    // sibling file, so its bound is captured while both are in place),
                    // and re-impose it on the salvaged output below, exactly as the
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
                        Err(read_err) if is_transient_io(&read_err) => return Err(read_err),
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
                        match quarantine_file(
                            &*folder_fs,
                            &table_base_folder,
                            &table_path,
                            &file_name,
                            config.sync_mode,
                        ) {
                            Ok(dest) => {
                                // FLAG-DEPENDENT set-aside: mark it so a
                                // resurrection repair reclaims and salvages it.
                                // On marker failure, undo the set-aside so a
                                // retry re-runs the classification instead of
                                // leaving an unreclaimable file.
                                if let Err(e) =
                                    mark_resurrectable(&*folder_fs, &dest, config.sync_mode)
                                {
                                    restore_quarantined(
                                        &*folder_fs,
                                        &dest,
                                        &table_path,
                                        config.encryption.as_deref(),
                                        config.sync_mode,
                                    )?;
                                    return Err(e);
                                }
                                crate::restrict_bound::remove(
                                    &*folder_fs,
                                    &table_path,
                                    config.sync_mode,
                                );
                                unreadable_files.push((
                                    table_path,
                                    format!(
                                        "punched SST with no recoverable restriction bound \
                                         (missing / corrupt sidecar and failed recovery); set \
                                         aside to {} (a resurrection repair reclaims it)",
                                        dest.display()
                                    ),
                                ));
                            }
                            Err(e) => return Err(e),
                        }
                        continue;
                    }
                    // Whole-file recovery failed structurally; try block-level
                    // salvage: the corrupt original is quarantined and a fresh SST
                    // holding its recoverable blocks is written in its place. A
                    // FAILED quarantine aborts the repair (the `?`): a manifest
                    // omitting a still-in-place file would let the next open's
                    // orphan cleanup delete the only copy.
                    let quarantined = quarantine_file(
                        &*folder_fs,
                        &table_base_folder,
                        &table_path,
                        &file_name,
                        config.sync_mode,
                    )?;
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
                            quarantined: &quarantined,
                            table_path: &table_path,
                            table_id,
                            reject_punched_without_bound: reject_punched,
                            blob_rewrite: None,
                            output_id: None,
                        },
                    ) {
                        Ok(SalvageOutcome::Salvaged(salvaged)) => {
                            // Re-impose the tight-space restriction on the salvaged
                            // output (fail-closed unless resurrection is on), the
                            // shared path both salvage arms use.
                            let table = restrict_salvaged_output(
                                &*folder_fs,
                                config,
                                &table_path,
                                &SalvageRollback::Restore(&quarantined),
                                salvaged,
                                restrict_bound,
                                allow_resurrection,
                            )?;
                            record_best(
                                &mut recovered_by_id,
                                &mut unreadable_files,
                                table_id,
                                table,
                                false,
                                &folder_fs,
                                &table_base_folder,
                                &table_path,
                                &file_name,
                                config.sync_mode,
                            )?;
                        }
                        Ok(SalvageOutcome::Unusable) => {
                            unreadable_files.push((
                                table_path,
                                format!(
                                    "unrecoverable ({e}); original quarantined, nothing salvageable"
                                ),
                            ));
                        }
                        Ok(SalvageOutcome::PunchedBoundLost) => {
                            // FLAG-DEPENDENT set-aside: mark the quarantined
                            // ORIGINAL (the punched source; the rejected
                            // salvage byproduct stays unmarked) so a
                            // resurrection repair reclaims and re-salvages it.
                            // On marker failure, undo the set-aside so a retry
                            // re-runs the classification.
                            if let Err(marker_err) =
                                mark_resurrectable(&*folder_fs, &quarantined, config.sync_mode)
                            {
                                restore_quarantined(
                                    &*folder_fs,
                                    &quarantined,
                                    &table_path,
                                    config.encryption.as_deref(),
                                    config.sync_mode,
                                )?;
                                return Err(marker_err);
                            }
                            unreadable_files.push((
                                table_path,
                                format!(
                                    "punched SST with no recoverable restriction bound \
                                     (missing / corrupt sidecar and failed recovery, punched \
                                     extents found during salvage); set aside ({e}); a \
                                     resurrection repair reclaims it"
                                ),
                            ));
                        }
                        Err(salvage_err) => {
                            // Transient I/O salvage failure: restore the original
                            // and abort so a retry can recover it (see the sibling
                            // salvage arm above). A structural failure is recorded.
                            if is_transient_io(&salvage_err) {
                                restore_quarantined(
                                    &*folder_fs,
                                    &quarantined,
                                    &table_path,
                                    config.encryption.as_deref(),
                                    config.sync_mode,
                                )?;
                                return Err(salvage_err);
                            }
                            unreadable_files.push((
                                table_path,
                                format!("recovery failed ({e}); salvage failed ({salvage_err})"),
                            ));
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
                    if is_transient_io(&e) {
                        return Err(e);
                    }
                    // QUARANTINE before recording unreadable: the rebuilt manifest
                    // omits this file, so a later `Tree::open` would orphan-clean
                    // (DELETE) it from `tables/`, contradicting the report's promise
                    // that an operator can still investigate / recover it. Moving it
                    // to the repair quarantine preserves it. A FAILED quarantine
                    // aborts the whole repair (the file must not be both omitted and
                    // left in place).
                    match quarantine_file(
                        &*folder_fs,
                        &table_base_folder,
                        &table_path,
                        &file_name,
                        config.sync_mode,
                    ) {
                        Ok(dest) => unreadable_files.push((
                            table_path,
                            format!("{e}; quarantined to {}", dest.display()),
                        )),
                        Err(qe) => return Err(qe),
                    }
                }
            }
        }
    }

    // Collect the best copy per id, carrying each candidate's completeness so
    // `salvaged` can be derived from the tables that actually make the
    // manifest — after the blob-dependency filtering below, not before (a
    // salvaged table quarantined for an unrecoverable blob dependency must not
    // count, or `salvaged` could exceed `recovered`). A lossy copy superseded
    // by a complete duplicate is likewise already gone from the candidates.
    let mut recovered_tables: Vec<(Table, bool)> = recovered_by_id
        .into_values()
        .map(|c| (c.table, c.complete))
        .collect();

    // Newest first: higher sequence number nearer the L0 head, matching the
    // ordering the merge reader expects for its newest-run-first short-circuit.
    // Tie-broken by DESCENDING id: two tables can share a highest seqno (a
    // caller may reuse an explicit seqno across separate flushed batches), and
    // sorting by seqno alone would leave their order to the directory scan and
    // the candidate map's iteration — so which run answers first would vary
    // between runs of the same repair over the same files. Ids are allocated in
    // increasing order, so the higher id is the later table and belongs nearer
    // the L0 head.
    recovered_tables.sort_by_key(|(t, _)| {
        (
            std::cmp::Reverse(t.get_highest_seqno()),
            std::cmp::Reverse(t.id()),
        )
    });

    // Fresh ids for copies published BESIDE their source (the blob-handle
    // rewrite). Starts one past the highest id ANY artifact in the table
    // folders claims, so a new name can never displace a file this repair still
    // has to read — and never inherits the meaning of one it does not.
    // Sidecars count: the scan skips them, so a `.restrict-bound` whose table
    // is gone still names an id, and publishing an UNRESTRICTED rewrite under
    // it would make a later manifest-loss repair match that sidecar by id and
    // restrict the replacement at an unrelated bound, dropping its prefix.
    // Clearing the sidecar is best-effort; not taking the id is not.
    // `None` = exhausted, which fails the repair at the point of use rather
    // than reusing an id.
    let mut highest_claimed_id: Option<TableId> =
        recovered_tables.iter().map(|(t, _)| t.id()).max();
    for (table_base_folder, folder_fs) in config.all_tables_folders() {
        if !folder_fs.exists(&table_base_folder)? {
            continue;
        }
        for dirent in folder_fs.read_dir(&table_base_folder)? {
            let claimed = crate::restrict_bound::table_id_from_sidecar_name(&dirent.file_name)
                .or_else(|| dirent.file_name.parse::<TableId>().ok());
            if let Some(id) = claimed {
                highest_claimed_id = Some(highest_claimed_id.map_or(id, |max| max.max(id)));
            }
        }
    }
    let mut next_table_id: Option<TableId> =
        highest_claimed_id.map_or(Some(0), |max| max.checked_add(1));
    // Sources whose rewritten copy is in the rebuilt manifest. Set aside only
    // after `persist_version` — until then they are the only copy of their
    // rows, and a crash must leave them where a retry looks for them.
    // Each entry carries the source's OWN backend: a per-level route stores its
    // tables through a different `Fs`, where the primary one cannot see them.
    let mut stale_table_originals: Vec<(Arc<dyn crate::fs::Fs>, PathBuf, String)> = Vec::new();

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
    let mut blob_files_salvaged: Vec<(PathBuf, String)> = Vec::new();
    let mut blob_frag = crate::blob_tree::FragmentationMap::default();
    // Damaged blob originals whose replacement is in the rebuilt manifest.
    // Set aside only after `persist_version` — see `BlobRecovery::stale`.
    let mut stale_blob_originals: Vec<(PathBuf, String)> = Vec::new();
    // Blob files the rebuilt manifest omits because nothing references them.
    // Removed only after `persist_version`, for the same reason.
    let mut unreferenced_blob_files: Vec<PathBuf> = Vec::new();
    let (tree_type, mut blob_file_list) = if config.kv_separation_opts.is_some() {
        let recovery = recover_blob_files(config)?;
        unreadable_files.extend(recovery.unreadable);
        blob_rewrites = recovery.rewrites;
        blob_frag = recovery.frag;
        stale_blob_originals = recovery.stale;
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
        let mut kept: Vec<(Table, bool)> = Vec::with_capacity(recovered_tables.len());
        for (table, complete) in recovered_tables {
            // One reference read drives everything below: the missing-id check
            // and the rewrite decision.
            let links = match table.list_blob_file_references() {
                Ok(links) => links,
                Err(e) if is_transient_io(&e) => return Err(e),
                Err(e) => {
                    set_aside_table(
                        table,
                        &format!("blob-file reference list unreadable ({e})"),
                        &mut unreadable_files,
                        config.sync_mode,
                    )?;
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
                    config.sync_mode,
                )?;
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
                        Err(e) if is_transient_io(&e) => return Err(e),
                        Err(e) => {
                            set_aside_table(
                                table,
                                &format!("blob handles unreadable ({e})"),
                                &mut unreadable_files,
                                config.sync_mode,
                            )?;
                            continue;
                        }
                    }
                }
            }
            if !needs_rewrite {
                kept.push((table, complete));
                continue;
            }
            // Rewrite through the salvage pipeline: re-emit every entry with
            // re-targeted handles, dropping only entries whose blob record no
            // longer exists. The rewritten table counts as salvaged (its
            // content may be lossy relative to the original).
            //
            // The copy is published under a FRESH id beside its untouched
            // source, and the source is set aside only AFTER the manifest
            // commit. Moving it first would open a window in which a crash
            // leaves the only copy in `repair-quarantine/`, which the retry
            // (it scans `tables/`) never looks at: the table's keys would
            // silently vanish from the rebuilt manifest. Nothing has to be
            // carried across runs to make this repeatable — the rewrite reads
            // an untouched source and writes a separate output, so a retry
            // simply redoes the same deterministic rewrite from the same
            // inputs.
            let source_id = table.id();
            let path = (*table.path).clone();
            let Some(base) = path.parent().map(std::path::Path::to_path_buf) else {
                return Err(crate::Error::Unrecoverable);
            };
            let Some(new_id) = next_table_id else {
                log::error!("repair: table ids exhausted; cannot rewrite blob handles");
                return Err(crate::Error::Unrecoverable);
            };
            next_table_id = new_id.checked_add(1);
            let output_path = base.join(new_id.to_string());
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
                    quarantined: &path,
                    table_path: &output_path,
                    table_id: source_id,
                    reject_punched_without_bound: false,
                    blob_rewrite: Some(Arc::clone(&blob_rewrites)),
                    output_id: Some(new_id),
                },
            ) {
                Ok(SalvageOutcome::Salvaged(rewritten)) => {
                    let rewritten = restrict_salvaged_output(
                        &*fs,
                        config,
                        &output_path,
                        &SalvageRollback::DiscardOutput,
                        rewritten,
                        restrict_bound,
                        allow_resurrection,
                    )?;
                    kept.push((rewritten, false));
                    stale_table_originals.push((
                        Arc::clone(&fs),
                        path,
                        format!("blob handles rewritten into table {new_id}"),
                    ));
                }
                Ok(SalvageOutcome::Unusable | SalvageOutcome::PunchedBoundLost) => {
                    set_aside_path(
                        &*fs,
                        &base,
                        &path,
                        "blob-handle rewrite produced nothing",
                        &mut unreadable_files,
                        config.sync_mode,
                    )?;
                }
                // A retryable failure leaves the source where it was found, so
                // the retry re-derives the same rewrite from it; nothing to
                // restore.
                Err(e) if is_transient_io(&e) => return Err(e),
                Err(e) => {
                    set_aside_path(
                        &*fs,
                        &base,
                        &path,
                        &format!("blob-handle rewrite failed ({e})"),
                        &mut unreadable_files,
                        config.sync_mode,
                    )?;
                }
            }
        }
        recovered_tables = kept;

        // Only blob files a surviving table REFERENCES go into the manifest.
        // An unreferenced one holds no reachable value by definition, and
        // admitting it would strand it there forever: repair cannot rebuild
        // fragmentation stats from a directory scan, and blob GC retires a
        // file only once its recorded stale bytes reach the totals it never
        // gets. That also settles what a crashed earlier attempt leaves
        // behind — a fully written salvage replacement that the crash kept
        // out of any manifest — which would otherwise be admitted as an
        // ordinary blob and pin a whole copy per failed attempt.
        let mut referenced: crate::HashSet<crate::vlog::BlobFileId> = crate::HashSet::default();
        for (table, _) in &recovered_tables {
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

    // `salvaged` is a subset of `recovered`, so derive it from the tables that
    // survived every filter above. The live progress counter follows the same
    // rule: a candidate displaced by deduplication or dropped by dependency
    // filtering never counts, so the snapshot cannot claim more tables than
    // the rebuilt manifest holds.
    let salvaged = recovered_tables
        .iter()
        .filter(|(_, complete)| !complete)
        .count();
    let recovered_tables: Vec<Table> = recovered_tables.into_iter().map(|(t, _)| t).collect();
    if let Some(p) = &config.recovery_progress {
        p.tables_recovered_add(recovered_tables.len() as u64);
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
    let version_id = match highest_existing_version_id(&*config.fs, &config.path)? {
        Some(max) => max.checked_add(1).ok_or(crate::Error::Unrecoverable)?,
        None => 0,
    };

    // Seeded with the punched prefixes' garbage: those frames can never be
    // observed by a future compaction, so an empty map would pin every punched
    // file's stale count below its whole-file metadata totals forever.
    let version = Version::from_levels(version_id, tree_type, levels, blob_file_list, blob_frag);

    // Persist with the tree's own runtime config, not defaults: it drives the
    // manifest framing (checksum algorithm, page ECC, footer mirror, manifest
    // KV checksums), so defaulting it would rewrite a recovered tree's manifest
    // metadata to settings it never used. The last live runtime config died with
    // the lost manifest; the config supplied to `repair` is the authoritative
    // replacement.
    crate::version::persist_version(
        &config.path,
        &version,
        config.comparator.name(),
        &*config.fs,
        Arc::new(config.initial_runtime_config.clone()),
        config.encryption.clone(),
        config.sync_mode,
    )?;

    // A rebuilt snapshot is a complete generation on its own. Sweep every stale
    // edit log so nothing is replayed on top of it: the lost manifest's
    // generation left its log under an OLDER snapshot id (the rebuilt snapshot
    // uses `max(v*) + 1`), so removing only `edits-{version_id}` would normally
    // miss it. Drop all `edits-*` — none belong to the fresh snapshot.
    for dirent in config.fs.read_dir(&config.path)? {
        if dirent.is_dir || !dirent.file_name.starts_with("edits-") {
            continue;
        }
        match config.fs.remove_file(&dirent.path) {
            Ok(()) => {}
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }

    // POST-COMMIT cleanup. The manifest is durable and names the salvaged
    // replacements, so the damaged originals they superseded are now
    // unreferenced and can be set aside for the operator. This runs AFTER the
    // commit on purpose: until then those originals are what the tables the
    // rewrite had not reached still point at, so moving one earlier would let
    // a failed repair leave a table referencing a blob id that no longer
    // exists — and the retry would set that table aside as unrecoverable.
    //
    // NOT best-effort. A superseded original left in `blobs/` is outside the
    // committed manifest, so the next open classifies it as an orphan and
    // removes it — and if the directory refuses the removal the way it just
    // refused the rename, that open FAILS. Reporting a successful repair for a
    // tree that will not open is the one outcome recovery must never produce,
    // so the failure propagates: the manifest is already durable, and a retry
    // once the filesystem is fixed finishes the set-aside on the same inputs.
    for (path, note) in stale_blob_originals {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let blobs_folder = config.path.join(crate::file::BLOBS_FOLDER);
        match quarantine_file(&*config.fs, &blobs_folder, &path, name, config.sync_mode) {
            Ok(dest) => blob_files_salvaged.push((
                path,
                format!("{note}; original preserved at {}", dest.display()),
            )),
            Err(e) => {
                log::error!(
                    "repair: cannot set aside the superseded blob original {} ({e}); \
                     failing the repair — left in blobs/ it is an orphan the next \
                     open must remove, and that removal would hit the same error",
                    path.display(),
                );
                return Err(e);
            }
        }
    }

    // Same rule for the SST sources whose rewritten copy the manifest now
    // names: they are unreferenced from here on, and a file left in `tables/`
    // outside the manifest is an orphan the next open DELETES — so the
    // set-aside is what preserves them, and a failure to perform it fails the
    // repair rather than reporting success over a tree that loses them.
    for (fs, path, note) in stale_table_originals {
        let (Some(base), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str()))
        else {
            return Err(crate::Error::Unrecoverable);
        };
        // Through the SOURCE's backend: a routed table lives in a namespace the
        // primary `Fs` cannot see, where this rename would either fail
        // `NotFound` (after the manifest already committed) or, worse, move a
        // same-named file from the primary namespace.
        match quarantine_file(&*fs, base, &path, name, config.sync_mode) {
            Ok(dest) => log::info!(
                "repair: {note}; superseded source preserved at {}",
                dest.display(),
            ),
            Err(e) => {
                log::error!(
                    "repair: cannot set aside the superseded table {} ({e}); \
                     failing the repair — left in tables/ it is an orphan the next \
                     open must remove, and that removal would hit the same error",
                    path.display(),
                );
                return Err(e);
            }
        }
    }

    // Same rule for the blob files the manifest omits because nothing
    // references them: their data is unreachable, so there is nothing to
    // preserve — but leaving one behind hands the next open an orphan to
    // sweep, and a sweep that fails there fails the open. Remove them here and
    // propagate, so a repair that reports success leaves an openable tree.
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
                return Err(e.into());
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

    Ok(RepairReport {
        recovered,
        salvaged,
        unreadable: unreadable_files.len(),
        unreadable_files,
        blob_files_salvaged,
        method: "all-to-L0 with sequence-number ordering",
        warnings,
    })
}

#[cfg(test)]
mod tests;
