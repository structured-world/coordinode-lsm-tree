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

    /// Description of the level-assignment strategy used (constant for now;
    /// surfaced so the report is self-explanatory and forward-compatible).
    pub method: &'static str,

    /// Operator-facing caveats about the rebuilt state.
    pub warnings: Vec<&'static str>,
}

/// Streams `path` start to end through XXH3-128, matching the digest a normal
/// table write accumulates via `ChecksummedWriter`.
pub(crate) fn compute_table_checksum(
    fs: &dyn crate::fs::Fs,
    path: &std::path::Path,
) -> crate::Result<u128> {
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break; // EOF
        }
        // `get(..n)` rather than `buf[..n]` to satisfy
        // `deny(clippy::indexing_slicing)`; `Read::read` guarantees
        // `n <= buf.len()`, so this slice is always present.
        let Some(chunk) = buf.get(..n) else { break };
        hasher.update(chunk);
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
    fs.create_dir_all(&quarantine_dir)?;
    let dest = quarantine_dir.join(file_name);
    fs.rename(src, &dest)?;
    // A rename is durable only once BOTH affected directory entries are on
    // disk: the destination gains the file and the source loses it. Without
    // syncing both, a power loss after repair returns can drop the destination
    // entry or restore the source under `tables/`, and the next open's orphan
    // cleanup then deletes the only copy meant for manual recovery.
    if let Some(src_dir) = src.parent() {
        fs.sync_directory_with(src_dir, sync_mode)?;
    }
    fs.sync_directory_with(&quarantine_dir, sync_mode)?;
    Ok(dest)
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
fn block_verify_verdict(
    config: &Config,
    folder_fs: &Arc<dyn crate::fs::Fs>,
    table_path: &std::path::Path,
    table: &Table,
) -> BlockVerifyVerdict {
    let report = crate::verify::verify_sst_file_with_context(
        &**folder_fs,
        table_path,
        config.encryption.as_deref(),
        // Repair KNOWS the durable id (recovery already cross-checked it
        // against the file name), so the verify probe enforces the same meta
        // id check — a checksum-clean forged tail meta falls back to the
        // intact MID mirror instead of dictating a forged ECC descriptor.
        Some(table.metadata.id),
    );
    // A non-parity error is corruption regardless of any warnings.
    if !report
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
    } else if table.verify_kv_checksums().is_err() {
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
    } else if table.verify_blob_links().is_err() {
        // Same reasoning for the blob-link list: the section carries no
        // per-section checksum, so the walk can only validate its SHAPE — a
        // flipped blob id passes it. Cross-check against the table's own
        // indirection entries (a no-op without the section); a mismatch is
        // corruption, and salvage derives the links from the recovered
        // indirections rather than copying the forged list.
        BlockVerifyVerdict::Corrupt
    } else if table.verify_tli_mirrors().is_err() {
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
    } else if table.verify_seqno_bounds().is_err() {
        // The seqno_bounds block is checksum-clean to the walk even when
        // its payload was re-stamped to another structurally valid map, and
        // scan_since_seqno trusts it to SKIP blocks — keeping the table
        // would silently omit live entries from every seqno-scoped scan.
        // Salvage re-derives the bounds from the re-emitted entries.
        BlockVerifyVerdict::Corrupt
    } else if table.verify_block_entry_counts().is_err() {
        // The out-of-band walk verifies only the outer frame and the per-KV
        // gate is a no-op without footers, so a checksum-clean block whose
        // trailer declares more entries than it decodes (a valid prefix, a
        // malformed tail) grades clean while a later scan silently omits the
        // tail. Full-decode every block; a count mismatch routes the table
        // through salvage (whose row path drops the under-decoding block).
        BlockVerifyVerdict::Corrupt
    } else if table.verify_zone_map().is_err() {
        // A checksum-clean zone_map re-stamped to another structurally valid
        // map would let a predicate scan skip blocks its forged min/max
        // excludes, silently omitting matching rows. Diverging stats are
        // corruption; salvage re-derives the zone map from the re-emitted
        // blocks.
        BlockVerifyVerdict::Corrupt
    } else if table.verify_locator().is_err() {
        // A checksum-clean locator re-stamped to resolve a key to a block
        // other than its newest-version block would make point_read return a
        // stale value without falling back to the sorted index. A mapping
        // that disagrees with the decoded blocks is corruption; salvage
        // rebuilds the locator from the re-emitted entries.
        BlockVerifyVerdict::Corrupt
    } else if table
        .verify_filter(config.prefix_extractor.as_ref())
        .is_err()
    {
        // A checksum-clean filter re-stamped to another parseable filter
        // makes check_bloom silently skip point reads for any key turned
        // into a false negative. An existing key the filter reports as
        // definitely absent is corruption; salvage rebuilds the filter from
        // the re-emitted keys.
        BlockVerifyVerdict::Corrupt
    } else if table.verify_block_layout().is_err() {
        // A checksum-clean block_layout re-stamped to another structurally
        // valid boundary set mis-maps the partial range-read path's
        // decompression bounds, silently omitting keys. Boundaries that
        // disagree with the frames' actual inner blocks are corruption;
        // salvage re-derives the layout when re-encoding.
        BlockVerifyVerdict::Corrupt
    } else if table.verify_point_read_reachability().is_err() {
        // A checksum-clean embedded hash / binary index re-stamped to hide a
        // key (a MARKER_FREE bucket, a misdirected offset) makes point_read
        // miss existing data. Keys the block decodes but point_read cannot
        // retrieve are corruption; salvage re-emits the block with fresh
        // indexes.
        BlockVerifyVerdict::Corrupt
    } else if table.verify_metadata_bounds().is_err() {
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
    }
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
enum RepairKeepDecision {
    /// The table joins the rebuilt manifest as-is.
    Keep,
    /// The table is routed through block salvage (quarantine + rewrite).
    Salvage,
    /// The table can be neither trusted nor faithfully salvaged: it is
    /// QUARANTINED (protecting it from the orphan cleanup a later open runs)
    /// and reported unreadable with this reason.
    Quarantine(&'static str),
}

/// Whether the on-disk TOC catalogue could HIDE a deletion section — see
/// [`crate::verify::toc_may_hide_deletion_section`]. A read failure grades
/// `true` (fail closed): if the catalogue cannot be re-read to prove no section
/// is hidden, salvage must not trust the parsed absence of deletion metadata.
fn toc_may_hide_deletions(
    folder_fs: &Arc<dyn crate::fs::Fs>,
    table_path: &std::path::Path,
) -> bool {
    let Ok(mut file) = folder_fs.open(table_path, &crate::fs::FsOpenOptions::new().read(true))
    else {
        return true;
    };
    match crate::sfa::Reader::from_reader(&mut file) {
        Ok(reader) => crate::verify::toc_may_hide_deletion_section(reader.toc(), reader.toc_pos()),
        Err(_) => true,
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
/// handle-based check) and cannot be re-emitted — is quarantined for the
/// operator instead of riding unverified into the rebuilt manifest.
fn verify_keep_decision(
    config: &Config,
    folder_fs: &Arc<dyn crate::fs::Fs>,
    table_path: &std::path::Path,
    table: &Table,
) -> RepairKeepDecision {
    match block_verify_verdict(config, folder_fs, table_path, table) {
        BlockVerifyVerdict::Clean => RepairKeepDecision::Keep,
        BlockVerifyVerdict::Corrupt => {
            // A `Corrupt` verdict from a catalogue that could HIDE a deletion
            // section (an omitted / renamed / shadowed `range_tombstones` or
            // `delete_bitmap`) must NOT salvage: the positional salvage walk
            // reopens the same forged TOC, sees no deletion section in the
            // parsed state, and re-emits the suppressed rows as LIVE —
            // resurrecting data the deletion metadata masked. The salvage-side
            // resurrection guard only inspects the PARSED deletion state, which
            // the concealment defeats, so the refusal has to happen here.
            // Quarantine for manual recovery unless the tiling proves no
            // section is hidden.
            if toc_may_hide_deletions(folder_fs, table_path) {
                RepairKeepDecision::Quarantine(
                    "TOC corruption may hide deletion metadata (range tombstones \
                     / delete bitmap); salvage would reopen the same forged \
                     catalogue and resurrect suppressed rows — quarantined for \
                     manual recovery",
                )
            } else {
                RepairKeepDecision::Salvage
            }
        }
        BlockVerifyVerdict::DegradedButReadable => {
            if table.range_tombstones().is_empty() {
                RepairKeepDecision::Salvage
            } else {
                log::warn!(
                    "table {} at {}: every payload verified clean but its ECC is \
                     partially uncheckable or rotted, and salvage cannot re-emit its \
                     range tombstones — keeping the table as-is; recompact to re-stamp \
                     it under fresh, verifiable parity",
                    table.metadata.id,
                    table_path.display(),
                );
                RepairKeepDecision::Keep
            }
        }
        BlockVerifyVerdict::DegradedUnscanned => {
            if table.range_tombstones().is_empty() {
                RepairKeepDecision::Salvage
            } else {
                RepairKeepDecision::Quarantine(
                    "ECC descriptor unrecognized (the block walk cannot verify the \
                     table) and salvage cannot re-emit its range tombstones; \
                     quarantined for manual recovery — recompact it under a \
                     supported scheme",
                )
            }
        }
    }
}

fn try_salvage_table(
    config: &Config,
    fs: &Arc<dyn crate::fs::Fs>,
    // The already-quarantined corrupt original (the salvage source). The
    // CALLER performs the quarantine move and aborts the whole repair when it
    // fails — a manifest omitting a still-in-place file would let the next
    // open's orphan cleanup delete the only copy. An error returned from HERE
    // is therefore always post-quarantine: the original is safely preserved,
    // and the caller records the failure instead of aborting.
    quarantined: &std::path::Path,
    table_path: &std::path::Path,
    table_id: TableId,
) -> crate::Result<Option<Table>> {
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
            // Automated repair never silently resurrects deleted rows: a
            // delete-bearing SST whose bitmap cannot be applied fails salvage
            // (the corrupt original stays in quarantine). An operator who
            // accepts the degradation salvages it explicitly via the opt-in.
            allow_delete_resurrection: false,
            // The recovered SST is persisted at the tree's configured
            // durability, matching the manifest rebuilt around it.
            sync_mode: config.sync_mode,
            // The extractor is configuration, not persisted state: without
            // it the rebuilt filter loses the source's prefix hashes and
            // prefix scans see the salvaged copy as definitely absent.
            prefix_extractor: config.prefix_extractor.clone(),
        },
    )?;
    if report.salvaged_path.is_none() {
        return Ok(None);
    }
    if !report.dropped.is_empty() {
        log::warn!(
            "salvaged table {table_id}: recovered {} block(s), dropped {} corrupt block(s)",
            report.blocks_salvaged,
            report.dropped.len(),
        );
    }

    // Reopen the freshly-written (clean) salvaged SST so it joins the rebuilt
    // manifest like any cleanly-recovered table.
    let checksum = crate::Checksum::from_raw(compute_table_checksum(&**fs, table_path)?);
    let table = Table::recover(
        table_path.to_path_buf(),
        checksum,
        0,
        0,
        table_id,
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
    Ok(Some(table))
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
///
/// Returns the recovered blob files and the per-file failure reasons (merged
/// into the repair report's `unreadable_files`).
fn recover_blob_files(
    config: &Config,
) -> crate::Result<(Vec<crate::vlog::BlobFile>, UnreadableFiles)> {
    let blobs_folder = config.path.join(crate::file::BLOBS_FOLDER);
    let mut blob_files: Vec<crate::vlog::BlobFile> = Vec::new();
    let mut unreadable: UnreadableFiles = Vec::new();

    // No `blobs/` folder = no blob files (a blob tree that never spilled a value
    // to the value log). Nothing to recover; the manifest records an empty list.
    if !config.fs.exists(&blobs_folder)? {
        return Ok((blob_files, unreadable));
    }

    // Guard against the same id surfacing twice (symlinked / aliased entries).
    let mut seen_ids: crate::HashSet<crate::vlog::BlobFileId> = crate::HashSet::default();

    for dirent in config.fs.read_dir(&blobs_folder)? {
        let crate::fs::FsDirEntry {
            path: blob_path,
            file_name,
            is_dir,
        } = dirent;

        if is_dir || file_name == ".DS_Store" || file_name.starts_with("._") {
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

        if !seen_ids.insert(blob_id) {
            continue;
        }

        let checksum = match compute_table_checksum(&*config.fs, &blob_path) {
            Ok(c) => crate::Checksum::from_raw(c),
            Err(e) => {
                seen_ids.remove(&blob_id);
                unreadable.push((blob_path, e.to_string()));
                continue;
            }
        };

        match crate::vlog::recover_blob_file(&blob_path, blob_id, checksum, 0, &config.fs) {
            Ok(bf) => blob_files.push(bf),
            Err(e) => {
                seen_ids.remove(&blob_id);
                unreadable.push((blob_path, e.to_string()));
            }
        }
    }

    Ok((blob_files, unreadable))
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
        repair_tree(self, false)
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
        repair_tree(self, salvage)
    }
}

/// Core repair routine. Separated from the [`Config::repair`] entry point so the
/// logic is testable against a borrowed config.
fn repair_tree(config: &Config, salvage: bool) -> crate::Result<RepairReport> {
    // Hold the cross-process directory lock for the whole repair: it rewrites
    // CURRENT, writes a fresh snapshot, and sweeps `edits-*` in place, so a
    // concurrent open / repair of the same directory would corrupt the manifest.
    // A second acquirer fails fast with `Error::Locked`. Dropped at function
    // return, releasing the lock. The directory is expected to exist (repair
    // operates on an existing tree).
    #[cfg(feature = "std")]
    let _directory_lock =
        crate::config::acquire_directory_lock(&*config.fs, &config.path, config.directory_lock)?;

    let mut recovered_tables: Vec<Table> = Vec::new();
    let mut salvaged = 0usize;
    let mut unreadable_files: Vec<(PathBuf, String)> = Vec::new();
    // Guard against the same file surfacing twice (symlinked / aliased table
    // folders) so a table is not added to two L0 runs.
    let mut seen_ids: crate::HashSet<TableId> = crate::HashSet::default();

    for (table_base_folder, folder_fs) in config.all_tables_folders() {
        if !folder_fs.exists(&table_base_folder)? {
            continue;
        }

        for dirent in folder_fs.read_dir(&table_base_folder)? {
            let crate::fs::FsDirEntry {
                path: table_path,
                file_name,
                is_dir,
            } = dirent;

            // https://en.wikipedia.org/wiki/.DS_Store
            if is_dir || file_name == ".DS_Store" || file_name.starts_with("._") {
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

            if !seen_ids.insert(table_id) {
                // Already recovered via another scanned folder; skip silently.
                continue;
            }

            let checksum = match compute_table_checksum(&*folder_fs, &table_path) {
                Ok(c) => crate::Checksum::from_raw(c),
                Err(e) => {
                    // Mirror the `Table::recover` failure path below: free the id
                    // so an aliased copy in another scanned folder can still be
                    // retried.
                    seen_ids.remove(&table_id);
                    unreadable_files.push((table_path, e.to_string()));
                    continue;
                }
            };

            // global_seqno = 0: a recovered table's intrinsic sequence numbers
            // are authoritative; there is no ingestion-time translation offset
            // to reapply. tree_id = 0 and descriptor_table = None keep the
            // transient open from polluting any shared cache keyed by the real
            // tree id (the tree is reopened fresh after repair).
            let recovered = Table::recover(
                table_path.clone(),
                checksum,
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
            );

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
                    match verify_keep_decision(config, &folder_fs, &table_path, &table) {
                        RepairKeepDecision::Keep => recovered_tables.push(table),
                        RepairKeepDecision::Quarantine(reason) => {
                            drop(table);
                            seen_ids.remove(&table_id);
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
                                // meant to be preserved for manual recovery.
                                Err(e) => return Err(e),
                            }
                        }
                        RepairKeepDecision::Salvage => {
                            drop(table);
                            // Quarantine BEFORE salvage, aborting the repair
                            // on failure: a manifest omitting a still-in-place
                            // file would let the next open's orphan cleanup
                            // delete the only copy. A salvage error AFTER a
                            // successful move is recorded instead — the
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
                                &quarantined,
                                &table_path,
                                table_id,
                            ) {
                                Ok(Some(table)) => {
                                    salvaged += 1;
                                    recovered_tables.push(table);
                                }
                                Ok(None) => {
                                    seen_ids.remove(&table_id);
                                    unreadable_files.push((
                                        table_path,
                                        "verify found corrupt blocks; nothing salvageable"
                                            .to_string(),
                                    ));
                                }
                                Err(salvage_err) => {
                                    seen_ids.remove(&table_id);
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
                Ok(table) => recovered_tables.push(table),
                Err(e) if salvage => {
                    // Whole-file recovery failed; try block-level salvage: the
                    // corrupt original is quarantined and a fresh SST holding
                    // its recoverable blocks is written in its place. A FAILED
                    // quarantine aborts the repair (the `?`): a manifest
                    // omitting a still-in-place file would let the next open's
                    // orphan cleanup delete the only copy.
                    let quarantined = quarantine_file(
                        &*folder_fs,
                        &table_base_folder,
                        &table_path,
                        &file_name,
                        config.sync_mode,
                    )?;
                    match try_salvage_table(config, &folder_fs, &quarantined, &table_path, table_id)
                    {
                        Ok(Some(table)) => {
                            salvaged += 1;
                            recovered_tables.push(table);
                        }
                        Ok(None) => {
                            seen_ids.remove(&table_id);
                            unreadable_files.push((
                                table_path,
                                format!(
                                    "unrecoverable ({e}); original quarantined, nothing salvageable"
                                ),
                            ));
                        }
                        Err(salvage_err) => {
                            seen_ids.remove(&table_id);
                            unreadable_files.push((
                                table_path,
                                format!("recovery failed ({e}); salvage failed ({salvage_err})"),
                            ));
                        }
                    }
                }
                Err(e) => {
                    seen_ids.remove(&table_id);
                    unreadable_files.push((table_path, e.to_string()));
                }
            }
        }
    }

    // Newest first: higher sequence number nearer the L0 head, matching the
    // ordering the merge reader expects for its newest-run-first short-circuit.
    recovered_tables.sort_by_key(|t| std::cmp::Reverse(t.get_highest_seqno()));

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

    // KV-separated (blob) trees additionally carry a blob-file list. Discover the
    // blob files from the `blobs/` folder (no manifest to filter against) and
    // record them in the rebuilt manifest with the matching `TreeType::Blob` so
    // the tree reopens (the reopened tree's type must match its config's
    // `kv_separation_opts`). Fragmentation stats are NOT reconstructable from a
    // directory scan (they are derived from compaction history), so they start
    // empty: blob GC is advisory and re-learns them over time. The empty start
    // never drops live data; it only resets GC's view of reclaimable space.
    let (tree_type, blob_file_list) = if config.kv_separation_opts.is_some() {
        let (blob_files, blob_unreadable) = recover_blob_files(config)?;
        unreadable_files.extend(blob_unreadable);
        let map: crate::HashMap<crate::vlog::BlobFileId, crate::vlog::BlobFile> =
            blob_files.into_iter().map(|bf| (bf.id(), bf)).collect();
        (TreeType::Blob, BlobFileList::new(map))
    } else {
        (
            TreeType::Standard,
            BlobFileList::new(crate::HashMap::default()),
        )
    };

    let version = Version::from_levels(
        version_id,
        tree_type,
        levels,
        blob_file_list,
        crate::blob_tree::FragmentationMap::default(),
    );

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

    let mut warnings = vec![
        "All recovered tables placed at L0; background compaction will redistribute them",
        "Recent unlogged version edits (in-flight compactions, recent deletions) are lost",
    ];
    if config.kv_separation_opts.is_some() {
        warnings.push(
            "Blob fragmentation stats reset to empty; blob GC will re-learn reclaimable space over time",
        );
    }

    Ok(RepairReport {
        recovered,
        salvaged,
        unreadable: unreadable_files.len(),
        unreadable_files,
        method: "all-to-L0 with sequence-number ordering",
        warnings,
    })
}

#[cfg(test)]
mod tests;
