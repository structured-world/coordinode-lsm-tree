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
//! KV-separated (blob) trees are not yet supported: their manifest also tracks
//! blob-file fragmentation statistics that cannot be reconstructed from a
//! directory scan alone. Repairing one returns [`crate::Error::FeatureUnsupported`].

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
    /// rebuilt manifest.
    pub recovered: usize,

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
fn compute_table_checksum(fs: &dyn crate::fs::Fs, path: &std::path::Path) -> crate::Result<u128> {
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
) -> crate::Result<PathBuf> {
    let quarantine_dir = table_base_folder
        .parent()
        .unwrap_or(table_base_folder)
        .join("repair-quarantine");
    fs.create_dir_all(&quarantine_dir)?;
    let dest = quarantine_dir.join(file_name);
    fs.rename(src, &dest)?;
    Ok(dest)
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
            let dest = quarantine_file(&*config.fs, &blobs_folder, &blob_path, &file_name)?;
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
        repair_tree(self)
    }
}

/// Core repair routine. Separated from the [`Config::repair`] entry point so the
/// logic is testable against a borrowed config.
fn repair_tree(config: &Config) -> crate::Result<RepairReport> {
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
                let dest =
                    quarantine_file(&*folder_fs, &table_base_folder, &table_path, &file_name)?;
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
                Ok(table) => recovered_tables.push(table),
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

    let version_id = highest_existing_version_id(&*config.fs, &config.path)?
        .map_or(0, |max| max.saturating_add(1));

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
        unreadable: unreadable_files.len(),
        unreadable_files,
        method: "all-to-L0 with sequence-number ordering",
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::{compute_table_checksum, highest_existing_version_id};
    use crate::fs::StdFs;
    use test_log::test;

    #[test]
    fn compute_table_checksum_matches_oneshot_xxh3() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("000007");
        // Larger than the 256 KiB read buffer so the chunked read loop is
        // exercised across multiple iterations.
        let payload: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &payload)?;

        let got = compute_table_checksum(&StdFs, &path)?;
        let expected = xxhash_rust::xxh3::xxh3_128(&payload);
        assert_eq!(
            got, expected,
            "streamed digest must equal the one-shot xxh3-128 digest",
        );
        Ok(())
    }

    #[test]
    fn highest_existing_version_id_picks_the_max_and_ignores_non_versions() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        for name in ["v2", "v10", "v3", "current", "vNaN", "notaversion"] {
            std::fs::write(dir.path().join(name), b"x")?;
        }
        assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, Some(10));
        Ok(())
    }

    #[test]
    fn highest_existing_version_id_none_when_no_versions_present() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("current"), b"x")?;
        assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, None);
        Ok(())
    }
}
