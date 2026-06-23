// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::path::{Path, PathBuf};
use crate::{checksum::Checksum, coding::Decode, io, table::TableId, table::block::Header};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

/// Describes a single integrity error found during verification.
///
/// Full-file integrity (hashing whole files by path) uses `std::fs` directly and
/// is gated to `std`; the `no_std` verify path is block-level over the injected
/// [`Fs`](crate::fs::Fs) backend (see [`verify_block_checksums`]).
#[cfg(feature = "std")]
#[derive(Debug)]
#[non_exhaustive]
pub enum IntegrityError {
    /// Full-file checksum mismatch for an SST table.
    SstFileCorrupted {
        /// Table ID
        table_id: TableId,
        /// Path to the corrupted file
        path: PathBuf,
        /// Checksum stored in the manifest
        expected: Checksum,
        /// Checksum computed from disk
        got: Checksum,
    },

    /// Full-file checksum mismatch for a blob file.
    BlobFileCorrupted {
        /// Blob file ID
        blob_file_id: u64,
        /// Path to the corrupted file
        path: PathBuf,
        /// Checksum stored in the manifest
        expected: Checksum,
        /// Checksum computed from disk
        got: Checksum,
    },

    /// I/O error while reading a file during verification.
    IoError {
        /// Path to the file that could not be read
        path: PathBuf,
        /// The underlying I/O error
        error: io::Error,
    },
}

#[cfg(feature = "std")]
impl core::fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::SstFileCorrupted {
                table_id,
                path,
                expected,
                got,
            } => write!(
                f,
                "SST table {table_id} corrupted at {}: expected {expected}, got {got}",
                path.display()
            ),
            Self::BlobFileCorrupted {
                blob_file_id,
                path,
                expected,
                got,
            } => write!(
                f,
                "blob file {blob_file_id} corrupted at {}: expected {expected}, got {got}",
                path.display()
            ),
            Self::IoError { path, error } => {
                write!(f, "I/O error reading {}: {}", path.display(), error)
            }
        }
    }
}

#[cfg(feature = "std")]
impl core::error::Error for IntegrityError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::IoError { error, .. } => Some(error),
            _ => None,
        }
    }
}

/// Result of an integrity verification scan.
///
/// The `sst_files_checked` and `blob_files_checked` counters reflect
/// the number of files *attempted* — including those that produced I/O
/// errors. This lets callers reconcile the total against the manifest
/// even when some files were unreadable.
#[cfg(feature = "std")]
#[derive(Debug)]
#[non_exhaustive]
pub struct IntegrityReport {
    /// Number of SST table files checked (includes I/O errors).
    pub sst_files_checked: usize,

    /// Number of blob files checked (includes I/O errors).
    pub blob_files_checked: usize,

    /// Integrity errors found during verification.
    pub errors: Vec<IntegrityError>,
}

#[cfg(feature = "std")]
impl IntegrityReport {
    /// Returns `true` if no errors were found.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Total number of files checked (SST + blob).
    #[must_use]
    pub fn files_checked(&self) -> usize {
        self.sst_files_checked + self.blob_files_checked
    }
}

/// Computes a streaming XXH3 128-bit checksum for a file without loading it entirely into memory.
#[cfg(feature = "std")]
fn stream_checksum(path: &std::path::Path) -> std::io::Result<Checksum> {
    use std::io::Read;

    let mut reader = std::fs::File::open(path)?;
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let n = match reader.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if n == 0 {
            break;
        }
        // Safety: Read::read guarantees n <= buf.len(), so get(..n) always
        // returns Some. We use .get() instead of direct indexing to satisfy
        // the crate-wide #[deny(clippy::indexing_slicing)] lint.
        if let Some(chunk) = buf.get(..n) {
            hasher.update(chunk);
        }
    }

    Ok(Checksum::from_raw(hasher.digest128()))
}

/// Verifies full-file checksums for all SST and blob files in the given tree.
///
/// Each file's content is read from disk and hashed with XXHash-3 128-bit,
/// then compared against the checksum stored in the version manifest.
///
/// This detects silent bit-rot, partial writes, and other on-disk corruption.
///
/// Per-file errors (e.g., unreadable files, checksum mismatches) are collected
/// into [`IntegrityReport::errors`] — the scan always runs to completion.
#[cfg(feature = "std")]
#[must_use]
pub fn verify_integrity(tree: &impl crate::AbstractTree) -> IntegrityReport {
    let version = tree.current_version();

    let mut report = IntegrityReport {
        sst_files_checked: 0,
        blob_files_checked: 0,
        errors: Vec::new(),
    };

    // Verify all SST table files
    for table in version.iter_tables() {
        let path = &*table.path;
        let expected = table.checksum();

        match stream_checksum(path) {
            Ok(got) if got != expected => {
                report.errors.push(IntegrityError::SstFileCorrupted {
                    table_id: table.id(),
                    path: (*table.path).clone(),
                    expected,
                    got,
                });
            }
            Ok(_) => {}
            Err(e) => {
                report.errors.push(IntegrityError::IoError {
                    path: (*table.path).clone(),
                    error: e.into(),
                });
            }
        }

        report.sst_files_checked += 1;
    }

    // Verify all blob files
    for blob_file in version.blob_files.iter() {
        let path = blob_file.path();
        let expected = blob_file.checksum();

        match stream_checksum(path) {
            Ok(got) if got != expected => {
                report.errors.push(IntegrityError::BlobFileCorrupted {
                    blob_file_id: blob_file.id(),
                    path: path.to_path_buf(),
                    expected,
                    got,
                });
            }
            Ok(_) => {}
            Err(e) => {
                report.errors.push(IntegrityError::IoError {
                    path: path.to_path_buf(),
                    error: e.into(),
                });
            }
        }

        report.blob_files_checked += 1;
    }

    report
}

// ── Block-level scrub ─────────────────────────────────────────────────────
// `verify_integrity` above hashes each SST as one opaque byte stream and
// compares the digest to the per-file checksum stored in the manifest. That
// catches whole-file corruption but identifies the bad region only at file
// granularity. The functions below walk every block inside every SST and
// verify per-block XXH3 against the value embedded in each block's own
// header, so a corrupt block can be reported with its exact `(file, offset)`
// without re-running the manifest-level scan.

/// Per-block verification error.
#[derive(Debug)]
#[non_exhaustive]
pub enum BlockVerifyError {
    /// SST file could not be opened or its trailer parsed.
    SstFileUnreadable {
        /// Table ID.
        table_id: TableId,
        /// Path to the SST file.
        path: PathBuf,
        /// Underlying I/O / format error.
        error: io::Error,
    },

    /// A block header at the given offset failed to parse — either
    /// XXH3 mismatch on the header itself, or invalid magic bytes /
    /// length fields that point at on-disk corruption.
    HeaderCorrupted {
        /// Table ID.
        table_id: TableId,
        /// Path to the SST file.
        path: PathBuf,
        /// File offset where the corrupt header was read from.
        offset: u64,
        /// Short description of the failure surfaced by header decoding.
        reason: String,
    },

    /// A block's data XXH3 did not match the value stored in its header.
    /// Indicates bit-rot or torn write on the block payload.
    DataCorrupted {
        /// Table ID.
        table_id: TableId,
        /// Path to the SST file.
        path: PathBuf,
        /// File offset where the block header sits (the data follows it).
        offset: u64,
        /// Length of the on-disk data segment, in bytes.
        data_length: u32,
        /// Checksum stored in the block header.
        expected: Checksum,
        /// Checksum computed from the on-disk bytes.
        got: Checksum,
    },

    /// The block header was successfully decoded (its own XXH3
    /// matched) but the subsequent fixed-length read of the data
    /// segment failed at the filesystem layer — truncated file,
    /// unexpected EOF, transient I/O error. Distinct from
    /// `HeaderCorrupted` because the header itself was clean: the
    /// failure is on the bytes that should follow it.
    DataReadError {
        /// Table ID.
        table_id: TableId,
        /// Path to the SST file.
        path: PathBuf,
        /// File offset where the (clean) header sits; the read for
        /// its data segment started at `offset + Header::header_len(block_type)`.
        offset: u64,
        /// Length the (clean) header advertised for the data segment.
        data_length: u32,
        /// Underlying I/O error from the failed data-segment read.
        /// Kept as `std::io::Error` (matching `SstFileUnreadable`) so
        /// `ErrorKind` / OS code stay available to callers and so
        /// `Error::source()` produces a coherent chain.
        error: io::Error,
    },

    /// SFA TOC-level corruption: a named section's length / position
    /// fields are inconsistent (overflow on addition), or seeking to
    /// its declared start offset fails before any block is read.
    /// Distinct from `HeaderCorrupted` (which is per-block) so
    /// callers can tell "the section catalogue itself is bad" apart
    /// from "block N inside an otherwise-walkable section is bad" —
    /// e.g. a `TocCorrupted` makes the whole section unreachable,
    /// while a `HeaderCorrupted` only stops that section's walk.
    TocCorrupted {
        /// Table ID.
        table_id: TableId,
        /// Path to the SST file.
        path: PathBuf,
        /// Section name from the TOC entry (e.g. `b"data"`,
        /// `b"tli"`). Stored verbatim, not lossy-decoded, because
        /// SFA section names are byte strings.
        section_name: Vec<u8>,
        /// File offset where the section *would* start per the TOC
        /// entry. Useful for forensics even when the start is
        /// unreachable.
        section_offset: u64,
        /// Short description of the failure (overflow on
        /// start+length, seek error, etc.).
        reason: String,
    },
}

impl core::fmt::Display for BlockVerifyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::SstFileUnreadable {
                table_id,
                path,
                error,
            } => write!(
                f,
                "SST table {table_id} at {} could not be opened/parsed: {error}",
                path.display(),
            ),
            Self::HeaderCorrupted {
                table_id,
                path,
                offset,
                reason,
            } => write!(
                f,
                "SST table {table_id} at {}: block header at offset {offset} is corrupt ({reason})",
                path.display(),
            ),
            Self::DataCorrupted {
                table_id,
                path,
                offset,
                data_length,
                expected,
                got,
            } => write!(
                f,
                "SST table {table_id} at {}: block at offset {offset} ({data_length} bytes) data \
                 checksum mismatch, expected {expected}, got {got}",
                path.display(),
            ),
            Self::DataReadError {
                table_id,
                path,
                offset,
                data_length,
                error,
            } => write!(
                f,
                "SST table {table_id} at {}: failed to read {data_length}-byte data segment for \
                 block at offset {offset}: {error}",
                path.display(),
            ),
            Self::TocCorrupted {
                table_id,
                path,
                section_name,
                section_offset,
                reason,
            } => write!(
                f,
                "SST table {table_id} at {}: TOC section {:?} at offset {section_offset} is \
                 unreachable ({reason})",
                path.display(),
                String::from_utf8_lossy(section_name),
            ),
        }
    }
}

impl core::error::Error for BlockVerifyError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::SstFileUnreadable { error, .. } | Self::DataReadError { error, .. } => {
                Some(error)
            }
            _ => None,
        }
    }
}

/// A non-fatal finding from a scrub run: the data is intact, but something
/// about a table could not be fully checked.
#[derive(Debug)]
#[non_exhaustive]
pub enum BlockVerifyWarning {
    /// The table's `descriptor#page_ecc` decodes to an ECC scheme this build
    /// cannot apply (an unimplemented scheme, page granularity, an unknown
    /// kind, or a non-canonical descriptor). Block payloads still verify by
    /// their own checksums, but the parity trailer length is not derivable
    /// from a scheme, so the sequential block walk cannot size it and ECC
    /// verification was skipped for this table. Recompaction re-stamps the
    /// table with a supported scheme.
    UnrecognizedEcc {
        /// Table the warning applies to.
        table_id: TableId,
        /// On-disk path of the SST.
        path: PathBuf,
    },
}

/// Aggregated result of a per-block scrub run.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct BlockVerifyReport {
    /// Number of SST table files visited (one per scan).
    pub sst_files_scanned: usize,
    /// Total blocks successfully header-read across all SSTs. Includes
    /// blocks where the data checksum subsequently failed.
    pub blocks_scanned: usize,
    /// Per-block errors collected during the scan. The scan always
    /// runs to completion across all SSTs even if individual blocks
    /// or whole files are corrupt.
    pub errors: Vec<BlockVerifyError>,
    /// Non-fatal findings: data verified, but ECC could not be checked for
    /// some tables (unrecognized scheme — recompaction recommended). Distinct
    /// from `errors`: warnings do NOT make [`Self::is_ok`] false.
    pub warnings: Vec<BlockVerifyWarning>,
}

impl BlockVerifyReport {
    /// `true` if every block in every SST verified clean. Warnings (e.g. an
    /// unrecognized ECC scheme whose data still checksum-verified) do NOT
    /// make this false — only real corruption (`errors`) does.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// `true` if the scrub produced any non-fatal warning.
    #[must_use]
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Options for the block-checksum scrubber
/// ([`verify_block_checksums_with`] / [`AbstractTree::verify_checksum_with`](crate::AbstractTree::verify_checksum_with)).
#[derive(Clone, Debug)]
pub struct VerifyOptions {
    /// Number of SSTs to scan concurrently. Clamped to `>= 1` and to the table
    /// count. `1` (the default) scans sequentially in table order with no
    /// thread spawn. Per-SST scans are independent (each opens its own file
    /// through the table's `Fs` handle), so they parallelize cleanly.
    pub parallelism: usize,

    /// Minimum delay each worker waits after finishing one SST before taking
    /// the next, capping I/O pressure on a production box during a scrub.
    /// `None` (default) runs at full speed.
    pub throttle: Option<core::time::Duration>,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            parallelism: 1,
            throttle: None,
        }
    }
}

impl VerifyOptions {
    /// Sets the number of SSTs to scan concurrently.
    #[must_use]
    pub const fn parallelism(mut self, workers: usize) -> Self {
        self.parallelism = workers;
        self
    }

    /// Sets the per-worker inter-SST throttle delay.
    #[must_use]
    pub const fn throttle(mut self, delay: core::time::Duration) -> Self {
        self.throttle = Some(delay);
        self
    }
}

/// Merges a per-SST partial report into an accumulator.
fn merge_report(dst: &mut BlockVerifyReport, src: BlockVerifyReport) {
    dst.sst_files_scanned += src.sst_files_scanned;
    dst.blocks_scanned += src.blocks_scanned;
    dst.errors.extend(src.errors);
    dst.warnings.extend(src.warnings);
}

/// Scans one SST and returns a partial report (`sst_files_scanned == 1`).
///
/// Self-contained per table: opens the file through the table's own `Fs`
/// handle, sizes encryption overhead and ECC params from the table's
/// descriptor, so it can run on its own worker thread without shared state.
fn scan_one_table(table: &crate::table::Table) -> BlockVerifyReport {
    let mut report = BlockVerifyReport {
        sst_files_scanned: 1,
        ..BlockVerifyReport::default()
    };
    let path: &Path = &table.path;
    let table_id = table.id();

    // Tables whose ECC descriptor decodes to a scheme this build can't apply
    // can't have their SST-block parity trailers sized (the length isn't
    // derivable without the scheme), so those sections are skipped with a
    // warning rather than mis-walked. The self-describing `meta` / `meta_mid`
    // sections are still walked (parity sized from their own `block_flags`),
    // so corruption there is NOT downgraded. The per-block read path still
    // serves the data (framed by data_length, checksum-verified), hence a
    // warning, not an error.
    let ecc_unrecognized = table.metadata.ecc_unrecognized;
    if ecc_unrecognized {
        log::warn!(
            "table {table_id} at {}: unrecognized ECC scheme — skipping the \
             ECC-dependent block sections; recompact to re-stamp with a \
             supported scheme",
            path.display(),
        );
        report.warnings.push(BlockVerifyWarning::UnrecognizedEcc {
            table_id,
            path: path.to_path_buf(),
        });
    }

    // Use each Table's own `Fs` handle (StdFs, MemFs, IoUring, …).
    // Encryption overhead is per-table (different keys / AEAD suites can attach
    // to different SSTs), so feed each table's `max_overhead()` separately.
    let max_enc_overhead = table.encryption.as_ref().map_or(0u32, |e| e.max_overhead());
    match scan_sst_blocks(
        &*table.fs,
        path,
        table_id,
        max_enc_overhead,
        table.metadata.ecc_params,
        ecc_unrecognized,
    ) {
        Ok(per_file) => {
            report.blocks_scanned += per_file.blocks_scanned;
            report.errors.extend(per_file.errors);
        }
        Err(error) => {
            report.errors.push(BlockVerifyError::SstFileUnreadable {
                table_id,
                path: path.to_path_buf(),
                error,
            });
        }
    }
    report
}

/// Walks every block in every SST referenced by the tree's current
/// version and verifies each block's XXH3 checksum.
///
/// Pipeline per SST:
///
/// 1. Open the file and parse the SFA trailer to obtain the TOC.
/// 2. For each TOC section, skip if its name is in `RAW_FORMAT_SECTIONS`
///    (those payloads are not `Header`-prefixed and are covered by the
///    SFA-trailer checksum). Otherwise seek to the section's start
///    offset and walk it as a contiguous block region in
///    `[start, start + length)`.
/// 3. Inside each block region, decode each block's `Header` (which
///    validates the header's own XXH3), read the data segment, and
///    compare a fresh XXH3 over the data against `header.checksum`.
///    Advance by `Header::header_len(block_type) + data_length` until the
///    section end. A corrupt header inside a section stops that
///    section's walk and is reported; the next section is still walked.
///
/// This is the read-side scrub primitive: it catches the same bit-rot
/// signal a live read would surface, ahead of time, with per-block
/// `(file, offset)` granularity. Decompression and decryption errors
/// are out of scope here — those depend on per-level/per-block context
/// (compression policy, encryption key, dictionary) that the scrub
/// path does not need to reach checksum-level corruption.
#[must_use]
pub fn verify_block_checksums(tree: &impl crate::AbstractTree) -> BlockVerifyReport {
    verify_block_checksums_with(tree, &VerifyOptions::default())
}

/// Like [`verify_block_checksums`] but with configurable parallelism and
/// throttle (see [`VerifyOptions`]).
///
/// With `parallelism == 1` (default) SSTs are scanned sequentially in table
/// order. With `> 1`, up to that many worker threads pull SSTs from a shared
/// cursor and scan them concurrently (each scan is independent — its own file
/// handle through the table's `Fs`), then their partial reports are merged.
/// Parallel runs report the same findings as a sequential run; only the order
/// of `errors` / `warnings` may differ. `throttle` makes each worker pause
/// between SSTs so a scrub does not saturate production I/O.
#[must_use]
pub fn verify_block_checksums_with(
    tree: &impl crate::AbstractTree,
    options: &VerifyOptions,
) -> BlockVerifyReport {
    let version = tree.current_version();
    let tables: Vec<crate::table::Table> = version.iter_tables().cloned().collect();

    // `parallelism` + `throttle` only drive the std thread-fan-out + sleep below.
    #[cfg(not(feature = "std"))]
    let _ = options;

    // Parallel scan (std only): up to `parallelism` worker threads pull SSTs from
    // a shared cursor and scan them concurrently. A `no_std` build has no
    // threads, so it always takes the serial path below.
    #[cfg(feature = "std")]
    {
        let workers = options.parallelism.max(1).min(tables.len().max(1));
        if workers > 1 {
            let cursor = core::sync::atomic::AtomicUsize::new(0);
            let partials = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..workers)
                    .map(|_| {
                        scope.spawn(|| {
                            let mut local = BlockVerifyReport::default();
                            let mut idx =
                                cursor.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                            while let Some(table) = tables.get(idx) {
                                merge_report(&mut local, scan_one_table(table));
                                // Claim the next SST first; only pause if this
                                // worker actually has another table to scan.
                                idx = cursor.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                                if tables.get(idx).is_some()
                                    && let Some(delay) = options.throttle
                                {
                                    std::thread::sleep(delay);
                                }
                            }
                            local
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| match handle.join() {
                        Ok(local) => local,
                        // A scrub worker panicking is a bug, not a corruption
                        // finding — propagate rather than drop its SSTs.
                        Err(payload) => std::panic::resume_unwind(payload),
                    })
                    .collect::<Vec<_>>()
            });

            let mut report = BlockVerifyReport::default();
            for partial in partials {
                merge_report(&mut report, partial);
            }
            return report;
        }
    }

    // Serial scan: every `no_std` build, and `std` with `parallelism <= 1`. Scans
    // SSTs in deterministic table order, each over its own `Fs` handle.
    let mut report = BlockVerifyReport::default();
    for (idx, table) in tables.iter().enumerate() {
        merge_report(&mut report, scan_one_table(table));
        // Inter-SST throttle (std only — `no_std` has no sleep primitive). Skip
        // after the final table so a finished scrub returns promptly instead of
        // waiting one extra throttle interval.
        #[cfg(feature = "std")]
        if idx + 1 < tables.len()
            && let Some(delay) = options.throttle
        {
            std::thread::sleep(delay);
        }
        #[cfg(not(feature = "std"))]
        let _ = idx;
    }
    report
}

/// Verifies the per-KV checksum footer of every data block across all SST
/// tables in the tree (the paranoid / scrub integrity path).
///
/// Footer presence is a per-SST property read from each table's descriptor
/// (`ParsedMeta::kv_checksum_algo`), not a per-block header flag — SST data
/// blocks omit the `block_flags` byte. A table whose descriptor reports no
/// footers is skipped wholesale.
///
/// This is stronger than [`verify_block_checksums`]: for footer-bearing
/// tables it decodes each block and recomputes every entry's logical-content
/// digest, localising which entry diverged rather than only flagging the
/// block. Tables written without per-KV footers carry no per-KV digests and
/// are covered by [`verify_block_checksums`] only.
///
/// Returns the first error encountered (`ChecksumMismatch` on a per-entry
/// digest disagreement, or an I/O / decode error). `Ok(())` means every
/// per-KV-checked table verified. A tree written entirely with
/// `kv_checksums = Off` has no footer-bearing tables, so this is a no-op
/// returning `Ok(())`.
///
/// # Errors
///
/// Propagates [`crate::Error::ChecksumMismatch`] on a detected per-entry
/// corruption, or any I/O / decode error from loading a block.
pub fn verify_kv_checksums(tree: &impl crate::AbstractTree) -> crate::Result<()> {
    let version = tree.current_version();
    for table in version.iter_tables() {
        table.verify_kv_checksums()?;
    }
    Ok(())
}

/// Out-of-band variant of [`verify_block_checksums`].
///
/// Walks one SST file directly from a filesystem path, without
/// needing a live `Tree` or the version manifest. Intended for
/// offline diagnostic tools (`tools/sst-dump verify`, `repair_db`,
/// forensics CLIs) that operate on a single file in isolation — for
/// example when the manifest itself is corrupt or the surrounding
/// tree directory has been moved.
///
/// Uses [`StdFs`](crate::fs::StdFs) (the only `Fs` backend that
/// makes sense for an out-of-band tool — `MemFs` / `IoUring` trees
/// never produce files at real filesystem paths) and stamps
/// `table_id = 0` in error reports. The caller's downstream
/// filtering / logging should refer to the file by path, not by
/// table id.
///
/// AEAD overhead is conservatively assumed to be zero: out-of-band
/// tools don't carry the per-table encryption provider that would let
/// them recover the real `max_overhead()`. Encrypted SSTs near the
/// 256 MiB plaintext ceiling may therefore false-flag as
/// [`BlockVerifyError::HeaderCorrupted`]. In practice block sizes are
/// typically a few KiB, so this only matters on artificially-
/// constructed huge blocks; encrypted-aware verification should go
/// through [`verify_block_checksums`] on a live tree.
///
/// The returned [`BlockVerifyReport`] has `sst_files_scanned == 1`
/// (always) plus per-block errors collected during the walk.
#[cfg(feature = "std")]
#[must_use]
pub fn verify_sst_file(path: &std::path::Path) -> BlockVerifyReport {
    use crate::fs::StdFs;

    let fs = StdFs;
    let mut report = BlockVerifyReport {
        sst_files_scanned: 1,
        ..BlockVerifyReport::default()
    };

    // SST blocks omit the block_flags byte, so the parity-trailer presence and
    // shard layout the walk must skip come from the per-SST ECC descriptor —
    // read it from the meta block. If it can't be determined (corrupt meta, or
    // an encrypted SST with no key out-of-band), DO NOT assume disabled:
    // walking an ECC-bearing SST without skipping parity trailers mis-aligns
    // the scan and reports spurious corruption. Surface the indeterminacy and
    // skip the walk.
    let mut ecc_unrecognized = false;
    let ecc = match read_ecc_params_out_of_band(&fs, path) {
        Ok(Some(ScrubEcc::Off)) => None,
        Ok(Some(ScrubEcc::Scheme(params))) => Some(params),
        // The descriptor decodes to a scheme this build can't apply: the
        // SST-block trailer length isn't derivable, so those sections are
        // skipped during the walk. The self-describing `meta` / `meta_mid`
        // sections still size parity from `block_flags`, so corruption there
        // is NOT downgraded. Warn + continue (don't drop the whole scrub).
        Ok(Some(ScrubEcc::Unrecognized)) => {
            log::warn!(
                "{}: unrecognized ECC scheme — skipping the ECC-dependent block \
                 sections; recompact to re-stamp with a supported scheme",
                path.display(),
            );
            report.warnings.push(BlockVerifyWarning::UnrecognizedEcc {
                table_id: 0,
                path: path.to_path_buf(),
            });
            ecc_unrecognized = true;
            None
        }
        // File + trailer readable, but neither meta block decodes (corrupt
        // meta, or an encrypted SST with no key out-of-band). The ECC scheme is
        // undeterminable; skip the walk rather than mis-walk an ECC-bearing SST.
        Ok(None) => {
            report.errors.push(BlockVerifyError::SstFileUnreadable {
                table_id: 0,
                path: path.to_path_buf(),
                error: io::Error::new(
                    io::ErrorKind::InvalidData,
                    "could not decode the SST meta block to determine the ECC scheme \
                     (corrupt meta, or an encrypted SST with no key out-of-band); \
                     skipping the block walk — use verify_block_checksums on a live \
                     tree for ECC-aware verification",
                ),
            });
            return report;
        }
        // Real file-open / SFA-trailer failure — preserve the underlying error
        // rather than collapsing it into the undeterminable message above.
        Err(error) => {
            report.errors.push(BlockVerifyError::SstFileUnreadable {
                table_id: 0,
                path: path.to_path_buf(),
                error: error.into(),
            });
            return report;
        }
    };

    match scan_sst_blocks(&fs, path, 0, 0, ecc, ecc_unrecognized) {
        Ok(per_file) => {
            report.blocks_scanned = per_file.blocks_scanned;
            report.errors = per_file.errors;
        }
        Err(error) => {
            report.errors.push(BlockVerifyError::SstFileUnreadable {
                table_id: 0,
                path: path.to_path_buf(),
                error,
            });
        }
    }

    report
}

/// Per-SST ECC state as seen by the out-of-band scrub.
#[cfg(feature = "std")]
enum ScrubEcc {
    /// ECC off — no parity trailer to skip.
    Off,
    /// A recognized + applicable scheme — size + verify the trailer with it.
    Scheme(crate::table::block::EccParams),
    /// An ECC scheme this build can't apply (unimplemented / unknown /
    /// non-canonical). The trailer length isn't derivable, so the walk must
    /// be skipped with a warning.
    Unrecognized,
}

/// Best-effort read of the per-SST ECC state from an SST file's meta
/// descriptor, for the out-of-band scrub (no live `Table` to consult).
///
/// Returns `Ok(Some(state))` when a meta block decodes. The authoritative
/// tail `meta` section is tried first; if its block is corrupt / undecodable
/// the early `meta_mid` mirror (which the writer emits so one bad meta block
/// can't lose the descriptor) is tried next. The `Ok(None)` outer means the
/// file and SFA trailer are readable but NEITHER meta block decodes (both
/// corrupt, or an encrypted SST whose key the out-of-band tool doesn't have) —
/// the scheme is genuinely UNDETERMINABLE. Returns `Err` when the file can't be
/// opened or its SFA trailer can't be parsed.
///
/// The caller MUST NOT treat `Ok(None)` as "ECC disabled": walking an
/// ECC-bearing SST without skipping the parity trailers mis-aligns the block
/// scan and reports spurious corruption, so the caller skips the walk and
/// surfaces the indeterminacy instead.
#[cfg(feature = "std")]
fn read_ecc_params_out_of_band(
    fs: &dyn crate::fs::Fs,
    path: &std::path::Path,
) -> std::io::Result<Option<ScrubEcc>> {
    let mut probe = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    let sfa_reader = crate::sfa::Reader::from_reader(&mut probe)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let toc = sfa_reader.toc();
    // Tail `meta` is authoritative; `meta_mid` is the early mirror written so a
    // single corrupt meta block doesn't lose the per-SST descriptor.
    for name in [b"meta".as_slice(), b"meta_mid".as_slice()] {
        let Some((pos, len)) = toc.section(name).map(|e| (e.pos(), e.len())) else {
            continue;
        };
        let Ok(size) = u32::try_from(len) else {
            continue;
        };
        let handle = crate::table::BlockHandle::new(crate::table::BlockOffset(pos), size);
        // table_id is moot here: this scrub path reads unencrypted meta
        // (encryption = None), so the AAD identity is unused.
        if let Ok(meta) =
            crate::table::meta::ParsedMeta::load_with_handle(probe.as_ref(), &handle, None, None)
        {
            let state = if meta.ecc_unrecognized {
                ScrubEcc::Unrecognized
            } else if let Some(params) = meta.ecc_params {
                ScrubEcc::Scheme(params)
            } else {
                ScrubEcc::Off
            };
            return Ok(Some(state));
        }
    }
    Ok(None)
}

struct PerFileScan {
    blocks_scanned: usize,
    errors: Vec<BlockVerifyError>,
}

/// Walks every block of one SST. Returns `Err` only on file-open or
/// SFA trailer-parse failure (those make the whole walk impossible).
/// Per-block AND per-section errors — corrupt block headers, mismatched
/// data checksums, post-header data-read failures, and TOC sections we
/// cannot seek to — all land inside `PerFileScan::errors` and never
/// cause an early return; the walker proceeds to the next section so
/// one bad TOC entry cannot mask corruption in the others.
fn scan_sst_blocks(
    fs: &dyn crate::fs::Fs,
    path: &Path,
    table_id: TableId,
    max_enc_overhead: u32,
    ecc: Option<crate::table::block::EccParams>,
    ecc_unrecognized: bool,
) -> io::Result<PerFileScan> {
    use io::BufReader;
    #[cfg(not(feature = "std"))]
    use io::{Seek, SeekFrom};
    #[cfg(feature = "std")]
    use std::io::{Seek, SeekFrom};

    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;

    // The SFA trailer + TOC live at the tail of the file.
    // crate::sfa::Reader::from_reader leaves the cursor at an undefined
    // offset; each per-section walk below explicitly seeks to the
    // section's `pos()` first so the unknown post-trailer position
    // doesn't matter.
    // Wrap the sfa error as the inner cause of the io::Error rather
    // than format!-stringifying it, so the original variant
    // (InvalidHeader / InvalidVersion / ChecksumMismatch / underlying
    // Io) stays reachable via `Error::source()` for downstream
    // diagnostics. crate::sfa::Error implements `std::error::Error`.
    let sfa_reader = crate::sfa::Reader::from_reader(&mut file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, alloc::format!("{e:?}")))?;
    let toc = sfa_reader.toc();
    // SFA TOC layout for an SST. The writer opens the file and
    // immediately calls `crate::sfa::Writer::start("data")`, so the first
    // TOC entry is named (not unnamed) and covers the data-block
    // region. Other named sections, in writer order:
    //
    //   - `data`              : block-format (data blocks)
    //   - `index`             : block-format (partitioned index leaf
    //                           blocks; absent for full-index tables,
    //                           emitted before `tli` by
    //                           `PartitionedIndexWriter::finish`)
    //   - `tli`               : block-format (top-level index, both
    //                           full and partitioned variants)
    //   - `filter`            : block-format (filter blocks)
    //   - `filter_tli`        : block-format (top-level filter for
    //                           partitioned filters; absent for full
    //                           filters, emitted after `filter` by
    //                           `PartitionedFilterWriter::finish`)
    //   - `range_tombstones`  : block-format (optional)
    //   - `meta_mid`          : block-format (early mirror of `meta`)
    //   - `linked_blob_files` : RAW length-prefixed list of u64s
    //   - `table_version`     : RAW single byte
    //   - `meta_separator`    : RAW 4 KiB zero padding
    //   - `tli_tail`          : block-format (tail mirror of `tli`)
    //   - `meta`              : block-format (metadata, authoritative)
    //
    // Block-format sections are walked block-by-block (each block
    // prefixed with the standard `Header`). Raw-format sections are
    // skipped — their integrity is covered by the SFA-trailer
    // checksum verified at table-open time. New section names default
    // to "walk" (must be added to `RAW_FORMAT_SECTIONS` if they're
    // raw), so a forgotten-to-handle section fails loud rather than
    // silently passing a corruption.

    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut blocks_scanned: usize = 0;
    let mut errors: Vec<BlockVerifyError> = Vec::new();
    // One reusable data buffer across the whole SST — sized up via
    // `resize` per block instead of a fresh `vec![0u8; N]` allocation
    // each iteration. On large trees this turns thousands of malloc
    // calls into a single growing allocation that settles at the
    // largest block size seen.
    let mut data_buf: Vec<u8> = Vec::new();

    for entry in toc.iter() {
        if RAW_FORMAT_SECTIONS.contains(&entry.name()) {
            continue;
        }
        let start = entry.pos();
        // `checked_add` (not `saturating_add`) so a corrupted or
        // forged TOC length cannot silently collapse to `u64::MAX`
        // and let the walk treat the whole address space as one
        // section. On overflow we surface the section as a
        // file-level `TocCorrupted` and skip walking it — the other
        // (still-walkable) sections of the same SST are honoured.
        // `TocCorrupted` rather than `HeaderCorrupted` because the
        // failure is at the section-catalogue layer, not inside any
        // individual block.
        let Some(end) = start.checked_add(entry.len()) else {
            errors.push(BlockVerifyError::TocCorrupted {
                table_id,
                path: path.to_path_buf(),
                section_name: entry.name().to_vec(),
                section_offset: start,
                reason: format!(
                    "section length {} overflows u64 when added to start offset {start}",
                    entry.len(),
                ),
            });
            continue;
        };
        // Mid-walk seek failure: don't propagate as a file-level Err
        // (that would discard everything already scanned and report
        // the whole SST as unreadable, which contradicts the
        // function's contract). Surface as a `TocCorrupted` for this
        // section and skip walking it; subsequent sections still run.
        // Again `TocCorrupted` (not `HeaderCorrupted`): we never even
        // reached a block to decode its header.
        if let Err(e) = reader.seek(SeekFrom::Start(start)) {
            errors.push(BlockVerifyError::TocCorrupted {
                table_id,
                path: path.to_path_buf(),
                section_name: entry.name().to_vec(),
                section_offset: start,
                reason: format!("seek to section start failed: {e}"),
            });
            continue;
        }
        let mut ctx = WalkCtx {
            reader: &mut reader,
            table_id,
            path,
            data_buf: &mut data_buf,
            blocks_scanned: &mut blocks_scanned,
            errors: &mut errors,
            max_data_length: block_data_length_cap(max_enc_overhead),
            ecc,
            ecc_unrecognized,
        };
        walk_block_region(&mut ctx, start, end);
    }

    Ok(PerFileScan {
        blocks_scanned,
        errors,
    })
}

/// SFA TOC section names whose payload is NOT a sequence of `Block`s
/// (i.e. NOT prefixed with the standard `Header`). The scrub skips
/// these sections — their integrity is covered by the SFA-trailer
/// checksum verified at table-open time. Every other section
/// (`data` / `tli` / `tli_tail` / `index` / `filter_tli` / `filter` /
/// `range_tombstones` / `meta` / `meta_mid`) is a `Header`-prefixed
/// block run and gets walked. See `scan_sst_blocks` for the full
/// section catalogue and the writer-side source of truth.
///
/// `meta_separator` is the 4 KiB zero-padding section the writer
/// emits between the MID and TAIL meta blocks so a single bad
/// filesystem sector cannot take out both copies — it carries no
/// blocks and must be skipped here, otherwise the walker would try
/// to decode zeros as a `Header` and report a spurious
/// `HeaderCorrupted` on every clean SST.
const RAW_FORMAT_SECTIONS: &[&[u8]] = &[b"linked_blob_files", b"table_version", b"meta_separator"];

/// Plaintext upper bound on a single block's on-disk data segment
/// length, mirroring `table::block::MAX_DECOMPRESSION_SIZE` (256 MiB).
/// Encrypted blocks legitimately exceed this by up to the AEAD
/// provider's `max_overhead()`; see `block_data_length_cap` for the
/// effective per-walk cap that adds that overhead in.
const MAX_BLOCK_DATA_LENGTH: u64 = 256 * 1024 * 1024;

/// Effective `data_length` cap for one scan, mirroring the size
/// validation in `Block::from_file`: plaintext cap + the table's AEAD
/// `max_overhead()` (0 when encryption is disabled). A value above
/// this is treated as `HeaderCorrupted` regardless of TOC bounds,
/// defending against DoS-by-allocation if both the block header and
/// the enclosing TOC entry are simultaneously corrupted / forged.
fn block_data_length_cap(max_enc_overhead: u32) -> u64 {
    MAX_BLOCK_DATA_LENGTH + u64::from(max_enc_overhead)
}

/// Walks the contiguous block range `[start_offset, end_offset)`,
/// decoding each block's header (which validates the header's own
/// XXH3) and then re-hashing the data segment against
/// `header.checksum`. Stops at the first un-parseable header inside
/// the range — that block is reported as `HeaderCorrupted` and the
/// rest of the range is skipped because subsequent offsets become
/// unrecoverable without a valid length field.
/// Mutable cursor + scratch state threaded through `walk_block_region`.
/// Bundles the per-walk accumulators (file cursor, reused data
/// buffer, counters, error sink) into one borrow so the function
/// signature stays under clippy's argument-count cap.
struct WalkCtx<'a> {
    reader: &'a mut io::BufReader<Box<dyn crate::fs::FsFile>>,
    table_id: TableId,
    path: &'a Path,
    data_buf: &'a mut Vec<u8>,
    blocks_scanned: &'a mut usize,
    errors: &'a mut Vec<BlockVerifyError>,
    /// Effective `data_length` cap (plaintext limit + AEAD overhead).
    /// Matches the bound `Block::from_file` applies on the read path,
    /// so the scrub does not false-flag legitimate encrypted blocks
    /// near the 256 MiB plaintext limit as `HeaderCorrupted`.
    max_data_length: u64,
    /// Per-SST Page-ECC shard layout. SST blocks (`Data` / `Index` / `Filter` /
    /// `RangeTombstone`) omit the `block_flags` byte, so their parity-trailer
    /// presence AND shard layout are NOT derivable from the header — both come
    /// from this table-wide descriptor scheme. When `Some`, each such block
    /// carries `expected_parity_len(data_length, scheme)` parity bytes after
    /// the payload that the walk must skip (sized by the scheme) to stay
    /// aligned. Meta / Manifest / `ManifestFooter` blocks keep the byte and
    /// self-describe parity via their `ECC_PARITY` bit, sized with the fixed
    /// RS(4,2) layout the writer uses for them, regardless of this field.
    ecc: Option<crate::table::block::EccParams>,
    /// `true` when the table's ECC descriptor decodes to a scheme this build
    /// can't apply. The trailer length of its SST blocks (`Data` / `Index` /
    /// `Filter` / `RangeTombstone`) isn't derivable, so those sections are
    /// skipped (the caller warns once). Self-describing sections (`meta` /
    /// `meta_mid`) still size parity from `block_flags` and ARE walked.
    ecc_unrecognized: bool,
}

fn walk_block_region(ctx: &mut WalkCtx<'_>, start_offset: u64, end_offset: u64) {
    #[cfg(not(feature = "std"))]
    use io::Read;
    #[cfg(feature = "std")]
    use std::io::Read;

    let mut offset = start_offset;

    while offset < end_offset {
        // Confine reads to the declared section before touching
        // Header::decode_from. Without this pre-check, a TOC entry
        // whose `len` puts `end_offset` inside the first block's
        // header region would let `decode_from` consume up to
        // `header_len` bytes — reading past the section boundary
        // into the next section's payload, where random bytes might
        // happen to parse as a "valid" header and silently corrupt
        // the walk. Treat the under-sized tail as `HeaderCorrupted`
        // and stop this section's walk; subsequent sections still
        // run because `walk_block_region` returns rather than
        // bubbling the error up.
        let remaining_in_section = end_offset - offset;
        // Lower bound: the header is at least MIN_LEN (the exact length, with
        // or without the block_flags byte, is known only after decode).
        if remaining_in_section < Header::MIN_LEN as u64 {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "section has only {remaining_in_section} bytes left at this offset, \
                     less than Header::MIN_LEN = {}",
                    Header::MIN_LEN,
                ),
            });
            return;
        }
        let header = match Header::decode_from(ctx.reader) {
            Ok(h) => h,
            Err(e) => {
                ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                    table_id: ctx.table_id,
                    path: ctx.path.to_path_buf(),
                    offset,
                    reason: format!("{e:?}"),
                });
                return;
            }
        };

        // Unrecognized-ECC table: SST blocks (no `block_flags` byte) carry a
        // parity trailer whose length we can't derive without the descriptor
        // scheme, so this section can't be walked — stop here (the caller has
        // already warned). Self-describing blocks (`block_flags` present) size
        // parity from their `ECC_PARITY` bit, so those sections still walk.
        // Checked before the scanned-count increment so skipped blocks aren't
        // tallied. Sections are homogeneous in block type, so the first block
        // decides the whole section.
        if ctx.ecc_unrecognized && !Header::has_block_flags(header.block_type) {
            return;
        }

        // Count the block as "header-read" immediately on successful
        // decode — matches the BlockVerifyReport.blocks_scanned docs
        // ("includes blocks where the data checksum subsequently
        // failed"). Without this early increment, blocks that emit
        // DataReadError / data-length-bounds HeaderCorrupted would
        // be silently uncounted, contradicting the documented
        // semantics.
        // Block counter; a tree cannot hold 2^64 blocks, so a plain add cannot
        // overflow.
        *ctx.blocks_scanned += 1;

        // Actual header length for this block (variable: SST blocks omit the
        // block_flags byte). Used for the section-bounds math and the offset
        // advance so the walk tracks what `decode_from` actually consumed.
        let header_len = Header::header_len(header.block_type) as u64;

        // Page-ECC parity trailer that follows the payload on disk. Presence
        // depends on the block type: Meta / Manifest / ManifestFooter keep the
        // block_flags byte and self-describe via the ECC_PARITY bit; SST blocks
        // omit the byte, so parity presence is the per-SST `page_ecc` flag. The
        // trailer length is derived from data_length (never stored). The walk
        // must skip these bytes — otherwise the next iteration would read parity
        // as the following block's header and mis-align the whole section.
        // Parity-trailer scheme to skip for this block. Self-describing blocks
        // (Meta / Manifest / `ManifestFooter`) carry the `block_flags` byte and
        // are written with the fixed RS(4,2) layout; SST blocks size their
        // trailer from the per-SST descriptor scheme threaded in via `ctx.ecc`.
        let block_ecc = if Header::has_block_flags(header.block_type) {
            (header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0)
                .then_some(crate::table::block::EccParams::RS_4_2)
        } else {
            ctx.ecc
        };
        let parity_len = block_ecc.map_or(0, |scheme| {
            u64::from(crate::table::block::expected_parity_len(
                header.data_length,
                scheme,
            ))
        });

        // Validate data_length against TWO bounds before allocating
        // / reading:
        //
        // 1. Hard cap (MAX_BLOCK_DATA_LENGTH = 256 MiB, mirroring
        //    table::block::MAX_DECOMPRESSION_SIZE). Catches the case
        //    where BOTH the block header AND the enclosing TOC entry
        //    are simultaneously corrupted/forged so that `remaining`
        //    becomes arbitrarily large. Without this, a forged TOC
        //    entry with len=u64::MAX could let the section-bounds
        //    check pass and trigger a multi-GB Vec::resize.
        //
        // 2. Remaining bytes in this TOC section. Header::decode_from
        //    already verified the header's own XXH3, so a data_length
        //    that overruns the section bounds is either bit-flip
        //    corruption that happened to keep the header digest
        //    valid (rare but possible), or fuzz input. Honouring it
        //    would read past `end_offset` into the next section.
        //
        // Both bounds are reported as HeaderCorrupted — the header
        // was technically parseable but its length field is invalid.
        let data_length_u64 = u64::from(header.data_length);
        if data_length_u64 > ctx.max_data_length {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "header data_length {data_length_u64} exceeds hard cap {}",
                    ctx.max_data_length,
                ),
            });
            return;
        }
        // A header whose own bytes cross the section boundary is corrupt and must
        // be rejected here: clamping `remaining` to zero would let a header with a
        // zero-length declared payload slip past the `>` check below even though
        // the header itself ran past the section end. Reuse the plain
        // `remaining_in_section` (the loop invariant `offset < end_offset` keeps
        // it non-negative) rather than recomputing it.
        if header_len > remaining_in_section {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "block header ({header_len} bytes) extends past the section end \
                     ({remaining_in_section} bytes remain)",
                ),
            });
            return;
        }
        let remaining = remaining_in_section - header_len;
        // `data_length_u64` is already capped at `ctx.max_data_length` (checked
        // above) and `parity_len` is derived from it, so the sum is bounded well
        // within u64 — a plain add cannot overflow.
        let on_disk_payload = data_length_u64 + parity_len;
        if on_disk_payload > remaining {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "header data_length {data_length_u64} + parity {parity_len} exceeds \
                     remaining section bytes {remaining}",
                ),
            });
            return;
        }

        let data_length = header.data_length as usize;
        ctx.data_buf.resize(data_length, 0);
        // `as_mut_slice` returns the whole `Vec` (exactly `data_length`
        // bytes after the resize above) — full-slice access dodges
        // the crate-wide `#[deny(clippy::indexing_slicing)]`.
        if let Err(e) = ctx.reader.read_exact(ctx.data_buf.as_mut_slice()) {
            // Header was clean (XXH3 matched) but the data segment
            // that should follow it could not be read in full —
            // truncated SST, unexpected EOF, transient I/O.
            // Semantically distinct from HeaderCorrupted; reported
            // under its own variant so callers pattern-matching on
            // the error kind aren't surprised to find post-header
            // I/O failures bucketed with header-parse failures.
            ctx.errors.push(BlockVerifyError::DataReadError {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                data_length: header.data_length,
                error: e.into(),
            });
            return;
        }

        let computed = Checksum::from_raw(crate::hash::hash128(ctx.data_buf));
        if computed != header.checksum {
            ctx.errors.push(BlockVerifyError::DataCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                data_length: header.data_length,
                expected: header.checksum,
                got: computed,
            });
        }

        // Consume the parity trailer (if any) so the reader cursor lands on
        // the next block's header. The payload checksum above already covers
        // correctness; parity is only consulted for ECC recovery on the live
        // read path, so the scrub discards it — but it MUST still skip exactly
        // `parity_len` bytes or the next iteration mis-reads parity as a header.
        if parity_len > 0 {
            // Discard the parity trailer so the cursor lands on the next block's
            // header. `crate::io` has no `copy`/`sink`, so drain exactly
            // `parity_len` bytes through a small scratch buffer.
            let mut scratch = [0u8; 512];
            let mut remaining = parity_len;
            let drained: io::Result<u64> = loop {
                if remaining == 0 {
                    break Ok(parity_len);
                }
                let want =
                    usize::try_from(remaining.min(scratch.len() as u64)).unwrap_or(scratch.len());
                let (head, _) = scratch.split_at_mut(want);
                match ctx.reader.read(head) {
                    Ok(0) => break Ok(parity_len - remaining),
                    Ok(n) => remaining -= n as u64,
                    Err(e) => break Err(e.into()),
                }
            };
            match drained {
                Ok(n) if n == parity_len => {}
                Ok(n) => {
                    ctx.errors.push(BlockVerifyError::DataReadError {
                        table_id: ctx.table_id,
                        path: ctx.path.to_path_buf(),
                        offset,
                        data_length: header.data_length,
                        error: io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            alloc::format!(
                                "parity trailer truncated: read {n} of {parity_len} bytes"
                            ),
                        ),
                    });
                    return;
                }
                Err(e) => {
                    ctx.errors.push(BlockVerifyError::DataReadError {
                        table_id: ctx.table_id,
                        path: ctx.path.to_path_buf(),
                        offset,
                        data_length: header.data_length,
                        error: e,
                    });
                    return;
                }
            }
        }

        // blocks_scanned was already incremented right after a
        // successful Header::decode_from above — do not double-count
        // here.
        // Advance past this block. Each term is bounded (data_length capped
        // above, parity derived from it, header a const) and `offset` is bounded
        // by the section end, so the running cursor cannot overflow u64.
        offset += header_len + data_length_u64 + parity_len;
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "test assertions")]
mod block_verify_tests {
    use super::*;
    // `AbstractTree` looks unused at a glance but the test bodies below
    // call `.insert()`, `.flush_active_memtable()`, and
    // `.current_version()` on `AnyTree` values — those are trait
    // methods, not inherent ones, so the trait MUST be in scope for
    // method resolution. Removing the import is a compile error, not
    // a clippy nit.
    use crate::{
        AbstractTree, Config, SequenceNumberCounter, compression::CompressionType,
        config::CompressionPolicy,
    };
    use std::io::{Read, Seek, SeekFrom, Write};
    // Shadows the built-in `#[test]` so `#[test]`-annotated functions
    // below resolve to `test_log::test` (which wires up logging for
    // failing tests). This matches every other test module in the
    // crate — the import looks unused at a glance but the proc-macro
    // attribute name resolution consumes it.
    use test_log::test;

    fn populate_tree(dir: &std::path::Path, items: usize) {
        let cfg = Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
        let tree = cfg.open().unwrap();
        for i in 0u64..items as u64 {
            let key = format!("k{i:08}");
            let val = format!("v{i:08}");
            tree.insert(key.as_bytes(), val.as_bytes(), 1 + i);
        }
        tree.flush_active_memtable(1 + items as u64).unwrap();
        // Drop the tree so all files are closed before the test that
        // mutates SST bytes on disk reopens them via Verify.
        drop(tree);
    }

    fn reopen_tree(dir: &std::path::Path) -> crate::AnyTree {
        Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .open()
        .unwrap()
    }

    /// Populates a tree with per-KV checksums enabled (`AllLevels`) so the
    /// flushed SST carries data blocks with the `KV_CHECKSUM_FOOTER` flag
    /// set and a per-entry checksum footer.
    fn populate_tree_kv_checked(dir: &std::path::Path, items: usize) {
        use crate::AbstractTree;
        use crate::runtime_config::KvChecksumPolicy;

        let cfg = Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
        let any = cfg.open().unwrap();
        let crate::AnyTree::Standard(tree) = any else {
            panic!("expected Standard tree");
        };
        tree.update_runtime_config(|c| {
            c.kv_checksums = KvChecksumPolicy::AllLevels;
        })
        .unwrap();
        for i in 0u64..items as u64 {
            let key = format!("k{i:08}");
            let val = format!("v{i:08}");
            tree.insert(key.as_bytes(), val.as_bytes(), 1 + i);
        }
        tree.flush_active_memtable(1 + items as u64).unwrap();
        drop(tree);
    }

    #[test]
    fn verify_block_checksums_clean_tree_has_no_errors() {
        let dir = tempfile::tempdir().unwrap();
        populate_tree(dir.path(), 1_000);

        let tree = reopen_tree(dir.path());
        let report = verify_block_checksums(&tree);
        assert!(
            report.is_ok(),
            "expected clean tree to verify with zero errors, got {:?}",
            report.errors
        );
        assert!(
            report.blocks_scanned > 0,
            "expected at least one block scanned",
        );
        assert!(
            report.sst_files_scanned >= 1,
            "expected at least one SST scanned",
        );
    }

    #[cfg(feature = "page_ecc")]
    #[test]
    fn verify_block_checksums_clean_page_ecc_tree_has_no_errors() {
        // Regression: with page_ecc on, every SST data / index / filter block
        // carries a Reed-Solomon parity trailer after its payload. SST blocks
        // omit the block_flags byte, so the scrub learns parity presence from
        // the per-SST descriptor and must skip `expected_parity_len(data_length)`
        // bytes per block. Without that skip the walk mis-reads parity as the
        // next block's header and reports spurious HeaderCorrupted. Enough items
        // to spill multiple data blocks so cross-block alignment is exercised.
        use crate::AbstractTree;

        let dir = tempfile::tempdir().unwrap();
        {
            let any = Config::new(
                dir.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
            .page_ecc(true)
            .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
                data_shards: 4,
                parity_shards: 2,
            })
            .open()
            .unwrap();
            for i in 0u64..2_000 {
                let key = format!("k{i:08}");
                let val = format!("v{i:08}");
                any.insert(key.as_bytes(), val.as_bytes(), 1 + i);
            }
            any.flush_active_memtable(2_001).unwrap();
            drop(any);
        }

        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .page_ecc(true)
        .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
            data_shards: 4,
            parity_shards: 2,
        })
        .open()
        .unwrap();
        let report = verify_block_checksums(&tree);
        assert!(
            report.is_ok(),
            "page_ecc tree must verify with zero errors (parity trailers skipped \
             per block), got {:?}",
            report.errors,
        );
        assert!(
            report.blocks_scanned > 1,
            "expected multiple blocks scanned to exercise cross-block alignment",
        );
    }

    #[cfg(feature = "page_ecc")]
    #[test]
    fn verify_block_checksums_clean_nondefault_ecc_tree_has_no_errors() {
        // Regression: the scrub must size each SST's parity trailer from the
        // per-SST descriptor scheme, NOT a hardcoded RS(4,2). A table written
        // with a non-default scheme (RS(8,2), different shard size → different
        // trailer length) is mis-walked if the scrub assumes RS(4,2): the
        // wrong trailer length mis-aligns the next block and reports spurious
        // corruption. With descriptor-driven sizing the walk stays aligned.
        use crate::AbstractTree;

        let dir = tempfile::tempdir().unwrap();
        {
            let any = Config::new(
                dir.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
            .page_ecc(true)
            .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
                data_shards: 8,
                parity_shards: 2,
            })
            .open()
            .unwrap();
            for i in 0u64..2_000 {
                let key = format!("k{i:08}");
                let val = format!("v{i:08}");
                any.insert(key.as_bytes(), val.as_bytes(), 1 + i);
            }
            any.flush_active_memtable(2_001).unwrap();
            drop(any);
        }

        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .page_ecc(true)
        .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        })
        .open()
        .unwrap();
        let report = verify_block_checksums(&tree);
        assert!(
            report.is_ok(),
            "non-default-scheme ECC tree must verify with zero errors \
             (parity sized from the descriptor, not RS(4,2)), got {:?}",
            report.errors,
        );
        assert!(
            report.blocks_scanned > 1,
            "expected multiple blocks scanned to exercise cross-block alignment",
        );
    }

    /// Returns the on-disk path of the first SST registered with the
    /// tree's current version. Drops the tree before returning so the
    /// caller can mutate the file safely (no descriptor cache, no
    /// file lock). Going through `current_version().iter_tables()`
    /// instead of a filesystem walk keeps the test coupled to the
    /// verifier's actual input set — a new on-disk file under the
    /// tree directory cannot accidentally become the corruption
    /// target.
    fn pick_first_sst_path(dir: &std::path::Path) -> std::path::PathBuf {
        let tree = reopen_tree(dir);
        let path = tree
            .current_version()
            .iter_tables()
            .next()
            .map(|table| (*table.path).clone())
            .expect("at least one populated SST file");
        drop(tree);
        path
    }

    #[test]
    fn verify_block_checksums_detects_flipped_byte_in_data_block() {
        use crate::table::block::Header;
        let dir = tempfile::tempdir().unwrap();
        populate_tree(dir.path(), 1_000);

        let sst_path = pick_first_sst_path(dir.path());

        // The flip target is the first byte AFTER the first block's
        // Header — that lands squarely inside the data segment of the
        // first data block, so the header's own XXH3 stays valid (no
        // HeaderCorrupted) but the data XXH3 will now mismatch.
        let flip_offset = Header::MIN_LEN as u64;
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&sst_path)
                .unwrap();
            f.seek(SeekFrom::Start(flip_offset)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(flip_offset)).unwrap();
            f.write_all(&byte).unwrap();
            f.sync_all().unwrap();
        }

        let tree = reopen_tree(dir.path());
        let report = verify_block_checksums(&tree);
        assert!(
            !report.is_ok(),
            "expected corruption to surface as report errors, got {report:?}",
        );
        let has_data_corruption = report.errors.iter().any(|e| {
            matches!(
                e,
                BlockVerifyError::DataCorrupted { path, .. } if path == &sst_path,
            )
        });
        assert!(
            has_data_corruption,
            "expected a DataCorrupted error for {}, got {:?}",
            sst_path.display(),
            report.errors,
        );
    }

    #[test]
    fn verify_kv_checksums_clean_kv_checked_tree_passes() {
        // A tree written with per-KV checksums enabled must pass the
        // per-KV scrub with no error.
        let dir = tempfile::tempdir().unwrap();
        populate_tree_kv_checked(dir.path(), 500);

        let tree = reopen_tree(dir.path());
        let crate::AnyTree::Standard(tree) = tree else {
            panic!("expected Standard tree");
        };
        verify_kv_checksums(&tree).expect("clean kv-checked tree must pass per-KV scrub");
    }

    #[test]
    fn verify_kv_checked_detects_corrupted_digest_under_valid_block_checksum() {
        // The per-KV verifier must catch a divergence that the block-level
        // XXH3 does NOT: corrupt a stored digest, then write the block so its
        // block-level checksum is computed over the already-corrupted bytes.
        // The block therefore loads cleanly (block checksum valid) and only
        // the per-KV recompute disagrees. (Flipping a payload byte in the
        // file without recomputing the block checksum would be caught at
        // load, never reaching the per-KV path — which would prove nothing.)
        use crate::InternalValue;
        use crate::ValueType::Value;
        use crate::comparator::default_comparator;
        use crate::runtime_config::ChecksumAlgorithm;
        use crate::table::block::header::block_flags;
        use crate::table::block::{Block, BlockIdentity, BlockTransform, BlockType, kv_checksum};
        use crate::table::data_block::DataBlock;

        let algo = ChecksumAlgorithm::Xxh3_64;
        let items = [
            InternalValue::from_components(b"alpha".to_vec(), b"one".to_vec(), 3, Value),
            InternalValue::from_components(b"bravo".to_vec(), b"two".to_vec(), 2, Value),
        ];
        let digests: Vec<u64> = items
            .iter()
            .map(|it| kv_checksum::kv_digest(it, algo).expect("xxh3 always available"))
            .collect();

        let mut payload = Vec::new();
        DataBlock::encode_kv_checked_into(&mut payload, &items, &digests, algo, 2, 0.0).unwrap();

        // Corrupt the first stored digest (its first byte sits right after the
        // inner payload, where the digest array begins).
        let inner_len = kv_checksum::split_inner(&payload).unwrap().len();
        *payload.get_mut(inner_len).expect("digest array byte") ^= 0xFF;

        // Write with a VALID block-level checksum over the corrupted payload.
        // Data blocks omit the `block_flags` byte (footer presence is a per-SST
        // descriptor property), so the KV_CHECKSUM_FOOTER flag passed here is
        // dropped on encode — the footer rides inside the payload structurally,
        // and `verify_kv_checked` splits it without consulting the header bit.
        let id = BlockIdentity::for_test(0, BlockType::Data);
        let mut buf = Vec::new();
        Block::write_into_with_flags(
            &mut buf,
            &payload,
            id,
            &BlockTransform::PLAIN,
            block_flags::KV_CHECKSUM_FOOTER,
        )
        .unwrap();

        // Block loads fine (block-level checksum matches the corrupted bytes).
        let block = Block::from_reader(&mut &buf[..], id, &BlockTransform::PLAIN).unwrap();

        // Only the per-KV verifier catches the bad digest. `None` skips the
        // algorithm cross-check — this test exercises the digest-mismatch path.
        let err =
            DataBlock::verify_kv_checked(&block.data, block.header, default_comparator(), None)
                .expect_err("corrupted stored digest must fail the per-KV verifier");
        assert!(
            matches!(err, crate::Error::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {err:?}"
        );
    }

    #[test]
    fn verify_kv_checked_rejects_non_data_block_type() {
        // The scrub verifies only Data blocks. A header whose block_type is
        // not Data (corruption or a caller bug) must be rejected with
        // InvalidTag, not silently coerced to Data and verified as if it were
        // a data block — coercion would defeat the scrub's purpose.
        use crate::InternalValue;
        use crate::ValueType::Value;
        use crate::comparator::default_comparator;
        use crate::runtime_config::ChecksumAlgorithm;
        use crate::table::block::header::block_flags;
        use crate::table::block::{Block, BlockIdentity, BlockTransform, BlockType, kv_checksum};
        use crate::table::data_block::DataBlock;

        let algo = ChecksumAlgorithm::Xxh3_64;
        let items = [
            InternalValue::from_components(b"alpha".to_vec(), b"one".to_vec(), 3, Value),
            InternalValue::from_components(b"bravo".to_vec(), b"two".to_vec(), 2, Value),
        ];
        let digests: Vec<u64> = items
            .iter()
            .map(|it| kv_checksum::kv_digest(it, algo).expect("xxh3 always available"))
            .collect();

        let mut payload = Vec::new();
        DataBlock::encode_kv_checked_into(&mut payload, &items, &digests, algo, 2, 0.0).unwrap();

        let id = BlockIdentity::for_test(0, BlockType::Data);
        let mut buf = Vec::new();
        Block::write_into_with_flags(
            &mut buf,
            &payload,
            id,
            &BlockTransform::PLAIN,
            block_flags::KV_CHECKSUM_FOOTER,
        )
        .unwrap();
        let block = Block::from_reader(&mut &buf[..], id, &BlockTransform::PLAIN).unwrap();

        // The footer + inner bytes form a valid data block, so only the
        // block_type gate can catch a tampered type: flip it to a non-Data
        // variant and require InvalidTag.
        let mut bad_header = block.header;
        bad_header.block_type = BlockType::Index;
        let err = DataBlock::verify_kv_checked(&block.data, bad_header, default_comparator(), None)
            .expect_err("non-Data block_type must be rejected, not coerced");
        assert!(
            matches!(err, crate::Error::InvalidTag(("BlockType", _))),
            "expected InvalidTag(BlockType), got {err:?}"
        );
    }

    /// Exercises the out-of-band wrapper on a real clean SST file.
    /// `verify_sst_file` is the entry point sst-dump calls; this pins
    /// that it stamps `sst_files_scanned = 1`, reports no errors on a
    /// healthy file, and propagates the block count through the
    /// `StdFs` -> `scan_sst_blocks` -> `BlockVerifyReport` path.
    #[test]
    fn verify_sst_file_clean_file_has_no_errors() {
        let dir = tempfile::tempdir().unwrap();
        populate_tree(dir.path(), 1_000);
        let sst_path = pick_first_sst_path(dir.path());

        let report = verify_sst_file(&sst_path);
        assert!(
            report.is_ok(),
            "expected clean SST to verify with zero errors, got {:?}",
            report.errors,
        );
        assert_eq!(
            report.sst_files_scanned, 1,
            "wrapper must always stamp sst_files_scanned = 1",
        );
        assert!(
            report.blocks_scanned > 0,
            "expected at least one block scanned in a populated SST",
        );
    }

    /// Exercises the file-open failure branch (the only path through
    /// `verify_sst_file` that converts an underlying `io::Error` into
    /// a `BlockVerifyError::SstFileUnreadable`). A missing file is the
    /// simplest trigger; an unreadable-due-to-permissions trigger
    /// would require root or chmod-induced state and is overkill for
    /// pinning the variant routing.
    #[test]
    fn verify_sst_file_missing_file_reports_unreadable() {
        // Build the missing-file path under a fresh tempdir so it
        // resolves the same way on Linux / macOS / Windows runners.
        // A hardcoded Unix-style absolute path would either skip the
        // test on Windows (no `/this/...` semantics) or risk a flaky
        // pass if the path happened to exist.
        let dir = tempfile::tempdir().unwrap();
        let missing_path = dir.path().join("does-not-exist-sst-12345.sst");
        // Sanity: tempdir() guarantees the directory is empty.
        assert!(
            !missing_path.exists(),
            "tempdir entry must be absent for this test to exercise the missing-file branch",
        );

        let report = verify_sst_file(&missing_path);
        assert_eq!(
            report.sst_files_scanned, 1,
            "wrapper stamps sst_files_scanned = 1 even on file-open failure \
             so callers see the attempt was made",
        );
        assert_eq!(
            report.blocks_scanned, 0,
            "no blocks could be walked because the file couldn't be opened",
        );
        assert_eq!(
            report.errors.len(),
            1,
            "expected exactly one error, got {:?}",
            report.errors,
        );
        let err = report.errors.first().unwrap();
        assert!(
            matches!(
                err,
                BlockVerifyError::SstFileUnreadable { table_id: 0, path, .. }
                    if path == &missing_path,
            ),
            "expected SstFileUnreadable for {}, got {err:?}",
            missing_path.display(),
        );
    }

    /// Pins the routing of post-header short-read failures to
    /// `BlockVerifyError::DataReadError`. Regression guard for #315:
    /// a refactor that collapses the `read_exact` failure branch back
    /// into `HeaderCorrupted` (which is what a naive "any read error
    /// inside the walker is a header problem" cleanup would do) loses
    /// the distinction between "the file's TOC lies about where the
    /// section ends" and "the header itself fails its own XXH3", and
    /// downstream tooling (`sst-dump`, `repair_db`, lazy block repair)
    /// pattern-matches on the variant to decide whether the block is
    /// recoverable. Demoting truncated-data to `HeaderCorrupted` would
    /// make those tools fall back to whole-section discard instead of
    /// per-block surgery.
    ///
    /// Setup forges an SFA archive whose `data` TOC entry claims a
    /// section length large enough for one full block (header + N
    /// bytes), but the underlying file contains only the header.
    /// Result: `Header::decode_from` succeeds (the header's XXH3
    /// matches its own bytes), the bounds check passes (`data_length`
    /// fits within the lied section length), and the data-segment
    /// `read_exact` hits EOF after consuming a handful of trailing
    /// TOC + trailer bytes. The only valid landing variant is
    /// `DataReadError`.
    #[test]
    #[expect(
        clippy::indexing_slicing,
        clippy::cast_possible_truncation,
        reason = "synthetic SFA forgery — offsets are all in-bounds by \
                  construction (we just wrote the bytes ourselves), and \
                  the u64 -> usize cast cannot overflow on any target \
                  the test runs on (the forged archive is < 1 KiB)"
    )]
    fn walk_block_region_reports_data_read_error_on_truncated_data_segment() {
        use crate::coding::Encode;
        use crate::fs::{Fs, FsOpenOptions, MemFs};
        use crate::table::block::{BlockType, Header};

        // Trailer layout (38 bytes at the tail of an SFA archive):
        //   MAGIC(4) | version(1) | csum_type(1) | toc_checksum(16) | toc_pos(8) | toc_len(8)
        const TRAILER_LEN: usize = 4 + 1 + 1 + 16 + 8 + 8;
        const DATA_LENGTH: u32 = 4096;
        const HEADER_LEN: u64 = Header::MIN_LEN as u64;

        let header = Header {
            // Arbitrary sentinel; the walker reaches `read_exact` and
            // bails BEFORE any data-segment XXH3 comparison, so this
            // value is never checked.
            checksum: Checksum::from_raw(0xDEAD_BEEF_DEAD_BEEF),
            data_length: DATA_LENGTH,
            uncompressed_length: DATA_LENGTH,
            ..Header::test_dummy(BlockType::Data)
        };

        // Build a minimal SFA archive: one section "data" containing
        // exactly one Header (33 bytes) and zero following data bytes.
        let mut archive_bytes: Vec<u8> = Vec::new();
        {
            let mut writer =
                crate::sfa::Writer::from_writer(std::io::Cursor::new(&mut archive_bytes));
            writer.start("data").unwrap();
            writer.write_all(&header.encode_into_vec()).unwrap();
            writer.finish().unwrap();
        }

        // Parse the trailer at the file tail.
        let trailer_start = archive_bytes.len() - TRAILER_LEN;
        let toc_pos_bytes: [u8; 8] = archive_bytes[trailer_start + 22..trailer_start + 30]
            .try_into()
            .unwrap();
        let toc_len_bytes: [u8; 8] = archive_bytes[trailer_start + 30..trailer_start + 38]
            .try_into()
            .unwrap();
        let toc_pos = u64::from_le_bytes(toc_pos_bytes) as usize;
        let toc_len = u64::from_le_bytes(toc_len_bytes) as usize;

        // TOC payload layout: `TOC!`(4) | entry_count(4 LE) | entries.
        // Each entry: pos(8 LE) | len(8 LE) | name_len(2 LE) | name.
        // The first (only) entry begins at toc_pos + 8.
        let first_entry_offset = toc_pos + 4 + 4;
        let len_field_offset = first_entry_offset + 8;

        // Inflate the section length so end_offset = HEADER_LEN +
        // DATA_LENGTH. The walker then computes remaining = DATA_LENGTH
        // (passes the bounds check), tries to `read_exact(DATA_LENGTH)`,
        // and hits EOF after the few trailing TOC + trailer bytes.
        let lied_len: u64 = HEADER_LEN + u64::from(DATA_LENGTH);
        archive_bytes[len_field_offset..len_field_offset + 8]
            .copy_from_slice(&lied_len.to_le_bytes());

        // Recompute the TOC checksum (xxh3_128 over the TOC payload)
        // and patch the trailer's stored checksum so crate::sfa::Reader still
        // accepts the file.
        let new_toc_checksum = crate::hash::hash128(&archive_bytes[toc_pos..toc_pos + toc_len]);
        let csum_field_offset = trailer_start + 4 + 1 + 1;
        archive_bytes[csum_field_offset..csum_field_offset + 16]
            .copy_from_slice(&new_toc_checksum.to_le_bytes());

        // Materialize the forged archive in MemFs and run the scanner.
        let fs = MemFs::new();
        let path = std::path::Path::new("/forged.sst");
        {
            let mut f = fs
                .open(
                    path,
                    &FsOpenOptions::new().write(true).create(true).truncate(true),
                )
                .unwrap();
            f.write_all(&archive_bytes).unwrap();
        }

        let table_id: TableId = 42;
        let scan = scan_sst_blocks(&fs, path, table_id, 0, None, false)
            .expect("forged SFA must parse cleanly");
        assert_eq!(
            scan.errors.len(),
            1,
            "expected exactly one error, got {:?}",
            scan.errors,
        );
        let err = scan.errors.first().unwrap();
        assert!(
            matches!(
                err,
                BlockVerifyError::DataReadError {
                    table_id: t,
                    offset: 0,
                    data_length: d,
                    ..
                } if *t == table_id && *d == DATA_LENGTH,
            ),
            "expected DataReadError {{ table_id: {table_id}, offset: 0, \
             data_length: {DATA_LENGTH}, .. }}; got {err:?}",
        );
        assert_eq!(
            scan.blocks_scanned, 1,
            "header decoded successfully, so blocks_scanned must count this block \
             even though the data segment read failed",
        );
    }

    /// A block header whose own bytes extend past the section boundary must be
    /// reported as `HeaderCorrupted`, not slip through with a clamped-to-zero
    /// remaining payload.
    ///
    /// Setup forges a section whose lied length is exactly `Header::MIN_LEN`
    /// (so the `< MIN_LEN` guard passes) and stores a `Meta` block, whose
    /// `header_len` is `MIN_LEN + 1`. `Header::decode_from` reads the full
    /// `MIN_LEN + 1` header bytes from the file (they are physically present,
    /// followed by the TOC), but those bytes cross the section boundary, so the
    /// boundary guard fires.
    #[test]
    #[expect(
        clippy::indexing_slicing,
        clippy::cast_possible_truncation,
        reason = "synthetic SFA forgery — offsets are in-bounds by construction \
                  and the forged archive is < 1 KiB"
    )]
    fn walk_block_region_reports_header_crossing_section_boundary() {
        use crate::coding::Encode;
        use crate::fs::{Fs, FsOpenOptions, MemFs};
        use crate::table::block::{BlockType, Header};

        const TRAILER_LEN: usize = 4 + 1 + 1 + 16 + 8 + 8;

        // `Meta` blocks carry the block_flags byte, so header_len == MIN_LEN + 1.
        let header = Header {
            checksum: Checksum::from_raw(0xDEAD_BEEF_DEAD_BEEF),
            data_length: 0,
            uncompressed_length: 0,
            ..Header::test_dummy(BlockType::Meta)
        };
        assert_eq!(
            Header::header_len(BlockType::Meta) as u64,
            Header::MIN_LEN as u64 + 1,
        );

        let mut archive_bytes: Vec<u8> = Vec::new();
        {
            let mut writer =
                crate::sfa::Writer::from_writer(std::io::Cursor::new(&mut archive_bytes));
            writer.start("data").unwrap();
            writer.write_all(&header.encode_into_vec()).unwrap();
            writer.finish().unwrap();
        }

        let trailer_start = archive_bytes.len() - TRAILER_LEN;
        let toc_pos_bytes: [u8; 8] = archive_bytes[trailer_start + 22..trailer_start + 30]
            .try_into()
            .unwrap();
        let toc_len_bytes: [u8; 8] = archive_bytes[trailer_start + 30..trailer_start + 38]
            .try_into()
            .unwrap();
        let toc_pos = u64::from_le_bytes(toc_pos_bytes) as usize;
        let toc_len = u64::from_le_bytes(toc_len_bytes) as usize;

        let first_entry_offset = toc_pos + 4 + 4;
        let len_field_offset = first_entry_offset + 8;

        // Lie that the section is exactly MIN_LEN bytes: one byte short of the
        // Meta header, so the header decode crosses the section boundary.
        let lied_len: u64 = Header::MIN_LEN as u64;
        archive_bytes[len_field_offset..len_field_offset + 8]
            .copy_from_slice(&lied_len.to_le_bytes());

        let new_toc_checksum = crate::hash::hash128(&archive_bytes[toc_pos..toc_pos + toc_len]);
        let csum_field_offset = trailer_start + 4 + 1 + 1;
        archive_bytes[csum_field_offset..csum_field_offset + 16]
            .copy_from_slice(&new_toc_checksum.to_le_bytes());

        let fs = MemFs::new();
        let path = std::path::Path::new("/forged-boundary.sst");
        {
            let mut f = fs
                .open(
                    path,
                    &FsOpenOptions::new().write(true).create(true).truncate(true),
                )
                .unwrap();
            f.write_all(&archive_bytes).unwrap();
        }

        let table_id: TableId = 7;
        let scan = scan_sst_blocks(&fs, path, table_id, 0, None, false)
            .expect("forged SFA must parse cleanly");
        assert_eq!(
            scan.errors.len(),
            1,
            "expected exactly one error, got {:?}",
            scan.errors,
        );
        let err = scan.errors.first().unwrap();
        assert!(
            matches!(
                err,
                BlockVerifyError::HeaderCorrupted { table_id: t, offset: 0, reason, .. }
                    if *t == table_id && reason.contains("extends past the section end"),
            ),
            "expected a section-boundary HeaderCorrupted; got {err:?}",
        );
    }

    /// Builds a tree with `batches` separate L0 SSTs (one flush per batch) so
    /// the parallel scrubber actually has multiple files to fan out over.
    fn populate_multi_sst(dir: &std::path::Path, batches: usize, per_batch: usize) {
        let cfg = Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
        let tree = cfg.open().unwrap();
        let mut seqno = 1u64;
        for b in 0..batches {
            for i in 0..per_batch {
                let key = format!("b{b:03}k{i:08}");
                tree.insert(key.as_bytes(), b"v".as_slice(), seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(seqno).unwrap();
            seqno += 1;
        }
        drop(tree);
    }

    #[test]
    fn verify_checksum_method_on_clean_tree_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        populate_tree(dir.path(), 500);
        let tree = reopen_tree(dir.path());
        let report = tree.verify_checksum();
        assert!(report.is_ok(), "clean tree must verify clean: {report:?}");
        assert!(report.sst_files_scanned >= 1);
        assert!(report.blocks_scanned >= 1);
    }

    #[test]
    fn verify_checksum_with_parallel_matches_sequential() {
        let dir = tempfile::tempdir().unwrap();
        populate_multi_sst(dir.path(), 5, 300);
        let tree = reopen_tree(dir.path());

        let seq = tree.verify_checksum_with(&VerifyOptions::default());
        let par = tree.verify_checksum_with(&VerifyOptions::default().parallelism(4));

        assert!(
            seq.sst_files_scanned >= 2,
            "need >1 SST to exercise parallelism, got {}",
            seq.sst_files_scanned,
        );
        // Parallel run reports the SAME findings as sequential — only order may
        // differ. Counts must match exactly.
        assert_eq!(seq.sst_files_scanned, par.sst_files_scanned);
        assert_eq!(seq.blocks_scanned, par.blocks_scanned);
        assert_eq!(seq.errors.len(), par.errors.len());
        assert!(
            seq.is_ok() && par.is_ok(),
            "clean tree: seq={seq:?} par={par:?}"
        );
    }

    #[test]
    fn verify_checksum_with_parallel_detects_corruption() {
        use crate::table::block::Header;
        let dir = tempfile::tempdir().unwrap();
        populate_multi_sst(dir.path(), 4, 300);

        let sst_path = pick_first_sst_path(dir.path());
        let flip_offset = Header::MIN_LEN as u64;
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&sst_path)
                .unwrap();
            f.seek(SeekFrom::Start(flip_offset)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(flip_offset)).unwrap();
            f.write_all(&byte).unwrap();
            f.sync_all().unwrap();
        }

        let tree = reopen_tree(dir.path());
        let report = tree.verify_checksum_with(&VerifyOptions::default().parallelism(4));
        assert!(
            !report.is_ok(),
            "parallel scrub must surface the flipped byte: {report:?}",
        );
    }

    #[test]
    fn verify_checksum_with_throttle_completes_clean() {
        let dir = tempfile::tempdir().unwrap();
        populate_multi_sst(dir.path(), 3, 200);
        let tree = reopen_tree(dir.path());
        let opts = VerifyOptions::default()
            .parallelism(2)
            .throttle(std::time::Duration::from_millis(1));
        let report = tree.verify_checksum_with(&opts);
        assert!(
            report.is_ok(),
            "throttled scrub must still verify clean: {report:?}"
        );
        assert!(report.sst_files_scanned >= 2);
    }

    #[test]
    fn verify_checksum_with_throttle_does_not_sleep_after_last_sst() {
        // Regression: the throttle is an INTER-SST pause and must not fire after
        // the final SST. A single-SST tree scrubbed with a large throttle must
        // return promptly; the bug slept one full throttle interval after the
        // only table, making a finished scrub look hung. Sequential path
        // (parallelism 1, one table) so this pins the single-worker loop.
        let dir = tempfile::tempdir().unwrap();
        populate_multi_sst(dir.path(), 1, 50);
        let tree = reopen_tree(dir.path());
        let throttle = std::time::Duration::from_millis(400);
        let opts = VerifyOptions::default().parallelism(1).throttle(throttle);
        let start = std::time::Instant::now();
        let report = tree.verify_checksum_with(&opts);
        let elapsed = start.elapsed();
        assert!(report.is_ok(), "clean single-SST scrub: {report:?}");
        assert_eq!(report.sst_files_scanned, 1, "test needs exactly one SST");
        assert!(
            elapsed < throttle / 2,
            "a single-SST scrub must not sleep the inter-SST throttle after the \
             last table: took {elapsed:?} with a {throttle:?} throttle",
        );
    }
}
