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

/// Computes a streaming XXH3 128-bit checksum over `[start, end)` of a file,
/// without loading it entirely into memory. Pass `start = 0` for a whole file.
///
/// A tight-space RESTRICTED table's `[0, punch_offset)` prefix — and a
/// relocated blob file's prefix below its live-data frontier — is hole-punched
/// (reads as zeros) once a superseding output owns that data, so the manifest
/// digest covers only the live suffix; verification must digest from the same
/// `start`.
#[cfg(feature = "std")]
pub(crate) fn stream_checksum_from(
    path: &std::path::Path,
    start: u64,
) -> std::io::Result<Checksum> {
    use std::io::{Read, Seek, SeekFrom};

    let mut reader = std::fs::File::open(path)?;
    if start != 0 {
        reader.seek(SeekFrom::Start(start))?;
    }
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

        // A tight-space RESTRICTED view digests only its live suffix (the
        // punched prefix reads as zeros and is not part of its identity).
        let start = match table.restrict_lower_bound() {
            Some(bound) => match table.punch_offset_for(bound) {
                Ok(offset) => offset,
                // A TRANSIENT index-read failure (EINTR / EAGAIN) while resolving
                // the punch offset is retryable I/O, not corruption: falling back
                // to `0` would digest the hole-punched prefix and report a healthy
                // restricted table as corrupted. Mirror `scan_one_table`, which
                // routes the same transient failure to an unreadable/IoError
                // classification and skips the checksum comparison.
                Err(crate::Error::Io(error)) if error.kind().is_transient() => {
                    report.errors.push(IntegrityError::IoError {
                        path: (*table.path).clone(),
                        error,
                    });
                    report.sst_files_checked += 1;
                    continue;
                }
                // A PERSISTENT I/O failure (`Other` / EIO) and a STRUCTURAL
                // failure both fall back to `0` (fail closed): the whole-file
                // digest then mismatches and the table is reported rather than
                // silently passing.
                Err(_) => 0,
            },
            None => 0,
        };
        match stream_checksum_from(path, start) {
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

        // A blob file whose consumed prefix was reclaimed in place records its
        // digest over the LIVE suffix only: hashing the whole file would fold
        // in the punched (zeroed) prefix and report a healthy file as corrupt.
        // `0` for a whole, unreclaimed file.
        match stream_checksum_from(path, blob_file.live_data_start()) {
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

    /// A block's Page-ECC parity trailer did not match parity freshly
    /// computed over its (checksum-clean) payload. The payload itself is
    /// intact — but the block's ECC is dead: a later payload fault could no
    /// longer be recovered from this trailer. Reported only when the payload
    /// checksum matched (a corrupt payload legitimately mismatches the
    /// original trailer and is already reported as `DataCorrupted`).
    EccParityMismatch {
        /// Table ID.
        table_id: TableId,
        /// Path to the SST file.
        path: PathBuf,
        /// File offset where the block header sits.
        offset: u64,
        /// Length of the on-disk data segment, in bytes.
        data_length: u32,
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
            Self::EccParityMismatch {
                table_id,
                path,
                offset,
                data_length,
            } => write!(
                f,
                "SST table {table_id} at {}: block at offset {offset} ({data_length} bytes) has a \
                 clean payload but its ECC parity trailer does not match freshly computed parity \
                 (dead ECC — recompact or heal in place)",
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
///
/// Warnings do not fail [`BlockVerifyReport::is_ok`], so any consumer that
/// renders a verdict (a CLI, an operator report) MUST surface them alongside
/// it: a bare "OK" over a non-empty warning list misreads "nothing broken
/// among what was checkable" as "everything verified". The skipped surface
/// each variant names (an unverifiable parity trailer, an unwalkable ECC
/// section) is exactly where silent rot would otherwise hide.
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

    /// The table carries a RECOGNIZED Page-ECC scheme, but this build was
    /// compiled without the ECC codecs (the `page_ecc` feature), so its parity
    /// trailers were consumed for walk alignment but could NOT be verified.
    /// Block payloads still verify by their own checksums — parity-only rot is
    /// what stays invisible on this build. Verify on a `page_ecc`-enabled
    /// build, or recompact (on this build the rewrite is parity-less) to leave
    /// only verifiable bytes.
    ParityUnverifiable {
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
    /// Set when the scan could NOT walk some SST-block sections — an unrecognized
    /// ECC descriptor makes those blocks' parity-trailer length underivable, so
    /// the walk skips them (including the DATA blocks) entirely. A report with no
    /// errors but this flag set has verified LESS than the whole file, so it is
    /// NOT a clean verdict: [`Self::is_ok`] returns `false`. (The data may still
    /// be readable through the live point-read path, which frames blocks by
    /// `data_length`; recompaction re-stamps the SST under a supported scheme.)
    pub incomplete: bool,
}

impl BlockVerifyReport {
    /// `true` only if every SST section was walked AND every block verified
    /// clean. A parity-unverifiable WARNING (whose blocks were still walked and
    /// payload-checksummed) does not make this false, but a real error
    /// (`errors`) or an INCOMPLETE walk that skipped sections
    /// ([`Self::incomplete`], e.g. an unrecognized ECC descriptor) does — a
    /// skipped section was never verified, so reporting it clean would be a false
    /// success.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty() && !self.incomplete
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
    // An incomplete partial (an SST whose sections were skipped unwalked) taints
    // the whole merged report: once ANY table could not be fully scanned, the
    // aggregate `is_ok()` must not claim a clean verdict.
    dst.incomplete |= src.incomplete;
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
        // The block walk will skip every non-self-describing section (the data
        // blocks included), so the scan is incomplete: a clean report here would
        // falsely claim the data verified.
        report.incomplete = true;
    }

    // A recognized scheme on a build WITHOUT the ECC codecs: trailers are
    // consumed for alignment but cannot be verified (parity-only rot stays
    // invisible) — surface the gap, mirroring the out-of-band walk.
    #[cfg(not(feature = "page_ecc"))]
    if table.metadata.ecc_params.is_some() {
        report
            .warnings
            .push(BlockVerifyWarning::ParityUnverifiable {
                table_id,
                path: path.to_path_buf(),
            });
    }

    // Use each Table's own `Fs` handle (StdFs, MemFs, IoUring, …).
    // Encryption overhead is per-table (different keys / AEAD suites can attach
    // to different SSTs), so feed each table's `max_overhead()` separately.
    let max_enc_overhead = table.encryption.as_ref().map_or(0u32, |e| e.max_overhead());
    // A restricted view digests / walks only its live suffix: skip the punched
    // data-block prefix. A TRANSIENT punch-offset lookup failure (a flaky
    // partitioned-index read: EINTR / EAGAIN) is recorded as an unreadable-file
    // I/O error and the walk is skipped — falling back to `0` would walk the
    // hole-punched prefix and report its zeroed blocks as a false whole-file
    // checksum mismatch on a healthy restricted table. A PERSISTENT I/O failure
    // (`Other` / EIO, not retryable) and a STRUCTURAL lookup failure both fall
    // back to `0` (walk everything, fail closed) so an unresolvable or corrupt
    // index cannot exempt blocks.
    let data_start = match table.restrict_lower_bound() {
        Some(bound) => match table.punch_offset_for(bound) {
            Ok(offset) => offset,
            Err(crate::Error::Io(error)) if error.kind().is_transient() => {
                report.errors.push(BlockVerifyError::SstFileUnreadable {
                    table_id,
                    path: path.to_path_buf(),
                    error,
                });
                return report;
            }
            Err(_) => 0,
        },
        None => 0,
    };
    match scan_sst_blocks(
        &*table.fs,
        path,
        table_id,
        max_enc_overhead,
        table.metadata.ecc_params,
        ecc_unrecognized,
        data_start,
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
/// 2. For each TOC section, if its name is in `RAW_FORMAT_SECTIONS` (those
///    payloads are not `Header`-prefixed and carry no per-section checksum)
///    validate its structural shape instead of walking blocks. Otherwise
///    seek to the section's start offset and walk it as a contiguous block
///    region in `[start, start + length)`.
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
    let fs: alloc::sync::Arc<dyn crate::fs::Fs> = alloc::sync::Arc::new(crate::fs::StdFs);
    verify_sst_file_with_fs(&fs, path)
}

/// As [`verify_sst_file`], but reads `path` through the given filesystem.
///
/// `pub(crate)` so `repair` can block-verify an SST on the tree's own `Fs`
/// before deciding whether to salvage it, rather than assuming `StdFs`.
#[cfg(feature = "std")]
pub(crate) fn verify_sst_file_with_fs(
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    path: &std::path::Path,
) -> BlockVerifyReport {
    verify_sst_file_with_context(fs, path, None, None, 0)
}

/// As [`verify_sst_file_with_fs`], but with an encryption context for
/// ENCRYPTED SSTs and an optional caller-known durable table id. Block headers
/// and payload checksums are plaintext, so the section walk itself needs no
/// decryption — the provider (and the AAD-bound id) are used only to decode
/// the meta block for the per-SST ECC descriptor. This makes the full
/// out-of-band walk (every section, raw checksums — which flag even
/// ECC-correctable persistent faults) available for encrypted tables, applying
/// the same verification standard as the unencrypted path.
///
/// `known_table_id`: `Some` when the caller knows the durable id out-of-band
/// (repair — the SST file name), enforcing the meta payload cross-check even
/// on UNENCRYPTED reads so a checksum-clean forged tail meta falls back to the
/// intact MID mirror instead of dictating a forged ECC descriptor to the walk;
/// `None` for standalone tools with no id knowledge (reports then stamp
/// `table_id = 0`).
#[cfg(feature = "std")]
pub(crate) fn verify_sst_file_with_context(
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    path: &std::path::Path,
    encryption: Option<&alloc::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
    known_table_id: Option<crate::TableId>,
    // Byte offset to start the DATA-section walk at: `0` for a normal table, the
    // punch offset for a tight-space RESTRICTED view (its `[0, data_start)` data
    // blocks were hole-punched and read as zeros). A caller holding the table
    // supplies it directly; a standalone walk derives it below.
    data_start: u64,
) -> BlockVerifyReport {
    let table_id = known_table_id.unwrap_or(0);
    // A caller-known punch offset wins; with none, derive the live frontier of
    // a possibly-RESTRICTED SST from its colocated sidecar so a standalone
    // walk does not condemn the intentionally punched prefix as corruption.
    let data_start = if data_start == 0 {
        restricted_data_start(fs, path, encryption, known_table_id)
    } else {
        data_start
    };
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
    let provider = encryption.map(|e| &**e);
    let probe = match read_ecc_params_out_of_band(&**fs, path, provider, known_table_id) {
        Ok(p) => p,
        // Real file-open / SFA-trailer failure — preserve the underlying error
        // rather than collapsing it into the undeterminable message below.
        Err(error) => {
            report.errors.push(BlockVerifyError::SstFileUnreadable {
                table_id,
                path: path.to_path_buf(),
                error: error.into(),
            });
            return report;
        }
    };
    // Both mirrors decode but their FULL metadata disagrees: one is
    // forged/rotted to another internally-consistent payload (e.g. a changed
    // compression tag with the ECC descriptor untouched). Every byte-level
    // check passes on both, so this comparison is the only out-of-band
    // detector — a recovery preferring the altered tail would misread every
    // data block. Report and keep walking (block-level findings still add
    // signal).
    if probe.mirrors_diverge {
        report.errors.push(BlockVerifyError::TocCorrupted {
            table_id,
            path: path.to_path_buf(),
            section_name: b"meta".to_vec(),
            section_offset: 0,
            reason: alloc::string::String::from(
                "the tail meta and meta_mid mirrors decode to different metadata; \
                 one copy is forged or rotted behind a re-stamped checksum",
            ),
        });
    }
    let ecc = match probe.ecc {
        Some(ScrubEcc::Off) => None,
        Some(ScrubEcc::Scheme(params)) => Some(params),
        // The descriptor decodes to a scheme this build can't apply: the
        // SST-block trailer length isn't derivable, so those sections are
        // skipped during the walk. The self-describing `meta` / `meta_mid`
        // sections still size parity from `block_flags`, so corruption there
        // is NOT downgraded. Warn + continue (don't drop the whole scrub).
        Some(ScrubEcc::Unrecognized) => {
            log::warn!(
                "{}: unrecognized ECC scheme — skipping the ECC-dependent block \
                 sections; recompact to re-stamp with a supported scheme",
                path.display(),
            );
            report.warnings.push(BlockVerifyWarning::UnrecognizedEcc {
                table_id,
                path: path.to_path_buf(),
            });
            // The walk below skips the non-self-describing sections (data blocks
            // included), so the scan is incomplete: a clean report would falsely
            // claim the data verified.
            report.incomplete = true;
            ecc_unrecognized = true;
            None
        }
        // File + trailer readable, but neither meta block decodes (corrupt
        // meta, or an encrypted SST with no key out-of-band). The ECC scheme is
        // undeterminable; skip the walk rather than mis-walk an ECC-bearing SST.
        None => {
            report.errors.push(BlockVerifyError::SstFileUnreadable {
                table_id,
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
    };

    // A recognized scheme on a build WITHOUT the ECC codecs: the trailers are
    // consumed for walk alignment but cannot be verified, so parity-only rot
    // stays invisible. Surface that as a warning — the repair gate requires a
    // warning-free report, so such a table routes to salvage (whose rewrite is
    // parity-less on this build, leaving only verifiable bytes) instead of
    // being stamped into a rebuilt manifest with unchecked trailer bytes.
    #[cfg(not(feature = "page_ecc"))]
    if ecc.is_some() {
        report
            .warnings
            .push(BlockVerifyWarning::ParityUnverifiable {
                table_id,
                path: path.to_path_buf(),
            });
    }

    // Encrypted blocks legitimately exceed the plaintext data_length cap by
    // up to the provider's AEAD overhead (mirroring `Block::from_file`); a
    // zero here would false-flag a healthy encrypted block just over the cap
    // as HeaderCorrupted and send the whole table to salvage.
    let max_enc_overhead =
        provider.map_or(0u32, crate::encryption::EncryptionProvider::max_overhead);
    match scan_sst_blocks(
        &**fs,
        path,
        table_id,
        max_enc_overhead,
        ecc,
        ecc_unrecognized,
        data_start,
    ) {
        Ok(per_file) => {
            report.blocks_scanned = per_file.blocks_scanned;
            // extend, NOT assign: the mirror-divergence finding above must
            // survive the block walk's own error list.
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

/// Per-SST ECC state as seen by the out-of-band scrub.
// `PartialEq` + `Copy`: the probe compares the states decoded from the two
// meta copies to arbitrate a forged descriptor.
#[derive(Clone, Copy, PartialEq, Eq)]
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
    encryption: Option<&dyn crate::encryption::EncryptionProvider>,
    known_table_id: Option<crate::TableId>,
) -> std::io::Result<EccProbe> {
    let mut probe = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    let sfa_reader = crate::sfa::Reader::from_reader(&mut probe)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let toc = sfa_reader.toc();
    // Tail `meta` is authoritative for CONTENT; for the ECC descriptor the
    // two copies ARBITRATE each other: a single forged copy (its table id
    // intact, so the cross-check passes) must not dictate the walk's trailer
    // sizing, whether the forge decodes to an unrecognized value or to a
    // DIFFERENT recognized state (a forged `Off` would make the walk read
    // parity bytes as block headers and condemn a healthy SST). Both copies
    // are read; when two decodable copies disagree in ANY way the probe
    // fails safe with `Unrecognized` (skip the ECC-dependent sections with a
    // warning) — nothing out-of-band can tell which copy is legitimate.
    let mut unrecognized_seen = false;
    let mut recognized: Vec<ScrubEcc> = Vec::new();
    // The FULL decoded mirrors: a tail re-stamped to another internally-consistent
    // payload is detectable only by disagreeing with the intact `meta_mid`. Both
    // are written from one parameter set, so any decoded difference is corruption
    // or a forge. The divergence comparison below masks the ECC descriptor ONLY
    // when a mirror is unrecognized; see `mirrors_diverge`.
    let mut decoded: Vec<crate::table::meta::ParsedMeta> = Vec::new();
    for name in [b"meta".as_slice(), b"meta_mid".as_slice()] {
        let Some((pos, len)) = toc.section(name).map(|e| (e.pos(), e.len())) else {
            continue;
        };
        let Ok(size) = u32::try_from(len) else {
            continue;
        };
        let handle = crate::table::BlockHandle::new(crate::table::BlockOffset(pos), size);
        // The meta block is the ONLY read here that needs the provider: block
        // HEADERS and payload checksums are plaintext, so the section walk
        // below works on encrypted files without decrypting anything — only
        // the ECC descriptor (inside the meta payload) requires decryption.
        // The expected-id cross-check mirrors recovery's: enforced for
        // encrypted reads (the AAD binds the id anyway) AND for unencrypted
        // reads with a caller-known durable id — a checksum-clean forged tail
        // then fails the check and this loop falls back to the intact MID
        // mirror, instead of the forged tail dictating a wrong ECC descriptor
        // to the walk. Only a standalone id-less diagnostic read skips it.
        let expected_id = if encryption.is_some() {
            Some(known_table_id.unwrap_or(0))
        } else {
            known_table_id
        };
        match crate::table::meta::ParsedMeta::load_with_handle(
            probe.as_ref(),
            &handle,
            expected_id,
            encryption,
        ) {
            Ok(meta) => {
                if meta.ecc_unrecognized {
                    unrecognized_seen = true;
                } else {
                    recognized.push(if let Some(params) = meta.ecc_params {
                        ScrubEcc::Scheme(params)
                    } else {
                        ScrubEcc::Off
                    });
                }
                // Keep the FULL decoded mirror; the divergence comparison below
                // masks the ECC descriptor only when a mirror is unrecognized.
                decoded.push(meta);
            }
            // An ENVIRONMENTAL read fault must not silently drop a mirror from
            // arbitration: with one mirror gone the divergence check goes false
            // and could admit an SST under the surviving (possibly forged) copy
            // that a retry — or the right key — would expose. Propagate it. A
            // read failure on the DATA (a bad sector) or a STRUCTURAL decode
            // failure keeps the existing fallback (skip this mirror) so the
            // remaining decoded copy can still supply the ECC state.
            Err(e) if e.is_environmental() => {
                // This probe answers in `io::Result`; a non-I/O environmental
                // cause (a missing key or dictionary) carries its own message
                // through `Other` rather than being flattened into a decode
                // failure the caller would read as damage.
                return Err(match e {
                    crate::Error::Io(io) => io.into(),
                    other => std::io::Error::other(other),
                });
            }
            Err(_) => {}
        }
    }
    // Two recognized mirrors are compared in FULL: a descriptor disagreement
    // between two decodable schemes is a genuine forge. But when EITHER mirror
    // carries an unrecognized descriptor, mask the ECC fields: the arbitration
    // above tolerates a lone unrecognized sibling, so a descriptor-only forge
    // must not condemn a healthy table, while a change to a real field (e.g.
    // `created_at`) hidden behind that descriptor must still diverge.
    let mirrors_diverge = match decoded.as_slice() {
        [a, b] if unrecognized_seen => a.clone().without_ecc() != b.clone().without_ecc(),
        [a, b] => a != b,
        _ => false,
    };
    let ecc = match recognized.as_slice() {
        // Two decodable copies that agree: trustworthy.
        [a, b] if a == b => Some(*a),
        // Two decodable copies that DISAGREE: one is forged/rotted and the
        // probe cannot tell which — fail safe.
        [_, _] => Some(ScrubEcc::Unrecognized),
        // One decodable recognized copy: a lone unrecognized sibling does
        // not override it (a descriptor-only forge must not condemn a
        // healthy table whose mirror still holds the valid descriptor).
        // A genuinely newer-scheme table with the OTHER mirror re-stamped to a
        // recognized value would mis-size parity here; disambiguating the two by
        // the block data (rather than trusting one descriptor) is tracked in
        // issue #582.
        [one] => Some(*one),
        [] if unrecognized_seen => Some(ScrubEcc::Unrecognized),
        [..] => None,
    };
    Ok(EccProbe {
        ecc,
        mirrors_diverge,
    })
}

/// Result of [`read_ecc_params_out_of_band`]: the arbitrated ECC state plus
/// whether the two FULLY-decoded meta mirrors disagree in any field.
/// The data-walk start offset for a possibly-RESTRICTED SST verified
/// out-of-band with no caller-known punch offset. A valid colocated
/// `.restrict-bound` sidecar proves a committed tight-space restriction (it is
/// written strictly after the slice's install commits), so an all-zero run
/// inside the data section is an intentionally hole-punched consumed block.
/// The walk starts past the LAST such run — not at the first nonzero byte:
/// the reclaim punches top-down and stops at its first failure, so a partial
/// reclaim leaves intact consumed blocks BELOW the holes it did punch, and
/// anchoring at the first nonzero byte would put those holes back inside the
/// walk and condemn a healthy SST. Without the sidecar the derive returns `0`
/// and every zero stays part of the walk, flagging loudly — zeroed-out data on
/// an unrestricted table is destruction, not reclaim.
///
/// `known_table_id`: a sidecar recorded for a DIFFERENT id is ignored — a
/// stale or foreign sidecar must not silence zeroed blocks of an unrelated
/// table. A standalone tool passes `None`, and the identity then comes from the
/// SST's own file name (tables are stored under their numeric id). A name that
/// carries no id leaves the sidecar unmatchable, and an unmatchable sidecar
/// never skips: the zeros stay in the walk and flag, which is the fail-closed
/// direction (destruction misread as reclaim would pronounce the file healthy).
///
/// Best-effort: any probe or read failure falls back to `0` (the loud
/// default). An ENCRYPTED sidecar with no provider reads as corrupt and also
/// falls back — encrypted restricted SSTs need the provider-carrying path.
#[cfg(feature = "std")]
fn restricted_data_start(
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    path: &std::path::Path,
    encryption: Option<&alloc::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
    known_table_id: Option<crate::TableId>,
) -> u64 {
    // The frontier is derived by WALKING THE FRAMES, never by searching for
    // zero runs at arbitrary byte positions. A punch reclaims whole blocks, so
    // a reclaimed region is exactly a run of block extents that read as zeros —
    // and only positions the walk has proven to be block boundaries are ever
    // tested. Scanning raw byte runs instead would accept a live block whose
    // VALUE payload happens to end in zeros followed by the next real header,
    // moving the frontier past an intact block and making the verifier skip it
    // (and any corruption inside it) while still reporting OK.
    // The caller's id when it has one, else the id the file name carries.
    let expected_id = known_table_id.or_else(|| {
        path.file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.parse::<crate::TableId>().ok())
    });
    let bound = match crate::restrict_bound::read(&**fs, path, encryption.map(|e| &**e)) {
        Ok(crate::restrict_bound::SidecarRead::Present(sidecar_id, bound))
            if expected_id == Some(sidecar_id) =>
        {
            bound
        }
        _ => return 0,
    };
    // Where that bound actually falls, read from the table's own index. This is
    // the only authority on the frontier: a committed restriction does NOT make
    // every zero region a reclaimed one, because the prefix punch runs
    // highest-block-first and stops at its first failure — a failure on the very
    // first call leaves no hole at all, and then the first zeros the walk meets
    // are destroyed live data. The walk's answer is kept as an upper bound: the
    // standalone path cannot know a custom comparator, so an index lookup that
    // lands too high can never widen the skip beyond what the geometry shows.
    let index_frontier = index_derived_frontier(fs, path, encryption, expected_id, &bound);
    let Ok(mut file) = fs.open(path, &crate::fs::FsOpenOptions::new().read(true)) else {
        return 0;
    };
    let Ok(meta) = crate::fs::FsFile::metadata(&*file) else {
        return 0;
    };
    let file_len = meta.len;
    // Scan only the DATA section: other sections legitimately contain long
    // zero stretches (padding, sparse index entries) that must not move the
    // data frontier. Without a readable TOC there is no section to scan.
    let Ok(reader) = crate::sfa::Reader::from_reader(&mut file) else {
        return 0;
    };
    let Some((data_pos, data_len)) = reader
        .toc()
        .iter()
        .find(|e| e.name() == b"data")
        .map(|e| (e.pos(), e.len()))
    else {
        return 0;
    };
    let data_end = data_pos.saturating_add(data_len).min(file_len);
    let mut offset = data_pos;
    // End of the last block extent proven to be wholly zeroed.
    let mut frontier = data_pos;
    while offset < data_end {
        // A live frame steps over itself WITHOUT its payload being inspected,
        // so whatever bytes a value happens to hold can never be mistaken for
        // reclaimed space.
        if let Some(header) = block_header_at(&*file, offset) {
            let step = u64::from(header.on_disk_size());
            if step == 0 {
                return 0; // Malformed length: refuse to guess a frontier.
            }
            offset = offset.saturating_add(step);
            continue;
        }
        // No frame here. Either this is reclaimed space or the file is
        // damaged; the two are told apart by whether the bytes up to the NEXT
        // frame boundary are all zero.
        let Some(next) = next_block_header(&*file, offset, data_end) else {
            // Nothing frames the rest of the section: a zero tail is reclaimed
            // space, anything else is damage this must not paper over.
            if extent_is_zeroed(&*file, offset, data_end) {
                frontier = data_end;
            }
            break;
        };
        if extent_is_zeroed(&*file, offset, next) {
            // The FIRST reclaimed gap fixes the frontier, and the derivation
            // ends there. A reclaim works top-down from the start of the data
            // section, so its holes are the earliest ones in the file — a
            // partially completed pass can leave an intact block ahead of them
            // (it stops at its first failure), but never live data ahead of a
            // LATER hole. So a gap that appears after this one is a live block
            // that damage DESTROYED, and letting it advance the frontier too
            // would start verification past the loss and pronounce the file
            // healthy.
            frontier = next;
            break;
        }
        offset = next;
    }
    // `data_pos` means no validated punched extent was found: nothing to skip.
    if frontier == data_pos {
        return 0;
    }
    // Both answers bound the skip: the index says where the restriction ends,
    // the walk says how far the reclaimed geometry actually reaches. Skipping
    // past either would step over live data.
    index_frontier.map_or(0, |from_index| from_index.min(frontier))
}

/// The offset the restriction `bound` maps to in this SST's block index, or
/// `None` when the table cannot be opened (which is often the very reason it is
/// being verified) — the caller then skips nothing.
///
/// Opened with the DEFAULT comparator: a standalone verification has no tree
/// context. A custom-comparator tree can therefore land on a different block,
/// which is why the caller uses this as one of two bounds rather than as the
/// frontier outright.
#[cfg(feature = "std")]
fn index_derived_frontier(
    fs: &alloc::sync::Arc<dyn crate::fs::Fs>,
    path: &std::path::Path,
    encryption: Option<&alloc::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
    table_id: Option<crate::TableId>,
    bound: &[u8],
) -> Option<u64> {
    // Through the CALLER's filesystem, not `std::fs`: the table being verified
    // may live on any backend.
    let checksum =
        crate::Checksum::from_raw(crate::repair::compute_table_checksum_from(&**fs, path, 0).ok()?);
    let mut params = crate::table::RecoverParams::new(
        path.to_path_buf(),
        checksum,
        table_id.unwrap_or(0),
        alloc::sync::Arc::clone(fs),
        crate::comparator::default_comparator(),
        alloc::sync::Arc::new(crate::cache::Cache::with_capacity_bytes(1_000_000)),
    );
    params.encryption = encryption.map(alloc::sync::Arc::clone);
    let table = crate::table::Table::recover(params).ok()?;
    table.punch_offset_for(bound).ok()
}

/// Decodes the block header at `offset`, or `None` when no frame starts there.
#[cfg(feature = "std")]
fn block_header_at(
    file: &dyn crate::fs::FsFile,
    offset: u64,
) -> Option<crate::table::block::Header> {
    use crate::coding::Decode;
    let bytes = crate::file::read_exact(file, offset, crate::table::block::Header::MAX_LEN).ok()?;
    crate::table::block::Header::decode_from(&mut &bytes[..]).ok()
}

/// The offset of the next decodable block header at or after `from`, bounded
/// by `end`. Used to bound a candidate reclaimed extent by the frame that
/// follows it rather than by an arbitrary byte position.
///
/// A header always opens with [`crate::file::MAGIC_BYTES`], so candidate
/// offsets are found by scanning bulk-read chunks for that first byte and
/// decoding only there. A reclaimed prefix is zeros, which contain no candidate
/// at all — without this filter a multi-gigabyte prefix would cost one
/// header-sized read PER BYTE, which turns the diagnostic verifier into a hang
/// on exactly the tight-space files it exists to inspect.
#[cfg(feature = "std")]
fn next_block_header(file: &dyn crate::fs::FsFile, from: u64, end: u64) -> Option<u64> {
    const CHUNK: usize = 64 * 1024;
    let lead = *crate::file::MAGIC_BYTES.first()?;
    let mut at = from;
    while at < end {
        let want = usize::try_from(end - at).unwrap_or(CHUNK).min(CHUNK);
        let chunk = crate::file::read_exact(file, at, want).ok()?;
        // Candidates are located in the chunk but DECODED from the file at
        // their absolute offset, so a header whose bytes run past the chunk end
        // is still read in full — no chunk overlap is needed.
        for (i, _) in chunk.iter().enumerate().filter(|&(_, &b)| b == lead) {
            let offset = at.saturating_add(i as u64);
            if block_header_at(file, offset).is_some() {
                return Some(offset);
            }
        }
        at = at.saturating_add(want as u64);
    }
    None
}

/// Whether `[start, end)` reads back as all zeros — the hole-punch signature.
#[cfg(feature = "std")]
fn extent_is_zeroed(file: &dyn crate::fs::FsFile, start: u64, end: u64) -> bool {
    const CHUNK: usize = 64 * 1024;
    let mut at = start;
    while at < end {
        let want = usize::try_from(end - at).unwrap_or(CHUNK).min(CHUNK);
        let Ok(bytes) = crate::file::read_exact(file, at, want) else {
            return false;
        };
        if bytes.iter().any(|&b| b != 0) {
            return false;
        }
        at += want as u64;
    }
    end > start
}

/// Result of [`read_ecc_params_out_of_band`]: the arbitrated ECC state plus
/// whether the two FULLY-decoded meta mirrors disagree in any field.
#[cfg(feature = "std")]
struct EccProbe {
    ecc: Option<ScrubEcc>,
    mirrors_diverge: bool,
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
    // Byte offset to START the DATA-section walk at: `0` for a normal table, or
    // the punch offset of a tight-space RESTRICTED view whose `[0, data_start)`
    // data blocks were hole-punched (they read as zeros and would false-flag as
    // corruption). All other sections (index, meta, TLI …) sit past the data
    // region and are always walked in full.
    data_start: u64,
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
    // Capture the sfa error's Debug form in the message. crate::io::Error is
    // message-only (no source chain) so it stays portable on no_std; the `{:?}`
    // repr keeps the original variant (InvalidHeader / InvalidVersion /
    // ChecksumMismatch / underlying Io) visible for downstream diagnostics, just
    // as a string rather than a downcastable `Error::source()`.
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
    // prefixed with the standard `Header`). Raw-format sections carry
    // NO per-section checksum (the SFA-trailer checksum covers only
    // the TOC bytes), so they get structural shape validation via
    // `raw_section_shape_error` instead of a block walk. New section
    // names default to "walk" (must be added to `RAW_FORMAT_SECTIONS`
    // if they're raw), so a forgotten-to-handle section fails loud
    // rather than silently passing a corruption.

    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut blocks_scanned: usize = 0;
    let mut errors: Vec<BlockVerifyError> = Vec::new();

    // The writer emits sections strictly back-to-back (the first at offset 0,
    // each next where the previous ended, the last ending where the TOC
    // begins), so the entries must exactly tile `[0, toc_pos)`. The SFA
    // trailer checksum is unkeyed, so a re-stamped TOC could otherwise OMIT a
    // correctness-bearing entry entirely — `delete_bitmap` and
    // `range_tombstones` are optional at parse time, so a vanished section
    // resurrects deleted rows while every remaining block still passes its
    // byte-level checks. The tiling gap the omission leaves is the only
    // out-of-band trace; report it and keep walking the sections that ARE
    // present (their findings are still valid).
    // Classify catalogue-structure defects (duplicate / shadowing names, tiling
    // gaps, unrecognized names, a trailing hole) through the SAME pass
    // `toc_may_hide_deletion_section` uses, so the walk's `TocCorrupted` findings
    // and the salvage-verdict concealment check can never diverge. A re-stamped
    // TOC that duplicates a recognized name, or renames a section to hide it,
    // preserves the byte-level checks yet steers `Toc::section` / `ParsedRegions`
    // away from the real section — resurrecting the range it masked.
    for defect in toc_catalogue_defects(toc, sfa_reader.toc_pos()) {
        errors.push(BlockVerifyError::TocCorrupted {
            table_id,
            path: path.to_path_buf(),
            section_name: defect.name,
            section_offset: defect.offset,
            reason: defect.reason,
        });
    }
    // One reusable data buffer across the whole SST — sized up via
    // `resize` per block instead of a fresh `vec![0u8; N]` allocation
    // each iteration. On large trees this turns thousands of malloc
    // calls into a single growing allocation that settles at the
    // largest block size seen.
    let mut data_buf: Vec<u8> = Vec::new();
    // Same story for the Page-ECC parity trailer (read for alignment and,
    // when the codecs are compiled in, verified against fresh parity).
    let mut parity_buf: Vec<u8> = Vec::new();

    for entry in toc.iter() {
        if RAW_FORMAT_SECTIONS.contains(&entry.name()) {
            // Raw sections carry NO per-section checksum (the SFA trailer
            // checksum covers only the TOC bytes), so validate their SHAPE
            // where one is defined; a heal-enabled scrub relies on this walk
            // before restamping the manifest digest, and skipping a broken
            // blob-link list would launder it. Rot INSIDE a structurally
            // valid payload (a flipped id byte) remains undetectable here —
            // these sections have no integrity bytes to check against.
            match raw_section_shape_error(&mut reader, entry.name(), entry.pos(), entry.len()) {
                Ok(Some(reason)) => {
                    errors.push(BlockVerifyError::TocCorrupted {
                        table_id,
                        path: path.to_path_buf(),
                        section_name: entry.name().to_vec(),
                        section_offset: entry.pos(),
                        reason,
                    });
                }
                Ok(None) => {}
                // A transient read validating a raw section is retryable I/O, not
                // corruption: record it as a DataReadError the repair verdict aborts on.
                Err(e) => {
                    errors.push(BlockVerifyError::DataReadError {
                        table_id,
                        path: path.to_path_buf(),
                        offset: entry.pos(),
                        data_length: 0,
                        error: e,
                    });
                }
            }
            continue;
        }
        // A restricted view's punched data-block prefix reads as zeros; start
        // the DATA walk at `data_start` so those blocks are not framed (only the
        // data section is punched — every other section is walked in full). The
        // straddling block at `data_start` is intact (the punch begins at its
        // boundary), so `start.max(data_start)` lands on a real block header.
        let start = if entry.name() == b"data" {
            entry.pos().max(data_start)
        } else {
            entry.pos()
        };
        // `checked_add` (not `saturating_add`) so a corrupted or
        // forged TOC length cannot silently collapse to `u64::MAX`
        // and let the walk treat the whole address space as one
        // section. On overflow we surface the section as a
        // file-level `TocCorrupted` and skip walking it — the other
        // (still-walkable) sections of the same SST are honoured.
        // `TocCorrupted` rather than `HeaderCorrupted` because the
        // failure is at the section-catalogue layer, not inside any
        // individual block.
        let Some(end) = entry.pos().checked_add(entry.len()) else {
            // Report the DECLARED TOC offset, not the walk start: the overflow
            // is computed from `entry.pos()`, and for a restricted `data`
            // section `start` is the live frontier — a different number, which
            // would send repair and forensic readers to the wrong entry.
            let declared = entry.pos();
            errors.push(BlockVerifyError::TocCorrupted {
                table_id,
                path: path.to_path_buf(),
                section_name: entry.name().to_vec(),
                section_offset: declared,
                reason: format!(
                    "section length {} overflows u64 when added to start offset {declared}",
                    entry.len(),
                ),
            });
            continue;
        };
        // Mid-walk seek failure: a forged offset still seeks fine, so a seek
        // failure is a TRANSIENT I/O fault, not catalogue corruption. Record it as
        // a `DataReadError` (which carries the I/O kind) so the repair verdict
        // treats it as retryable and aborts, rather than routing a healthy SST
        // through salvage over a flaky read. Keep walking other sections (the
        // finding still surfaces; the caller decides).
        if let Err(e) = reader.seek(SeekFrom::Start(start)) {
            errors.push(BlockVerifyError::DataReadError {
                table_id,
                path: path.to_path_buf(),
                offset: start,
                data_length: 0,
                error: e.into(),
            });
            continue;
        }
        // Skip a section name this build does not know: its role expectation is
        // unknowable, so a walk would prove nothing. `toc_catalogue_defects`
        // above already reported it as a `TocCorrupted` finding (a re-stamped
        // TOC can RENAME a known section out of every reader's sight while its
        // blocks still pass their byte-level checks).
        let Some(expected_roles) = expected_section_roles(entry.name()) else {
            continue;
        };
        let mut ctx = WalkCtx {
            reader: &mut reader,
            table_id,
            path,
            data_buf: &mut data_buf,
            parity_buf: &mut parity_buf,
            blocks_scanned: &mut blocks_scanned,
            errors: &mut errors,
            max_data_length: block_data_length_cap(max_enc_overhead),
            ecc,
            ecc_unrecognized,
            expected_roles,
        };
        walk_block_region(&mut ctx, start, end);
    }

    Ok(PerFileScan {
        blocks_scanned,
        errors,
    })
}

/// SFA TOC section names whose payload is NOT a sequence of `Block`s
/// (i.e. NOT prefixed with the standard `Header`). These sections carry NO
/// per-section checksum (the SFA-trailer checksum covers only the TOC
/// bytes), so the walk validates their SHAPE via
/// [`raw_section_shape_error`] instead of decoding blocks. Every other
/// section (`data` / `tli` / `tli_tail` / `index` / `filter_tli` /
/// `filter` / `range_tombstones` / `meta` / `meta_mid`) is a
/// `Header`-prefixed block run and gets walked. See `scan_sst_blocks` for
/// the full section catalogue and the writer-side source of truth.
///
/// `meta_separator` is the 4 KiB zero-padding section the writer
/// emits between the MID and TAIL meta blocks so a single bad
/// filesystem sector cannot take out both copies — it carries no
/// blocks and must be skipped here, otherwise the walker would try
/// to decode zeros as a `Header` and report a spurious
/// `HeaderCorrupted` on every clean SST.
const RAW_FORMAT_SECTIONS: &[&[u8]] = &[b"linked_blob_files", b"table_version", b"meta_separator"];

/// Block ROLE(S) the writer emits into each named block-format SFA section.
/// The walk cross-checks every decoded header against its section: a
/// checksum-clean block whose `block_type` was re-stamped (a filter block
/// relabeled as Data) passes every byte-level check, so this is the only
/// out-of-band detector before the heal's digest reconciliation would
/// launder the forge into the manifest.
///
/// `None` for a section name this build does not know — the CALLER fails
/// closed on it (an error, not a skipped check): the SFA trailer checksum is
/// unkeyed, so a re-stamped TOC can RENAME a known section (hiding it from
/// every reader — vanished range tombstones resurrect deleted ranges) while
/// each block inside still passes its byte-level checks. A future section
/// name therefore requires extending this map in the same change that adds
/// the writer section.
///
/// This role check is BYTE-LEVEL only. A section whose block is
/// checksum-clean and correctly-roled but whose PAYLOAD was re-stamped to
/// another structurally valid value (a redirected `locator`, a shrunk
/// `zone_map` range, a widened `seqno_bounds`) passes here yet still lies to
/// the read path. Those SEMANTIC cross-checks — comparing the section's
/// decoded content against the blocks it summarizes — live on `Table`
/// (`verify_locator` / `verify_zone_map` / `verify_seqno_bounds` /
/// `verify_tli_mirrors` / `verify_block_entry_counts`) and are driven by the
/// repair verdict and the heal digest reconciliation, not by this walk.
fn expected_section_roles(name: &[u8]) -> Option<&'static [crate::table::block::BlockType]> {
    use crate::table::block::BlockType;
    Some(match name {
        b"data" => &[BlockType::Data, BlockType::Columnar],
        // `filter_tli` is the top-level index OVER filter partitions — the
        // writer emits it with the Index role (same encoding as the data
        // TLI), so expecting Filter here would flag a healthy
        // partitioned-filter SST as corrupt.
        b"index" | b"tli" | b"tli_tail" | b"filter_tli" => &[BlockType::Index],
        b"filter" => &[BlockType::Filter],
        b"range_tombstones" => &[BlockType::RangeTombstone],
        b"meta" | b"meta_mid" => &[BlockType::Meta],
        b"block_layout" => &[BlockType::BlockLayout],
        b"seqno_bounds" => &[BlockType::SeqnoBounds],
        b"zone_map" => &[BlockType::ZoneMap],
        b"delete_bitmap" => &[BlockType::DeleteBitmap],
        b"locator" => &[BlockType::Locator],
        _ => return None,
    })
}

/// One structural defect in the SFA TOC catalogue: a section a reader could not
/// reach or that hides another. Carries the offending entry's name, its declared
/// offset, and a human-readable reason.
struct TocCatalogueDefect {
    name: Vec<u8>,
    offset: u64,
    reason: String,
}

/// Classifies every structural defect in the TOC catalogue in one pass: a
/// duplicate / shadowing name, a gap in the `[0, toc_pos)` tiling, an
/// unrecognized (renamed) name, or a trailing hole. Empty when the catalogue
/// tiles the whole data region with unique, recognized names.
///
/// The single source of truth for what a valid catalogue looks like:
/// [`scan_sst_blocks`] turns each defect into a `TocCorrupted` finding and
/// [`toc_may_hide_deletion_section`] fails closed on any, so the two can never
/// disagree. The recognized-name set is `expected_section_roles` ∪
/// [`RAW_FORMAT_SECTIONS`]. A `pos + len` overflow stops the tiling scan (the
/// per-section walk reports that entry via its own `checked_add`), so it is not
/// duplicated here.
fn toc_catalogue_defects(toc: &crate::sfa::Toc, toc_pos: u64) -> Vec<TocCatalogueDefect> {
    let mut defects = Vec::new();
    let mut expected_pos: u64 = 0;
    // A handful of section names — a linear scan keeps this no-std-clean.
    let mut seen: Vec<&[u8]> = Vec::new();
    for entry in toc.iter() {
        let name = entry.name();
        if seen.contains(&name) {
            defects.push(TocCatalogueDefect {
                name: name.to_vec(),
                offset: entry.pos(),
                reason: format!(
                    "duplicate TOC section name {:?}; a renamed section can shadow \
                     another and hide it from the readers that look it up by name",
                    alloc::string::String::from_utf8_lossy(name),
                ),
            });
        } else {
            seen.push(name);
        }
        if entry.pos() != expected_pos {
            defects.push(TocCatalogueDefect {
                name: name.to_vec(),
                offset: entry.pos(),
                reason: format!(
                    "section starts at {} but the previous section ended at \
                     {expected_pos}; the gap hides an omitted TOC entry",
                    entry.pos(),
                ),
            });
        }
        if expected_section_roles(name).is_none() && !RAW_FORMAT_SECTIONS.contains(&name) {
            defects.push(TocCatalogueDefect {
                name: name.to_vec(),
                offset: entry.pos(),
                reason: String::from(
                    "unrecognized block-format section name; a renamed TOC entry \
                     hides a known section from every reader",
                ),
            });
        }
        let Some(end) = entry.pos().checked_add(entry.len()) else {
            expected_pos = u64::MAX;
            break;
        };
        expected_pos = end;
    }
    if expected_pos != toc_pos {
        defects.push(TocCatalogueDefect {
            name: b"<tiling>".to_vec(),
            offset: expected_pos,
            reason: format!(
                "sections end at {expected_pos} but the TOC begins at {toc_pos}; a \
                 trailing TOC entry was omitted or truncated",
            ),
        });
    }
    defects
}

/// Whether the SST's TOC catalogue could HIDE an optional deletion section
/// (`range_tombstones` / `delete_bitmap`) from the name-based readers. These
/// sections are optional at parse time, so an unkeyed re-stamp that OMITS,
/// RENAMES, or SHADOWS one leaves the parsed table reporting no deletions
/// while every remaining block still passes its byte-level checks — a positional
/// salvage would then re-emit the suppressed rows as live.
///
/// Returns `true` for the concealment classes: a duplicate/shadowing name, a
/// gap or trailing hole in the `[0, toc_pos)` tiling, an unrecognized (renamed)
/// name, or a length overflow. Returns `false` only when the catalogue tiles
/// the whole data region with UNIQUE, RECOGNIZED names — then no section is
/// hidden and the physical absence of any deletion section is established.
///
/// The recognized-name set mirrors the walk in [`scan_sst_blocks`] exactly
/// (`expected_section_roles` ∪ [`RAW_FORMAT_SECTIONS`]), so a healthy table
/// grades `false`.
///
/// Consumed by salvage-mode repair: a `Corrupt` verdict caused by one of these
/// classes must be QUARANTINED, not salvaged, because the positional salvage
/// walk reopens the same forged catalogue and resurrects the suppressed rows.
///
/// This catches only concealment that DISTURBS the catalogue (a missing,
/// duplicated, or unrecognized name, or a tiling gap). A relabel that keeps the
/// catalogue uniquely named and perfectly tiled — a deletion section RENAMED to
/// an unused recognized name with its block re-roled — grades `false` here; it
/// is caught instead inside salvage, which fails closed when the open degrades a
/// rebuildable section that did not decode as its claimed type (see
/// `Table::salvage_degraded_a_rebuildable_section`).
pub(crate) fn toc_may_hide_deletion_section(toc: &crate::sfa::Toc, toc_pos: u64) -> bool {
    !toc_catalogue_defects(toc, toc_pos).is_empty()
}

/// Structural validation for the raw (non-block-format) sections; returns a
/// human-readable reason when the section's payload cannot have the shape
/// the writer emits.
///
/// - `linked_blob_files`: `u32 count` followed by `count` fixed 32-byte
///   records — the length must be exactly `4 + count * 32`.
/// - `table_version`: exactly one byte.
/// - `meta_separator`: pure padding, any content is acceptable.
///
/// This is SHAPE validation only: these sections carry no checksum, so rot
/// inside a structurally valid payload is undetectable out-of-band.
///
/// `Err` is a TRANSIENT read/seek fault (retryable I/O), kept distinct from a
/// structural shape defect (`Ok(Some(reason))`) so the caller can route it to an
/// I/O finding the repair verdict treats as retryable rather than as corruption.
fn raw_section_shape_error(
    reader: &mut io::BufReader<Box<dyn crate::fs::FsFile>>,
    name: &[u8],
    pos: u64,
    len: u64,
) -> Result<Option<String>, io::Error> {
    use alloc::string::ToString as _;
    #[cfg(not(feature = "std"))]
    use io::{Read as _, Seek as _, SeekFrom};
    #[cfg(feature = "std")]
    use std::io::{Read as _, Seek as _, SeekFrom};

    match name {
        b"linked_blob_files" => {
            if len < 4 {
                return Ok(Some(format!(
                    "linked_blob_files section is {len} bytes, too short for its count prefix"
                )));
            }
            reader.seek(SeekFrom::Start(pos))?;
            let mut count_le = [0u8; 4];
            reader.read_exact(&mut count_le)?;
            let count = u64::from(u32::from_le_bytes(count_le));
            // 4 fixed u64 fields per record.
            let expected = count
                .checked_mul(32)
                .and_then(|records| records.checked_add(4));
            if expected != Some(len) {
                return Ok(Some(format!(
                    "blob-link count {count} disagrees with the section length {len} \
                     (expected {} bytes)",
                    expected.map_or_else(|| "overflowing".to_string(), |e| e.to_string()),
                )));
            }
            Ok(None)
        }
        b"table_version" => {
            Ok((len != 1).then(|| format!("table_version section is {len} bytes, expected 1")))
        }
        // Padding: carries no data, nothing to validate.
        _ => Ok(None),
    }
}

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
    /// Reused buffer for each block's Page-ECC parity trailer: consumed for
    /// walk alignment and, on a build with the ECC codecs, verified against
    /// parity freshly recomputed over the payload.
    parity_buf: &'a mut Vec<u8>,
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
    /// Roles the current section's blocks may legitimately carry (from
    /// [`expected_section_roles`]; the caller fails closed on an unknown
    /// name before building this context). A decoded header whose
    /// `block_type` is not in the list is reported — see the helper's docs
    /// for why this check is load-bearing.
    expected_roles: &'static [crate::table::block::BlockType],
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
            // A TRANSIENT read fault decoding the header is retryable, not
            // corruption: record it as a DataReadError so the repair verdict
            // aborts instead of salvaging a healthy table over a flaky read.
            Err(crate::Error::Io(e)) => {
                ctx.errors.push(BlockVerifyError::DataReadError {
                    table_id: ctx.table_id,
                    path: ctx.path.to_path_buf(),
                    offset,
                    data_length: 0,
                    error: e,
                });
                return;
            }
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

        // Role cross-check: a checksum-clean block whose `block_type` was
        // re-stamped (a filter block relabeled as Data) passes every
        // byte-level check below, so the section-vs-role comparison is the
        // only out-of-band detector. Reported and then walked normally —
        // the header is internally valid, so the offsets stay trustworthy.
        if !ctx.expected_roles.contains(&header.block_type) {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "block role {:?} does not belong to this section (expected one of {:?})",
                    header.block_type, ctx.expected_roles,
                ),
            });
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
        // Hard cap on the parity trailer, mirroring the data_length cap
        // below: a syntactically valid but absurd shard layout (e.g.
        // RS(1,255), every payload byte amplified 255x into parity) drives
        // `expected_parity_len` toward its u32::MAX saturation point, and a
        // lying TOC length (forged, or a sparse file) would let the buffered
        // verify reserve that whole multi-GB trailer before any corruption
        // is reported. No real configuration produces a trailer above the
        // payload cap itself.
        if parity_len > MAX_BLOCK_DATA_LENGTH {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "parity trailer length {parity_len} exceeds hard cap {MAX_BLOCK_DATA_LENGTH}",
                ),
            });
            return;
        }

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
        let payload_clean = computed == header.checksum;
        if !payload_clean {
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
        // the next block's header — it MUST advance exactly `parity_len` bytes
        // or the next iteration mis-reads parity as a header. The trailer is
        // read into a buffer (not drained) so a build with the ECC codecs can
        // also VERIFY it: the payload checksum never covers the trailer, so
        // rot confined to parity reads as a clean block while its ECC is dead.
        if parity_len > 0 {
            let parity_usize = usize::try_from(parity_len).unwrap_or(usize::MAX);
            ctx.parity_buf.resize(parity_usize, 0);
            // A short read (EOF before `parity_len`) and an underlying read
            // error are the same outcome for the scrub: the trailer cannot be
            // consumed, so report a single DataReadError. (`read_exact`
            // retries `Interrupted` internally.)
            if let Err(e) = ctx.reader.read_exact(ctx.parity_buf.as_mut_slice()) {
                ctx.errors.push(BlockVerifyError::DataReadError {
                    table_id: ctx.table_id,
                    path: ctx.path.to_path_buf(),
                    offset,
                    data_length: header.data_length,
                    error: e.into(),
                });
                return;
            }
            // Compare the stored trailer against parity freshly computed over
            // the payload — only when the payload itself is checksum-clean (a
            // corrupt payload legitimately mismatches its original trailer and
            // is already reported as DataCorrupted above). Only a build with
            // the ECC codecs can recompute parity; without `page_ecc` the
            // trailer is consumed for alignment but stays unverified (that
            // build cannot consume it on the read path either).
            #[cfg(feature = "page_ecc")]
            if payload_clean && let Some(scheme) = block_ecc {
                let fresh = match scheme {
                    crate::table::block::EccParams::Secded => {
                        Some(crate::secded::encode_block_parity(ctx.data_buf))
                    }
                    crate::table::block::EccParams::Shard { .. } => {
                        let (ds, ps) = scheme.as_shards();
                        crate::ecc::encode_parity(ctx.data_buf, ds, ps).ok()
                    }
                };
                // An encoder that rejects a shape the writer accepted, or a
                // trailer that differs from the recomputed parity, both mean
                // the block's ECC cannot be trusted — fail loud either way.
                if fresh.as_deref() != Some(ctx.parity_buf.as_slice()) {
                    ctx.errors.push(BlockVerifyError::EccParityMismatch {
                        table_id: ctx.table_id,
                        path: ctx.path.to_path_buf(),
                        offset,
                        data_length: header.data_length,
                    });
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
mod block_verify_tests;
