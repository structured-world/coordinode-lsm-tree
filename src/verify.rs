// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{checksum::Checksum, coding::Decode, table::TableId, table::block::Header};
use std::path::{Path, PathBuf};

/// Describes a single integrity error found during verification.
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
        error: std::io::Error,
    },
}

impl std::fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::error::Error for IntegrityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
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
                    error: e,
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
                    error: e,
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
        error: std::io::Error,
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
        /// its data segment started at `offset + Header::serialized_len()`.
        offset: u64,
        /// Length the (clean) header advertised for the data segment.
        data_length: u32,
        /// Underlying I/O error from the failed data-segment read.
        /// Kept as `std::io::Error` (matching `SstFileUnreadable`) so
        /// `ErrorKind` / OS code stay available to callers and so
        /// `Error::source()` produces a coherent chain.
        error: std::io::Error,
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

impl std::fmt::Display for BlockVerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::error::Error for BlockVerifyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SstFileUnreadable { error, .. } | Self::DataReadError { error, .. } => {
                Some(error)
            }
            _ => None,
        }
    }
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
}

impl BlockVerifyReport {
    /// `true` if every block in every SST verified clean.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
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
///    Advance by `Header::serialized_len() + data_length` until the
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
    let version = tree.current_version();
    let mut report = BlockVerifyReport::default();

    for table in version.iter_tables() {
        let path: &Path = &table.path;
        let table_id = table.id();
        report.sst_files_scanned += 1;

        // Use each Table's own `Fs` handle (StdFs, MemFs, IoUring, …).
        // `std::fs::File::open` is wrong here: it skips the pluggable
        // backend and produces NotFound on MemFs-only trees. Encryption
        // overhead is also per-table (different keys / AEAD suites can
        // attach to different SSTs once #251 lands), so feed each
        // table's `max_overhead()` separately.
        let max_enc_overhead = table.encryption.as_ref().map_or(0u32, |e| e.max_overhead());
        match scan_sst_blocks(&*table.fs, path, table_id, max_enc_overhead) {
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
    }

    report
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

    match scan_sst_blocks(&fs, path, 0, 0) {
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
) -> std::io::Result<PerFileScan> {
    use std::io::{BufReader, Seek, SeekFrom};

    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;

    // The SFA trailer + TOC live at the tail of the file.
    // sfa::Reader::from_reader leaves the cursor at an undefined
    // offset; each per-section walk below explicitly seeks to the
    // section's `pos()` first so the unknown post-trailer position
    // doesn't matter.
    // Wrap the sfa error as the inner cause of the io::Error rather
    // than format!-stringifying it, so the original variant
    // (InvalidHeader / InvalidVersion / ChecksumMismatch / underlying
    // Io) stays reachable via `Error::source()` for downstream
    // diagnostics. sfa::Error implements `std::error::Error`.
    let sfa_reader = sfa::Reader::from_reader(&mut file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let toc = sfa_reader.toc();
    // SFA TOC layout for an SST. The writer opens the file and
    // immediately calls `sfa::Writer::start("data")`, so the first
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
    reader: &'a mut std::io::BufReader<Box<dyn crate::fs::FsFile>>,
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
}

fn walk_block_region(ctx: &mut WalkCtx<'_>, start_offset: u64, end_offset: u64) {
    use std::io::Read;

    let header_len = Header::serialized_len() as u64;
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
        if remaining_in_section < header_len {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "section has only {remaining_in_section} bytes left at this offset, \
                     less than Header::serialized_len() = {header_len}",
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

        // Count the block as "header-read" immediately on successful
        // decode — matches the BlockVerifyReport.blocks_scanned docs
        // ("includes blocks where the data checksum subsequently
        // failed"). Without this early increment, blocks that emit
        // DataReadError / data-length-bounds HeaderCorrupted would
        // be silently uncounted, contradicting the documented
        // semantics.
        *ctx.blocks_scanned = ctx.blocks_scanned.saturating_add(1);

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
        let remaining = end_offset.saturating_sub(offset).saturating_sub(header_len);
        if data_length_u64 > remaining {
            ctx.errors.push(BlockVerifyError::HeaderCorrupted {
                table_id: ctx.table_id,
                path: ctx.path.to_path_buf(),
                offset,
                reason: format!(
                    "header data_length {data_length_u64} exceeds remaining section bytes {remaining}",
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
                error: e,
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

        // blocks_scanned was already incremented right after a
        // successful Header::decode_from above — do not double-count
        // here.
        offset = offset
            .saturating_add(header_len)
            .saturating_add(data_length_u64);
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
        let flip_offset = Header::serialized_len() as u64;
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
        const HEADER_LEN: u64 = Header::serialized_len() as u64;

        let header = Header {
            block_type: BlockType::Data,
            // Arbitrary sentinel; the walker reaches `read_exact` and
            // bails BEFORE any data-segment XXH3 comparison, so this
            // value is never checked.
            checksum: Checksum::from_raw(0xDEAD_BEEF_DEAD_BEEF),
            data_length: DATA_LENGTH,
            uncompressed_length: DATA_LENGTH,
        };

        // Build a minimal SFA archive: one section "data" containing
        // exactly one Header (33 bytes) and zero following data bytes.
        let mut archive_bytes: Vec<u8> = Vec::new();
        {
            let mut writer = sfa::Writer::from_writer(std::io::Cursor::new(&mut archive_bytes));
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
        // and patch the trailer's stored checksum so sfa::Reader still
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
        let scan = scan_sst_blocks(&fs, path, table_id, 0).expect("forged SFA must parse cleanly");
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
}
