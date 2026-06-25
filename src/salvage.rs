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
use alloc::string::String;
use alloc::vec::Vec;
use std::path::PathBuf;

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
    /// Total data blocks the walk inspected (recovered plus dropped).
    pub blocks_total: usize,
    /// Data blocks successfully re-emitted into the salvaged SST.
    pub blocks_salvaged: usize,
    /// Entries recovered into the salvaged SST.
    pub entries_salvaged: u64,
    /// Blocks the walk had to drop, with their key ranges where known.
    pub dropped: Vec<DroppedBlock>,
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
    ///     entries_salvaged: 100,
    ///     dropped: Vec::new(),
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
/// The salvaged copy preserves the source's data-block compression and
/// error-correcting parameters. A columnar source is recovered as rows (the
/// loader reconstructs row entries), holding the same keys and values; the
/// columnar sidecars (zone map, delete bitmap) are not carried over yet. The
/// source is opened in salvage mode, so a corrupt delete-bitmap degrades to
/// "all rows live" rather than failing the open.
///
/// The walk is positional (block-index order), so it uses the default
/// comparator regardless of the source's: iteration is not comparator-driven
/// and the recovered entries keep their on-disk order.
///
/// # Errors
///
/// Returns an error when `source` cannot be opened at all (its metadata, index,
/// or SFA trailer is unreadable) or when writing `dest` fails. Per-block
/// corruption is not an error: such blocks are dropped and listed in the
/// returned [`SalvageReport`].
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
    use alloc::sync::Arc;

    let comparator = crate::comparator::default_comparator();
    // Digest the source through the injected `Fs`, not `std::fs`: salvage runs
    // over MemFs / fault-injected / routed backends (repair passes its own `fs`),
    // where a direct `std::fs` read would miss the file or hash the wrong bytes.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, source)?);
    let cache = Arc::new(crate::cache::Cache::with_capacity_bytes(8 * 1024 * 1024));
    let descriptor = Arc::new(crate::descriptor_table::DescriptorTable::new(64));
    #[cfg(feature = "metrics")]
    let metrics = Arc::new(crate::Metrics::default());

    let table = crate::table::Table::recover_inner(
        source.to_path_buf(),
        checksum,
        0,
        0,
        0,
        cache,
        Some(descriptor),
        Arc::clone(fs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        comparator.clone(),
        #[cfg(feature = "metrics")]
        metrics,
        // Salvage mode: a corrupt delete-bitmap / missing zone map degrades to
        // "all rows live" instead of failing, so a damaged sidecar still opens.
        true,
    )?;

    let writer = crate::table::Writer::new(dest.clone(), table.id(), 0, Arc::clone(fs))?
        .use_data_block_compression(table.metadata.data_block_compression)
        .use_ecc(table.metadata.ecc_params);

    let walk = match salvage_blocks(&table, writer, &comparator) {
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
        entries_salvaged: walk.entries_salvaged,
        dropped: walk.dropped,
    })
}

/// The tally a [`salvage_blocks`] walk returns: the report counters plus whether
/// a destination file was actually finished (`wrote`), which the caller uses to
/// decide between keeping `dest` and removing the empty placeholder.
struct SalvageWalk {
    blocks_total: usize,
    blocks_salvaged: usize,
    entries_salvaged: u64,
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

/// Walks `table`'s data blocks in index order, re-emitting every block that
/// loads and decodes cleanly into `writer` and recording the rest.
///
/// Consumes `writer`: on success it is finished (when at least one block was
/// emitted) or dropped (when none were). On a `write` / `finish` error the
/// writer is dropped as the error unwinds, so the caller must remove the partial
/// destination it left behind.
fn salvage_blocks(
    table: &crate::table::Table,
    mut writer: crate::table::Writer,
    comparator: &crate::comparator::SharedComparator,
) -> crate::Result<SalvageWalk> {
    use crate::table::block::ParsedItem;
    use alloc::format;

    let mut blocks_total = 0usize;
    let mut blocks_salvaged = 0usize;
    let mut entries_salvaged = 0u64;
    let mut dropped: Vec<DroppedBlock> = Vec::new();
    // Lower bound for a dropped block's range: the previous block's last key,
    // since the index stores each block's last key (so block N covers
    // `(end_key[N-1], end_key[N]]`).
    let mut prev_end: Option<UserKey> = None;

    for handle in table.data_block_handles() {
        blocks_total += 1;
        let keyed = match handle {
            Ok(k) => k,
            Err(e) => {
                // A corrupt index entry: the block's handle and range are
                // unknown, and once the index stream desyncs later offsets are
                // unknowable too, so stop the walk after reporting it.
                dropped.push(DroppedBlock {
                    offset: 0,
                    section: b"index".to_vec(),
                    reason: DropReason::HeaderCorrupted(format!("{e:?}")),
                    key_range: None,
                });
                break;
            }
        };
        let end_key = keyed.end_key().clone();
        let offset = *keyed.as_ref().offset();
        match table.load_data_block(keyed.as_ref()) {
            // `try_iter`, not `iter`: a checksum-clean but structurally malformed
            // block (e.g. an invalid trailer) must be reported as a dropped
            // `DecodeError`, never panic the salvage walk. `blocks_salvaged` is
            // counted only once the whole block decoded and was written.
            Ok(Some(block)) => match block.try_iter(comparator.clone()) {
                Ok(iter) => {
                    for parsed in iter {
                        writer.write(parsed.materialize(block.as_slice()))?;
                        entries_salvaged += 1;
                    }
                    blocks_salvaged += 1;
                }
                Err(e) => dropped.push(DroppedBlock {
                    offset,
                    section: b"data".to_vec(),
                    reason: DropReason::DecodeError(format!("{e:?}")),
                    key_range: Some((
                        prev_end.clone().unwrap_or_else(UserKey::empty),
                        end_key.clone(),
                    )),
                }),
            },
            // A wholly-deleted columnar block carries no live keys: nothing to
            // recover and nothing lost.
            Ok(None) => {}
            Err(e) => {
                // Classify the failure so the report distinguishes a bit-rot
                // checksum mismatch from a structural decode error from a raw
                // read / decompress failure.
                let reason = match &e {
                    crate::Error::ChecksumMismatch { .. } => DropReason::ChecksumMismatch,
                    crate::Error::InvalidHeader(_) | crate::Error::InvalidTag(_) => {
                        DropReason::DecodeError(format!("{e:?}"))
                    }
                    _ => DropReason::ReadError(format!("{e:?}")),
                };
                dropped.push(DroppedBlock {
                    offset,
                    section: b"data".to_vec(),
                    reason,
                    key_range: Some((
                        prev_end.clone().unwrap_or_else(UserKey::empty),
                        end_key.clone(),
                    )),
                });
            }
        }
        prev_end = Some(end_key);
    }

    let wrote = blocks_salvaged > 0;
    if wrote {
        writer.finish()?;
    } else {
        drop(writer);
    }

    Ok(SalvageWalk {
        blocks_total,
        blocks_salvaged,
        entries_salvaged,
        dropped,
        wrote,
    })
}

#[cfg(test)]
mod tests;
