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
    /// Total data blocks the walk inspected (recovered plus dropped).
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
    ///     blocks_copied_verbatim: 4,
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
        },
    )?;

    // Fail closed on range tombstones: the positional walk re-emits only point
    // entries, so salvaging an SST that carries range tombstones would drop them
    // and let lower-level keys they cover reappear after repair (a merge-semantics
    // violation). Reject until the writer path can re-emit them.
    if !table.range_tombstones().is_empty() {
        return Err(crate::Error::FeatureUnsupported(
            "salvage of an SST with range tombstones",
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
        || (!table.delete_bitmap().is_empty() && !table.delete_positions_verified());
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
        .mirror_from(&table.metadata, table.has_zone_map())
        .use_encryption(options.encryption.clone());
    #[cfg(zstd_any)]
    let writer = writer.use_zstd_dictionary(options.zstd_dictionary.clone());

    let walk = match salvage_blocks(&table, writer, comparator, !delete_mask_unpositionable) {
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
        dropped: walk.dropped,
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

/// Classifies a block load / read failure into a [`DroppedBlock`], distinguishing
/// a bit-rot checksum mismatch from a structural decode error from a raw
/// read / decompress failure, and attaching the block's `(prev_end, end_key]`
/// range as the lower/upper bound of the lost keys.
fn classify_drop(
    e: &crate::Error,
    offset: u64,
    prev_end: Option<&UserKey>,
    end_key: &UserKey,
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
        key_range: Some((
            prev_end.cloned().unwrap_or_else(UserKey::empty),
            end_key.clone(),
        )),
    }
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
) -> crate::Result<SalvageWalk> {
    use crate::table::block::ParsedItem;
    use alloc::format;

    let mut blocks_total = 0usize;
    let mut blocks_salvaged = 0usize;
    let mut blocks_copied_verbatim = 0usize;
    let mut entries_salvaged = 0u64;
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
                // Re-emit each block as a delete-masked batch so the recovered copy
                // keeps any (readable) deletes applied.
                match table.load_columnar_block_masked(keyed.as_ref()) {
                    Ok(Some(batch)) => {
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
                                    &end_key,
                                ));
                                prev_end = Some(end_key);
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
                                fold_blob_links(&mut derived_blob_links, &block_links);
                            }
                            Err(
                                e @ (crate::Error::InvalidHeader(_) | crate::Error::InvalidTag(_)),
                            ) => {
                                dropped.push(classify_drop(
                                    &e,
                                    offset,
                                    prev_end.as_ref(),
                                    &end_key,
                                ));
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    // Wholly-deleted block: nothing to recover, nothing lost.
                    Ok(None) => {}
                    Err(e) => dropped.push(classify_drop(&e, offset, prev_end.as_ref(), &end_key)),
                }
            } else {
                match table
                    .salvage_load_block(keyed.as_ref(), crate::table::block::BlockType::Columnar)
                {
                    // Row materialization validates the batch content (framing,
                    // value-type tags, key invariants) beyond the outer frame
                    // decode. A checksum-consistent block that fails EITHER step
                    // is malformed content — drop this one block and keep
                    // walking, exactly like a row-major block whose entries fail
                    // to decode. Only writer errors (I/O to the destination)
                    // stay hard errors.
                    Ok(sb) => match crate::table::columnar::ColumnBatch::decode(&sb.block.data)
                        .and_then(|batch| {
                            crate::table::columnar::column_batch_to_entries(&batch)
                                .map(|entries| (batch, entries))
                        }) {
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
                                &end_key,
                            ));
                        }
                        Ok((batch, entries)) => {
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
                                        &end_key,
                                    ));
                                    prev_end = Some(end_key);
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
                                // Clean: copy the block's raw bytes as-is.
                                Some((raw, header, layout)) => writer
                                    .append_verbatim_data_block(
                                        &raw, header, layout, &entries, comparator,
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
                                    entries_salvaged += rows;
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
                                        &end_key,
                                    ));
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(e) => {
                            dropped.push(classify_drop(&e, offset, prev_end.as_ref(), &end_key));
                        }
                    },
                    Err(e) => dropped.push(classify_drop(&e, offset, prev_end.as_ref(), &end_key)),
                }
            }
            prev_end = Some(end_key);
            continue;
        }

        // Row source: a clean block is byte-copied verbatim; an ECC-recovered block
        // is re-emitted entry by entry from its healed payload.
        match table.salvage_load_block(keyed.as_ref(), crate::table::block::BlockType::Data) {
            Ok(sb) => {
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
                    dropped.push(classify_drop(&e, offset, prev_end.as_ref(), &end_key));
                    prev_end = Some(end_key);
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
                                    &end_key,
                                ));
                                prev_end = Some(end_key);
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
                                    &end_key,
                                ));
                                prev_end = Some(end_key);
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
                                        &end_key,
                                    ));
                                    prev_end = Some(end_key);
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
                                                &raw, header, layout, &entries, comparator,
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
                                        &end_key,
                                    ));
                                }
                                Err(e) => return Err(e),
                            }
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
                    Err(e) => dropped.push(DroppedBlock {
                        offset,
                        section: b"data".to_vec(),
                        reason: DropReason::DecodeError(format!("{e:?}")),
                        key_range: Some((
                            prev_end.clone().unwrap_or_else(UserKey::empty),
                            end_key.clone(),
                        )),
                    }),
                }
            }
            Err(e) => dropped.push(classify_drop(&e, offset, prev_end.as_ref(), &end_key)),
        }
        prev_end = Some(end_key);
    }

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
    /// `(source_offset, salvaged_offset)` for every re-emitted record, in walk
    /// order. The salvaged file is written COMPACTED — after the first dropped
    /// record every later record lands at a NEW offset — so existing SST
    /// entries whose `ValueHandle::offset` points into the source file must be
    /// remapped through this table before the salvaged file can replace the
    /// original under the same id. A source offset absent from this map (and
    /// implied by [`Self::dropped`]) is lost: its handle has no target.
    pub offset_remap: Vec<(u64, u64)>,
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

/// Salvages the readable records of the blob (vlog) file at `source` into a fresh
/// blob file at `dest`.
///
/// Where [`crate::repair`] rebuilds the blob-file *manifest* around whole files,
/// this walks one blob file record by record and re-emits every record whose
/// checksum verifies, recording the rest. A single bit-rotted record costs only
/// itself: the record stream re-synchronizes at the next frame after a checksum
/// mismatch, so the walk keeps recovering. A structural break (corrupt frame
/// magic / header CRC / a frame that runs past the data section) cannot be
/// resynced, so the walk stops there and reports it.
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
) -> crate::Result<BlobSalvageReport> {
    use crate::vlog::blob_file::{scanner::Scanner, writer::Writer as BlobWriter};
    use alloc::format;

    // Read the source's metadata (this does not scan the data, so a data-corrupt
    // file still opens) to reject a compressed source: the scanner yields on-disk
    // (compressed) bytes, and re-emitting them verbatim under a no-compression
    // descriptor would store undecodable values. Fail closed, the same way SST
    // salvage fails closed on range tombstones.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, source)?);
    let source_handle = crate::vlog::recover_blob_file(source, blob_file_id, checksum, 0, fs)?;
    if source_handle.compression() != crate::CompressionType::None {
        return Err(crate::Error::FeatureUnsupported(
            "salvage of a compressed blob file",
        ));
    }

    let scanner = Scanner::new(source, &**fs, blob_file_id)?;
    // Destination ownership is decided by the writer's `create_new` open, and
    // the CONSTRUCTOR owns cleanup of any partial file it created: on a
    // constructor error this call created nothing (or the constructor already
    // removed it), so no caller-side cleanup — an existence pre-check here
    // would race a concurrent creator (TOCTOU) and delete a file this salvage
    // does not own. Later `write` / `finish` failures still clean up below:
    // by then `create_new` has proven `dest` is ours.
    let mut writer = BlobWriter::new(&dest, blob_file_id, 0, &**fs)?;

    let mut records_total = 0usize;
    let mut records_salvaged = 0usize;
    let mut offset_remap: Vec<(u64, u64)> = Vec::new();
    let mut dropped: Vec<DroppedBlob> = Vec::new();
    // Emit every recoverable record. A `write` failure here (not a per-record
    // checksum/corruption drop, which the match arms absorb) is a hard error: it
    // leaves a partial `dest`, removed on the error path below the same way the
    // SST salvage path removes its partial output.
    let walk = (|| -> crate::Result<()> {
        for item in scanner {
            records_total += 1;
            match item {
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
                // bytes actually stored (`on_disk_val_len`; this path only
                // salvages UNCOMPRESSED sources, so the two must be equal) is
                // rejected by the live blob reader — re-emitting it would
                // restamp a consistent length and launder a record live reads
                // treat as corrupt. Drop it; the scanner is already past the
                // frame, so the walk continues.
                Ok(entry) if entry.uncompressed_len as usize != entry.value.len() => {
                    dropped.push(DroppedBlob {
                        reason: BlobDropReason::Corrupt(
                            "frame's declared value length disagrees with its stored bytes"
                                .to_string(),
                        ),
                    });
                }
                Ok(entry) => {
                    // Record the frame relocation BEFORE the write advances the
                    // writer: existing SST ValueHandles point at SOURCE frame
                    // offsets, and the compacted rewrite shifts every record
                    // after the first drop, so the caller needs this map to
                    // re-target handles before the salvaged file can replace
                    // the original.
                    let salvaged_offset = writer.offset();
                    writer.write(&entry.key, entry.seqno, &entry.value)?;
                    offset_remap.push((entry.offset, salvaged_offset));
                    records_salvaged += 1;
                }
                // The scanner repositions at the next frame after a checksum miss,
                // so a bit-rotted record costs only itself and the walk continues.
                Err(crate::Error::ChecksumMismatch { .. }) => dropped.push(DroppedBlob {
                    reason: BlobDropReason::ChecksumMismatch,
                }),
                // A structural / IO error: the scanner does not re-sync from it
                // (only a checksum miss repositions), and an error that leaves the
                // read position before `data_end` without terminating would make
                // the iterator keep yielding it. Record the corruption and stop the
                // walk — this is the last record it can inspect.
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
