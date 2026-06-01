// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Out-of-band inspection of a single SST file.
//!
//! Companion to [`crate::verify`]: while `verify_block_checksums` walks
//! every block and checks per-block XXH3, this module exposes a public
//! read-only view of an SST's stored metadata ([`TableProperties`](crate::inspect::TableProperties)) for
//! diagnostic tooling like `sst-dump properties` that needs the
//! metadata fields without spinning up a [`Tree`](crate::Tree) or
//! recovering the manifest.
//!
//! The reader follows the same TAIL-first / MID-fallback pattern as
//! [`Table::recover`](crate::Table) (see PR #295): if the canonical
//! `meta` section at the file tail fails to decode, the MID-mirror
//! `meta_mid` section is attempted, and only if both copies are
//! unreadable does the call return an error.

use crate::CompressionType;
// `Fs` brought in for trait method resolution: the `fs.open(path, ...)`
// call below dispatches through the `Fs` trait, so the trait must be
// in scope. Removing it breaks the build with `method `open` not found
// for this struct` (rustc E0599). rustc correctly classifies this as
// USED — no `#[allow]` / `#[expect]` is needed; static-analysis
// passes that flag it as unused are false positives.
use crate::fs::{Fs, FsOpenOptions, StdFs};
use crate::table::meta::ParsedMeta;
use crate::table::regions::ParsedRegions;
use std::path::Path;

/// Read-only snapshot of an SST file's stored metadata.
///
/// Constructed by [`read_table_properties`]; not directly creatable by
/// external callers. Fields are a stable, documented subset of the
/// on-disk meta block (see
/// `src/table/writer/mod.rs::write_meta_section` for the full set of
/// emitted entries). The internal `ParsedMeta` parser carries
/// additional fields — notably the seqno range — that are not yet
/// exposed here; those are tracked as a separate API surface to keep
/// the public contract small while the meta layout still evolves.
/// `#[non_exhaustive]` so new fields can be added in a minor version
/// bump.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableProperties {
    /// Per-tree unique table id (the SST file's logical identifier).
    /// Plain `u64` rather than the crate-internal `TableId` alias so
    /// the public API does not couple to a `#[doc(hidden)]` type.
    pub id: u64,
    /// Logical size of the data-blocks region as recorded by the
    /// writer. This is `*self.meta.file_pos` at the moment the meta
    /// section was flushed — i.e. the byte offset just past the last
    /// data block — and does NOT include the index / filter /
    /// range-tombstone / linked-blob / meta / SFA-trailer sections
    /// that follow. To get the actual on-disk file size, `stat` the
    /// file directly. See `write_meta_section`'s `file_size` field
    /// for the writer-side definition.
    pub file_size: u64,
    /// Smallest user key present in the table. Owned `Vec<u8>` rather
    /// than the crate-internal `KeyRange` / `UserKey` types so the
    /// public API does not couple to `#[doc(hidden)]` re-exports.
    pub min_key: Vec<u8>,
    /// Largest user key present in the table. See [`Self::min_key`]
    /// for the rationale on the owned-bytes representation.
    pub max_key: Vec<u8>,
    /// Number of live (non-tombstone) KV entries.
    pub item_count: u64,
    /// Number of strong tombstone entries (delete markers that
    /// suppress all older versions of a key).
    pub tombstone_count: u64,
    /// Number of weak tombstone entries (single-version delete
    /// markers).
    pub weak_tombstone_count: u64,
    /// Number of weak tombstones eligible for reclamation during
    /// compaction (already paired with their matching value entry).
    pub weak_tombstone_reclaimable: u64,
    /// Number of data blocks emitted by the writer.
    pub data_block_count: u64,
    /// Number of index blocks. For full-index tables this is 1
    /// (the TLI itself acts as the index); for partitioned-index
    /// tables this is the count of leaf index blocks under the TLI.
    pub index_block_count: u64,
    /// Codec used to compress data blocks.
    pub data_block_compression: CompressionType,
    /// Codec used to compress index blocks. Often `None` since the
    /// TLI is small and compression overhead dominates the win.
    pub index_block_compression: CompressionType,
    /// Wall-clock nanoseconds since the Unix epoch when the writer
    /// finalised the table. Recovered identically from MID or TAIL
    /// meta — the writer snapshots `unix_timestamp()` once and emits
    /// the same value to both copies.
    pub created_at_nanos: u128,
}

/// Reads `path` and returns its on-disk metadata as
/// [`TableProperties`].
///
/// Always opens through [`StdFs`]: this is a path-based out-of-band
/// helper, not tied to a live `Tree`'s configured `Fs` backend.
/// `IoUringFs` also operates on real on-disk paths and would work
/// here mechanically; the reason for not threading the backend
/// through is that the caller (typically a diagnostic CLI) starts
/// from a filesystem path with no `Tree` in scope, so `StdFs` is
/// always the right default. Parses the SFA trailer to locate the
/// `meta` and optional `meta_mid` sections, then decodes the meta
/// block via the same `ParsedMeta` machinery `Table::recover` uses
/// on a live tree open.
///
/// Recovery semantics mirror [`Table::recover`](crate::Table):
/// the canonical tail `meta` section is tried first; on
/// decode/checksum/decrypt failure the MID mirror `meta_mid` is tried.
/// Both copies failing returns the original tail error. Tables
/// written before the meta-mirror change (#295) have no `meta_mid`
/// section and the reader falls straight through to the tail copy.
///
/// Encryption: this function does not accept an encryption provider
/// because it is intended for out-of-band diagnostic use. Encrypted
/// SSTs will fail to decode their meta block here. Encrypted-aware
/// property inspection is tracked under #251 / #256 once the
/// AAD-bound wire format lands.
///
/// # Errors
///
/// Returns an error if:
/// - the file cannot be opened or read (`std::io::Error`),
/// - the SFA trailer is missing / malformed and cannot be parsed to
///   locate the `meta` / `meta_mid` sections,
/// - the canonical tail `meta` block fails to decode AND either the
///   `meta_mid` section is absent or the mid copy also fails to
///   decode (block header / XXH3 / structural mismatch). In the
///   both-copies-fail case the original tail error is returned and
///   the mid failure is dropped on the floor (no diagnostic logging
///   here — callers wanting per-copy attribution should walk the
///   meta sections themselves),
/// - the table is encrypted: this function does not take an
///   encryption provider, so the AEAD-protected meta block fails to
///   decode and the failure surfaces as a regular decrypt error.
#[cfg(feature = "std")]
pub fn read_table_properties(path: &Path) -> crate::Result<TableProperties> {
    let fs = StdFs;
    let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;

    let sfa_reader = crate::sfa::Reader::from_reader(&mut file)?;
    let toc = sfa_reader.toc();
    let regions = ParsedRegions::parse_from_toc(toc)?;

    // TAIL first (authoritative copy by convention). On decode /
    // decrypt / checksum failure, fall back to MID. Mirrors
    // `Table::recover` so a corrupted-tail SST that the live open
    // path can still recover also produces inspectable properties
    // here.
    let meta = match ParsedMeta::load_with_handle(&*file, &regions.metadata, None) {
        Ok(m) => m,
        Err(tail_err) => {
            if let Some(mid_handle) = regions.metadata_mid {
                match ParsedMeta::load_with_handle(&*file, &mid_handle, None) {
                    Ok(mid) => mid,
                    Err(_) => return Err(tail_err),
                }
            } else {
                return Err(tail_err);
            }
        }
    };

    Ok(TableProperties {
        id: meta.id,
        file_size: meta.file_size,
        min_key: meta.key_range.min().to_vec(),
        max_key: meta.key_range.max().to_vec(),
        item_count: meta.item_count,
        tombstone_count: meta.tombstone_count,
        weak_tombstone_count: meta.weak_tombstone_count,
        weak_tombstone_reclaimable: meta.weak_tombstone_reclaimable,
        data_block_count: meta.data_block_count,
        index_block_count: meta.index_block_count,
        data_block_compression: meta.data_block_compression,
        index_block_compression: meta.index_block_compression,
        created_at_nanos: *meta.created_at,
    })
}

/// One entry parsed from an SST's top-level index (TLI) block.
///
/// What this points at depends on the SST's index layout:
///
/// - **Full-index tables**: the SST has no separate `index` SFA
///   section. The TLI directly carries data-block handles, so each
///   `IndexEntry` corresponds to one data block in the SST.
/// - **Partitioned-index tables**: the SST has an `index` SFA section
///   containing sub-index leaf blocks. The TLI carries handles
///   pointing at those leaves, so each `IndexEntry` corresponds to
///   one leaf, NOT a data block; walking the leaves to enumerate
///   individual data blocks is a separate operation.
///
/// The distinction is not derivable from `TableProperties` fields
/// alone — in particular `TableProperties.index_block_count == 1`
/// can mean either a full-index table OR a partitioned-index table
/// with a single leaf partition. The authoritative signal is the
/// presence of an `index` SFA section in the SFA TOC; this facade
/// does not currently expose that signal. Callers needing to
/// classify the layout should consult the SST file's TOC directly.
///
/// `#[non_exhaustive]` so new fields can be added in a minor version
/// bump.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct IndexEntry {
    /// Last user-key covered by the pointed-at block. For binary
    /// search the TLI is searched on `end_key`, so this is the
    /// authoritative sort key for the entry.
    pub end_key: Vec<u8>,
    /// Highest seqno of any item in the pointed-at block. Used by
    /// the read path to decide whether a snapshot can skip the
    /// block entirely.
    pub seqno: u64,
    /// On-disk byte offset of the pointed-at block (data-block start
    /// for full-index tables, sub-index-block start for partitioned).
    pub offset: u64,
    /// On-disk size of the pointed-at block in bytes, including the
    /// block `Header` prefix.
    pub size: u32,
}

/// Reads `path` and returns the parsed entries of its top-level index
/// (TLI) block.
///
/// Same out-of-band path-based open semantics as
/// [`read_table_properties`] (uses [`StdFs`], no encryption provider).
/// Recovery semantics mirror [`Table::read_tli`](crate::Table)'s
/// TAIL-first / HEAD-fallback path from #296: the tail `tli_tail`
/// mirror section is attempted first when present; on
/// decode / decrypt / checksum failure the canonical head `tli`
/// section is tried; both copies failing returns the original tail
/// error. Tables written before the TLI-mirror change have no
/// `tli_tail` section and fall straight through to the head copy.
///
/// The function returns owned `IndexEntry` records — the inner
/// `IndexBlock` and its underlying `Slice` are dropped on return, so
/// the caller does not need to keep the file mapping alive. Memory
/// cost is `O(entries × end_key.len())`.
///
/// # Errors
///
/// Returns an error if:
/// - the file cannot be opened or read (`std::io::Error`),
/// - the SFA trailer is missing / malformed and cannot be parsed,
/// - the `meta` block fails to decode (needed to determine
///   `index_block_compression`),
/// - both the head `tli` and the tail `tli_tail` (if present) fail
///   to decode (block header / XXH3 / structural mismatch). In the
///   both-copies-fail case the original tail error is returned,
/// - the TLI block trailer is malformed (e.g. `restart_interval == 0`),
/// - the table is encrypted: this function does not take an
///   encryption provider, so the AEAD-protected blocks fail to
///   decode and the failure surfaces as a regular decrypt error.
#[cfg(feature = "std")]
pub fn read_top_level_index_entries(path: &Path) -> crate::Result<Vec<IndexEntry>> {
    use crate::table::block_index::iter::OwnedIndexBlockIter;
    use crate::table::{IndexBlock, KeyedBlockHandle};

    let fs = StdFs;
    let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;

    let sfa_reader = crate::sfa::Reader::from_reader(&mut file)?;
    let toc = sfa_reader.toc();
    let regions = ParsedRegions::parse_from_toc(toc)?;

    // Load meta to know the index block's compression codec. Same
    // TAIL-first / MID-fallback as `read_table_properties` above —
    // factored together so this function gives identical recovery
    // behaviour on a meta-corrupted SST.
    let meta = match ParsedMeta::load_with_handle(&*file, &regions.metadata, None) {
        Ok(m) => m,
        Err(tail_err) => {
            if let Some(mid_handle) = regions.metadata_mid {
                match ParsedMeta::load_with_handle(&*file, &mid_handle, None) {
                    Ok(mid) => mid,
                    Err(_) => return Err(tail_err),
                }
            } else {
                return Err(tail_err);
            }
        }
    };
    let index_compression = meta.index_block_compression;
    let table_id = meta.id;

    // TLI tail mirror tried first when present (most-recently fsynced
    // copy); on failure fall back to the head copy. Mirrors
    // `Table::read_tli` so a partially-corrupted TLI behaves the
    // same here as in a live open.
    let tli_block = if let Some(tail_handle) = regions.tli_tail {
        match load_index_block(&*file, tail_handle, table_id, index_compression) {
            Ok(b) => b,
            Err(tail_err) => {
                match load_index_block(&*file, regions.tli, table_id, index_compression) {
                    Ok(b) => b,
                    Err(_) => return Err(tail_err),
                }
            }
        }
    } else {
        load_index_block(&*file, regions.tli, table_id, index_compression)?
    };

    let block = IndexBlock::new(tli_block);
    let iter = OwnedIndexBlockIter::from_block(block, crate::comparator::default_comparator())?;

    let entries = iter
        .map(|h: KeyedBlockHandle| IndexEntry {
            end_key: h.end_key().to_vec(),
            seqno: h.seqno(),
            offset: *h.offset(),
            size: h.size(),
        })
        .collect();

    Ok(entries)
}

/// Helper: load and validate a single index block from disk. Shared
/// between the tail and head TLI load sites so both paths produce the
/// same error shape on a malformed block.
#[cfg(feature = "std")]
fn load_index_block(
    file: &dyn crate::fs::FsFile,
    handle: crate::table::BlockHandle,
    table_id: crate::table::TableId,
    compression: CompressionType,
) -> crate::Result<crate::table::Block> {
    use crate::table::block::{Block, BlockIdentity, BlockType};

    let block = Block::from_file(
        file,
        handle,
        BlockIdentity {
            tree_id: 0,
            table_id,
            // Match the writer: index blocks are emitted with
            // `block_offset: 0` (see `Table::read_tli`'s comment for
            // the writer-side rationale). When #251 wires real
            // offsets into AAD this needs the SFA section offset.
            block_offset: 0,
            block_type: BlockType::Index,
            dict_id: 0,
            window_log: 0,
        },
        // Inspect-side index loader doesn't accept an encryption
        // provider (out-of-band facade) and never threads a zstd
        // dict. The transform therefore resolves to either:
        //   - `Plain` when `compression == None`, or
        //   - `Compressed` for `Lz4` / `Zstd(level)`.
        // `CompressionType::ZstdDict { .. }` is rejected here with
        // `Error::ZstdDictMismatch` because the dict can't be
        // recovered without a live `Tree`; inspect such SSTs through
        // the owning tree instead.
        &crate::table::block::BlockTransform::from_parts(
            compression,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;

    if block.header.block_type != BlockType::Index {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    Ok(block)
}

/// One KV entry materialised from an SST data block.
///
/// `#[non_exhaustive]` so new fields can be added in a minor version
/// bump.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DataEntry {
    /// The user key bytes (no internal-key suffix, no seqno).
    pub key: Vec<u8>,
    /// The value bytes.
    ///
    /// Three reasons this can be empty:
    /// 1. The entry is a tombstone (see [`Self::is_tombstone`]).
    /// 2. The iterator was put into keys-only mode via
    ///    [`DataBlockEntryIter::keys_only`]; in that mode `Value`
    ///    entries also yield `value: Vec::new()` because the
    ///    underlying `Slice::to_vec()` allocation is deliberately
    ///    skipped. The caller asked not to pay it.
    /// 3. The on-disk value is a zero-length payload (legal for
    ///    `Value` entries: an empty byte-string is distinct from
    ///    a tombstone).
    ///
    /// Use [`Self::value_type`] to disambiguate (1) from the
    /// other two, and the iterator construction site to know
    /// whether (2) applies; this field cannot tell them apart on
    /// its own.
    ///
    /// For [`crate::ValueType::Indirection`] entries the bytes
    /// are the encoded blob handle, NOT the resolved blob payload.
    pub value: Vec<u8>,
    /// Per-entry sequence number as stored in the internal key on
    /// disk. This is the **table-local** (or "local") seqno: the
    /// `Table::get` / `Iter` read path adds the table's `global_seqno`
    /// offset on top of this for ingested SSTs (see
    /// `src/table/mod.rs` for the translation), so the value the
    /// running tree sees can be higher than the byte here. For an
    /// SST written by the normal writer path `global_seqno == 0`
    /// and the local seqno IS the visible seqno; for an ingested
    /// SST the visible seqno is `global_seqno + local_seqno` and
    /// this field gives the local component only.
    ///
    /// Diagnostic surface: this facade deliberately exposes the
    /// on-disk byte, not the translated value, because operators
    /// using `sst-dump` are usually trying to correlate against the
    /// raw on-disk state. If you need the live-tree-visible seqno,
    /// pair this with the `global_seqno` stored in the table's meta
    /// block (currently not exposed on [`TableProperties`]; tracked
    /// for a future API addition).
    pub seqno: u64,
    /// Underlying internal-key value type. Data blocks can contain
    /// `Value`, `Tombstone`, `WeakTombstone`, `MergeOperand`, and
    /// `Indirection` (blob pointer) entries; the convenience predicate
    /// [`Self::is_tombstone`] folds the two tombstone variants into a
    /// single boolean, but callers that need to distinguish merge
    /// operands or blob pointers from regular values must read this
    /// field directly. Going through the underlying enum keeps the
    /// facade aligned with the crate's own tombstone predicate
    /// ([`crate::ValueType::is_tombstone`]) as a single source of
    /// truth so a future variant addition does not silently start
    /// flagging unrelated entries as tombstones.
    pub value_type: crate::ValueType,
}

impl DataEntry {
    /// `true` iff [`Self::value_type`] is `Tombstone` or `WeakTombstone`.
    ///
    /// Convenience wrapper that delegates to
    /// [`crate::ValueType::is_tombstone`]; `MergeOperand` and
    /// `Indirection` entries return `false` here even though their
    /// `value` bytes do not represent a regular plaintext value.
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        self.value_type.is_tombstone()
    }
}

/// Streaming iterator over every KV entry in an SST's data section.
///
/// Returned by [`iter_data_block_entries`]. Owns the underlying file
/// handle and the list of pending data-block handles; loads exactly
/// one data block at a time and drops it before moving to the next.
///
/// Memory cost is **`O(N_data_blocks) * 16 B + largest_data_block +
/// key.len + value.len`**, where the first term is the upfront
/// `Vec<BlockHandle>` collected from the TLI at construction time
/// (one 16-byte handle per data block). For typical SSTs this is
/// negligible: a 64 MiB SST with 4 KiB data blocks has ~16 K
/// handles ≈ 256 KiB — small compared to a single decompressed
/// data block. A 1 GiB SST gives ~4 MiB of handles. The trade-off
/// buys constant-time `next()` per block lookup; truly streaming
/// the TLI through the iterator would require nested self-cells
/// (the `OwnedIndexBlockIter` already self-references its
/// `IndexBlock`) which is structurally awkward for the rare case
/// where the handle vec actually matters in absolute terms.
///
/// Encrypted SSTs and partitioned-index SSTs both return an error
/// at construction time — see [`iter_data_block_entries`] for the
/// full contract.
#[cfg(feature = "std")]
pub struct DataBlockEntryIter {
    file: Box<dyn crate::fs::FsFile>,
    table_id: crate::table::TableId,
    data_block_compression: CompressionType,
    /// Remaining data-block handles (FIFO). Drained as blocks are
    /// loaded. Held as a `Vec` rather than a `VecDeque` because the
    /// TLI walks the handles in sorted order already, and `Vec::pop`
    /// from the tail with reversed input is cheaper than the
    /// double-ended queue overhead.
    ///
    /// Stores plain `BlockHandle` (offset + size) rather than
    /// `KeyedBlockHandle`. `KeyedBlockHandle` carries `end_key:
    /// Slice` which is a view into the TLI block buffer; collecting
    /// the keyed variant would keep the entire TLI block alive for
    /// the iterator's lifetime. Stripping to the bare handle lets
    /// the TLI block drop after enumeration completes, leaving only
    /// the 16-byte-per-block `Vec` here. See the struct-level
    /// docstring for the memory-cost analysis.
    remaining_handles: Vec<crate::table::BlockHandle>,
    /// Iterator over the currently-loaded block, or `None` when we
    /// haven't loaded a block yet or just finished one.
    current: Option<crate::table::iter::OwnedDataBlockIter>,
    /// When `true`, skip materialising `DataEntry.value` — leave it
    /// as an empty `Vec`. Toggled via [`Self::keys_only`]. Saves the
    /// `Slice::to_vec()` allocation per entry for callers (e.g.
    /// `sst-dump dump --keys-only`) that don't need the value bytes.
    keys_only: bool,
}

#[cfg(feature = "std")]
impl DataBlockEntryIter {
    /// Suppress value materialisation: yielded `DataEntry` values
    /// have `value: Vec::new()` instead of the per-entry
    /// `to_vec()`-copy of the on-disk bytes. Use for keys-only
    /// walks (the entire SST data section can be processed without
    /// allocating any value bytes; only the key copy plus the
    /// constant-size struct fields stay).
    ///
    /// Builder-style — chain off the constructor:
    ///
    /// ```ignore
    /// let iter = iter_data_block_entries(path)?.keys_only();
    /// ```
    #[must_use]
    pub const fn keys_only(mut self) -> Self {
        self.keys_only = true;
        self
    }
}

#[cfg(feature = "std")]
impl Iterator for DataBlockEntryIter {
    type Item = crate::Result<DataEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Drain the current block before considering the next one.
            if let Some(iter) = self.current.as_mut() {
                if let Some(internal) = iter.next() {
                    let value = if self.keys_only {
                        // Skip the `Slice::to_vec()` allocation; the
                        // caller has opted out of value bytes for
                        // this walk. An empty `Vec` is cheap (no
                        // heap allocation in current `Vec::new()`
                        // implementations on stable Rust).
                        Vec::new()
                    } else {
                        internal.value.to_vec()
                    };
                    let entry = DataEntry {
                        key: internal.key.user_key.to_vec(),
                        value,
                        seqno: internal.key.seqno,
                        value_type: internal.key.value_type,
                    };
                    return Some(Ok(entry));
                }
                self.current = None;
            }

            // Advance to the next data block.
            let handle = self.remaining_handles.pop()?;
            match load_data_block_iter(
                &*self.file,
                &handle,
                self.table_id,
                self.data_block_compression,
            ) {
                Ok(iter) => {
                    self.current = Some(iter);
                }
                Err(e) => {
                    // Surface the block-load failure but stop the
                    // walk: subsequent blocks would likely fail the
                    // same way and the operator only needs the first
                    // diagnostic.
                    self.remaining_handles.clear();
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Reads `path` and returns a streaming iterator over every KV entry
/// in the SST's data blocks.
///
/// Same out-of-band path-based open semantics as
/// [`read_table_properties`] etc.: uses [`StdFs`], no encryption
/// provider. Walks the top-level index (TLI) to enumerate data-block
/// handles, then yields entries one block at a time so memory cost
/// stays bounded.
///
/// # Scope
///
/// Only **full-index** SSTs are supported by this facade. SSTs with
/// a separate `index` SFA section (partitioned index) point their
/// TLI entries at sub-index leaves rather than data blocks; walking
/// the leaves to enumerate individual data blocks is a separate
/// operation not yet wired here. Partitioned-index SSTs return an
/// `Io(ErrorKind::Unsupported)` error.
///
/// **Custom user comparator: forward iteration works, but bounds /
/// seek operations do not.** Forward iteration over the data section
/// is positional — the index walks blocks in their on-disk file
/// order and the block iterator yields entries in their on-disk
/// encoded order, neither of which depends on the comparator. So a
/// bounds-free walk of a custom-comparator SST through this facade
/// streams entries in the SST's own sort order correctly. What this
/// facade can't do is run `crate::comparator::default_comparator`
/// for seek / point-read / range-bounded operations layered on top
/// (e.g. `sst-dump dump --from/--to` applies bytewise bounds and
/// breaks early on the upper bound, which only makes sense for the
/// default lexicographic comparator). Callers that need
/// comparator-correct range queries on a custom-comparator SST
/// should use the owning `Tree`'s regular read APIs.
///
/// **Zstd-dictionary blocks: not supported.** This facade calls
/// [`crate::table::Block::from_file`] with no
/// [`crate::compression::ZstdDictionary`] attached (it has no way to
/// fetch one without a live `Tree`), so blocks compressed with
/// [`crate::CompressionType::ZstdDict`] fail with
/// [`crate::Error::ZstdDictMismatch`] even though they are otherwise
/// valid. Inspect such SSTs through the owning `Tree` instead.
///
/// # Errors
///
/// Returns an error if:
/// - the file cannot be opened or read,
/// - the SFA trailer is missing / malformed,
/// - the `meta` block fails to decode (needed for
///   `data_block_compression`),
/// - the SST has a partitioned-index layout (`index` SFA section
///   present),
/// - the TLI block cannot be loaded or its trailer is malformed,
/// - the table is encrypted: this function does not take an
///   encryption provider, so AEAD-protected blocks fail to decode.
///
/// Per-entry errors are surfaced by the returned iterator's
/// `Item = Result<DataEntry>` shape: a single block failing to load
/// yields one `Err` and ends the walk.
#[cfg(feature = "std")]
pub fn iter_data_block_entries(path: &Path) -> crate::Result<DataBlockEntryIter> {
    use crate::table::IndexBlock;
    use crate::table::block_index::iter::OwnedIndexBlockIter;

    let fs = StdFs;
    let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;

    let sfa_reader = crate::sfa::Reader::from_reader(&mut file)?;
    let toc = sfa_reader.toc();
    let regions = ParsedRegions::parse_from_toc(toc)?;

    if regions.index.is_some() {
        return Err(crate::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "partitioned-index SST (separate `index` section present) is not yet supported \
             by iter_data_block_entries; walking sub-index leaves to enumerate data blocks \
             is a follow-up surface",
        )));
    }

    let meta = match ParsedMeta::load_with_handle(&*file, &regions.metadata, None) {
        Ok(m) => m,
        Err(tail_err) => {
            if let Some(mid_handle) = regions.metadata_mid {
                match ParsedMeta::load_with_handle(&*file, &mid_handle, None) {
                    Ok(mid) => mid,
                    Err(_) => return Err(tail_err),
                }
            } else {
                return Err(tail_err);
            }
        }
    };
    let table_id = meta.id;
    let data_block_compression = meta.data_block_compression;
    let index_compression = meta.index_block_compression;

    // Load TLI (with tail-mirror fallback). For full-index tables
    // each TLI entry IS a data-block handle, so this list is what
    // we stream over.
    let tli_block = if let Some(tail_handle) = regions.tli_tail {
        match load_index_block(&*file, tail_handle, table_id, index_compression) {
            Ok(b) => b,
            Err(tail_err) => {
                match load_index_block(&*file, regions.tli, table_id, index_compression) {
                    Ok(b) => b,
                    Err(_) => return Err(tail_err),
                }
            }
        }
    } else {
        load_index_block(&*file, regions.tli, table_id, index_compression)?
    };

    let block = IndexBlock::new(tli_block);
    let iter = OwnedIndexBlockIter::from_block(block, crate::comparator::default_comparator())?;
    // Materialise each handle to the bare `BlockHandle` (offset +
    // size) right here, so the surrounding `IndexBlock` and its
    // backing `Slice` can drop as soon as this collect finishes.
    // `KeyedBlockHandle.end_key` is a view into the TLI block bytes;
    // holding `KeyedBlockHandle` in the iterator struct would keep
    // the entire TLI block alive for the iterator's lifetime, which
    // contradicts the "memory bounded to one data block" guarantee.
    // `BlockHandle` is `Copy`, so `into_inner()` is cheap and
    // releases the `Slice` view at the same time.
    let mut handles: Vec<crate::table::BlockHandle> = iter
        .map(crate::table::KeyedBlockHandle::into_inner)
        .collect();
    // Reverse so `pop` from the tail yields the smallest end_key
    // first — matches the natural left-to-right read order
    // operators expect from `dump`.
    handles.reverse();

    Ok(DataBlockEntryIter {
        file,
        table_id,
        data_block_compression,
        remaining_handles: handles,
        current: None,
        keys_only: false,
    })
}

#[cfg(feature = "std")]
fn load_data_block_iter(
    file: &dyn crate::fs::FsFile,
    handle: &crate::table::BlockHandle,
    table_id: crate::table::TableId,
    compression: CompressionType,
) -> crate::Result<crate::table::iter::OwnedDataBlockIter> {
    use crate::table::DataBlock;
    use crate::table::block::{Block, BlockIdentity, BlockType};
    use crate::table::iter::OwnedDataBlockIter;

    // Inspect-side data-block loader: no encryption provider (out-
    // of-band facade) and never a zstd dict. Same transform-
    // resolution rule as load_index_block above:
    //   - compression == None  →  BlockTransform::Plain
    //   - compression == Lz4 / Zstd(level)  →  BlockTransform::Compressed
    //   - compression == ZstdDict { .. }  →  rejected via
    //     Error::ZstdDictMismatch (no dict can be threaded without a
    //     live Tree).
    let block = Block::from_file(
        file,
        *handle,
        BlockIdentity {
            tree_id: 0,
            table_id,
            // Same writer / reader agreement as TLI / filter: writer
            // emits data blocks with `block_offset` set to a
            // running cursor that we don't have exposed here; the
            // BlockIdentity is ignored by `Block::from_file` today
            // and only becomes load-bearing when #251 wires it into
            // AEAD AAD. Holding at 0 keeps this facade consistent
            // with how it loads the meta / TLI / filter blocks
            // above. Once real offsets are threaded, all three
            // load sites need updating together.
            block_offset: 0,
            block_type: BlockType::Data,
            dict_id: 0,
            window_log: 0,
        },
        &crate::table::block::BlockTransform::from_parts(
            compression,
            None,
            #[cfg(zstd_any)]
            None,
        )?,
    )?;

    if block.header.block_type != BlockType::Data {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    // `from_loaded` strips the per-KV checksum footer when the block's
    // KV_CHECKSUM_FOOTER flag is set so the inspection iterator decodes the
    // inner payload normally.
    let data_block = DataBlock::from_loaded(block)?;
    OwnedDataBlockIter::try_new(data_block, |b| {
        b.try_iter(crate::comparator::default_comparator())
    })
}
/// Read-only stats for a single-block (full) `BuRR` / Ribbon filter
/// section.
///
/// Returned by [`read_filter_stats`]. For partitioned-filter tables
/// (those with a `filter_tli` SFA section) this struct is not
/// populated — see `read_filter_stats` for the contract. Stats are
/// derived from the public
/// [`BurrFilterReader`](crate::table::filter::ribbon::burr::BurrFilterReader)
/// surface plus the SFA section size; no internal filter bits are
/// exposed here.
///
/// `#[non_exhaustive]` so new fields can be added in a minor version
/// bump.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FilterStats {
    /// On-disk size of the `filter` SFA section in bytes, including
    /// the block `Header` prefix.
    pub filter_section_bytes: u64,
    /// Number of `BuRR` / Ribbon "layers" the writer emitted. Each
    /// layer is a Bumped-Ribbon-Retrieval pass; more layers means a
    /// larger filter at a given key count but tighter FPR. Fixed-width
    /// `u64` so the public layout doesn't vary between 32-bit and
    /// 64-bit targets, matching the rest of the inspect facade.
    pub layer_count: u64,
    /// Number of keys the meta block reports for the table. Used as
    /// the denominator for `bits_per_key`; sourced from
    /// `TableProperties.item_count` and copied here so callers can
    /// compute the rate without a second `read_table_properties`
    /// call.
    pub item_count: u64,
    /// Approximate average bits-per-key the filter consumes:
    /// `filter_section_bytes * 8 / max(item_count, 1)`. This is a
    /// SIZE metric, not the true theoretical `BuRR` overhead — it
    /// includes the block `Header`, the `BuRR` wire-format header,
    /// per-layer payload framing, and any zero-padding bits at the
    /// end of each layer's storage word array. Treat it as an
    /// upper bound on the actual ribbon parameter `bits_per_key`.
    pub bits_per_key: f64,
}
/// Reads `path` and returns `BuRR` filter sizing stats for the SST's
/// `filter` section, or `Ok(None)` if the SST has no filter section
/// at all (filter-less table).
///
/// **Scope:** only the single-block (full) filter layout is
/// supported by this facade. SSTs with a `filter_tli` SFA section
/// (partitioned filter) return an error — per-partition stats need
/// a different surface that walks the TLI and reports a
/// `Vec<FilterStats>` or aggregate metrics, and that is a separate
/// public-API decision not yet made.
///
/// Same out-of-band path-based open semantics as
/// [`read_table_properties`]: uses [`StdFs`], no encryption provider.
/// Recovery for the meta block (needed to source `item_count`)
/// mirrors the TAIL-first / MID-fallback pattern from #295. The
/// filter block itself is written uncompressed (see
/// `FullFilterWriter::finish`), so no compression-codec dependency on
/// the read path here.
///
/// # Errors
///
/// Returns an error if:
/// - the file cannot be opened or read,
/// - the SFA trailer is missing / malformed,
/// - the `meta` block fails to decode (needed for `item_count`),
/// - the table has a `filter_tli` SFA section (partitioned filter,
///   not supported by this facade): returned as
///   `Error::FeatureUnsupported("filter_tli")` (see
///   [`crate::Error::FeatureUnsupported`]) so callers can match the
///   typed variant instead of parsing message strings,
/// - the `filter` block header / payload is malformed,
/// - the `BuRR` wire format cannot be parsed (magic mismatch,
///   unsupported version, structurally invalid header).
///
/// Returns `Ok(None)` for both on-disk shapes of "no filter
/// installed":
///
/// 1. the SST has no `filter` SFA section at all, or
/// 2. the section is present but carries a zero-byte payload (the
///    [`crate::table::filter::block::FilterBlock`] sentinel for "no
///    filter"; the writer emits it when the filter policy resolves
///    to no usable filter at flush time, which is structurally
///    equivalent to the absent-section case).
///
/// A tree configured with
/// [`FilterPolicy::disabled`](crate::config::FilterPolicy::disabled)
/// (or any policy whose per-level entry is
/// [`FilterPolicyEntry::None`](crate::config::FilterPolicyEntry::None))
/// produces filter-less SSTs that take one of these two shapes;
/// either way callers see the same `Ok(None)` result.
#[cfg(feature = "std")]
pub fn read_filter_stats(path: &Path) -> crate::Result<Option<FilterStats>> {
    use crate::table::block::{Block, BlockIdentity, BlockType};
    use crate::table::filter::ribbon::burr::BurrFilterReader;

    let fs = StdFs;
    let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;

    let sfa_reader = crate::sfa::Reader::from_reader(&mut file)?;
    let toc = sfa_reader.toc();
    let regions = ParsedRegions::parse_from_toc(toc)?;

    if regions.filter_tli.is_some() {
        // Partitioned filter: a `filter_tli` SFA section is present
        // alongside `filter`, and the contents of `filter` are a
        // concatenation of per-partition `BuRR` payloads, not a
        // single parseable wire buffer. Surface this as the typed
        // `Error::FeatureUnsupported("filter_tli")` so callers can
        // match on the marker without parsing message strings; the
        // payload literal is the SFA section name an operator can
        // confirm via the TOC.
        return Err(crate::Error::FeatureUnsupported("filter_tli"));
    }

    let Some(filter_handle) = regions.filter else {
        return Ok(None);
    };
    let filter_section_bytes = u64::from(filter_handle.size());

    // Meta block carries `item_count`. Same TAIL-first / MID-fallback
    // as `read_table_properties` so a partially-corrupted meta still
    // yields stats from the surviving copy.
    let meta = match ParsedMeta::load_with_handle(&*file, &regions.metadata, None) {
        Ok(m) => m,
        Err(tail_err) => {
            if let Some(mid_handle) = regions.metadata_mid {
                match ParsedMeta::load_with_handle(&*file, &mid_handle, None) {
                    Ok(mid) => mid,
                    Err(_) => return Err(tail_err),
                }
            } else {
                return Err(tail_err);
            }
        }
    };
    let item_count = meta.item_count;
    let table_id = meta.id;

    // Inspect-side filter loader. Two design choices here:
    //
    // 1. **Transform is `Plain` — chosen here, not dictated by the
    //    writer.** `FullFilterWriter::finish` does NOT always emit
    //    filter blocks as `Plain`: when the tree has an encryption
    //    provider configured, it emits `Encrypted` instead (no
    //    compression in either case — filter blocks aren't worth
    //    compressing). This facade chooses `Plain` because it has
    //    no encryption provider plumbed through (out-of-band path,
    //    no live `Tree`), so for SSTs whose filter block was
    //    written encrypted, `Block::from_file` here verifies the
    //    XXH3 over the ciphertext bytes and then fails downstream —
    //    typically `InvalidHeader` from `uncompressed_length`
    //    disagreeing with the ciphertext length, not a clean
    //    `Error::Decrypt`. Encrypted-aware filter inspection is
    //    tracked alongside #251 / #256.
    //
    // 2. **`block_offset` held at 0.** `filter_handle.offset()`
    //    carries the real on-disk position, but the writer emits
    //    the filter block with `BlockIdentity { block_offset: 0,
    //    ... }`. Reader and writer must agree on `BlockIdentity`
    //    for AEAD verification once #251 wires it into AAD;
    //    switching only this side to `filter_handle.offset()`
    //    would break that agreement. Threading real offsets
    //    through both sides is a coordinated change tracked
    //    alongside #251.
    let block = Block::from_file(
        &*file,
        filter_handle,
        BlockIdentity {
            tree_id: 0,
            table_id,
            block_offset: 0,
            block_type: BlockType::Filter,
            dict_id: 0,
            window_log: 0,
        },
        &crate::table::block::BlockTransform::PLAIN,
    )?;
    if block.header.block_type != BlockType::Filter {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    // Empty data slice is the "no filter installed" sentinel (see
    // `FilterBlock::maybe_contains_hash`). The on-disk shape of "no
    // filter present" can be either the section absent entirely
    // OR the section present with a zero-byte payload; both mean
    // the same thing semantically. Collapse the empty-payload case
    // to the same `Ok(None)` result the section-absent branch
    // above returns, so the CLI prints the documented
    // "no filter section installed" line for either shape and the
    // public API stays consistent with the FilterBlock sentinel
    // contract.
    if block.data.is_empty() {
        return Ok(None);
    }

    // BurrFilterReader::layer_count returns usize; widen to u64 at
    // the public-API boundary. usize -> u64 is lossless on every
    // target Rust supports (u64 is at least as wide as usize on
    // 32-bit and identical on 64-bit).
    let layer_count: u64 = BurrFilterReader::new(&block.data)?.layer_count() as u64;

    // `item_count` is `u64`; cast to `f64` is lossy for values above
    // 2^53 (~9 quadrillion), which is well past anything a real SST
    // holds. The lossy cast is the standard pattern for size
    // statistics here — clippy's `cast_precision_loss` lint is
    // already allowed crate-wide for this kind of arithmetic.
    #[expect(
        clippy::cast_precision_loss,
        reason = "filter stats are diagnostic; precision loss above 2^53 keys is irrelevant"
    )]
    let denom = item_count.max(1) as f64;
    #[expect(
        clippy::cast_precision_loss,
        reason = "filter stats are diagnostic; precision loss above 2^53 bytes is irrelevant"
    )]
    let bits = (filter_section_bytes * 8) as f64;
    let bits_per_key = bits / denom;

    Ok(Some(FilterStats {
        filter_section_bytes,
        layer_count,
        item_count,
        bits_per_key,
    }))
}
