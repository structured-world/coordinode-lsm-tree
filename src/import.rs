// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Building a tree's files from parts another store already holds, for the
//! offline converter that carries a store from the previous major format.
//!
//! Everything written here goes through the engine's own writers: a table
//! through [`crate::table::Writer`], a manifest through the version persist
//! the engine runs on every install. The parts come in already decoded and
//! verified by the code that wrote them; nothing here reads the previous
//! format.

use crate::comparator::SharedComparator;
use crate::config::ManifestRecoveryMode;
use crate::encryption::EncryptionProvider;
use crate::fs::Fs;
use crate::path::{Path, PathBuf};
use crate::table::Writer;
use crate::{Checksum, CompressionType, InternalValue, SeqNo, TableId, TreeType, UserKey};
use alloc::sync::Arc;

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;

/// How a table being imported is encoded: what its blocks were compressed
/// with in the source, and what this table's frames carry.
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent property of the table, not a state"
)]
pub struct TableSettings {
    /// The codec the source compressed the data blocks with; the imported
    /// payloads are written as they are, so this must match them.
    pub data_compression: CompressionType,
    /// The codec the index blocks are compressed with.
    pub index_compression: CompressionType,
    /// The restart interval the source encoded data blocks with.
    pub data_restart_interval: u8,
    /// The restart interval of the index blocks.
    pub index_restart_interval: u8,
    /// Encryption at rest, as the tree is opened with.
    pub encryption: Option<Arc<dyn EncryptionProvider>>,
    /// The dictionary the source compressed the data blocks against, when
    /// the codec names one.
    #[cfg(zstd_any)]
    pub zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    /// The table's L0 recency key.
    pub recency: TableId,
    /// The table's age, nanoseconds since the Unix epoch, as the source
    /// recorded it.
    pub created_at: u128,
    /// The per-KV checksum footer the imported payloads carry, if any; every
    /// data block of the table carries it or none does.
    pub kv_checksum: Option<crate::runtime_config::ChecksumAlgorithm>,
    /// The parity scheme every block of the table is written with.
    pub ecc: Option<crate::table::block::EccParams>,
    /// Whether the index is split into a top-level index and index blocks.
    pub partitioned_index: bool,
    /// Whether the table keeps per-block seqno bounds, derived from the rows.
    pub seqno_bounds: bool,
    /// Whether the table keeps a zone map, derived from the rows.
    pub zone_map: bool,
    /// The bulk-ingest provenance, when the source recorded it.
    pub bulk_ingested: Option<bool>,
    /// The compaction lineage the source recorded.
    pub lineage: TableLineage,
    /// Whether the table is columnar. Its rows are then encoded again in this
    /// format's layout through [`TableImport::append_rows`].
    pub columnar: bool,
    /// Whether a columnar table stores each value split into the caller's
    /// fields, one column each, as an ingested batch does. Its batches then
    /// come through [`TableImport::append_column_batch`].
    pub split_fields: bool,
    /// The membership filter, carried over as solved; `None` writes none.
    pub filter: Option<FilterImage>,
    /// The retrieval locator; `None` writes none.
    pub locator: Option<LocatorImport>,
    /// The first key the table serves when a tight-space compaction
    /// restricted it. Rows below it are hidden by the restriction, so the
    /// blob links are derived from the rows at and past it only.
    pub restriction: Option<UserKey>,
}

/// How a table's retrieval locator is imported.
#[derive(Clone, Debug)]
pub enum LocatorImport {
    /// Carried over as solved: the table holds the source's blocks, in order.
    Carried(LocatorImage),
    /// Built again over the table's own blocks, addressing slots at the
    /// precision the source's did (its on-disk byte, see
    /// [`LocatorImage::precision`]): for a table whose blocks are not the
    /// source's, encoded again or carried from a later one on.
    Rebuilt {
        /// What a slot addresses.
        precision: u8,
    },
}

/// What a table records of the compaction that wrote it.
///
/// Manifest repair reads it to tell a derived output from its inputs. The
/// converted tables keep their ids, so the ids here still name the same
/// tables.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableLineage {
    /// The compaction inputs the table was merged from; `None` for a flush or
    /// an ingest.
    pub inputs: Option<Vec<TableId>>,
    /// The previous output of the same compaction run.
    pub prev: Option<TableId>,
    /// Whether a compaction filter transformed the table's window.
    pub transformed: bool,
    /// Whether the table closed its compaction run.
    pub last: bool,
}

/// What a `BuRR` solution stores per key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BurrKind {
    /// A fingerprint: the solution answers membership.
    Membership,
    /// A caller value: the solution answers retrieval.
    Retrieval,
}

/// One layer of a `BuRR` solution.
#[derive(Clone, Debug)]
pub struct BurrLayerImage {
    /// The layer's slot count.
    pub m: u32,
    /// One bumping threshold per block of slots.
    pub thresholds: Vec<u8>,
    /// The solution rows, one per slot, each holding its `r` result bits.
    pub rows: Vec<u64>,
}

/// A `BuRR` solution: its parameters, root seed and layers. Written in this
/// format's packed layout, holding the same values; nothing is solved again.
#[derive(Clone, Debug)]
pub struct BurrImage {
    /// What the solution stores.
    pub kind: BurrKind,
    /// Result bits per key.
    pub r: u8,
    /// Band width.
    pub w: u8,
    /// Slots per threshold block.
    pub b: u8,
    /// The seed the layer seeds derive from.
    pub root_seed: u64,
    /// The layers, in the order a probe walks them.
    pub layers: Vec<BurrLayerImage>,
}

/// A membership filter carried over as solved.
#[derive(Clone, Debug)]
pub enum FilterImage {
    /// One filter over the whole table.
    Full(BurrImage),
    /// Filters over key ranges, in key order: the last key each covers and
    /// the filter. Every key the table receives must fall in one of them.
    Partitioned(Vec<(UserKey, BurrImage)>),
}

/// A retrieval locator carried over as solved, addressing the same blocks
/// and slots in the imported table.
#[derive(Clone, Debug)]
pub struct LocatorImage {
    /// What a slot addresses, as stored: 0 = a restart index, 1 = an entry
    /// index, 2 = none (the block alone).
    pub precision: u8,
    /// Bits of a located value that hold the block ordinal.
    pub block_id_bits: u8,
    /// Bits of a located value that hold the slot.
    pub slot_bits: u8,
    /// The retrieval solution.
    pub solution: BurrImage,
}

/// `image` in this format's wire layout, read back as the probe reads it
/// before it is returned.
///
/// # Errors
///
/// Returns [`crate::Error::InvalidHeader`] for a solution whose layers do not
/// match their parameters (a row count other than `m`, a threshold count
/// other than one per block, a row with bits above `r`) or that the reader
/// refuses.
fn burr_bytes(image: &BurrImage) -> crate::Result<Vec<u8>> {
    use crate::table::filter::ribbon::burr::wire;

    if !(1..=64).contains(&image.r) || image.b == 0 || image.layers.is_empty() {
        return Err(crate::Error::InvalidHeader("imported BuRR parameters"));
    }
    let high_bits = if image.r == 64 {
        0
    } else {
        !((1u64 << image.r) - 1)
    };
    let mut layers = Vec::with_capacity(image.layers.len());
    for layer in &image.layers {
        let m = usize::try_from(layer.m)
            .map_err(|_| crate::Error::InvalidHeader("imported BuRR layer"))?;
        if layer.rows.len() != m
            || layer.thresholds.len() != m.div_ceil(usize::from(image.b))
            || layer.rows.iter().any(|row| row & high_bits != 0)
        {
            return Err(crate::Error::InvalidHeader("imported BuRR layer"));
        }
        layers.push(wire::LayerParts {
            m,
            thresholds: &layer.thresholds,
            rows: &layer.rows,
        });
    }
    let tag = match image.kind {
        BurrKind::Membership => wire::BURR_FILTER_TYPE_BYTE,
        BurrKind::Retrieval => wire::BURR_RETRIEVAL_TYPE_BYTE,
    };
    let bytes = wire::encode_layers(tag, (image.r, image.w, image.b), image.root_seed, &layers);
    wire::decode_as(&bytes, tag)?;
    Ok(bytes)
}

/// The `locator` section holding `image`.
fn locator_section(image: &LocatorImage) -> crate::Result<Vec<u8>> {
    if image.solution.kind != BurrKind::Retrieval
        || u16::from(image.block_id_bits) + u16::from(image.slot_bits)
            != u16::from(image.solution.r)
    {
        return Err(crate::Error::InvalidHeader("imported locator"));
    }
    let mut section = vec![
        crate::table::locator::SECTION_VERSION,
        image.precision,
        image.block_id_bits,
        image.slot_bits,
    ];
    section.extend(burr_bytes(&image.solution)?);
    Ok(section)
}

/// A table being written from blocks another store holds.
pub struct TableImport {
    writer: Writer,
    comparator: SharedComparator,
    /// The blob files the rows reference, derived from the rows as they come.
    blob_links: crate::HashMap<crate::vlog::BlobFileId, crate::table::writer::LinkedFile>,
    /// The blob objects the rows' cells own.
    owned_cells: Vec<(crate::vlog::BlobFileId, u64)>,
    /// Whether rows are encoded again rather than carried in their blocks.
    columnar: bool,
    /// See [`TableSettings::split_fields`].
    split_fields: bool,
    /// Rows written through [`Self::append_rows`]: the position the next one
    /// takes.
    rows_written: u32,
    /// See [`TableSettings::restriction`].
    restriction: Option<UserKey>,
    /// The first and last keys of the entries received, and the range every
    /// range tombstone recorded so far covers, under the comparator.
    key_range: Option<(UserKey, UserKey)>,
}

impl TableImport {
    /// Starts table `table_id` at `path`, to be placed at `level`.
    ///
    /// # Errors
    ///
    /// Returns any error creating the file.
    pub fn create(
        path: PathBuf,
        table_id: TableId,
        level: u8,
        fs: Arc<dyn Fs>,
        comparator: SharedComparator,
        settings: TableSettings,
    ) -> crate::Result<Self> {
        let writer = Writer::new(path, table_id, level, fs)?
            .use_data_block_compression(settings.data_compression)
            .use_index_block_compression(settings.index_compression)
            .use_data_block_restart_interval(settings.data_restart_interval)
            .use_index_block_restart_interval(settings.index_restart_interval)
            .use_encryption(settings.encryption)
            .use_recency(settings.recency)
            .use_created_at(settings.created_at)
            // A build without parity writes none: a scheme recorded here would
            // size every block handle for a trailer that is never written.
            .use_ecc(settings.ecc.filter(|_| cfg!(feature = "page_ecc")))
            .use_seqno_in_index(settings.seqno_bounds)
            .use_zone_map(settings.zone_map)
            .use_bulk_ingested(settings.bulk_ingested)
            .use_lineage(settings.lineage.inputs)
            .use_lineage_prev(settings.lineage.prev)
            .use_lineage_transformed(settings.lineage.transformed)
            .use_lineage_last(settings.lineage.last);
        let writer = if settings.partitioned_index {
            writer.use_partitioned_index()
        } else {
            writer
        };
        // A table's data blocks either all carry the footer or none does, so
        // the policy that applies to every block reproduces it.
        let writer = match settings.kv_checksum {
            Some(algo) => {
                writer.use_kv_checksums(crate::runtime_config::KvChecksumPolicy::AllLevels, algo)
            }
            None => writer,
        };
        #[cfg(zstd_any)]
        let writer = match settings.zstd_dictionary {
            Some(dict) => writer.use_zstd_dictionary(Some(dict)),
            None => writer,
        };
        let writer = match &settings.filter {
            Some(FilterImage::Full(image)) => writer.use_prebuilt_filter(
                crate::table::writer::PrebuiltFilter::Full(burr_bytes(image)?),
            ),
            Some(FilterImage::Partitioned(partitions)) => {
                // The writer counts each partition's keys as the blocks come.
                let partitions = partitions
                    .iter()
                    .map(|(end_key, image)| Ok((end_key.clone(), burr_bytes(image)?, 0)))
                    .collect::<crate::Result<Vec<_>>>()?;
                writer.use_prebuilt_filter(crate::table::writer::PrebuiltFilter::Partitioned(
                    partitions,
                ))
            }
            // A source table without a filter gets none.
            None => {
                writer.use_bloom_policy(crate::config::BloomConstructionPolicy::BitsPerKey(0.0))
            }
        };
        let writer = match &settings.locator {
            // Re-encoded blocks are not the source's, so a carried locator,
            // which names blocks by ordinal, would point at the wrong ones.
            Some(LocatorImport::Carried(_)) if settings.columnar => {
                return Err(crate::Error::InvalidHeader(
                    "a columnar table's locator is built again over its new blocks",
                ));
            }
            Some(LocatorImport::Carried(image)) => {
                writer.use_prebuilt_locator(locator_section(image)?)
            }
            Some(LocatorImport::Rebuilt { precision }) => {
                let precision = match *precision {
                    0 => crate::config::LocatorPrecision::Restart,
                    1 => crate::config::LocatorPrecision::Entry,
                    2 => crate::config::LocatorPrecision::Block,
                    _ => return Err(crate::Error::InvalidHeader("imported locator")),
                };
                writer.use_locator(crate::config::LocatorPolicyEntry::Enabled {
                    precision,
                    block_id_bits: None,
                    slot_bits: None,
                })
            }
            None => writer,
        };
        let writer = writer.use_columnar(settings.columnar);
        let writer = if settings.split_fields {
            writer.use_value_layout(crate::table::meta::ValueLayout::Split)
        } else {
            writer
        };
        Ok(Self {
            writer,
            comparator,
            blob_links: crate::HashMap::default(),
            owned_cells: Vec::new(),
            columnar: settings.columnar,
            split_fields: settings.split_fields,
            rows_written: 0,
            restriction: settings.restriction,
            key_range: None,
        })
    }

    /// Widens the table's key range to `[start, end]`.
    fn widen_key_range(&mut self, start: &UserKey, end: &UserKey) {
        let cmp = &self.comparator;
        self.key_range = Some(match self.key_range.take() {
            None => (start.clone(), end.clone()),
            Some((lo, hi)) => (
                if cmp.compare(start, &lo) == core::cmp::Ordering::Less {
                    start.clone()
                } else {
                    lo
                },
                if cmp.compare(end, &hi) == core::cmp::Ordering::Greater {
                    end.clone()
                } else {
                    hi
                },
            ),
        });
    }

    /// The blob references of the rows the table serves: all of them, or for
    /// a restricted table those at or past its bound.
    fn served_refs(
        &self,
        rows: &[InternalValue],
    ) -> crate::Result<Vec<crate::blob_tree::links::RecoveredRef>> {
        let mut refs = crate::blob_tree::links::collect_indirections(rows)?;
        if let Some(bound) = &self.restriction {
            refs.retain(|(key, _, _)| {
                self.comparator.compare(key, bound) != core::cmp::Ordering::Less
            });
        }
        Ok(refs)
    }

    /// Appends rows to a columnar table, in key order after every row
    /// appended before them, encoded in this format's layout. `deleted` are
    /// the indexes into `rows` the source's delete bitmap marks, ascending.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] on a table that is not
    /// columnar or a deleted index past the rows, an error when a row
    /// references a blob in a value that does not decode or falls outside
    /// every partition of a carried filter, and any error writing them.
    pub fn append_rows(&mut self, rows: Vec<InternalValue>, deleted: &[u32]) -> crate::Result<()> {
        let invalid = || crate::Error::InvalidHeader("imported columnar rows");
        if !self.columnar
            || self.split_fields
            || deleted
                .iter()
                .any(|&i| usize::try_from(i).map_or(true, |i| i >= rows.len()))
        {
            return Err(invalid());
        }
        let refs = self.served_refs(&rows)?;
        if let (Some(lo), Some(hi)) = (rows.first(), rows.last()) {
            self.widen_key_range(&lo.key.user_key, &hi.key.user_key);
        }
        let first = self.rows_written;
        for row in rows {
            self.writer.write_counted(row, &self.comparator)?;
            // A position is a u32: a table past it cannot record its deletes.
            self.rows_written = self.rows_written.checked_add(1).ok_or_else(invalid)?;
        }
        for &index in deleted {
            let position = first.checked_add(index).ok_or_else(invalid)?;
            self.writer.delete_bitmap_mut().insert(position);
        }
        crate::blob_tree::links::fold_blob_links(
            &mut self.blob_links,
            &mut self.owned_cells,
            &refs,
        );
        Ok(())
    }

    /// Appends a column batch to a table whose values are split into fields,
    /// in key order after every row appended before it, its field columns and
    /// per-row seqnos stored as they are. `deleted` are the indexes of the
    /// batch's rows the source's delete bitmap marks, ascending.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] on a table whose values are not
    /// split into fields or a deleted index past the rows, and the errors of
    /// [`Self::append_rows`] otherwise.
    #[cfg(feature = "columnar")]
    pub fn append_column_batch(
        &mut self,
        batch: &crate::table::columnar::ColumnBatch,
        deleted: &[u32],
    ) -> crate::Result<()> {
        let invalid = || crate::Error::InvalidHeader("imported column batch");
        if !self.split_fields || deleted.iter().any(|&i| i >= batch.row_count) {
            return Err(invalid());
        }
        let entries = crate::table::columnar::column_batch_to_entries(batch)?;
        let refs = self.served_refs(&entries)?;
        self.writer
            .count_prebuilt_keys(&entries, &self.comparator)?;
        self.writer
            .write_columnar_block_verbatim(batch, &self.comparator)?;
        if let (Some(lo), Some(hi)) = (entries.first(), entries.last()) {
            self.widen_key_range(&lo.key.user_key, &hi.key.user_key);
        }
        let first = self.rows_written;
        self.rows_written = self
            .rows_written
            .checked_add(batch.row_count)
            .ok_or_else(invalid)?;
        for &index in deleted {
            let position = first.checked_add(index).ok_or_else(invalid)?;
            self.writer.delete_bitmap_mut().insert(position);
        }
        crate::blob_tree::links::fold_blob_links(
            &mut self.blob_links,
            &mut self.owned_cells,
            &refs,
        );
        Ok(())
    }

    /// Appends a data block from its compressed payload and its rows, in key
    /// order after every block appended before it.
    ///
    /// # Errors
    ///
    /// Returns an error if the rows are out of order, a row references a blob
    /// in a value that does not decode, the payload disagrees with its
    /// uncompressed length, or the write fails.
    pub fn append_block(
        &mut self,
        payload: &[u8],
        uncompressed_length: u32,
        rows: &[InternalValue],
    ) -> crate::Result<()> {
        if self.columnar {
            return Err(crate::Error::InvalidHeader(
                "a columnar table takes its rows, not its source's blocks",
            ));
        }
        let refs = self.served_refs(rows)?;
        self.writer.append_compressed_data_block(
            payload,
            uncompressed_length,
            rows,
            &self.comparator,
        )?;
        if let (Some(lo), Some(hi)) = (rows.first(), rows.last()) {
            self.widen_key_range(&lo.key.user_key, &hi.key.user_key);
        }
        crate::blob_tree::links::fold_blob_links(
            &mut self.blob_links,
            &mut self.owned_cells,
            &refs,
        );
        Ok(())
    }

    /// Records a range tombstone deleting `[start, end)` at `seqno`. The
    /// table's key range grows to cover it, as a read checks a tombstone only
    /// in a table whose range holds the key.
    pub fn range_tombstone(&mut self, start: UserKey, end: UserKey, seqno: SeqNo) {
        self.widen_key_range(&start, &end);
        self.writer
            .write_range_tombstone(crate::range_tombstone::RangeTombstone::new(
                start, end, seqno,
            ));
    }

    /// Finishes the table and returns its whole-file checksum.
    ///
    /// # Errors
    ///
    /// Returns any error writing the table's sections, and
    /// [`crate::Error::InvalidHeader`] for a table that received nothing.
    pub fn finish(mut self) -> crate::Result<Checksum> {
        let mut links: Vec<_> = self.blob_links.into_values().collect();
        links.sort_unstable_by_key(|l| l.blob_file_id);
        for link in links {
            self.writer.link_blob_file(link);
        }
        self.writer.own_blob_objects(self.owned_cells);
        // A source holding only range tombstones carries the entry its writer
        // synthesized for them, which counts as no KV.
        self.writer.exclude_carried_sentinel();
        if let Some((start, end)) = self.key_range.take() {
            self.writer.cover_key_range(start, end);
        }
        self.writer
            .finish()?
            .map(|(_, checksum)| checksum)
            .ok_or(crate::Error::InvalidHeader(
                "imported table holds no entries",
            ))
    }
}

/// A blob file being written from value frames another store holds: each
/// value as stored, under the file's codec, at the offset the source held it,
/// so every value handle that names it still does.
pub struct BlobFileImport {
    writer: crate::vlog::blob_file::writer::Writer,
    fs: Arc<dyn Fs>,
    restriction: Option<BlobFileRestriction>,
}

/// A source blob file whose values below a frontier were reclaimed.
///
/// The imported file is restricted the same way: its first live value's
/// offset, and what the source's metadata counts over the whole file as
/// written. The garbage a file is charged with includes its reclaimed values,
/// so it is measured against these totals and not against the values the
/// import carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobFileRestriction {
    /// The offset of the first live value.
    pub live_from: u64,
    /// Values the source file was written with.
    pub item_count: u64,
    /// Their bytes as stored.
    pub compressed_bytes: u64,
    /// Their bytes once decompressed.
    pub uncompressed_bytes: u64,
    /// The first key the source file was written with.
    pub first_key: crate::UserKey,
    /// The last key the source file was written with.
    pub last_key: crate::UserKey,
}

impl BlobFileImport {
    /// Starts blob file `id` at `path`, holding values compressed with
    /// `compression` and recording `created_at` as its age. A source whose
    /// values below a frontier were reclaimed gets the same `restriction`: its
    /// first value lands at the frontier, the file is restricted to what
    /// follows, and its metadata records the source's totals.
    ///
    /// # Errors
    ///
    /// Returns any error creating the file.
    pub fn create(
        path: &Path,
        id: crate::vlog::BlobFileId,
        fs: Arc<dyn Fs>,
        compression: CompressionType,
        created_at: u128,
        restriction: Option<BlobFileRestriction>,
    ) -> crate::Result<Self> {
        let mut writer = crate::vlog::blob_file::writer::Writer::new(path, id, 0, &*fs)?;
        // The values arrive compressed: the writer stores them as they are and
        // records their codec.
        writer.metadata_compression_override = Some(compression);
        writer.created_at = Some(created_at);
        // The previous format records no lifetime class: every value it holds
        // is of class 0, the writer's own.
        writer.write_filler(restriction.as_ref().map_or(0, |r| r.live_from))?;
        Ok(Self {
            writer,
            fs,
            restriction,
        })
    }

    /// Appends one value as stored, which must land at `offset`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] when the value would land at
    /// another offset, which would leave every later handle naming the wrong
    /// bytes, and any error writing it.
    pub fn append(
        &mut self,
        offset: u64,
        key: &[u8],
        seqno: SeqNo,
        stored: &[u8],
        uncompressed_len: u32,
    ) -> crate::Result<()> {
        if self.writer.offset() != offset || key.is_empty() || u16::try_from(key.len()).is_err() {
            return Err(crate::Error::InvalidHeader("imported blob frame"));
        }
        self.writer
            .write_raw(key, seqno, stored, uncompressed_len)?;
        Ok(())
    }

    /// Finishes the file and returns the checksum its manifest entry records:
    /// of the whole file, or of what follows the frontier for a restricted
    /// one, whose prefix is then punched where the filesystem can.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] for a file that received no
    /// value, or for a restricted one that received more than its source's
    /// totals count, and any error writing its metadata or hashing its suffix.
    pub fn finish(mut self) -> crate::Result<Checksum> {
        if self.writer.item_count == 0 {
            return Err(crate::Error::InvalidHeader(
                "imported blob file holds no values",
            ));
        }
        let live_from = match self.restriction.take() {
            None => 0,
            Some(source) => {
                // The live suffix is part of what the source counted.
                if self.writer.item_count > source.item_count
                    || self.writer.written_blob_bytes > source.compressed_bytes
                    || self.writer.uncompressed_bytes > source.uncompressed_bytes
                {
                    return Err(crate::Error::InvalidHeader("imported blob file totals"));
                }
                self.writer.item_count = source.item_count;
                self.writer.written_blob_bytes = source.compressed_bytes;
                self.writer.uncompressed_bytes = source.uncompressed_bytes;
                self.writer.first_key = Some(source.first_key);
                self.writer.last_key = Some(source.last_key);
                source.live_from
            }
        };
        let path = self.writer.path.clone();
        let (_, checksum) = self.writer.finish()?;
        let checksum = if live_from == 0 {
            checksum
        } else {
            // The digest every reader of a restricted entry checks; taken
            // before the punch, which leaves the suffix as it is.
            let suffix = Checksum::from_raw(crate::repair::compute_table_checksum_from(
                &*self.fs, &path, live_from,
            )?);
            if self.fs.capabilities(&path).punch_hole {
                self.fs.punch_hole(&path, 0, live_from)?;
            }
            suffix
        };
        crate::file::fsync_directory(
            crate::file::entry_directory(&path),
            &*self.fs,
            crate::fs::SyncMode::default(),
        )?;
        Ok(checksum)
    }
}

/// One table as the imported manifest places it.
#[derive(Clone, Copy, Debug)]
pub struct TablePlacement {
    /// The table's id, also its file name in the tables folder.
    pub id: TableId,
    /// The checksum its writer returned.
    pub checksum: Checksum,
    /// The seqno added to every entry of the table on read.
    pub global_seqno: SeqNo,
    /// The table's L0 recency key.
    pub recency: TableId,
}

/// Stores `dict` in the dictionary folder of the tree in `folder`, sealed with
/// `encryption` when the tree has a provider, as a registration stores it.
///
/// # Errors
///
/// Returns any error the dictionary write returns.
#[cfg(zstd_any)]
pub fn store_dictionary(
    folder: &Path,
    dict: &crate::compression::ZstdDictionary,
    fs: &dyn Fs,
    encryption: Option<&dyn EncryptionProvider>,
) -> crate::Result<()> {
    crate::dicts::write(
        fs,
        &folder.join(crate::file::DICTS_FOLDER),
        dict,
        encryption,
        crate::fs::SyncMode::default(),
    )
}

/// One blob file as the imported manifest records it.
#[derive(Clone, Copy, Debug)]
pub struct BlobFilePlacement {
    /// The blob file's id, also its file name in the blobs folder.
    pub id: crate::vlog::BlobFileId,
    /// The checksum [`BlobFileImport::finish`] returned.
    pub checksum: Checksum,
    /// What it holds that no table references any more: objects, bytes as
    /// counted, bytes as stored.
    pub stale: (usize, u64, u64),
    /// The offset of its first live value: `0`, or the frontier below which a
    /// tight-space relocation reclaimed the source file.
    pub live_from: u64,
}

/// The state an imported manifest records.
pub struct ManifestImage {
    /// Whether the tree separates values into blob files.
    pub tree_type: TreeType,
    /// The version id the manifest is written under.
    pub version_id: u64,
    /// Tables by level, then run, then key order within the run.
    pub levels: Vec<Vec<Vec<TablePlacement>>>,
    /// Blob files, already written to the blobs folder.
    pub blob_files: Vec<BlobFilePlacement>,
    /// Tight-space restrictions, by table id, with the first key each serves.
    pub restrictions: Vec<(TableId, UserKey)>,
    /// The highest snapshot seqno the tree can no longer serve.
    pub retention_floor: SeqNo,
    /// The compression dictionaries the tree registers.
    pub dicts: Vec<u32>,
    /// The name of the comparator the tree is written under.
    pub comparator_name: &'static str,
}

/// What an imported table records, read back from its file as an open reads
/// it, for the caller to hold against what it imported.
#[derive(Clone, Debug, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent property the table records, not a state"
)]
pub struct RecordedTable {
    /// The table's id.
    pub id: TableId,
    /// Whether its data blocks are columnar.
    pub columnar: bool,
    /// Whether its columnar values are split into the caller's fields.
    pub split_fields: bool,
    /// The table's age, nanoseconds since the Unix epoch.
    pub created_at: u128,
    /// The per-KV checksum footer its data blocks carry, if any.
    pub kv_checksum: Option<crate::runtime_config::ChecksumAlgorithm>,
    /// The parity scheme its blocks carry, if any.
    pub ecc: Option<crate::table::block::EccParams>,
    /// Whether its index is split into a top-level index and index blocks.
    pub partitioned_index: bool,
    /// Whether it keeps per-block seqno bounds.
    pub seqno_bounds: bool,
    /// Whether it keeps a zone map.
    pub zone_map: bool,
    /// Its bulk-ingest provenance, when recorded.
    pub bulk_ingested: Option<bool>,
    /// Its compaction lineage.
    pub lineage: TableLineage,
    /// The blob files its entries reference, by id.
    pub blob_links: Vec<BlobLink>,
    /// The first key it serves when a tight-space compaction restricted it.
    pub restriction: Option<UserKey>,
    /// The lowest and highest local seqno over its entries and range
    /// tombstones.
    pub seqnos: (SeqNo, SeqNo),
    /// The highest local seqno over its entries alone.
    pub highest_kv_seqno: SeqNo,
}

/// What a table's entries hold of one blob file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobLink {
    /// The blob file.
    pub blob_file_id: crate::vlog::BlobFileId,
    /// Objects of the file the table owns.
    pub len: usize,
    /// Bytes of those objects, before compression.
    pub bytes: u64,
    /// Bytes of those objects, as stored.
    pub on_disk_bytes: u64,
}

impl RecordedTable {
    fn of(table: &crate::table::Table) -> crate::Result<Self> {
        let meta = &table.metadata;
        let mut blob_links: Vec<BlobLink> = table
            .blob_links()?
            .iter()
            .map(|link| BlobLink {
                blob_file_id: link.blob_file_id,
                len: link.len,
                bytes: link.bytes,
                on_disk_bytes: link.on_disk_bytes,
            })
            .collect();
        blob_links.sort_unstable_by_key(|link| link.blob_file_id);
        let (seqnos, highest_kv_seqno) = table.local_seqno_bounds();
        Ok(Self {
            id: table.id(),
            columnar: meta.columnar,
            split_fields: meta.value_layout == crate::table::meta::ValueLayout::Split,
            created_at: meta.created_at.into(),
            kv_checksum: meta.kv_checksum_algo,
            ecc: meta.ecc_params,
            partitioned_index: table.regions.index.is_some(),
            seqno_bounds: table.regions.seqno_bounds.is_some(),
            zone_map: table.regions.zone_map.is_some(),
            bulk_ingested: meta.bulk_ingested,
            lineage: TableLineage {
                inputs: meta.lineage.clone(),
                prev: meta.lineage_prev,
                transformed: meta.lineage_transformed,
                last: meta.lineage_last,
            },
            blob_links,
            restriction: table.restrict_lower_bound().cloned(),
            seqnos,
            highest_kv_seqno,
        })
    }
}

/// Writes the manifest of the tree in `folder` from `image`, whose tables are
/// already written, each level's to the folder `tables_folder` names for it,
/// and points `CURRENT` at it.
///
/// Every table is opened the way an open opens it before the manifest is
/// written, so a manifest is never written over a table that does not read.
/// Returns what each table records, in the order `image` places them.
///
/// # Errors
///
/// Returns any error opening a table or writing the manifest.
pub fn install_manifest(
    folder: &Path,
    tables_folder: &dyn Fn(usize) -> PathBuf,
    image: &ManifestImage,
    fs: &Arc<dyn Fs>,
    comparator: &SharedComparator,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    #[cfg(zstd_any)] dictionaries: &crate::compression::ZstdDictionaries,
) -> crate::Result<Vec<RecordedTable>> {
    let cache = Arc::new(crate::cache::Cache::with_capacity_bytes(0));
    let mut tables = Vec::new();
    let placements = image.levels.iter().enumerate().flat_map(|(level, runs)| {
        runs.iter()
            .flatten()
            .map(move |placement| (level, placement))
    });
    for (level, placement) in placements {
        let mut params = crate::table::RecoverParams::new(
            tables_folder(level).join(placement.id.to_string()),
            placement.checksum,
            placement.id,
            fs.clone(),
            comparator.clone(),
            cache.clone(),
        );
        params.global_seqno = placement.global_seqno;
        params.recency = Some(placement.recency);
        params.encryption.clone_from(&encryption);
        #[cfg(zstd_any)]
        {
            params.zstd_dictionaries = dictionaries.clone();
        }
        let table = crate::table::Table::recover(params)?;
        // A restricted table's manifest entry digests its live suffix, from
        // the block that holds its first served key on, as every reader of a
        // restricted entry checks it.
        let table = match image
            .restrictions
            .iter()
            .find(|(id, _)| *id == placement.id)
        {
            Some((_, bound)) => {
                let suffix = table.suffix_checksum_for(Some(bound))?;
                table.with_refreshed_checksum(suffix)
            }
            None => table,
        };
        tables.push(table);
    }

    let blobs_folder = folder.join(crate::file::BLOBS_FOLDER);
    let blob_files = image
        .blob_files
        .iter()
        .map(|file| {
            crate::vlog::recover_blob_file_from(
                &blobs_folder.join(file.id.to_string()),
                file.id,
                file.checksum,
                0,
                fs,
                file.live_from,
                #[cfg(zstd_any)]
                dictionaries,
            )
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let mut gc_stats = crate::blob_tree::FragmentationMap::default();
    for file in &image.blob_files {
        if file.stale != (0, 0, 0) {
            let (len, bytes, on_disk_bytes) = file.stale;
            gc_stats.insert(
                file.id,
                crate::blob_tree::FragmentationEntry::new(len, bytes, on_disk_bytes),
            );
        }
    }

    let recovery = crate::version::recovery::Recovery {
        tree_type: image.tree_type,
        snapshot_id: image.version_id,
        curr_version_id: image.version_id,
        table_ids: image
            .levels
            .iter()
            .map(|level| {
                level
                    .iter()
                    .map(|run| {
                        run.iter()
                            .map(|t| crate::version::recovery::RecoveredTable {
                                id: t.id,
                                checksum: tables
                                    .iter()
                                    .find(|table| table.id() == t.id)
                                    .map_or(t.checksum, crate::table::Table::checksum),
                                global_seqno: t.global_seqno,
                                recency: t.recency,
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect(),
        blob_file_ids: image
            .blob_files
            .iter()
            .map(|file| (file.id, file.checksum))
            .collect(),
        gc_stats,
        restrictions: image.restrictions.iter().cloned().collect(),
        blob_restrictions: image
            .blob_files
            .iter()
            .filter(|file| file.live_from > 0)
            .map(|file| (file.id, file.live_from))
            .collect(),
        retention_floor: image.retention_floor,
        dicts: image.dicts.clone(),
    };
    let version = crate::version::Version::from_recovery(
        recovery,
        &tables,
        &blob_files,
        comparator.as_ref(),
    )?;
    // Read from the version, where each table carries the restriction it is
    // installed under.
    let recorded = image
        .levels
        .iter()
        .flatten()
        .flatten()
        .map(|placement| {
            version
                .iter_tables()
                .find(|table| table.id() == placement.id)
                .ok_or(crate::Error::Unrecoverable)
                .and_then(RecordedTable::of)
        })
        .collect::<crate::Result<Vec<_>>>()?;
    crate::version::persist_version(
        folder,
        &version,
        image.comparator_name,
        &**fs,
        Arc::new(crate::runtime_config::RuntimeConfig::default()),
        encryption.clone(),
        crate::fs::SyncMode::default(),
    )?;
    // The manifest just written reads back under the strictest mode.
    crate::version::recovery::recover(
        folder,
        &**fs,
        ManifestRecoveryMode::AbsoluteConsistency,
        encryption,
    )?;
    Ok(recorded)
}
