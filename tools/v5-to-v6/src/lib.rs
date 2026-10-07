//! Converts a store written by the 5.x crate (on-disk format V5) into the 6.0
//! format (V6), offline.
//!
//! The source is read through the 5.x crate's read-only export, so every V5
//! byte is decoded, verified and, where its parity allows, repaired by the
//! code that wrote it. The converted store is written through the 6.0 crate's
//! import, so every V6 byte comes from the engine's own writers.
//!
//! The converted store is built beside the source in a staging folder inside
//! it, opened and checked there, and only then switched into place.

use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// What the source store was opened with that its files do not record.
#[derive(Clone, Default)]
pub struct Options {
    /// The encryption provider the store was written with, for both formats.
    pub encryption: Option<EncryptionPair>,
    /// Compression dictionaries the store was opened with but does not keep in
    /// its dictionary folder, as raw bytes. The converted store keeps them.
    pub dictionaries: Vec<Vec<u8>>,
}

/// One key, as the 5.x and the 6.0 crate each take it.
#[derive(Clone)]
pub struct EncryptionPair {
    /// The provider the 5.x crate reads with.
    pub v5: Arc<dyn lsm5::EncryptionProvider>,
    /// The provider the 6.0 crate writes with.
    pub v6: Arc<dyn lsm6::EncryptionProvider>,
}

/// What a conversion did.
#[derive(Debug, Default)]
pub struct Report {
    /// Tables converted.
    pub tables: usize,
    /// Data blocks carried over.
    pub data_blocks: usize,
    /// Blob files converted.
    pub blob_files: usize,
    /// Bytes the source's files take.
    pub source_bytes: u64,
    /// Bytes the converted store's files take.
    pub converted_bytes: u64,
    /// Whether this run finished the switch of an earlier interrupted run
    /// rather than converting; the counts are then zero.
    pub resumed: bool,
}

/// Why a conversion stopped.
#[derive(Debug)]
pub enum Error {
    /// The 5.x crate refused to read the source.
    Read(lsm5::Error),
    /// The 6.0 crate refused to write the converted store.
    Write(lsm6::Error),
    /// A filesystem step of the switch failed.
    Io(std::io::Error),
    /// The source holds something this converter does not carry yet.
    Unsupported(&'static str),
    /// A converted table read back other than its source recorded.
    Mismatch(Box<Mismatch>),
}

/// A converted table that does not record what its source recorded.
#[derive(Debug)]
pub struct Mismatch {
    /// What the source recorded.
    pub expected: lsm6::import::RecordedTable,
    /// What the converted table records.
    pub recorded: lsm6::import::RecordedTable,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(e) => write!(f, "reading the 5.x store: {e}"),
            Self::Write(e) => write!(f, "writing the 6.0 store: {e}"),
            Self::Io(e) => write!(f, "switching the stores: {e}"),
            Self::Unsupported(what) => write!(f, "not converted: {what}"),
            Self::Mismatch(m) => write!(
                f,
                "table {} reads back as {:?}, its source recorded {:?}",
                m.expected.id, m.recorded, m.expected
            ),
        }
    }
}

impl std::error::Error for Error {}

impl From<lsm5::Error> for Error {
    fn from(e: lsm5::Error) -> Self {
        Self::Read(e)
    }
}

impl From<lsm6::Error> for Error {
    fn from(e: lsm6::Error) -> Self {
        Self::Write(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// The folder inside the store the converted files are built in.
const STAGING: &str = "v6-convert";

/// The folder the source's entries are set aside in by the switch.
pub const BACKUP: &str = "v5-backup";

/// Marks a converted store that is complete and verified: from the moment it
/// exists the switch only rolls forward. Lists the source entries to set aside.
const READY: &str = "v6-convert.ready";

/// Marks a switch whose source entries are all set aside.
const SWAPPING: &str = "v6-convert.swapping";

/// Converts the store in `folder` in place, leaving the source's entries in
/// [`BACKUP`] inside it.
///
/// A conversion interrupted at any point is finished by running it again: one
/// stopped before the converted store was complete and verified starts over
/// from the untouched source, one stopped during the switch completes it.
///
/// # Errors
///
/// Returns the first error reading the source or writing the converted store;
/// the source is left untouched until the converted store has been built and
/// read back.
pub fn convert(folder: &Path, options: &Options) -> Result<Report, Error> {
    if folder.join(READY).exists() || folder.join(SWAPPING).exists() {
        switch(folder, &mut || Ok(()))?;
        return Ok(Report {
            resumed: true,
            ..Report::default()
        });
    }
    let report = prepare(folder, options)?;
    switch(folder, &mut || Ok(()))?;
    Ok(report)
}

/// Builds the converted store in the staging folder and verifies it, leaving
/// the source untouched.
fn prepare(folder: &Path, options: &Options) -> Result<Report, Error> {
    let backup = folder.join(BACKUP);
    if backup.exists() && std::fs::read_dir(&backup)?.next().is_some() {
        return Err(Error::Unsupported(
            "a store that holds the backup of an earlier conversion",
        ));
    }
    let v5_fs: Arc<dyn lsm5::fs::Fs> = Arc::new(lsm5::fs::StdFs);
    let v6_fs: Arc<dyn lsm6::fs::Fs> = Arc::new(lsm6::fs::StdFs);
    let v5_encryption = options.encryption.as_ref().map(|e| e.v5.clone());
    let v6_encryption = options.encryption.as_ref().map(|e| e.v6.clone());

    let state = lsm5::export::read_manifest(folder, &*v5_fs, v5_encryption.clone())?;
    let tree_type = match state.tree_type {
        lsm5::TreeType::Standard => lsm6::TreeType::Standard,
        lsm5::TreeType::Blob => lsm6::TreeType::Blob,
    };
    if !state.blob_restrictions.is_empty() {
        return Err(Error::Unsupported(
            "a blob file whose consumed prefix was reclaimed",
        ));
    }
    if state.comparator_name != "default" {
        return Err(Error::Unsupported("a tree with a custom comparator"));
    }
    // The dictionaries the store keeps, each verified against its name by the
    // 5.x read, and the ones the caller supplies.
    let mut v5_dictionaries =
        lsm5::export::read_dictionaries(folder, &*v5_fs, v5_encryption.as_deref())?;
    for raw in &options.dictionaries {
        v5_dictionaries = v5_dictionaries.with(Arc::new(lsm5::ZstdDictionary::new(raw)));
    }
    let context = lsm5::export::TableContext::new(
        &state,
        v5_fs.clone(),
        v5_encryption,
        v5_dictionaries.clone(),
        Arc::new(lsm5::DefaultUserComparator),
    )?;

    let mut report = Report::default();
    let staging = folder.join(STAGING);
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    let staging_tables = staging.join(lsm6::file::TABLES_FOLDER);
    std::fs::create_dir_all(&staging_tables)?;

    // Every dictionary goes to the converted store's folder, so the tables
    // and blob files compressed against one still resolve it after a reopen.
    let mut dictionaries = lsm6::ZstdDictionaries::new();
    for v5 in v5_dictionaries.iter() {
        let dict = Arc::new(lsm6::ZstdDictionary::new(v5.raw()));
        lsm6::import::store_dictionary(&staging, &dict, &*v6_fs, v6_encryption.as_deref())?;
        dictionaries = dictionaries.with(dict);
    }

    // Blob files first: each value lands at the offset the source held it, so
    // every value handle in the tables still names it.
    let mut blob_files = Vec::with_capacity(state.blob_files.len());
    if !state.blob_files.is_empty() {
        let staging_blobs = staging.join(lsm6::file::BLOBS_FOLDER);
        std::fs::create_dir_all(&staging_blobs)?;
        for record in &state.blob_files {
            let checksum = convert_blob_file(
                &folder
                    .join(lsm5::file::BLOBS_FOLDER)
                    .join(record.id.to_string()),
                record,
                &staging_blobs.join(record.id.to_string()),
                &v5_fs,
                &v6_fs,
            )?;
            report.blob_files += 1;
            let stale = state
                .blob_gc_stats
                .iter()
                .find(|s| s.id == record.id)
                .map_or((0, 0, 0), |s| {
                    (s.stale_items as usize, s.stale_bytes, s.stale_on_disk_bytes)
                });
            blob_files.push(lsm6::import::BlobFilePlacement {
                id: record.id,
                checksum,
                stale,
            });
        }
    }

    let max_table_id = state
        .levels
        .iter()
        .flatten()
        .flatten()
        .map(|t| t.id)
        .max()
        .unwrap_or(0);
    let restrictions: std::collections::HashMap<u64, lsm5::UserKey> =
        state.restrictions.iter().cloned().collect();

    let mut expected = Vec::new();
    let mut levels = Vec::with_capacity(state.levels.len());
    for (level_idx, level) in state.levels.iter().enumerate() {
        let mut runs = Vec::with_capacity(level.len());
        for (run_idx, run) in level.iter().enumerate() {
            // L0 is ordered by recency, newest run first. The converted runs
            // keep that order with keys at or below the largest table id, so
            // a later flush, which takes a higher id, stays newer.
            let recency_of = |id: u64| -> Result<u64, Error> {
                if level_idx == 0 {
                    max_table_id
                        .checked_sub(run_idx as u64)
                        .ok_or(Error::Unsupported("more L0 runs than table ids"))
                } else {
                    Ok(id)
                }
            };
            let mut placements = Vec::with_capacity(run.len());
            for record in run {
                let recency = recency_of(record.id)?;
                let export = lsm5::export::TableExport::open(
                    &folder
                        .join(lsm5::file::TABLES_FOLDER)
                        .join(record.id.to_string()),
                    record,
                    restrictions.get(&record.id),
                    &context,
                )?;
                let converted = convert_table(
                    &export,
                    staging_tables.join(record.id.to_string()),
                    recency,
                    v6_fs.clone(),
                    v6_encryption.clone(),
                    &dictionaries,
                    restrictions.get(&record.id),
                )?;
                report.tables += 1;
                report.data_blocks += converted.data_blocks;
                expected.push((converted.expected, converted.restricted));
                placements.push(lsm6::import::TablePlacement {
                    id: record.id,
                    checksum: converted.checksum,
                    global_seqno: record.global_seqno,
                    recency,
                });
            }
            runs.push(placements);
        }
        levels.push(runs);
    }

    let image = lsm6::import::ManifestImage {
        tree_type,
        version_id: state.version_id,
        levels,
        blob_files,
        restrictions: state
            .restrictions
            .iter()
            .map(|(id, key)| (*id, lsm6::UserKey::from(&**key)))
            .collect(),
        retention_floor: state.retention_floor,
        dicts: state.dicts.clone(),
        comparator_name: "default",
    };
    let recorded = lsm6::import::install_manifest(
        &staging,
        &image,
        &v6_fs,
        &(Arc::new(lsm6::DefaultUserComparator) as lsm6::SharedComparator),
        v6_encryption,
        &dictionaries,
    )?;
    // Every converted table, read back as an open reads it, records what its
    // source recorded; the source is not touched until it does.
    // The import reads back one record per placement, in placement order.
    debug_assert_eq!(expected.len(), recorded.len());
    for ((expected, restricted), recorded) in expected.into_iter().zip(recorded) {
        if !records_match(&expected, &recorded, restricted) {
            return Err(Error::Mismatch(Box::new(Mismatch { expected, recorded })));
        }
    }

    report.source_bytes = source_entries(folder)?
        .iter()
        .map(|name| bytes_under(&folder.join(name)))
        .sum::<std::io::Result<u64>>()?;
    report.converted_bytes = bytes_under(&staging)?;
    Ok(report)
}

/// Writes one table through the 6.0 import from what its 5.x export reads:
/// each data block's verified, still compressed payload with its rows, and
/// the table's range tombstones, under the properties the source recorded.
fn convert_table(
    export: &lsm5::export::TableExport,
    path: PathBuf,
    recency: u64,
    fs: Arc<dyn lsm6::fs::Fs>,
    encryption: Option<Arc<dyn lsm6::EncryptionProvider>>,
    dictionaries: &lsm6::ZstdDictionaries,
    restriction: Option<&lsm5::UserKey>,
) -> Result<Converted, Error> {
    // A table a tight-space compaction restricted holds nothing below its
    // live offset: those blocks were reclaimed. The ones from it on are
    // carried, and the manifest keeps the restriction, which hides the keys
    // below the bound the first carried block may still hold.
    let live_from = export.live_from()?;
    let properties = export.properties()?;
    // The filter and the locator are repacked, not rebuilt: the same solved
    // rows in the 6.0 layout, so they answer exactly what they answered.
    let filter = export.filter()?.map(|filter| match filter {
        lsm5::export::Filter::Full(solution) => {
            lsm6::import::FilterImage::Full(burr_image(solution))
        }
        lsm5::export::Filter::Partitioned(partitions) => lsm6::import::FilterImage::Partitioned(
            partitions
                .into_iter()
                .map(|p| {
                    (
                        lsm6::UserKey::from(&*p.entry.end_key),
                        burr_image(p.solution),
                    )
                })
                .collect(),
        ),
    });
    // A locator names blocks by ordinal: carried only when the converted
    // table holds every source block, in order.
    let locator = export.locator()?.map(|l| {
        if properties.columnar || live_from > 0 {
            lsm6::import::LocatorImport::Rebuilt {
                precision: l.precision,
            }
        } else {
            lsm6::import::LocatorImport::Carried(lsm6::import::LocatorImage {
                precision: l.precision,
                block_id_bits: l.block_id_bits,
                slot_bits: l.slot_bits,
                solution: burr_image(l.solution),
            })
        }
    });
    // What the converted table must record: everything the source recorded
    // that 6.0 keeps.
    let expected = lsm6::import::RecordedTable {
        id: export.id(),
        columnar: properties.columnar,
        created_at: properties.created_at,
        kv_checksum: properties.kv_checksum.map(checksum_algorithm),
        ecc: properties.ecc.map(ecc_params).transpose()?,
        partitioned_index: properties.partitioned_index,
        seqno_bounds: properties.seqno_bounds,
        zone_map: properties.zone_map,
        bulk_ingested: properties.bulk_ingested,
        lineage: lsm6::import::TableLineage {
            inputs: properties.lineage,
            prev: properties.lineage_prev,
            transformed: properties.lineage_transformed,
            last: properties.lineage_last,
        },
        blob_links: {
            let mut links: Vec<_> = export
                .linked_blob_files()?
                .unwrap_or_default()
                .into_iter()
                .map(|link| lsm6::import::BlobLink {
                    blob_file_id: link.blob_file_id,
                    len: link.len,
                    bytes: link.bytes,
                    on_disk_bytes: link.on_disk_bytes,
                })
                .collect();
            links.sort_unstable_by_key(|link| link.blob_file_id);
            links
        },
        restriction: restriction.map(|key| lsm6::UserKey::from(&**key)),
    };
    let data_compression = compression(properties.data_compression)?;
    let settings = lsm6::import::TableSettings {
        data_compression,
        index_compression: compression(properties.index_compression)?,
        data_restart_interval: properties.data_restart_interval,
        index_restart_interval: properties.index_restart_interval,
        encryption,
        zstd_dictionary: dictionaries.for_compression(data_compression)?,
        recency,
        created_at: expected.created_at,
        kv_checksum: expected.kv_checksum,
        ecc: expected.ecc,
        partitioned_index: expected.partitioned_index,
        seqno_bounds: expected.seqno_bounds,
        zone_map: expected.zone_map,
        bulk_ingested: expected.bulk_ingested,
        lineage: expected.lineage.clone(),
        columnar: expected.columnar,
        filter,
        locator,
    };
    let comparator: lsm6::SharedComparator = Arc::new(lsm6::DefaultUserComparator);
    let mut table = lsm6::import::TableImport::create(
        path,
        export.id(),
        properties.initial_level,
        fs,
        comparator,
        settings,
    )?;
    let blocks: Vec<_> = export
        .data_blocks()?
        .into_iter()
        .filter(|block| block.offset >= live_from)
        .collect();
    for block in &blocks {
        if expected.columnar {
            // The 6.0 columnar layout is not the 5.x one: the rows, deleted
            // ones included, are encoded again in it, and the delete bitmap
            // marks the same rows.
            let rows: Vec<lsm6::InternalValue> = export
                .rows(block)?
                .into_iter()
                .map(internal_value)
                .collect::<Result<_, _>>()?;
            table.append_rows(rows, &export.deleted_rows_in(block)?)?;
            continue;
        }
        let frame = export.frame(
            block.offset,
            block.size,
            lsm5::table::block::BlockType::Data,
        )?;
        let rows: Vec<lsm6::InternalValue> = export
            .rows(block)?
            .into_iter()
            .map(internal_value)
            .collect::<Result<_, _>>()?;
        table.append_block(&frame.payload, frame.uncompressed_length, &rows)?;
    }
    for rt in export.range_tombstones() {
        table.range_tombstone(
            lsm6::UserKey::from(&*rt.start),
            lsm6::UserKey::from(&*rt.end),
            rt.seqno,
        );
    }
    Ok(Converted {
        checksum: table.finish()?,
        data_blocks: blocks.len(),
        expected,
        restricted: live_from > 0,
    })
}

/// Whether `recorded`, what a converted table reads back as, is what its
/// source recorded in `expected`. A restricted source lists its blob links
/// over every row it ever held, the reclaimed ones included, so the
/// converted table's links, derived from the rows it holds, need only fit
/// inside them.
fn records_match(
    expected: &lsm6::import::RecordedTable,
    recorded: &lsm6::import::RecordedTable,
    restricted: bool,
) -> bool {
    if !restricted {
        return expected == recorded;
    }
    let links_fit = recorded.blob_links.iter().all(|link| {
        expected.blob_links.iter().any(|source| {
            source.blob_file_id == link.blob_file_id
                && link.len <= source.len
                && link.bytes <= source.bytes
                && link.on_disk_bytes <= source.on_disk_bytes
        })
    });
    let rest_equal = lsm6::import::RecordedTable {
        blob_links: Vec::new(),
        ..expected.clone()
    } == lsm6::import::RecordedTable {
        blob_links: Vec::new(),
        ..recorded.clone()
    };
    links_fit && rest_equal
}

/// Writes one blob file through the 6.0 import from the frames its 5.x export
/// reads, each verified, as stored and at its source offset. Returns the
/// converted file's checksum.
fn convert_blob_file(
    source: &Path,
    record: &lsm5::export::BlobFileRecord,
    path: &Path,
    v5_fs: &Arc<dyn lsm5::fs::Fs>,
    v6_fs: &Arc<dyn lsm6::fs::Fs>,
) -> Result<lsm6::Checksum, Error> {
    let export = lsm5::export::BlobFileExport::open(source, record, 0, v5_fs.clone())?;
    let mut file = lsm6::import::BlobFileImport::create(
        path,
        record.id,
        v6_fs.clone(),
        compression(export.compression())?,
        export.created_at(),
    )?;
    for frame in export.frames()? {
        file.append(
            frame.offset,
            &frame.key,
            frame.seqno,
            &frame.stored,
            frame.uncompressed_len,
        )?;
    }
    Ok(file.finish()?)
}

/// One table as [`convert_table`] wrote it.
struct Converted {
    /// The checksum its writer returned.
    checksum: lsm6::Checksum,
    /// How many data blocks it holds.
    data_blocks: usize,
    /// What it must record when read back.
    expected: lsm6::import::RecordedTable,
    /// Whether its source was restricted, so only its live suffix was carried.
    restricted: bool,
}

/// A 5.x solution as the 6.0 import takes it: the same parameters, seed and
/// rows.
fn burr_image(s: lsm5::export::BurrSolution) -> lsm6::import::BurrImage {
    let kind = match s.kind {
        lsm5::export::BurrKind::Membership => lsm6::import::BurrKind::Membership,
        lsm5::export::BurrKind::Retrieval => lsm6::import::BurrKind::Retrieval,
    };
    lsm6::import::BurrImage {
        kind,
        r: s.r,
        w: s.w,
        b: s.b,
        root_seed: s.root_seed,
        layers: s
            .layers
            .into_iter()
            .map(|l| lsm6::import::BurrLayerImage {
                m: l.m,
                thresholds: l.thresholds,
                rows: l.rows,
            })
            .collect(),
    }
}

/// One 5.x entry as the 6.0 crate holds it: same key, seqno, kind and value.
fn internal_value(v: lsm5::InternalValue) -> Result<lsm6::InternalValue, Error> {
    let kind = lsm6::ValueType::try_from(u8::from(v.key.value_type))
        .map_err(|()| Error::Unsupported("an entry kind 6.0 does not know"))?;
    Ok(lsm6::InternalValue::from_components(
        &*v.key.user_key,
        &*v.value,
        v.key.seqno,
        kind,
    ))
}

/// A 5.x codec as the 6.0 crate names it.
fn compression(v5: lsm5::CompressionType) -> Result<lsm6::CompressionType, Error> {
    Ok(match v5 {
        lsm5::CompressionType::None => lsm6::CompressionType::None,
        lsm5::CompressionType::Lz4 => lsm6::CompressionType::Lz4,
        lsm5::CompressionType::Zstd(level) => lsm6::CompressionType::Zstd(level),
        lsm5::CompressionType::ZstdDict { level, dict_id } => {
            lsm6::CompressionType::ZstdDict { level, dict_id }
        }
        _ => return Err(Error::Unsupported("a codec this converter does not know")),
    })
}

/// A 5.x per-KV checksum algorithm as the 6.0 crate names it; the footer
/// bytes are the same.
fn checksum_algorithm(
    v5: lsm5::runtime_config::ChecksumAlgorithm,
) -> lsm6::runtime_config::ChecksumAlgorithm {
    use lsm5::runtime_config::ChecksumAlgorithm as V5;
    use lsm6::runtime_config::ChecksumAlgorithm as V6;
    match v5 {
        V5::Xxh3_64 => V6::Xxh3_64,
        V5::Xxh3Low32 => V6::Xxh3Low32,
        V5::Crc32c => V6::Crc32c,
    }
}

/// A 5.x parity scheme as the 6.0 crate names it. The converted blocks get
/// fresh parity under the same scheme.
fn ecc_params(v5: lsm5::table::block::EccParams) -> Result<lsm6::table::block::EccParams, Error> {
    Ok(match v5 {
        lsm5::table::block::EccParams::Shard {
            data_shards,
            parity_shards,
        } => lsm6::table::block::EccParams::Shard {
            data_shards,
            parity_shards,
        },
        lsm5::table::block::EccParams::Secded => lsm6::table::block::EccParams::Secded,
        _ => {
            return Err(Error::Unsupported(
                "a parity scheme this converter does not know",
            ));
        }
    })
}

/// Whether `name` in a store's folder is the store's own: its version
/// pointer, a manifest snapshot or edit log, or a file folder. Anything else,
/// the directory lock included, is not the source's and stays where it is.
fn is_store_entry(name: &str) -> bool {
    let generation = |rest: &str| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit());
    name == lsm5::file::CURRENT_VERSION_FILE
        || name == lsm5::file::TABLES_FOLDER
        || name == lsm5::file::BLOBS_FOLDER
        || name == lsm5::file::DICTS_FOLDER
        || name.strip_prefix('v').is_some_and(generation)
        || name.strip_prefix("edits-").is_some_and(generation)
}

/// The source's entries in `folder`, by name.
fn source_entries(folder: &Path) -> std::io::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(folder)? {
        let name = entry?.file_name();
        if let Some(name) = name.to_str().filter(|n| is_store_entry(n)) {
            names.push(name.to_owned());
        }
    }
    names.sort_unstable();
    Ok(names)
}

/// Bytes the files at and under `path` take.
fn bytes_under(path: &Path) -> std::io::Result<u64> {
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_dir() {
        return Ok(meta.len());
    }
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        total += bytes_under(&entry?.path())?;
    }
    Ok(total)
}

/// Makes the entries of `dir` durable, as the engine does on this platform.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    Ok(lsm6::fs::Fs::sync_directory(&lsm6::fs::StdFs, dir)?)
}

/// Writes `contents` to `path` durably and atomically: a crash leaves either
/// no file or the whole one.
fn write_marker(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

/// Puts the converted store in the staging folder in place of the source,
/// whose entries are set aside in [`BACKUP`]. `step` runs before every change
/// to the folder, and an error it returns stops the switch there.
///
/// Every change is a rename, and the markers say how far the switch got, so
/// a switch stopped anywhere is finished by running it again:
///
/// 1. [`READY`] is written with the source's entries: the converted store is
///    complete, and the source is no longer what a rerun reads.
/// 2. Each listed entry moves into the backup; one already gone was moved.
/// 3. [`SWAPPING`] is written: the source is wholly set aside.
/// 4. Each staged entry moves into the folder, the version pointer last, so
///    the folder opens as the converted store only once all of it is there.
/// 5. The staging folder and the markers go, [`READY`] first: with only
///    [`SWAPPING`] left, a rerun has nothing to set aside.
fn switch(folder: &Path, step: &mut dyn FnMut() -> std::io::Result<()>) -> Result<(), Error> {
    let staging = folder.join(STAGING);
    let backup = folder.join(BACKUP);
    let ready = folder.join(READY);
    let swapping = folder.join(SWAPPING);

    if !swapping.exists() {
        if !ready.exists() {
            let entries = source_entries(folder)?;
            step()?;
            write_marker(&ready, &entries.join("\n"))?;
        }
        let listed = std::fs::read_to_string(&ready)?;
        if !backup.exists() {
            step()?;
            std::fs::create_dir(&backup)?;
            sync_dir(folder)?;
        }
        for name in listed.lines().filter(|n| !n.is_empty()) {
            let from = folder.join(name);
            if !from.exists() {
                continue;
            }
            let to = backup.join(name);
            if to.exists() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("{} is already in the backup", to.display()),
                )));
            }
            step()?;
            std::fs::rename(&from, &to)?;
        }
        sync_dir(&backup)?;
        sync_dir(folder)?;
        step()?;
        write_marker(&swapping, "")?;
    }

    if staging.exists() {
        let mut names: Vec<std::ffi::OsString> = std::fs::read_dir(&staging)?
            .map(|entry| entry.map(|e| e.file_name()))
            .collect::<std::io::Result<_>>()?;
        // The version pointer last: until it is in place the folder opens as
        // nothing, never as part of the converted store.
        names.sort_by_key(|name| name == lsm6::file::CURRENT_VERSION_FILE);
        for name in names {
            let to = folder.join(&name);
            if to.exists() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("{} is in the way of the converted store", to.display()),
                )));
            }
            step()?;
            std::fs::rename(staging.join(&name), &to)?;
        }
        sync_dir(folder)?;
        step()?;
        std::fs::remove_dir(&staging)?;
    }
    if ready.exists() {
        step()?;
        std::fs::remove_file(&ready)?;
    }
    step()?;
    std::fs::remove_file(&swapping)?;
    sync_dir(folder)?;
    Ok(())
}
