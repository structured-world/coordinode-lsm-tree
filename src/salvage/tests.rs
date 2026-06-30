use super::{BlobDropReason, DropReason, salvage_blob_file, salvage_sst};
// The options-bearing entry is exercised only by the encrypted / dictionary
// salvage tests, which are themselves feature-gated.
#[cfg(any(feature = "encryption", zstd_any))]
use super::{SalvageOptions, salvage_sst_with_options};
use crate::comparator::default_comparator;
use crate::fs::{Fs, StdFs};
use crate::table::{Table, Writer};
use crate::{InternalValue, ValueType};
use alloc::sync::Arc;
use tempfile::tempdir;
use test_log::test;

fn iv(i: u32) -> InternalValue {
    InternalValue::from_components(
        format!("key{i:05}").into_bytes(),
        format!("val{i:05}").into_bytes(),
        1,
        ValueType::Value,
    )
}

/// Opens an SST as a `Table`, stamping the open with the file's current digest
/// (the source may be corrupt; per-block checksums catch the actual damage).
fn open(path: std::path::PathBuf, fs: &Arc<dyn Fs>) -> crate::Result<Table> {
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, &path)?);
    Table::recover(
        path,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(fs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )
}

/// A reopen of a salvaged SST: recover it and return its live item count.
fn reopen_item_count(path: std::path::PathBuf, fs: &Arc<dyn Fs>) -> crate::Result<u64> {
    Ok(open(path, fs)?.metadata.item_count)
}

#[test]
fn salvage_of_a_healthy_sst_recovers_every_block() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Build a multi-block source SST: small data blocks force several blocks so
    // the per-block walk has more than one block to recover.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert!(
        report.is_complete(),
        "a healthy SST salvages with no dropped blocks: {report:?}",
    );
    assert!(
        report.blocks_total >= 2,
        "256-byte blocks over 200 entries should yield several data blocks, got {}",
        report.blocks_total,
    );
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block of a healthy SST is salvaged",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered",
    );
    assert_eq!(
        report.salvaged_path.as_deref(),
        Some(dest.as_path()),
        "a salvaged file is written when at least one block is recovered",
    );

    // The salvaged copy is a valid SST that reopens and holds every key.
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        u64::from(n),
        "the salvaged SST reopens with the full item count",
    );
    Ok(())
}

/// One deliberately corrupted data block: salvage drops exactly that block
/// (naming its key range) and recovers every other block, instead of failing
/// the whole file. This is the core block-granular contract.
#[test]
fn salvage_drops_a_corrupted_block_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Resolve the second data block's on-disk offset from the (intact) index,
    // then flip a byte a little past its header so the block's data checksum
    // fails on load. load_data_block reads the block by the index handle's size,
    // so the corruption surfaces as that one block failing, not a desync.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source SST must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert!(
        !report.is_complete(),
        "a corrupted block must be reported as dropped: {report:?}",
    );
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the one corrupted block is dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged,
        report.blocks_total - 1,
        "every block but the corrupted one is recovered",
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );
    assert!(
        report.dropped.first().is_some_and(|d| {
            matches!(d.reason, DropReason::ChecksumMismatch) && d.key_range.is_some()
        }),
        "the dropped block reports a checksum mismatch and names the key range it lost: {report:?}",
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    // The salvaged copy reopens and holds exactly the recovered entries.
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        report.entries_salvaged,
        "the salvaged SST holds exactly the entries the report counted",
    );
    Ok(())
}

/// A columnar source with one corrupted PAX data block: the columnar loader
/// fails to reconstruct that block (a torn sub-column frame), so salvage drops
/// it and recovers every other block, writing the survivors as a plain row SST.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_corrupted_columnar_block_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A columnar SST (PAX blocks + zone map), no deletes so there is no
    // delete-bitmap section to worry about here.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Corrupt the second columnar data block's bytes (offset from the intact
    // index, a little past its header) so its reconstruction fails on load.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source columnar SST must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the one corrupted columnar block is dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged,
        report.blocks_total - 1,
        "every columnar block but the corrupted one is recovered",
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    // The salvaged copy stays COLUMNAR (mirrored from the source) and holds the
    // recovered rows — no longer degraded to a row-major copy.
    let recovered = open(dest, &fs)?;
    assert_eq!(recovered.metadata.item_count, report.entries_salvaged);
    assert!(
        recovered.metadata.columnar,
        "a columnar source salvages into a columnar copy, not a row-major one",
    );
    Ok(())
}

/// A columnar source carrying deletes whose `delete_bitmap` section is
/// corrupted (data blocks intact): normal recovery refuses to open it (opening
/// would resurrect deleted rows), but salvage degrades to "all rows live" and
/// recovers every block.
#[cfg(feature = "columnar")]
#[test]
fn salvage_tolerates_a_corrupt_delete_bitmap_as_all_live() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    // Mark a few positions deleted so a delete-bitmap section is co-written.
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the middle of the `delete_bitmap` SFA section (the data blocks
    // stay intact, so only the sidecar is damaged).
    let (db_pos, db_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("source must carry a delete_bitmap section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(db_pos + db_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Normal recovery fails closed: a corrupt bitmap would resurrect deleted rows.
    assert!(
        open(source.clone(), &fs).is_err(),
        "normal recovery must fail closed on a corrupt delete-bitmap",
    );

    // Salvage degrades to "all rows live": every block recovers, nothing masked.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the sidecar was corrupt: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live, the corrupt bitmap is ignored",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// A columnar SST with deletes whose ZONE MAP is corrupt (the bitmap stays
/// readable): the bitmap cannot be positioned without the zone map, so normal
/// recovery fails closed, but salvage ignores the bitmap ("all rows live") and
/// recovers every row.
#[cfg(feature = "columnar")]
#[test]
fn salvage_ignores_a_delete_bitmap_without_a_readable_zone_map() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the zone_map section (the bitmap stays intact). The zone map
    // degrades to empty, leaving a readable bitmap that cannot be positioned.
    let (zm_pos, zm_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(zm_pos + zm_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Normal recovery fails closed: a bitmap with no positioning zone map.
    assert!(
        open(source.clone(), &fs).is_err(),
        "normal recovery must reject a bitmap with no readable zone map",
    );

    // Salvage ignores the unpositionable bitmap and recovers every row live.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the zone map was corrupt: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live once the unpositionable bitmap is ignored",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// When the source cannot be opened at all (a corrupt SFA trailer makes even
/// salvage-mode recovery fail), `salvage_sst` returns an error rather than
/// writing a partial file.
#[test]
fn salvage_sst_errors_when_the_source_cannot_be_opened() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..50 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Truncate away the tail (SFA trailer + section mirrors) so the container is
    // unparseable and even salvage-mode recovery cannot open it.
    let mut bytes = std::fs::read(&source)?;
    bytes.truncate(bytes.len() / 2);
    std::fs::write(&source, &bytes)?;

    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "an unparseable container must fail salvage, not write a partial file",
    );
    assert!(
        !dest.exists(),
        "no destination is written on an open failure"
    );
    Ok(())
}

/// A single-block SST whose only data block is corrupt salvages nothing: no
/// destination file is written and the report records the dropped block.
#[test]
fn salvage_sst_recovers_nothing_when_the_only_block_is_corrupt() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A handful of small keys fit in one default-sized data block.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..8 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Corrupt the sole data block (offset from the intact index).
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&only) = offsets.first() else {
            panic!("expected a single data block, got {offsets:?}");
        };
        assert_eq!(
            offsets.len(),
            1,
            "expected a single data block, got {offsets:?}"
        );
        only
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(report.blocks_salvaged, 0, "the only block was corrupt");
    assert_eq!(report.entries_salvaged, 0, "no entries recovered");
    assert_eq!(report.dropped.len(), 1, "the dropped block is reported");
    assert!(
        report.salvaged_path.is_none(),
        "nothing recoverable means no file is written",
    );
    assert!(!dest.exists(), "no destination file on an empty salvage");
    Ok(())
}

/// A columnar source whose delete-bitmap wholly covers its leading data
/// block(s): those blocks carry no live rows, so salvage skips them (nothing
/// salvaged, nothing dropped) and recovers the live rows of the rest.
#[cfg(feature = "columnar")]
#[test]
fn salvage_skips_a_wholly_deleted_block() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    // Delete the first 60 row positions: with 256-byte blocks this wholly covers
    // the leading data block(s), which then load as "no live rows".
    let deleted = 60u32;
    for pos in 0..deleted {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "wholly-deleted blocks are skipped, not dropped: {report:?}",
    );
    assert!(
        report.blocks_salvaged < report.blocks_total,
        "at least one leading block was wholly deleted and skipped: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n - deleted),
        "every live row is recovered, the deleted prefix is skipped",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n - deleted));
    Ok(())
}

/// An SST carrying range tombstones cannot be salvaged: the positional KV walk
/// re-emits only point entries, so the tombstones would be silently dropped and
/// lower-level keys they cover could reappear after repair. Until the writer
/// path re-emits them, salvage fails closed.
#[test]
fn salvage_rejects_an_sst_with_range_tombstones() -> crate::Result<()> {
    use crate::UserKey;
    use crate::range_tombstone::RangeTombstone;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..20 {
        writer.write(iv(i))?;
    }
    // A range tombstone over part of the key space: the salvaged copy must not
    // silently drop it.
    writer.write_range_tombstone(RangeTombstone::new(
        UserKey::from(b"key00005".as_slice()),
        UserKey::from(b"key00010".as_slice()),
        2,
    ));
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        matches!(result, Err(crate::Error::FeatureUnsupported(_))),
        "an SST with range tombstones must fail closed, got {result:?}",
    );
    assert!(
        !dest.exists(),
        "no salvaged file is written when salvage fails closed",
    );
    Ok(())
}

/// Salvage drives every read and write through the injected `Fs`: an SST that
/// lives only in an in-memory backend (never on the real filesystem) salvages
/// and reopens purely through that backend. A source-digest path that bypassed
/// `fs` and read through `std::fs` would fail to find the file at all.
#[test]
fn salvage_sst_reads_and_writes_through_the_injected_fs() -> crate::Result<()> {
    use crate::fs::MemFs;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    // `Writer::new` rewrites its path through `std::path::absolute`, which on
    // Windows resolves a `/`-rooted path against the current drive (`/memfs` ->
    // `D:\memfs`). Create the parent under that same absolutized form so the
    // writer's parent-directory check finds it on every platform (on Unix
    // `absolute` is a no-op, so this is just `/memfs`).
    let dir = std::path::absolute("/memfs")?;
    fs.create_dir_all(&dir)?;
    let source = dir.join("source");
    let dest = dir.join("salvaged");

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "in-memory source SST is non-empty"
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy in-memory SST salvages with no dropped blocks: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered through the in-memory backend",
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        u64::from(n),
        "the salvaged SST reopens through the same in-memory backend",
    );
    Ok(())
}

// --- Forwarded recovery context: encrypted + dictionary-compressed sources ---

/// Reads the second data block's on-disk offset from a context-aware reopen of
/// `source`, then flips a byte just past that block's header so its checksum /
/// AEAD tag fails on load while every other block stays intact.
#[cfg(any(feature = "encryption", zstd_any))]
fn corrupt_second_data_block(
    source: &std::path::Path,
    fs: &Arc<dyn Fs>,
    table_id: crate::table::TableId,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
) -> crate::Result<()> {
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, source)?);
    let table = Table::recover(
        source.to_path_buf(),
        checksum,
        0,
        0,
        // Open under the source's table id so an encrypted index (AAD binds the
        // id) decrypts when reading the block offsets.
        table_id,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(fs),
        false,
        false,
        encryption,
        #[cfg(zstd_any)]
        zstd_dictionary,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    let offsets: alloc::vec::Vec<u64> = table
        .data_block_handles()
        .filter_map(Result::ok)
        .map(|kh| *kh.as_ref().offset())
        .collect();
    let Some(&second) = offsets.get(1) else {
        panic!("source SST must have at least two data blocks, got {offsets:?}");
    };
    let flip = usize::try_from(second).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(source, &bytes)?;
    Ok(())
}

/// An encrypted source: salvage cannot open it without the provider (the gap this
/// closes), but with the provider in `SalvageOptions` it block-salvages like a
/// plain SST and the recovered copy reopens under the same encryption.
#[cfg(feature = "encryption")]
#[test]
fn salvage_recovers_an_encrypted_sst_with_the_provider() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let enc: Arc<dyn crate::encryption::EncryptionProvider> =
        Arc::new(crate::encryption::Aes256GcmProvider::new(&[0x42; 32]));

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_encryption(Some(Arc::clone(&enc)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source encrypted SST is non-empty"
    );

    corrupt_second_data_block(
        &source,
        &fs,
        0,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
    )?;

    // Without the provider, the encrypted source cannot even be opened.
    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "an encrypted SST must not salvage without the provider",
    );

    // With the provider, it block-salvages: the corrupt block is dropped, the
    // rest recovered, and the copy is written encrypted.
    let options = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: 0,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The salvaged copy reopens UNDER ENCRYPTION (a plaintext copy would fail the
    // encrypted reopen) and holds exactly the recovered entries.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the encrypted salvaged copy reopens with exactly the recovered entries",
    );
    Ok(())
}

/// A zstd-dictionary-compressed source: salvage cannot decompress it without the
/// dictionary, but with the dictionary in `SalvageOptions` it block-salvages and
/// the recovered copy reopens under the same dictionary.
#[cfg(zstd_any)]
#[test]
fn salvage_recovers_a_dictionary_sst_with_the_dictionary() -> crate::Result<()> {
    use crate::CompressionType;
    use crate::compression::ZstdDictionary;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A small training corpus so the dictionary has content to match against.
    let samples: alloc::vec::Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
    let dict = Arc::new(ZstdDictionary::new(&samples));
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_data_block_compression(compression)
        .use_zstd_dictionary(Some(Arc::clone(&dict)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source dictionary SST is non-empty"
    );

    corrupt_second_data_block(&source, &fs, 0, None, Some(Arc::clone(&dict)))?;

    // Without the dictionary, the source's blocks cannot be decompressed.
    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "a dictionary SST must not salvage without the dictionary",
    );

    let options = SalvageOptions {
        encryption: None,
        zstd_dictionary: Some(Arc::clone(&dict)),
        table_id: 0,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The salvaged copy reopens UNDER THE DICTIONARY with the recovered entries.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        None,
        Some(Arc::clone(&dict)),
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the dictionary salvaged copy reopens with exactly the recovered entries",
    );
    Ok(())
}

/// An encrypted source sealed under a NON-ZERO table id: the encrypted-block AAD
/// binds the table id, so salvage must be given that id. With the wrong id the
/// AAD-bound blocks cannot be decrypted (the gap repair hit when it passed a
/// hardcoded `0`); with the right id it block-salvages and the copy reopens.
#[cfg(feature = "encryption")]
#[test]
fn salvage_recovers_an_encrypted_sst_with_a_nonzero_table_id() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let enc: Arc<dyn crate::encryption::EncryptionProvider> =
        Arc::new(crate::encryption::Aes256GcmProvider::new(&[0x37; 32]));
    const TID: crate::table::TableId = 7;

    let mut writer = Writer::new(source.clone(), TID, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_encryption(Some(Arc::clone(&enc)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source encrypted SST is non-empty"
    );

    corrupt_second_data_block(
        &source,
        &fs,
        TID,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
    )?;

    // Wrong table id (the legacy hardcoded 0): the AAD-bound blocks cannot be
    // decrypted, so nothing is recovered (salvage either fails to open or drops
    // every block).
    let wrong = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: 0,
    };
    let recovered_wrong = salvage_sst_with_options(&source, dest.clone(), &fs, &wrong)
        .map_or(0, |r| r.entries_salvaged);
    assert_eq!(
        recovered_wrong, 0,
        "the wrong table id cannot decrypt the AAD-bound encrypted source",
    );

    // Right table id: block-salvages, dropping only the corrupt block.
    let options = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: TID,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The recovered copy reopens under the same table id + encryption.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        TID,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the recovered copy reopens under the same table id with the recovered entries",
    );
    Ok(())
}

// --- Blob (vlog) file record-granular salvage ---

use crate::vlog::blob_file::scanner::Scanner as BlobScanner;
use crate::vlog::blob_file::writer::Writer as BlobWriter;

/// Builds a blob file at `path` from `(key, value)` records (seqno 0, no
/// compression).
fn build_blob(
    path: &std::path::Path,
    fs: &Arc<dyn Fs>,
    records: &[(&[u8], &[u8])],
) -> crate::Result<()> {
    let mut writer = BlobWriter::new(path, 0, 0, &**fs)?;
    for (k, v) in records {
        writer.write(k, 0, v)?;
    }
    writer.finish()?;
    Ok(())
}

/// Scans a blob file into its `(key, value)` records (Ok records only).
fn scan_blob(path: &std::path::Path, fs: &Arc<dyn Fs>) -> crate::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    Ok(BlobScanner::new(path, &**fs, 0)?
        .filter_map(Result::ok)
        .map(|e| (e.key.to_vec(), e.value.to_vec()))
        .collect())
}

#[test]
fn salvage_blob_file_recovers_every_record_of_a_healthy_file() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"v0"),
        (b"k1", b"v1"),
        (b"k2", b"v2"),
        (b"k3", b"v3"),
    ];
    build_blob(&source, &fs, &records)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert!(
        report.is_complete(),
        "a healthy blob file drops nothing: {report:?}"
    );
    assert_eq!(report.records_salvaged, 4);
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    let recovered = scan_blob(&dest, &fs)?;
    let expected: Vec<(Vec<u8>, Vec<u8>)> = records
        .iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
    assert_eq!(
        recovered, expected,
        "every record round-trips through salvage"
    );
    Ok(())
}

#[test]
fn salvage_blob_file_drops_a_corrupt_record_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"value-zero"),
        (b"k1", b"value-one"),
        (b"k2", b"value-two"),
        (b"k3", b"value-three"),
    ];
    build_blob(&source, &fs, &records)?;

    // Flip the last byte of the second record's value: the checksum (over
    // key + value) fails, but the frame header (lengths, magic) stays intact, so
    // the scanner reports a checksum mismatch and re-syncs at the next record.
    let Some(second_frame_end) = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .nth(1)
        .map(|e| e.frame_end)
    else {
        panic!("source blob must have at least two records");
    };
    let flip = usize::try_from(second_frame_end - 1).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt record drops: {report:?}"
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(BlobDropReason::ChecksumMismatch)
        ),
        "the dropped record reports a checksum mismatch: {report:?}",
    );
    assert_eq!(
        report.records_salvaged, 3,
        "the other three records are recovered"
    );

    // The salvaged file holds every record except the corrupted k1.
    let recovered = scan_blob(&dest, &fs)?;
    let keys: Vec<Vec<u8>> = recovered.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![b"k0".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
        "the corrupt record's key is the only one missing",
    );
    Ok(())
}

/// A compressed blob source is rejected (fail-closed): the scanner yields on-disk
/// compressed bytes that this path cannot faithfully re-emit yet.
#[cfg(feature = "lz4")]
#[test]
fn salvage_blob_file_rejects_a_compressed_source() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    {
        let mut writer =
            BlobWriter::new(&source, 0, 0, &*fs)?.use_compression(crate::CompressionType::Lz4);
        writer.write(b"k0", 0, b"some compressible value aaaaaaaaaaaaaaaa")?;
        writer.finish()?;
    }

    assert!(
        matches!(
            salvage_blob_file(&source, dest, &fs, 0),
            Err(crate::Error::FeatureUnsupported(_)),
        ),
        "a compressed blob file must be rejected rather than mis-salvaged",
    );
    Ok(())
}

/// A columnar source carrying a per-field value sub-column salvages into a copy
/// that KEEPS the sub-column (verbatim `ColumnBatch` re-emit), instead of
/// collapsing it into a single value column via a row round-trip.
#[cfg(feature = "columnar")]
#[test]
fn salvage_preserves_columnar_value_subcolumns() -> crate::Result<()> {
    use crate::table::columnar::{Column, TypeTag, entries_to_column_batch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let cmp = default_comparator();

    // Two columnar blocks whose value is a single fixed-4 sub-column (id 3),
    // written verbatim through the ingest batch path (per-row seqno 0).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true);
    for block in 0..2u32 {
        let entries: Vec<InternalValue> = (0..4u32)
            .map(|i| {
                let k = format!("k{:04}", block * 4 + i);
                InternalValue::from_components(k.into_bytes(), b"x".to_vec(), 0, ValueType::Value)
            })
            .collect();
        let mut batch = entries_to_column_batch(&entries)?;
        batch.columns.pop();
        let mut data = Vec::new();
        for i in 0..4u32 {
            data.extend_from_slice(&(block * 4 + i).to_le_bytes());
        }
        batch.columns.push(Column {
            column_id: 3,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data,
        });
        writer.write_columnar_batch(&batch, &cmp)?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy columnar SST drops nothing: {report:?}"
    );

    // Reopen and project sub-column 3 via the per-SST scan: it survives as a
    // sub-column. A row round-trip would have collapsed it into the value column.
    let recovered = open(dest, &fs)?;
    assert!(
        recovered.metadata.columnar,
        "the recovered copy stays columnar"
    );
    let batches = recovered.columnar_scan(&[3], None)?;
    let rows: u32 = batches.iter().map(|b| b.row_count).sum();
    assert_eq!(rows, 8, "every row's sub-column is recovered");
    assert!(
        batches
            .iter()
            .all(|b| b.columns.iter().all(|c| c.column_id == 3)),
        "the value sub-column (id 3) is preserved verbatim, not collapsed",
    );
    Ok(())
}
